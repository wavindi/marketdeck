from __future__ import annotations

import asyncio
import contextlib
import email.utils
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Callable

import ccxt.pro as ccxtpro
import httpx
import yfinance as yf

from .models import NewsItem, Quote, instrument_id, now

LOG = logging.getLogger(__name__)
if os.getenv("MARKETDECK_VERBOSE_YAHOO", "0") != "1":
    # The library logs a full traceback for each failed handshake; our source
    # health event already captures the failure without flooding the terminal.
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)


def safe_error(error: Exception | str) -> str:
    message = str(error)
    for secret in (os.getenv("FRED_API_KEY", ""), os.getenv("SEC_USER_AGENT", "")):
        if secret:
            message = message.replace(secret, "[redacted]")
    return message


async def backoff_sleep(error: Exception, attempt: int) -> None:
    delay = min(120.0, 2 ** min(attempt, 7)) + random.random()
    if isinstance(error, httpx.HTTPStatusError):
        retry = error.response.headers.get("Retry-After")
        if retry:
            try:
                delay = max(delay, float(retry))
            except ValueError:
                try:
                    delay = max(delay, (email.utils.parsedate_to_datetime(retry) - now()).total_seconds())
                except (TypeError, OverflowError):
                    pass
        if error.response.status_code == 403:
            delay = max(delay, 300)
        elif error.response.status_code == 429:
            delay = max(delay, 60)
    await asyncio.sleep(delay)


class HTTPSource:
    def __init__(self, client: httpx.AsyncClient, interval: float = 0.5):
        self.client = client
        self.interval = interval
        self.lock = asyncio.Lock()
        self.next_at = 0.0
        self.cooldown_until = 0.0

    async def get(self, url: str, **params: Any) -> Any:
        response = await self.request(url, **params)
        return response.json()

    async def request(self, url: str, **params: Any) -> httpx.Response:
        async with self.lock:
            if time.monotonic() < self.cooldown_until:
                raise RuntimeError("Source is cooling down after a 403/429 response")
            wait = self.next_at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self.next_at = time.monotonic() + self.interval
        response = await self.client.get(url, params=params)
        if response.status_code in {403, 429}:
            retry = response.headers.get("Retry-After")
            delay = 300.0 if response.status_code == 403 else 60.0
            if retry:
                try:
                    delay = max(delay, float(retry))
                except ValueError:
                    try:
                        delay = max(delay, (email.utils.parsedate_to_datetime(retry) - now()).total_seconds())
                    except (TypeError, OverflowError):
                        pass
            self.cooldown_until = time.monotonic() + delay
        response.raise_for_status()
        return response


class Yahoo:
    def __init__(self):
        self.pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="yahoo")
        self.limit = asyncio.Semaphore(3)
        self.socket: Any = None
        self.symbols: set[str] = set()
        self.ready = asyncio.Event()
        self.socket_lock = asyncio.Lock()
        self.search_lock = asyncio.Lock()
        self.search_next = 0.0

    async def blocking(self, fn: Callable[[], Any]) -> Any:
        async with self.limit:
            return await asyncio.get_running_loop().run_in_executor(self.pool, fn)

    async def candles(self, symbol: str, period: str = "5d", interval: str = "15m") -> list[dict[str, Any]]:
        def fetch():
            frame = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False)
            rows = []
            for index, row in frame.tail(250).iterrows():
                rows.append({"time": int(index.timestamp()), "open": float(row["Open"]), "high": float(row["High"]), "low": float(row["Low"]), "close": float(row["Close"]), "volume": float(row["Volume"])})
            return rows
        return await self.blocking(fetch)

    async def options(self, symbol: str) -> dict[str, Any]:
        def fetch():
            ticker = yf.Ticker(symbol)
            dates = ticker.options
            if not dates:
                return {"expiration": None, "rows": []}
            expiry = dates[0]
            chain = ticker.option_chain(expiry)
            rows = []
            for side, frame in (("call", chain.calls), ("put", chain.puts)):
                for _, row in frame.iterrows():
                    rows.append({"side": side, "strike": float(row["strike"]), "last": safe_number(row.get("lastPrice")), "bid": safe_number(row.get("bid")), "ask": safe_number(row.get("ask")), "iv": safe_number(row.get("impliedVolatility")), "volume": safe_number(row.get("volume")), "open_interest": safe_number(row.get("openInterest"))})
            return {"expiration": expiry, "rows": rows}
        return await self.blocking(fetch)

    async def news(self, symbol: str) -> list[dict[str, Any]]:
        def fetch():
            search = yf.Search(symbol, news_count=20)
            return search.news or []
        return await self.blocking(fetch)

    async def search(self, query: str) -> list[dict[str, Any]]:
        async with self.search_lock:
            wait = self.search_next - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self.search_next = time.monotonic() + 2.0
        def fetch():
            search = yf.Search(query, max_results=12)
            return search.quotes or []
        return await self.blocking(fetch)

    async def stream(self, emit: Callable[[Quote], Any], health: Callable[[str, str, str | None], Any]) -> None:
        attempt = 0
        while True:
            try:
                await self.ready.wait()
                socket = yf.AsyncWebSocket(verbose=False)
                initial = sorted(self.symbols)
                await socket.subscribe(initial)
                self.socket = socket
                extra = sorted(self.symbols.difference(initial))
                if extra:
                    await socket.subscribe(extra)
                health("yahoo", "live", None)
                attempt = 0

                def on_message(payload: dict[str, Any]) -> None:
                    try:
                        symbol = str(payload["id"]).upper()
                        price = float(payload["price"])
                        stamp = float(payload.get("time") or time.time())
                        if stamp > 10_000_000_000:
                            stamp /= 1000
                        quote = Quote(instrument_id=instrument_id(symbol), symbol=symbol, source="Yahoo Finance", venue="Yahoo", observed_at=datetime.fromtimestamp(stamp, timezone.utc), currency=str(payload.get("currency") or "USD"), unit="price", update_frequency="stream", freshness="delayed", price=price, change_pct=safe_number(payload.get("changePercent")))
                        emit(quote)
                    except (KeyError, TypeError, ValueError, OverflowError) as exc:
                        LOG.debug("Invalid Yahoo message: %s", exc)

                await socket.listen(on_message)
                raise ConnectionError("Yahoo stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health("yahoo", "reconnecting", str(exc))
                attempt += 1
                await backoff_sleep(exc, attempt)
            finally:
                if self.socket:
                    with contextlib.suppress(Exception):
                        await self.socket.close()
                    self.socket = None

    async def subscribe(self, symbol: str) -> None:
        symbol = symbol.upper()
        if symbol in self.symbols:
            return
        self.symbols.add(symbol)
        self.ready.set()
        if self.socket:
            async with self.socket_lock:
                await self.socket.subscribe(symbol)

    async def unsubscribe(self, symbol: str) -> None:
        self.symbols.discard(symbol.upper())
        if self.socket:
            async with self.socket_lock:
                await self.socket.unsubscribe(symbol.upper())

    def close(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)


def safe_number(value: Any) -> float | None:
    try:
        result = float(value)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError):
        return None


class Crypto:
    def __init__(self, venue: str, emit: Callable[[Quote], Any], book: Callable[[str, dict[str, Any]], Any], health: Callable[[str, str, str | None], Any]):
        self.venue = venue
        self.emit = emit
        self.book = book
        self.health = health
        self.exchange: Any = None
        self.tasks: dict[str, asyncio.Task] = {}

    async def start(self) -> None:
        candidate = getattr(ccxtpro, self.venue)({"enableRateLimit": True, "newUpdates": True})
        try:
            await candidate.load_markets()
        except Exception:
            await candidate.close()
            raise
        self.exchange = candidate
        self.health(self.venue, "live", None)

    async def subscribe(self, symbol: str) -> None:
        if not self.exchange:
            await self.start()
        if symbol not in self.exchange.markets:
            raise ValueError(f"{symbol} unavailable on {self.venue}")
        if symbol not in self.tasks:
            self.tasks[symbol] = asyncio.create_task(self._run(symbol), name=f"{self.venue}:{symbol}")

    async def unsubscribe(self, symbol: str) -> None:
        task = self.tasks.pop(symbol, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _run(self, symbol: str) -> None:
        async def trades():
            attempt = 0
            while True:
                try:
                    batch = await self.exchange.watch_trades(symbol)
                    attempt = 0
                    for trade in batch[-1:]:
                        stamp = datetime.fromtimestamp((trade.get("timestamp") or time.time() * 1000) / 1000, timezone.utc)
                        self.emit(Quote(instrument_id=instrument_id(symbol, self.venue), symbol=symbol, source=self.venue, venue=self.venue, observed_at=stamp, currency=symbol.split("/")[-1], unit="price", update_frequency="stream", freshness="live", price=float(trade["price"])))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.health(self.venue, "reconnecting", str(exc))
                    attempt += 1
                    await backoff_sleep(exc, attempt)

        async def books():
            attempt = 0
            while True:
                try:
                    raw = await self.exchange.watch_order_book(symbol, limit=10)
                    attempt = 0
                    self.book(instrument_id(symbol, self.venue), {"instrument_id": instrument_id(symbol, self.venue), "source": self.venue, "venue": self.venue, "observed_at": now().isoformat(), "retrieved_at": now().isoformat(), "freshness": "live", "bids": raw.get("bids", [])[:10], "asks": raw.get("asks", [])[:10]})
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.health(self.venue, "reconnecting", str(exc))
                    attempt += 1
                    await backoff_sleep(exc, attempt)

        async with asyncio.TaskGroup() as group:
            group.create_task(trades())
            group.create_task(books())

    async def close(self) -> None:
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        if self.exchange:
            await self.exchange.close()


class Research:
    def __init__(self, client: httpx.AsyncClient):
        self.sec = HTTPSource(client, 0.5)
        self.fred = HTTPSource(client, 1.0)
        self.other = HTTPSource(client, 1.0)
        self.sec_agent = os.getenv("SEC_USER_AGENT", "")
        self.fred_key = os.getenv("FRED_API_KEY", "")

    async def sec_filings(self, cik: str) -> list[dict[str, Any]]:
        if not self.sec_agent or "@" not in self.sec_agent:
            raise RuntimeError("Set SEC_USER_AGENT to 'Name email@example.com' before SEC access")
        cik_number = int(cik)
        self.sec.client.headers["User-Agent"] = self.sec_agent
        data = await self.sec.get(f"https://data.sec.gov/submissions/CIK{cik_number:010d}.json")
        recent = data["filings"]["recent"]
        rows = []
        for index, accession in enumerate(recent["accessionNumber"][:40]):
            flat = accession.replace("-", "")
            doc = recent["primaryDocument"][index]
            rows.append({"cik": str(cik_number), "company": data.get("name"), "accession": accession, "form": recent["form"][index], "filed_at": recent["filingDate"][index], "url": f"https://www.sec.gov/Archives/edgar/data/{cik_number}/{flat}/{doc}", "source": "SEC EDGAR", "freshness": "reference", "retrieved_at": now().isoformat()})
        return rows

    async def sec_company_lookup(self, query: str) -> list[dict[str, Any]]:
        if not self.sec_agent or "@" not in self.sec_agent:
            raise RuntimeError("Set SEC_USER_AGENT to 'Name email@example.com' before SEC access")
        self.sec.client.headers["User-Agent"] = self.sec_agent
        data = await self.sec.get("https://www.sec.gov/files/company_tickers.json")
        term = query.casefold().strip()
        return [{"cik": str(int(item["cik_str"])), "ticker": item["ticker"], "title": item["title"]} for item in data.values() if term in item["ticker"].casefold() or term in item["title"].casefold()][:30]

    async def sec_rss(self, cik: str) -> list[dict[str, Any]]:
        if not self.sec_agent or "@" not in self.sec_agent:
            raise RuntimeError("Set SEC_USER_AGENT before SEC RSS access")
        from xml.etree import ElementTree
        self.sec.client.headers["User-Agent"] = self.sec_agent
        response = await self.sec.request("https://www.sec.gov/cgi-bin/browse-edgar", action="getcompany", CIK=str(int(cik)), owner="exclude", count=40, output="atom")
        root = ElementTree.fromstring(response.content)
        items = []
        for entry in root.iter():
            if entry.tag.rsplit("}", 1)[-1] != "entry":
                continue
            fields = {child.tag.rsplit("}", 1)[-1]: child.text for child in entry if child.text}
            link = next((child.attrib.get("href") for child in entry if child.tag.rsplit("}", 1)[-1] == "link"), None)
            items.append({"cik": str(int(cik)), "title": fields.get("title"), "observed_at": fields.get("updated"), "url": link, "source": "SEC EDGAR Atom", "retrieved_at": now().isoformat(), "freshness": "reference"})
        return items

    async def sec_facts(self, cik: str) -> dict[str, Any]:
        if not self.sec_agent or "@" not in self.sec_agent:
            raise RuntimeError("Set SEC_USER_AGENT to 'Name email@example.com' before SEC access")
        self.sec.client.headers["User-Agent"] = self.sec_agent
        payload = await self.sec.get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json")
        metrics = ("Assets", "Liabilities", "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "NetIncomeLoss", "EarningsPerShareDiluted", "StockholdersEquity")
        facts = []
        gaap = payload.get("facts", {}).get("us-gaap", {})
        for metric in metrics:
            item = gaap.get(metric)
            if not item:
                continue
            for unit, observations in item.get("units", {}).items():
                recent = sorted(observations, key=lambda x: (x.get("filed", ""), x.get("end", "")), reverse=True)[:5]
                for observation in recent:
                    accession = observation.get("accn", "")
                    facts.append({"metric": metric, "label": item.get("label", metric), "value": observation.get("val"), "unit": unit, "start": observation.get("start"), "end": observation.get("end"), "filed_at": observation.get("filed"), "form": observation.get("form"), "source": "SEC Company Facts", "freshness": "reference", "url": f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/" if accession else None})
        return {"cik": str(int(cik)), "company": payload.get("entityName"), "source": "SEC Company Facts", "retrieved_at": now().isoformat(), "freshness": "reference", "facts": facts}

    async def fred_series(self, series_id: str) -> dict[str, Any]:
        if not self.fred_key:
            raise RuntimeError("Set FRED_API_KEY for FRED series")
        result = await self.fred.get("https://api.stlouisfed.org/fred/series/observations", series_id=series_id.upper(), api_key=self.fred_key, file_type="json", sort_order="desc", limit=250)
        return {"series_id": series_id.upper(), "source": "FRED", "url": f"https://fred.stlouisfed.org/series/{series_id.upper()}", "retrieved_at": now().isoformat(), "freshness": "reference", "observations": result.get("observations", [])}

    async def fred_releases(self) -> dict[str, Any]:
        if not self.fred_key:
            raise RuntimeError("Set FRED_API_KEY for FRED releases")
        return await self.fred.get("https://api.stlouisfed.org/fred/releases/dates", api_key=self.fred_key, file_type="json", include_release_dates_with_no_data="true", limit=100, sort_order="desc")

    async def fred_updates(self) -> dict[str, Any]:
        if not self.fred_key:
            raise RuntimeError("Set FRED_API_KEY for FRED updates")
        return await self.fred.get("https://api.stlouisfed.org/fred/series/updates", api_key=self.fred_key, file_type="json", limit=100)

    async def ecb_fx(self) -> dict[str, Any]:
        response = await self.other.request("https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml")
        from xml.etree import ElementTree
        root = ElementTree.fromstring(response.content)
        rates = [{"currency": node.attrib["currency"], "rate": float(node.attrib["rate"])} for node in root.iter() if "currency" in node.attrib and "rate" in node.attrib]
        date = next((node.attrib["time"] for node in root.iter() if "time" in node.attrib), None)
        return {"source": "ECB", "observed_at": date, "retrieved_at": now().isoformat(), "freshness": "reference", "url": "https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html", "rates": rates}

    async def treasury_yields(self) -> dict[str, Any]:
        from xml.etree import ElementTree
        month = now().strftime("%Y%m")
        url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"
        response = await self.other.request(url, data="daily_treasury_yield_curve", field_tdr_date_value_month=month)
        root = ElementTree.fromstring(response.content)
        rows = []
        for entry in root.iter():
            if entry.tag.rsplit("}", 1)[-1].lower() != "entry":
                continue
            fields = {}
            for child in entry.iter():
                name = child.tag.rsplit("}", 1)[-1]
                if child.text and (name == "NEW_DATE" or name.startswith("BC_")):
                    fields[name] = child.text.strip()
            if fields:
                rows.append(fields)
        return {"source": "U.S. Treasury", "observed_at": rows[-1].get("NEW_DATE") if rows else None, "retrieved_at": now().isoformat(), "freshness": "reference", "url": "https://home.treasury.gov/treasury-daily-interest-rate-xml-feed", "unit": "percent", "rates": rows[-1] if rows else {}, "recent": rows[-10:]}
