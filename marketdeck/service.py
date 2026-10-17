from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

from .models import Health, Quote, instrument_id, now
from .providers import Crypto, Research, Yahoo, backoff_sleep, safe_error
from .storage import Store

LOG = logging.getLogger(__name__)


class MarketService:
    def __init__(self, db_path: Path | None = None):
        self.store = Store(db_path or Path(os.getenv("MARKETDECK_DB", "marketdeck.db")))
        self.http = httpx.AsyncClient(timeout=15, headers={"User-Agent": "MarketDeck/0.2 local research contact via SEC_USER_AGENT"})
        self.yahoo = Yahoo()
        self.research = Research(self.http)
        self.crypto: dict[str, Crypto] = {}
        self.quotes: dict[str, dict[str, Any]] = {}
        self.books: dict[str, dict[str, Any]] = {}
        self.health: dict[str, Health] = {}
        self.clients: set[asyncio.Queue] = set()
        self.sequence = 0
        self.events: deque[dict[str, Any]] = deque(maxlen=1000)
        self.subscriptions: dict[str, int] = {}
        self.desired_watchlist: set[str] = set()
        self.persistent: dict[str, str] = {}
        self.cache_expiry: dict[tuple[str, str], float] = {}
        self.failure_until: dict[tuple[str, str], float] = {}
        self.failure_count: dict[tuple[str, str], int] = {}
        self.cache_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self.tasks: list[asyncio.Task] = []
        self.sentiment_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=100)
        self.alert_check_at: dict[str, float] = {}

    async def start(self) -> None:
        saved = await self.store.list("quote")
        for key, quote in saved.items():
            quote["freshness"] = "stale"
            self.quotes[key] = quote
        self.tasks.append(asyncio.create_task(self.yahoo.stream(self.on_quote, self.on_health), name="yahoo-stream"))
        self.tasks.append(asyncio.create_task(self._sentiment_worker(), name="sentiment-worker"))
        self.tasks.append(asyncio.create_task(self._bootstrap_watchlist(), name="watchlist-bootstrap"))
        self.tasks.append(asyncio.create_task(self._freshness_monitor(), name="freshness-monitor"))
        self.tasks.append(asyncio.create_task(self._snapshot_quotes(), name="quote-snapshots"))
        if self.research.sec_agent:
            self.tasks.append(asyncio.create_task(self._sec_scheduler(), name="sec-scheduler"))
        if self.research.fred_key:
            self.tasks.append(asyncio.create_task(self._fred_scheduler(), name="fred-scheduler"))

    async def _sec_scheduler(self) -> None:
        watched = [part.strip() for part in os.getenv("MARKETDECK_WATCH_CIKS", "").split(",") if part.strip().isdigit()]
        if not watched:
            return
        while True:
            for cik in watched:
                try:
                    prior = await self.store.get("filings", cik)
                    fresh = await self.research.sec_filings(cik)
                    if prior and prior.get("data") and fresh and prior["data"][0]["accession"] != fresh[0]["accession"]:
                        self.publish("filing", fresh[0])
                        self.cache_expiry.pop(("fundamentals", cik), None)
                    await self.store.put("filings", cik, {"data": fresh, "retrieved_at": now().isoformat(), "freshness": "reference", "source": "SEC EDGAR"})
                    self.on_health("sec", "live", None)
                    try:
                        prior_feed = await self.store.get("filing-rss", cik)
                        feed = await self.research.sec_rss(cik)
                        if feed and prior_feed and prior_feed.get("data") and feed[0].get("url") != prior_feed["data"][0].get("url"):
                            self.publish("filing", feed[0])
                        await self.store.put("filing-rss", cik, {"data": feed, "retrieved_at": now().isoformat(), "freshness": "reference", "source": "SEC EDGAR Atom"})
                        self.on_health("sec-rss", "live", None)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.on_health("sec-rss", "unavailable", str(exc))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.on_health("sec", "unavailable", str(exc))
            # U.S. Eastern business hours, including daylight saving time.
            from zoneinfo import ZoneInfo
            eastern = now().astimezone(ZoneInfo("America/New_York"))
            active = eastern.weekday() < 5 and 8 <= eastern.hour < 19
            await asyncio.sleep(60 if active else 300)

    async def _fred_scheduler(self) -> None:
        tracked = [x.strip().upper() for x in os.getenv("MARKETDECK_WATCH_FRED", "").split(",") if x.strip()]
        if not tracked:
            return
        known: dict[str, str] = {}
        while True:
            interval = 1800
            try:
                releases = await self.cached("releases", "fred", 86400, self.research.fred_releases)
                today = now().date().isoformat()
                if any(item.get("date") == today for item in releases.get("data", {}).get("release_dates", [])):
                    interval = 300
                updates = await self.research.fred_updates()
                changed = {item.get("id"): item.get("last_updated", "") for item in updates.get("seriess", [])}
                for series_id in tracked:
                    if series_id not in known or (series_id in changed and known[series_id] != changed[series_id]):
                        record = await self.research.fred_series(series_id)
                        await self.store.put("macro", series_id, {"data": record, "retrieved_at": now().isoformat(), "freshness": "reference", "source": "FRED"})
                        self.publish("macro", {"series_id": series_id, "latest": record.get("observations", [{}])[0]})
                    known[series_id] = changed.get(series_id, known.get(series_id, ""))
                self.on_health("fred", "live", None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.on_health("fred", "unavailable", str(exc))
            await asyncio.sleep(interval)

    async def _bootstrap_watchlist(self) -> None:
        self.desired_watchlist = set(await self.store.watchlist())
        for symbol in self.desired_watchlist:
            self.tasks.append(asyncio.create_task(self._ensure_subscription(symbol), name=f"subscribe:{symbol}"))

    async def _ensure_subscription(self, symbol: str) -> None:
        attempt = 0
        while symbol in self.desired_watchlist:
            try:
                key = await self.subscribe(symbol)
                self.persistent[symbol] = key
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.on_health(symbol, "unavailable", str(exc))
                attempt += 1
                await backoff_sleep(exc, attempt)

    async def set_watchlist(self, name: str, symbols: list[str]) -> None:
        await self.store.set_watchlist(name, symbols)
        if name != "default":
            return
        desired = set(symbols)
        removed = self.desired_watchlist - desired
        added = desired - self.desired_watchlist
        self.desired_watchlist = desired
        for symbol in removed:
            key = self.persistent.pop(symbol, None)
            if key:
                await self.unsubscribe(key)
        for symbol in added:
            self.tasks.append(asyncio.create_task(self._ensure_subscription(symbol), name=f"subscribe:{symbol}"))

    async def _freshness_monitor(self) -> None:
        while True:
            await asyncio.sleep(10)
            for key, record in list(self.quotes.items()):
                if record["freshness"] in {"stale", "unavailable"}:
                    continue
                try:
                    age = (now() - datetime.fromisoformat(record["retrieved_at"])).total_seconds()
                except (KeyError, ValueError):
                    age = 10**9
                if age > (30 if key.startswith(("binance:", "kraken:")) else 120):
                    stale = {**record, "freshness": "stale"}
                    self.quotes[key] = stale
                    self.publish("quote", stale)

    async def close(self) -> None:
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for feed in self.crypto.values():
            await feed.close()
        for key, quote in self.quotes.items():
            await self.store.put("quote", key, quote)
        await self.http.aclose()
        self.yahoo.close()
        self.store.close()

    def on_health(self, source: str, state: str, error: str | None) -> None:
        old = self.health.get(source)
        health = Health(source=source, state=state, last_success=now() if state == "live" else (old.last_success if old else None), last_error=safe_error(error) if error else None, error_count=(old.error_count if old else 0) + (1 if error else 0))
        self.health[source] = health
        self.publish("health", health.model_dump(mode="json"))

    def on_quote(self, quote: Quote) -> None:
        payload = quote.model_dump(mode="json")
        self.quotes[quote.instrument_id] = payload
        self.publish("quote", payload)
        if time.monotonic() >= self.alert_check_at.get(quote.instrument_id, 0):
            self.alert_check_at[quote.instrument_id] = time.monotonic() + 0.2
            asyncio.create_task(self._check_alerts(quote.instrument_id, quote.price))

    async def _snapshot_quotes(self) -> None:
        while True:
            await asyncio.sleep(10)
            for key, quote in tuple(self.quotes.items()):
                await self.store.put("quote", key, quote)

    async def _check_alerts(self, key: str, price: float) -> None:
        for alert in await self.store.trigger_alerts(key, price):
            self.publish("alert", {**alert, "trigger_price": price, "triggered_at": now().isoformat()})

    def on_book(self, key: str, book: dict[str, Any]) -> None:
        self.books[key] = book
        self.publish("book", book)

    def publish(self, kind: str, payload: Any) -> None:
        self.sequence += 1
        event = {"type": kind, "seq": self.sequence, "data": payload}
        self.events.append(event)
        for queue in tuple(self.clients):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                # A slow consumer gets an explicit resync request; ingestion never waits.
                with contextlib.suppress(asyncio.QueueEmpty):
                    while True:
                        queue.get_nowait()
                queue.put_nowait({"type": "resync_required", "seq": self.sequence})

    async def subscribe(self, value: str) -> str:
        if ":" in value:
            venue, symbol = value.split(":", 1)
        else:
            venue, symbol = "yahoo", value
        venue, symbol = venue.lower().strip(), symbol.upper().strip()
        if not symbol or venue not in {"yahoo", "binance", "kraken"}:
            raise ValueError("Use a Yahoo ticker or venue:BASE/QUOTE for Binance/Kraken")
        key = instrument_id(symbol, venue)
        self.subscriptions[key] = self.subscriptions.get(key, 0) + 1
        if self.subscriptions[key] > 1:
            return key
        try:
            if venue == "yahoo":
                await self.yahoo.subscribe(symbol)
                self.tasks.append(asyncio.create_task(self._seed_yahoo_quote(symbol), name=f"seed:{symbol}"))
            else:
                if venue not in self.crypto:
                    self.crypto[venue] = Crypto(venue, self.on_quote, self.on_book, self.on_health)
                await self.crypto[venue].subscribe(symbol)
        except Exception:
            self.subscriptions.pop(key, None)
            raise
        return key

    async def _seed_yahoo_quote(self, symbol: str) -> None:
        key = instrument_id(symbol)
        if key in self.quotes and self.quotes[key]["freshness"] != "stale":
            return
        try:
            bars = (await self.candles(symbol)).get("data", [])
            if bars and (key not in self.quotes or self.quotes[key]["freshness"] == "stale"):
                bar = bars[-1]
                self.on_quote(Quote(instrument_id=key, symbol=symbol, source="Yahoo Finance", venue="Yahoo", observed_at=datetime.fromtimestamp(bar["time"], timezone.utc), currency="USD", unit="price", update_frequency="last 15m candle", freshness="reference", price=bar["close"]))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.on_health("yahoo-history", "unavailable", str(exc))

    async def unsubscribe(self, key: str) -> None:
        count = self.subscriptions.get(key, 0)
        if count > 1:
            self.subscriptions[key] = count - 1
            return
        if count == 0:
            return
        self.subscriptions.pop(key, None)
        venue, symbol = key.split(":", 1)
        if venue == "yahoo":
            await self.yahoo.unsubscribe(symbol)
        elif venue in self.crypto:
            await self.crypto[venue].unsubscribe(symbol)

    async def cached(self, kind: str, key: str, ttl: float, fetch: Callable[[], Awaitable[Any]]) -> dict[str, Any]:
        cache_key = (kind, key)
        lock = self.cache_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            existing = await self.store.get(kind, key)
            if existing is not None and time.monotonic() < self.cache_expiry.get(cache_key, 0):
                return existing
            if time.monotonic() < self.failure_until.get(cache_key, 0):
                if existing is not None:
                    return {**existing, "freshness": "stale", "error": "Source cooling down"}
                raise RuntimeError(f"{kind} is cooling down after a source error")
            try:
                data = await fetch()
                record = {"data": data, "retrieved_at": now().isoformat(), "freshness": "reference", "source": kind}
                await self.store.put(kind, key, record)
                self.cache_expiry[cache_key] = time.monotonic() + ttl
                self.failure_until.pop(cache_key, None)
                self.failure_count.pop(cache_key, None)
                self.on_health(kind, "live", None)
                return record
            except Exception as exc:
                self.on_health(kind, "unavailable", str(exc))
                failures = self.failure_count.get(cache_key, 0) + 1
                self.failure_count[cache_key] = failures
                rate_limited = "429" in str(exc) or "rate limit" in str(exc).casefold() or "403" in str(exc)
                self.failure_until[cache_key] = time.monotonic() + (max(60, min(600, 2 ** failures)) if rate_limited else min(60, 2 ** failures))
                if existing is not None:
                    existing["freshness"] = "stale"
                    existing["error"] = safe_error(exc)
                    return existing
                raise

    async def candles(self, symbol: str) -> dict[str, Any]:
        return await self.cached("candles", symbol.upper(), 60, lambda: self.yahoo.candles(symbol.upper()))

    async def options(self, symbol: str) -> dict[str, Any]:
        return await self.cached("options", symbol.upper(), 300, lambda: self.yahoo.options(symbol.upper()))

    async def news(self, symbol: str) -> dict[str, Any]:
        async def fetch():
            raw = await self.yahoo.news(symbol.upper())
            result = []
            for item in raw:
                content = item.get("content", item)
                title = content.get("title") or item.get("title")
                if not title:
                    continue
                link = content.get("canonicalUrl") or content.get("clickThroughUrl") or item.get("link")
                if isinstance(link, dict):
                    link = link.get("url")
                pub = content.get("pubDate") or item.get("providerPublishTime")
                if isinstance(pub, (int, float)):
                    pub = datetime.fromtimestamp(pub, timezone.utc).isoformat()
                record = {"instrument_id": instrument_id(symbol), "title": title, "summary": content.get("summary"), "url": link, "observed_at": pub, "retrieved_at": now().isoformat(), "source": "Yahoo Finance", "freshness": "reference", "sentiment": None}
                key = hashlib.sha256(f"{symbol}:{title}:{link}".encode()).hexdigest()
                record["id"] = key
                revision = os.getenv("FINBERT_REVISION", "unconfigured")
                normalized = " ".join(f"{title}. {record['summary'] or ''}".casefold().split())
                sentiment_key = hashlib.sha256(f"{revision}:{normalized}".encode()).hexdigest()
                record["sentiment_key"] = sentiment_key
                prior = await self.store.get("sentiment", sentiment_key)
                if prior:
                    record["sentiment"] = prior
                elif os.getenv("MARKETDECK_FINBERT", "0") == "1" and not self.sentiment_queue.full():
                    self.sentiment_queue.put_nowait((sentiment_key, f"{title}. {record['summary'] or ''}"))
                result.append(record)
            return list({item["id"]: item for item in result}.values())
        cached = await self.cached("news", symbol.upper(), 120, fetch)
        for item in cached.get("data", []):
            if item.get("sentiment_key"):
                item["sentiment"] = await self.store.get("sentiment", item["sentiment_key"])
        return cached

    async def _sentiment_worker(self) -> None:
        # Opt-in: first model download is large, so streaming starts without waiting.
        if os.getenv("MARKETDECK_FINBERT", "0") != "1":
            return
        revision = os.getenv("FINBERT_REVISION", "")
        if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision.lower()):
            self.on_health("finbert", "unavailable", "FINBERT_REVISION must be a pinned 40-character commit hash")
            return
        from concurrent.futures import ProcessPoolExecutor
        pool = ProcessPoolExecutor(max_workers=1)
        try:
            while True:
                key, text = await self.sentiment_queue.get()
                batch = [(key, text)]
                while len(batch) < 16 and not self.sentiment_queue.empty():
                    batch.append(self.sentiment_queue.get_nowait())
                try:
                    from langdetect import detect
                    english = []
                    for key, headline in batch:
                        try:
                            if detect(headline) == "en":
                                english.append((key, headline))
                        except Exception:
                            pass
                    results = await asyncio.get_running_loop().run_in_executor(pool, infer_finbert, english) if english else {}
                    for item_key, result in results.items():
                        await self.store.put("sentiment", item_key, result)
                    self.publish("sentiment", results)
                except Exception as exc:
                    self.on_health("finbert", "unavailable", str(exc))
                finally:
                    for _ in batch:
                        self.sentiment_queue.task_done()
        finally:
            pool.shutdown(wait=False, cancel_futures=True)


_PIPELINE: Any = None


def infer_finbert(batch: list[tuple[str, str]]) -> dict[str, Any]:
    global _PIPELINE
    if _PIPELINE is None:
        from transformers import pipeline
        revision = os.environ["FINBERT_REVISION"]
        _PIPELINE = pipeline("text-classification", model="ProsusAI/finbert", revision=revision, top_k=None)
    labels = _PIPELINE([text[:1000] for _, text in batch], truncation=True)
    revision = os.environ["FINBERT_REVISION"]
    return {key: {"model": "ProsusAI/finbert", "revision": revision, "processed_at": now().isoformat(), "probabilities": {x["label"].lower(): x["score"] for x in scores}, "label": max(scores, key=lambda x: x["score"])["label"].lower()} for (key, _), scores in zip(batch, labels)}
