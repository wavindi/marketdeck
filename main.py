"""MarketDeck: an asynchronous Bloomberg-inspired terminal dashboard.

Free feeds are convenient, not exchange-grade: Yahoo quotes may be delayed and
unofficial endpoints can change.  This application is for monitoring/education,
not order execution or investment decisions.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, TypeVar

import ccxt.pro as ccxtpro
import httpx
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal
from textual.message import Message
from textual.widgets import DataTable, Footer, Header, Input, Label, Static
from textual.worker import Worker
from textual_plotext import PlotextPlot


LOG = logging.getLogger("marketdeck")
T = TypeVar("T")


# ------------------------------- Configuration -------------------------------

DEFAULT_STOCKS = ["AAPL", "MSFT", "NVDA", "SPY"]
DEFAULT_CRYPTOS = ["BTC/USDT", "ETH/USDT"]
DEFAULT_FOCUS = "SPY"
NEWS_URL = "https://finviz.com/news.ashx"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


async def with_backoff(
    operation: Callable[[], Awaitable[T]],
    *,
    service: str,
    stop: asyncio.Event,
    initial: float = 1.0,
    maximum: float = 60.0,
) -> T:
    """Retry an async operation with capped exponential backoff and jitter."""
    delay = initial
    while not stop.is_set():
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("%s failed: %s; retrying in %.1fs", service, exc, delay)
            try:
                await asyncio.wait_for(stop.wait(), delay + random.random() * delay * 0.2)
            except TimeoutError:
                pass
            delay = min(maximum, delay * 2)
    raise asyncio.CancelledError


# ------------------------------- UI messages --------------------------------

@dataclass(frozen=True, slots=True)
class Quote:
    symbol: str
    price: float
    change_pct: float | None
    source: str
    timestamp: datetime
    bid: float | None = None
    ask: float | None = None


@dataclass(frozen=True, slots=True)
class OptionRow:
    strike: float
    call_last: float | None
    call_iv: float | None
    call_volume: int | None
    put_last: float | None
    put_iv: float | None
    put_volume: int | None


@dataclass(frozen=True, slots=True)
class Headline:
    time_text: str
    title: str
    url: str


class QuoteReceived(Message):
    def __init__(self, quote: Quote) -> None:
        super().__init__()
        self.quote = quote


class OptionsReceived(Message):
    def __init__(self, symbol: str, expiration: str, rows: list[OptionRow]) -> None:
        super().__init__()
        self.symbol, self.expiration, self.rows = symbol, expiration, rows


class CandlesReceived(Message):
    def __init__(self, symbol: str, frame: pd.DataFrame) -> None:
        super().__init__()
        self.symbol, self.frame = symbol, frame


class NewsReceived(Message):
    def __init__(self, headlines: list[Headline]) -> None:
        super().__init__()
        self.headlines = headlines


class FeedStatus(Message):
    def __init__(self, service: str, status: str, error: bool = False) -> None:
        super().__init__()
        self.service, self.status, self.error = service, status, error


# --------------------------------- Feeds -------------------------------------

class CryptoWebSocketClient:
    """CCXT Pro WebSocket client for public trades and top-of-book updates."""

    def __init__(
        self,
        emit: Callable[[Message], None],
        stop: asyncio.Event,
        symbols: Iterable[str] = DEFAULT_CRYPTOS,
        exchange_id: str = "binance",
    ) -> None:
        self.emit = emit
        self.stop = stop
        self.symbols = {s.upper() for s in symbols}
        self.exchange_id = exchange_id.lower()
        self.exchange: Any | None = None
        self._tasks: dict[tuple[str, str], asyncio.Task[None]] = {}

    async def run(self) -> None:
        exchange_class = getattr(ccxtpro, self.exchange_id, None)
        if exchange_class is None:
            self.emit(FeedStatus("crypto", f"unknown exchange: {self.exchange_id}", True))
            return

        self.exchange = exchange_class(
            {
                "enableRateLimit": True,
                "newUpdates": True,
                "options": {"tradesLimit": 50},
            }
        )
        try:
            await with_backoff(
                self.exchange.load_markets,
                service=f"{self.exchange_id} markets",
                stop=self.stop,
            )
            for symbol in tuple(self.symbols):
                await self.add_symbol(symbol)
            self.emit(FeedStatus("crypto", f"live · {self.exchange_id}"))
            await self.stop.wait()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.emit(FeedStatus("crypto", f"offline · {exc}", True))
        finally:
            for task in self._tasks.values():
                task.cancel()
            if self._tasks:
                await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            if self.exchange is not None:
                with contextlib.suppress(Exception):
                    await self.exchange.close()

    async def add_symbol(self, symbol: str) -> None:
        """Add a subscription at runtime; safe to call from a key action."""
        symbol = symbol.upper().strip()
        if not symbol or symbol in self.symbols and (symbol, "trade") in self._tasks:
            return
        if self.exchange is None:
            self.symbols.add(symbol)
            return
        if symbol not in self.exchange.markets:
            raise ValueError(f"{symbol} is not listed on {self.exchange_id}")
        self.symbols.add(symbol)
        self._tasks[(symbol, "trade")] = asyncio.create_task(
            self._watch_trades(symbol), name=f"crypto-trades:{symbol}"
        )
        if self.exchange.has.get("watchOrderBook"):
            self._tasks[(symbol, "book")] = asyncio.create_task(
                self._watch_book(symbol), name=f"crypto-book:{symbol}"
            )

    async def _watch_trades(self, symbol: str) -> None:
        delay = 1.0
        while not self.stop.is_set():
            try:
                trades = await self.exchange.watch_trades(symbol)
                delay = 1.0
                if not trades:
                    continue
                trade = trades[-1]
                price = float(trade["price"])
                stamp_ms = trade.get("timestamp") or int(time.time() * 1000)
                self.emit(
                    QuoteReceived(
                        Quote(symbol, price, None, self.exchange_id, datetime.fromtimestamp(stamp_ms / 1000, timezone.utc))
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.emit(FeedStatus("crypto", f"reconnecting · {type(exc).__name__}", True))
                await self._sleep_backoff(delay)
                delay = min(30.0, delay * 2)

    async def _watch_book(self, symbol: str) -> None:
        delay = 1.0
        while not self.stop.is_set():
            try:
                book = await self.exchange.watch_order_book(symbol, limit=5)
                delay = 1.0
                bid = float(book["bids"][0][0]) if book.get("bids") else None
                ask = float(book["asks"][0][0]) if book.get("asks") else None
                if bid is None and ask is None:
                    continue
                price = (bid + ask) / 2 if bid is not None and ask is not None else bid or ask
                self.emit(QuoteReceived(Quote(symbol, float(price), None, self.exchange_id, utc_now(), bid, ask)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.emit(FeedStatus("crypto", f"book reconnect · {type(exc).__name__}", True))
                await self._sleep_backoff(delay)
                delay = min(30.0, delay * 2)

    async def _sleep_backoff(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), delay + random.random() * 0.25)
        except TimeoutError:
            pass


class YahooStockStream:
    """Near-real-time public equity quotes through yfinance's async WebSocket."""

    def __init__(self, emit: Callable[[Message], None], stop: asyncio.Event, symbols: Iterable[str]) -> None:
        self.emit, self.stop = emit, stop
        self.symbols = {s.upper() for s in symbols}
        self.socket: Any | None = None
        self._previous: dict[str, float] = {}

    async def run(self) -> None:
        delay = 1.0
        while not self.stop.is_set():
            try:
                self.socket = yf.AsyncWebSocket(verbose=False)
                await self.socket.subscribe(sorted(self.symbols))
                self.emit(FeedStatus("stocks", "live · yahoo"))
                delay = 1.0
                await self.socket.listen(self._on_message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.emit(FeedStatus("stocks", f"reconnecting · {type(exc).__name__}", True))
                try:
                    await asyncio.wait_for(self.stop.wait(), delay + random.random() * 0.25)
                except TimeoutError:
                    pass
                delay = min(60.0, delay * 2)
            finally:
                if self.socket is not None:
                    with contextlib.suppress(Exception):
                        await self.socket.close()
                    self.socket = None

    def _on_message(self, payload: dict[str, Any]) -> None:
        symbol = str(payload.get("id", "")).upper()
        price = payload.get("price")
        if not symbol or price is None:
            return
        price = float(price)
        previous = self._previous.get(symbol)
        day_change = payload.get("changePercent")
        change = float(day_change) if day_change is not None else (
            ((price / previous) - 1) * 100 if previous else None
        )
        self._previous[symbol] = price
        stamp = payload.get("time") or int(time.time() * 1000)
        # Yahoo has used both seconds and milliseconds in feed payloads.
        stamp = stamp / 1000 if stamp > 10_000_000_000 else stamp
        self.emit(QuoteReceived(Quote(symbol, price, change, "yahoo", datetime.fromtimestamp(stamp, timezone.utc))))

    async def add_symbol(self, symbol: str) -> None:
        symbol = symbol.upper().strip()
        if not symbol or symbol in self.symbols:
            return
        self.symbols.add(symbol)
        if self.socket is not None:
            await self.socket.subscribe(symbol)


class YahooRepository:
    """Non-blocking facade around yfinance's blocking HTTP methods."""

    @staticmethod
    async def nearest_options(symbol: str) -> tuple[str, list[OptionRow]]:
        return await asyncio.to_thread(YahooRepository._nearest_options_sync, symbol)

    @staticmethod
    def _nearest_options_sync(symbol: str) -> tuple[str, list[OptionRow]]:
        ticker = yf.Ticker(symbol)
        expirations = ticker.options
        if not expirations:
            raise LookupError(f"no listed options for {symbol}")
        expiration = expirations[0]
        chain = ticker.option_chain(expiration)
        calls = chain.calls.set_index("strike")
        puts = chain.puts.set_index("strike")
        strikes = sorted(set(calls.index).union(puts.index))

        spot = None
        with contextlib.suppress(Exception):
            spot = float(ticker.fast_info["last_price"])
        if spot and math.isfinite(spot):
            strikes.sort(key=lambda value: abs(float(value) - spot))
            strikes = sorted(strikes[:18])
        else:
            strikes = strikes[:18]

        def value(frame: pd.DataFrame, strike: float, column: str) -> float | int | None:
            if strike not in frame.index or column not in frame.columns:
                return None
            raw = frame.at[strike, column]
            if pd.isna(raw):
                return None
            return int(raw) if column == "volume" else float(raw)

        rows = [
            OptionRow(
                float(strike),
                value(calls, strike, "lastPrice"),
                value(calls, strike, "impliedVolatility"),
                value(calls, strike, "volume"),
                value(puts, strike, "lastPrice"),
                value(puts, strike, "impliedVolatility"),
                value(puts, strike, "volume"),
            )
            for strike in strikes
        ]
        return expiration, rows

    @staticmethod
    async def candles(symbol: str) -> pd.DataFrame:
        def fetch() -> pd.DataFrame:
            frame = yf.Ticker(symbol).history(period="5d", interval="15m", auto_adjust=False)
            if frame.empty:
                raise LookupError(f"no candle history for {symbol}")
            return frame.tail(60).copy()

        return await asyncio.to_thread(fetch)


async def scrape_finviz_news(client: httpx.AsyncClient) -> list[Headline]:
    response = await client.get(NEWS_URL)
    if response.status_code == 429:
        raise httpx.HTTPStatusError("Finviz rate limit", request=response.request, response=response)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    rows: list[Headline] = []

    # Finviz has changed its markup over time.  These selectors cover the current
    # news table and the older table-row layout without tying the parser to classes.
    for anchor in soup.select("a.nn-tab-link, table a[href]"):
        title = anchor.get_text(" ", strip=True)
        href = anchor.get("href", "")
        if len(title) < 12 or not href.startswith("http"):
            continue
        parent_text = anchor.parent.get_text(" ", strip=True) if anchor.parent else ""
        time_text = parent_text.removesuffix(title).strip()[-12:] or "--"
        rows.append(Headline(time_text, title, href))
        if len(rows) >= 25:
            break
    if not rows:
        raise ValueError("Finviz returned no recognizable headlines")
    return rows


# ---------------------------------- App --------------------------------------

class MarketDeck(App[None]):
    TITLE = "MARKETDECK"
    SUB_TITLE = "Free real-time market monitor"

    CSS = """
    Screen {
        background: #071019;
        color: #d6e2ea;
    }
    #command-bar {
        dock: top;
        height: 3;
        display: none;
        background: #0d1b26;
        border-bottom: solid #e5b94b;
    }
    #command-bar.visible { display: block; }
    #dashboard {
        layout: grid;
        grid-size: 2 2;
        grid-columns: 1fr 1.35fr;
        grid-rows: 1fr 1fr;
        grid-gutter: 1 1;
        padding: 0 1;
    }
    .pane {
        border: round #284457;
        background: #09141e;
        height: 100%;
        min-height: 10;
    }
    .pane:focus-within { border: round #e5b94b; }
    .pane-title {
        height: 1;
        color: #e5b94b;
        text-style: bold;
        padding-left: 1;
        background: #102330;
    }
    DataTable {
        height: 1fr;
        background: #09141e;
    }
    PlotextPlot { height: 1fr; }
    #status-bar {
        dock: bottom;
        height: 1;
        background: #102330;
        color: #8ba4b5;
        padding: 0 1;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("ctrl+a", "add_symbol", "Add symbol"),
        ("r", "refresh", "Refresh"),
        ("c", "focus_chart", "Chart"),
        ("o", "focus_options", "Options"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.stop_event = asyncio.Event()
        self.quotes: dict[str, Quote] = {}
        self.statuses: dict[str, tuple[str, bool]] = {}
        self.focus_symbol = DEFAULT_FOCUS
        self.crypto: CryptoWebSocketClient | None = None
        self.stocks: YahooStockStream | None = None
        self._quote_dirty = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Input(
            placeholder="Add STOCK (e.g. TSLA) or CRYPTO pair (e.g. SOL/USDT), then Enter",
            id="command-bar",
        )
        with Container(id="dashboard"):
            with Container(classes="pane", id="tape-pane"):
                yield Label(" LIVE TICKER TAPE", classes="pane-title")
                yield DataTable(id="ticker-table", cursor_type="row", zebra_stripes=True)
            with Container(classes="pane", id="chart-pane"):
                yield Label(f" {self.focus_symbol} · 15 MIN CANDLES", classes="pane-title", id="chart-title")
                yield PlotextPlot(id="chart")
            with Container(classes="pane", id="options-pane"):
                yield Label(f" {self.focus_symbol} · NEAREST OPTIONS", classes="pane-title", id="options-title")
                yield DataTable(id="options-table", cursor_type="row", zebra_stripes=True)
            with Container(classes="pane", id="news-pane"):
                yield Label(" LIVE NEWS · FINVIZ", classes="pane-title")
                yield DataTable(id="news-table", cursor_type="row", zebra_stripes=True)
        yield Static("starting feeds…", id="status-bar")
        yield Footer()

    def on_mount(self) -> None:
        ticker = self.query_one("#ticker-table", DataTable)
        ticker.add_columns("SYMBOL", "LAST", "CHG%", "BID", "ASK", "FEED", "UTC")
        options = self.query_one("#options-table", DataTable)
        options.add_columns("CALL", "IV", "VOL", "STRIKE", "PUT", "IV", "VOL")
        news = self.query_one("#news-table", DataTable)
        news.add_columns("TIME", "HEADLINE")

        self.crypto = CryptoWebSocketClient(
            self.post_message,
            self.stop_event,
            exchange_id=os.getenv("MARKETDECK_EXCHANGE", "binance"),
        )
        self.stocks = YahooStockStream(self.post_message, self.stop_event, DEFAULT_STOCKS)

        # Textual run_worker creates supervised asyncio Tasks.  Feed code never
        # mutates widgets; it only uses post_message to cross the UI boundary.
        self.run_worker(self.crypto.run(), name="crypto-feed", exit_on_error=False)
        self.run_worker(self.stocks.run(), name="stock-feed", exit_on_error=False)
        self.run_worker(self._options_loop(), name="options-feed", exit_on_error=False)
        self.run_worker(self._candle_loop(), name="candle-feed", exit_on_error=False)
        self.run_worker(self._news_loop(), name="news-feed", exit_on_error=False)
        self.set_interval(0.10, self._paint_quotes)

    async def on_unmount(self) -> None:
        self.stop_event.set()

    async def _options_loop(self) -> None:
        retry_delay = 2.0
        while not self.stop_event.is_set():
            symbol = self.focus_symbol
            try:
                expiration, rows = await YahooRepository.nearest_options(symbol)
                self.post_message(OptionsReceived(symbol, expiration, rows))
                self.post_message(FeedStatus("options", f"{symbol} · {expiration}"))
                retry_delay = 2.0
                delay = 60.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.post_message(FeedStatus("options", f"{symbol} · {type(exc).__name__}", True))
                delay = retry_delay + random.random()
                retry_delay = min(60.0, retry_delay * 2)
            try:
                await asyncio.wait_for(self.stop_event.wait(), delay)
            except TimeoutError:
                pass

    async def _candle_loop(self) -> None:
        retry_delay = 2.0
        while not self.stop_event.is_set():
            symbol = self.focus_symbol
            try:
                frame = await YahooRepository.candles(symbol)
                self.post_message(CandlesReceived(symbol, frame))
                self.post_message(FeedStatus("chart", f"{symbol} · 15m"))
                retry_delay = 2.0
                delay = 30.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.post_message(FeedStatus("chart", f"{symbol} · {type(exc).__name__}", True))
                delay = retry_delay + random.random()
                retry_delay = min(60.0, retry_delay * 2)
            try:
                await asyncio.wait_for(self.stop_event.wait(), delay)
            except TimeoutError:
                pass

    async def _news_loop(self) -> None:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; MarketDeck/1.0; +https://finviz.com)",
            "Accept": "text/html,application/xhtml+xml",
        }
        async with httpx.AsyncClient(headers=headers, timeout=10.0, follow_redirects=True) as client:
            delay = 1.0
            while not self.stop_event.is_set():
                try:
                    headlines = await scrape_finviz_news(client)
                    self.post_message(NewsReceived(headlines))
                    self.post_message(FeedStatus("news", f"{len(headlines)} headlines"))
                    delay = 60.0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.post_message(FeedStatus("news", f"retry · {type(exc).__name__}", True))
                    delay = min(60.0, max(2.0, delay * 2))
                try:
                    await asyncio.wait_for(self.stop_event.wait(), delay + random.random())
                except TimeoutError:
                    pass

    def on_quote_received(self, message: QuoteReceived) -> None:
        self.quotes[message.quote.symbol] = message.quote
        self._quote_dirty = True

    def _paint_quotes(self) -> None:
        """Coalesce bursty WebSocket traffic to a smooth 10 FPS terminal render."""
        if not self._quote_dirty:
            return
        table = self.query_one("#ticker-table", DataTable)
        table.clear()
        for quote in sorted(self.quotes.values(), key=lambda q: (q.source, q.symbol)):
            change = "—" if quote.change_pct is None else f"{quote.change_pct:+.2f}%"
            change_cell = Text(change, style="green" if (quote.change_pct or 0) >= 0 else "red")
            table.add_row(
                quote.symbol,
                self._fmt_price(quote.price),
                change_cell,
                self._fmt_price(quote.bid),
                self._fmt_price(quote.ask),
                quote.source.upper(),
                quote.timestamp.strftime("%H:%M:%S"),
            )
        self._quote_dirty = False

    def on_options_received(self, message: OptionsReceived) -> None:
        if message.symbol != self.focus_symbol:
            return
        self.query_one("#options-title", Label).update(
            f" {message.symbol} · EXP {message.expiration} · NEAR MONEY"
        )
        table = self.query_one("#options-table", DataTable)
        table.clear()
        for row in message.rows:
            table.add_row(
                self._fmt_price(row.call_last), self._fmt_iv(row.call_iv), self._fmt_int(row.call_volume),
                f"{row.strike:.2f}",
                self._fmt_price(row.put_last), self._fmt_iv(row.put_iv), self._fmt_int(row.put_volume),
            )

    def on_candles_received(self, message: CandlesReceived) -> None:
        if message.symbol != self.focus_symbol:
            return
        self.query_one("#chart-title", Label).update(f" {message.symbol} · 15 MIN CANDLES · 5D")
        chart = self.query_one("#chart", PlotextPlot)
        plt = chart.plt
        plt.clear_data()
        frame = message.frame
        dates = list(range(len(frame)))
        candles = {
            "Open": frame["Open"].astype(float).tolist(),
            "High": frame["High"].astype(float).tolist(),
            "Low": frame["Low"].astype(float).tolist(),
            "Close": frame["Close"].astype(float).tolist(),
        }
        plt.candlestick(dates, candles)
        plt.title(f"{message.symbol}  {frame.index[-1].strftime('%Y-%m-%d %H:%M')}")
        plt.xlabel("15 minute bars")
        plt.ylabel("price")
        chart.refresh()

    def on_news_received(self, message: NewsReceived) -> None:
        table = self.query_one("#news-table", DataTable)
        table.clear()
        for headline in message.headlines:
            table.add_row(headline.time_text, headline.title)

    def on_feed_status(self, message: FeedStatus) -> None:
        self.statuses[message.service] = (message.status, message.error)
        parts = []
        for service in ("stocks", "crypto", "options", "chart", "news"):
            if service not in self.statuses:
                continue
            status, error = self.statuses[service]
            color = "red" if error else "green"
            parts.append(f"[{color}]{service.upper()}[/] {status}")
        self.query_one("#status-bar", Static).update("  │  ".join(parts))

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "ticker-table":
            return
        row = event.data_table.get_row(event.row_key)
        symbol = str(row[0])
        if "/" not in symbol:
            self._select_equity(symbol)

    def action_add_symbol(self) -> None:
        """Open the symbol command bar (Ctrl+A)."""
        command = self.query_one("#command-bar", Input)
        command.add_class("visible")
        command.value = ""
        command.focus()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        raw = event.value.strip().upper()
        event.input.remove_class("visible")
        self.query_one("#ticker-table", DataTable).focus()
        if not raw:
            return
        try:
            if "/" in raw:
                assert self.crypto is not None
                await self.crypto.add_symbol(raw)
                self.notify(f"Subscribed to {raw} on {self.crypto.exchange_id}")
            else:
                assert self.stocks is not None
                await self.stocks.add_symbol(raw)
                self._select_equity(raw)
                self.notify(f"Subscribed to {raw} on Yahoo")
        except Exception as exc:
            self.notify(str(exc), severity="error")

        # To add richer keyboard subscriptions, define another BINDINGS entry,
        # create its action_* method, and call crypto.add_symbol("SOL/USDT") or
        # stocks.add_symbol("TSLA").  Both methods update the live socket; there
        # is no need to restart the application or touch the rendering code.

    def _select_equity(self, symbol: str) -> None:
        self.focus_symbol = symbol
        self.query_one("#chart-title", Label).update(f" {symbol} · LOADING CANDLES…")
        self.query_one("#options-title", Label).update(f" {symbol} · LOADING OPTIONS…")
        self.run_worker(self._refresh_symbol(symbol), name=f"refresh:{symbol}", exclusive=True, exit_on_error=False)

    async def _refresh_symbol(self, symbol: str) -> None:
        options_result, candle_result = await asyncio.gather(
            YahooRepository.nearest_options(symbol), YahooRepository.candles(symbol), return_exceptions=True
        )
        if self.focus_symbol != symbol:
            return
        if isinstance(options_result, BaseException):
            self.post_message(FeedStatus("options", f"{symbol} · {type(options_result).__name__}", True))
        else:
            expiration, rows = options_result
            self.post_message(OptionsReceived(symbol, expiration, rows))
        if isinstance(candle_result, BaseException):
            self.post_message(FeedStatus("chart", f"{symbol} · {type(candle_result).__name__}", True))
        else:
            self.post_message(CandlesReceived(symbol, candle_result))

    def action_refresh(self) -> None:
        self._select_equity(self.focus_symbol)

    def action_focus_chart(self) -> None:
        self.query_one("#chart", PlotextPlot).focus()

    def action_focus_options(self) -> None:
        self.query_one("#options-table", DataTable).focus()

    def on_worker_state_changed(self, event: Worker.StateChanged) -> None:
        if event.state.name == "ERROR":
            self.post_message(FeedStatus(event.worker.name, "worker failed", True))

    @staticmethod
    def _fmt_price(value: float | None) -> str:
        if value is None or not math.isfinite(value):
            return "—"
        return f"{value:,.4f}" if abs(value) < 10 else f"{value:,.2f}"

    @staticmethod
    def _fmt_iv(value: float | None) -> str:
        return "—" if value is None or not math.isfinite(value) else f"{value * 100:.1f}%"

    @staticmethod
    def _fmt_int(value: int | None) -> str:
        return "—" if value is None else f"{value:,}"


if __name__ == "__main__":
    logging.basicConfig(
        filename="marketdeck.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    MarketDeck().run()
