from __future__ import annotations

import asyncio
import json
import os

import aiohttp
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal
from textual.message import Message
from textual.widgets import DataTable, Footer, Header, Input, Static


API = os.getenv("MARKETDECK_API", "http://127.0.0.1:8765")


class FeedEvent(Message):
    def __init__(self, event: dict):
        super().__init__()
        self.event = event


class FocusData(Message):
    def __init__(self, symbol: str, candles: dict, options: dict, news: dict):
        super().__init__()
        self.symbol, self.candles, self.options, self.news = symbol, candles, options, news


class MarketDeckTUI(App):
    TITLE = "MARKETDECK"
    SUB_TITLE = "Local financial workstation · source-aware data"
    CSS = """
    Screen { layout: vertical; background: #0b1020; color: #d9e4f5; }
    #toolbar { height: 3; padding: 0 1; background: #17223b; }
    #focus { width: 18; }
    #status { width: 1fr; content-align: right middle; color: #9cb6d8; }
    #grid { layout: grid; grid-size: 2 2; grid-columns: 1fr 1fr; grid-rows: 1fr 1fr; height: 1fr; }
    .pane { border: round #35547b; padding: 0 1; min-height: 8; }
    .title { color: #5ed4d0; text-style: bold; height: 2; }
    DataTable { height: 1fr; }
    #chart, #news { height: 1fr; overflow-y: auto; }
    .classic Screen { background: black; color: #f6b84b; }
    .classic .pane { border: solid #f6b84b; }
    .research Screen { background: #f3f5f8; color: #17223b; }
    .research .pane { border: round #8191ab; }
    """
    BINDINGS = [("ctrl+p", "focus_search", "Search"), ("ctrl+t", "cycle_theme", "Theme"), ("ctrl+r", "refresh_focus", "Refresh"), ("q", "quit", "Quit")]

    def __init__(self):
        super().__init__()
        self.focus_symbol = "SPY"
        self.deck_theme = "modern"
        self.quotes: dict[str, dict] = {}
        self.stream_task: asyncio.Task | None = None
        self.sequence = 0
        self.socket: aiohttp.ClientWebSocketResponse | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="toolbar"):
            yield Input(value="SPY", placeholder="Ticker or binance:BTC/USDT", id="focus")
            yield Static("Connecting…", id="status")
        with Container(id="grid"):
            with Container(classes="pane"):
                yield Static("WATCHLIST / QUOTES", classes="title")
                yield DataTable(id="quotes")
            with Container(classes="pane"):
                yield Static("CANDLES", classes="title")
                yield Static("Loading…", id="chart")
            with Container(classes="pane"):
                yield Static("OPTIONS", classes="title", id="options-title")
                yield DataTable(id="options")
            with Container(classes="pane"):
                yield Static("YAHOO NEWS / SENTIMENT", classes="title")
                yield Static("Loading…", id="news")
        yield Footer()

    async def on_mount(self) -> None:
        self.query_one("#quotes", DataTable).add_columns("ID", "PRICE", "CHG %", "SOURCE", "FRESH", "OBS UTC", "GOT UTC")
        self.query_one("#options", DataTable).add_columns("SIDE", "STRIKE", "LAST", "IV", "VOL")
        self.stream_task = asyncio.create_task(self._stream())
        asyncio.create_task(self.refresh_focus())

    async def on_unmount(self) -> None:
        if self.stream_task:
            self.stream_task.cancel()

    async def _stream(self) -> None:
        delay = 1
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(API.replace("http", "ws", 1) + "/api/v1/stream", heartbeat=20) as ws:
                        self.socket = ws
                        delay = 1
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                self.post_message(FeedEvent(json.loads(message.data)))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.post_message(FeedEvent({"type": "status", "data": f"Reconnecting: {exc}"}))
                await asyncio.sleep(delay)
                delay = min(30, delay * 2)

    def on_feed_event(self, message: FeedEvent) -> None:
        event = message.event
        kind = event.get("type")
        seq = event.get("seq", 0)
        if kind not in {"snapshot", "heartbeat", "status", "subscribed", "resync_required"} and self.sequence and seq != self.sequence + 1:
            if self.socket and not self.socket.closed:
                asyncio.create_task(self.socket.send_json({"type": "resync"}))
        if kind == "resync_required" and self.socket and not self.socket.closed:
            asyncio.create_task(self.socket.send_json({"type": "resync"}))
        if kind == "snapshot":
            self.sequence = seq
            self.quotes = {q["instrument_id"]: q for q in event.get("data", {}).get("quotes", [])}
            self._render_quotes()
        elif kind == "quote":
            self.sequence = seq
            quote = event["data"]
            self.quotes[quote["instrument_id"]] = quote
            self._render_quotes()
        elif kind == "status":
            self.query_one("#status", Static).update(event["data"])
        elif kind == "health":
            self.sequence = seq
            data = event["data"]
            self.query_one("#status", Static).update(f"{data['source']}: {data['state']}")
        elif kind in {"book", "sentiment"}:
            self.sequence = seq

    def _render_quotes(self) -> None:
        table = self.query_one("#quotes", DataTable)
        table.clear()
        for item in sorted(self.quotes.values(), key=lambda q: q["instrument_id"]):
            change = item.get("change_pct")
            table.add_row(item["instrument_id"], f"{item['price']:,.4f}", "—" if change is None else f"{change:+.2f}", item["source"], item["freshness"], item["observed_at"][11:19], item["retrieved_at"][11:19])

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "focus":
            self.focus_symbol = event.value.strip().upper() or "SPY"
            # Ctrl+P then Enter expands live subscriptions at runtime. Accept
            # Yahoo symbols or venue:BASE/QUOTE; the backend deduplicates feeds.
            if self.socket and not self.socket.closed:
                asyncio.create_task(self.socket.send_json({"type": "subscribe", "instruments": [self.focus_symbol]}))
            asyncio.create_task(self.refresh_focus())

    def action_focus_search(self) -> None:
        self.query_one("#focus", Input).focus()

    def action_cycle_theme(self) -> None:
        themes = ["modern", "classic", "research"]
        self.deck_theme = themes[(themes.index(self.deck_theme) + 1) % len(themes)]
        self.remove_class("modern", "classic", "research")
        self.add_class(self.deck_theme)
        self.query_one("#status", Static).update(f"Theme: {self.deck_theme}")

    async def action_refresh_focus(self) -> None:
        asyncio.create_task(self.refresh_focus())

    async def refresh_focus(self) -> None:
        symbol = self.focus_symbol.split(":")[-1].replace("/", "-")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            async def get(path: str):
                try:
                    async with session.get(API + path) as response:
                        return await response.json() if response.ok else {"error": f"HTTP {response.status}"}
                except Exception as exc:
                    return {"error": str(exc)}
            candles, options, news = await asyncio.gather(get(f"/api/v1/candles/{symbol}"), get(f"/api/v1/options/{symbol}"), get(f"/api/v1/news/{symbol}"))
        self.post_message(FocusData(symbol, candles, options, news))

    def on_focus_data(self, message: FocusData) -> None:
        if message.symbol != self.focus_symbol.split(":")[-1].replace("/", "-"):
            return
        symbol, candles, options, news = message.symbol, message.candles, message.options, message.news
        bars = candles.get("data", [])
        if bars:
            # Compact sparkline fallback keeps the TUI usable even without plotext.
            closes = [bar["close"] for bar in bars[-70:]]
            low, high = min(closes), max(closes)
            glyphs = "▁▂▃▄▅▆▇█"
            spark = "".join(glyphs[min(7, int((x - low) / (high - low or 1) * 7))] for x in closes)
            self.query_one("#chart", Static).update(f"{symbol} · Yahoo · {candles.get('freshness')} · {candles.get('retrieved_at')}\n{spark}\n{closes[-1]:,.2f}  (range {low:,.2f}–{high:,.2f})")
        else:
            self.query_one("#chart", Static).update(candles.get("error", "No candles"))
        table = self.query_one("#options", DataTable)
        self.query_one("#options-title", Static).update(f"OPTIONS · Yahoo · {options.get('freshness', 'unavailable')} · retrieved {options.get('retrieved_at', '—')}")
        table.clear()
        rows = options.get("data", {}).get("rows", [])
        for row in rows[:80]:
            table.add_row(row["side"], str(row["strike"]), str(row["last"] or "—"), str(round(row["iv"] * 100, 1)) + "%" if row["iv"] is not None else "—", str(row["volume"] or "—"))
        headlines = news.get("data", [])
        self.query_one("#news", Static).update("\n\n".join(f"• {item['title']}\n  {item.get('source')} · observed {item.get('observed_at')} · retrieved {item.get('retrieved_at')} · {item.get('sentiment', {}).get('label') if item.get('sentiment') else 'unscored'}\n  {item.get('url') or ''}" for item in headlines[:15]) or news.get("error", "No news"))


def main() -> None:
    MarketDeckTUI().run()
