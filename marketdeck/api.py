from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .models import instrument_id, now
from .service import MarketService
from .providers import safe_error


service = MarketService()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await service.start()
    try:
        yield
    finally:
        await service.close()


app = FastAPI(title="MarketDeck", version="0.2", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(Exception)
async def error_handler(_, exc: Exception):
    return JSONResponse(status_code=503, content={"error": type(exc).__name__, "detail": safe_error(exc)})


@app.get("/api/v1/health")
async def health():
    return {"sources": {key: value.model_dump(mode="json") for key, value in service.health.items()}, "sequence": service.sequence, "time": now().isoformat()}


@app.get("/api/v1/search")
async def search(q: str):
    if not q.strip():
        return {"data": [], "source": "Yahoo Finance", "freshness": "reference", "retrieved_at": now().isoformat()}
    try:
        query = q.strip()[:80]
        return await service.cached("search", query.casefold(), 300, lambda: service.yahoo.search(query))
    except Exception as exc:
        raise HTTPException(503, safe_error(exc)) from exc


@app.get("/api/v1/quotes")
async def quotes():
    return list(service.quotes.values())


@app.get("/api/v1/screener")
async def screener(min_price: float = 0, max_price: float = 1e15, source: str | None = None):
    if max_price < min_price:
        raise HTTPException(422, "max_price must be >= min_price")
    return [q for q in service.quotes.values() if min_price <= q["price"] <= max_price and (source is None or q["source"].casefold() == source.casefold())]


@app.get("/api/v1/quotes/{instrument_id:path}")
async def quote(instrument_id: str):
    if instrument_id not in service.quotes:
        raise HTTPException(404, "No current or cached quote")
    return service.quotes[instrument_id]


@app.get("/api/v1/candles/{symbol}")
async def candles(symbol: str):
    return await service.candles(symbol)


@app.get("/api/v1/options/{symbol}")
async def options(symbol: str):
    return await service.options(symbol)


@app.get("/api/v1/orderbooks/{instrument_id:path}")
async def orderbook(instrument_id: str):
    if instrument_id not in service.books:
        raise HTTPException(404, "Order book unavailable")
    return service.books[instrument_id]


@app.get("/api/v1/news/{symbol}")
async def news(symbol: str):
    return await service.news(symbol)


@app.get("/api/v1/sentiment/{symbol}")
async def sentiment(symbol: str, hours: int = 24):
    from datetime import datetime, timedelta
    if not 1 <= hours <= 168:
        raise HTTPException(422, "hours must be 1..168")
    record = await service.news(symbol)
    cutoff = now() - timedelta(hours=hours)
    scored = []
    for item in record.get("data", []):
        result = item.get("sentiment")
        published = item.get("observed_at")
        if not result or not published:
            continue
        try:
            stamp = datetime.fromisoformat(str(published).replace("Z", "+00:00"))
            if stamp.tzinfo and stamp >= cutoff:
                scored.append(result["probabilities"])
        except (KeyError, TypeError, ValueError):
            pass
    probabilities = {label: sum(p.get(label, 0) for p in scored) / len(scored) for label in ("positive", "negative", "neutral")} if scored else None
    return {"instrument_id": f"yahoo:{symbol.upper()}", "source": "Yahoo headlines / ProsusAI FinBERT", "window_hours": hours, "sample_size": len(scored), "universe": "latest 20 Yahoo search news items", "retrieved_at": now().isoformat(), "freshness": record["freshness"], "probabilities": probabilities, "disclaimer": "Text sentiment only; not an asset-price forecast"}


@app.get("/api/v1/filings/{cik}")
async def filings(cik: str):
    return await service.cached("filings", cik, 60, lambda: service.research.sec_filings(cik))


@app.get("/api/v1/filings/{cik}/rss")
async def filing_rss(cik: str):
    return await service.cached("filing-rss", cik, 60, lambda: service.research.sec_rss(cik))


@app.get("/api/v1/companies")
async def companies(q: str):
    return await service.cached("companies", q.casefold(), 86400, lambda: service.research.sec_company_lookup(q))


@app.get("/api/v1/fundamentals/{cik}")
async def fundamentals(cik: str):
    return await service.cached("fundamentals", cik, 86400, lambda: service.research.sec_facts(cik))


@app.get("/api/v1/macro/{series_id}")
async def macro(series_id: str):
    return await service.cached("macro", series_id.upper(), 300, lambda: service.research.fred_series(series_id))


@app.get("/api/v1/releases")
async def releases():
    return await service.cached("releases", "fred", 86400, service.research.fred_releases)


@app.get("/api/v1/fx")
async def fx():
    return await service.cached("fx", "ecb", 86400, service.research.ecb_fx)


@app.get("/api/v1/treasury/yields")
async def treasury_yields():
    return await service.cached("treasury", "yields", 86400, service.research.treasury_yields)


@app.get("/api/v1/watchlists/{name}")
async def watchlist(name: str):
    return {"name": name, "symbols": await service.store.watchlist(name)}


class WatchlistIn(BaseModel):
    symbols: list[str]


@app.put("/api/v1/watchlists/{name}")
async def set_watchlist(name: str, item: WatchlistIn):
    symbols = list(dict.fromkeys(item.symbols))[:100]
    await service.set_watchlist(name, symbols)
    return {"name": name, "symbols": symbols}


@app.get("/api/v1/workspaces/{name}")
async def workspace(name: str):
    return await service.store.get("workspace", name, {"name": name, "theme": "modern", "focus": "SPY"})


@app.put("/api/v1/workspaces/{name}")
async def set_workspace(name: str, data: dict[str, Any]):
    if data.get("theme") not in {"modern", "classic", "research"}:
        raise HTTPException(422, "Unknown theme")
    await service.store.put("workspace", name, data)
    return data


class PaperOrder(BaseModel):
    instrument_id: str
    side: str
    quantity: float = Field(gt=0)
    slippage_bps: float = Field(default=2, ge=0, le=1000)
    fee: float = Field(default=0, ge=0)


@app.post("/api/v1/paper/{portfolio}/orders")
async def paper_order(portfolio: str, order: PaperOrder):
    if order.side not in {"buy", "sell"}:
        raise HTTPException(422, "side must be buy or sell")
    quote = service.quotes.get(order.instrument_id)
    if not quote or quote["freshness"] not in {"live", "delayed"}:
        raise HTTPException(409, "A live or delayed market quote is required for a paper fill")
    price = float(quote["price"])
    factor = 1 + (1 if order.side == "buy" else -1) * order.slippage_bps / 10000
    trade = {"portfolio": portfolio, "instrument_id": order.instrument_id, "side": order.side, "quantity": order.quantity, "quote_price": price, "fill_price": price * factor, "fee": order.fee, "slippage_bps": order.slippage_bps, "quote_freshness": quote["freshness"], "quote_observed_at": quote["observed_at"]}
    trade["id"] = await service.store.paper_trade(trade)
    return trade


@app.get("/api/v1/paper/{portfolio}/trades")
async def paper_trades(portfolio: str):
    return await service.store.trades(portfolio)


@app.get("/api/v1/paper/{portfolio}")
async def paper_portfolio(portfolio: str):
    trades = await service.store.trades(portfolio)
    cash = 100_000.0
    positions: dict[str, float] = {}
    for trade in reversed(trades):
        direction = 1 if trade["side"] == "buy" else -1
        quantity = float(trade["quantity"])
        cash -= direction * quantity * float(trade["fill_price"]) + float(trade["fee"])
        positions[trade["instrument_id"]] = positions.get(trade["instrument_id"], 0) + direction * quantity
    marked = []
    complete = True
    equity = cash
    for key, quantity in positions.items():
        if not quantity:
            continue
        quote = service.quotes.get(key)
        mark = float(quote["price"]) if quote else None
        if mark is None:
            complete = False
        else:
            equity += quantity * mark
        marked.append({"instrument_id": key, "quantity": quantity, "mark_price": mark, "mark_freshness": quote["freshness"] if quote else "unavailable", "mark_value": quantity * mark if mark is not None else None})
    return {"portfolio": portfolio, "starting_cash": 100_000.0, "cash": cash, "equity": equity if complete else None, "positions": marked, "trades": len(trades), "retrieved_at": now().isoformat(), "note": "Equity is null when a position lacks a quote; stale marks are shown but not executable"}


@app.get("/api/v1/paper/{portfolio}/export.csv")
async def export_trades(portfolio: str):
    import csv
    import io
    rows = await service.store.trades(portfolio)
    output = io.StringIO()
    columns = ["id", "created_at", "instrument_id", "side", "quantity", "quote_price", "fill_price", "fee", "slippage_bps", "quote_freshness", "quote_observed_at"]
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv", headers={"Content-Disposition": f'attachment; filename="marketdeck-{portfolio}.csv"'})


class AlertIn(BaseModel):
    instrument_id: str
    direction: str
    threshold: float = Field(gt=0)


@app.get("/api/v1/alerts")
async def alerts():
    return await service.store.alerts()


@app.post("/api/v1/alerts")
async def create_alert(alert: AlertIn):
    if alert.direction not in {"above", "below"}:
        raise HTTPException(422, "direction must be above or below")
    alert_id = await service.store.add_alert(alert.instrument_id, alert.direction, alert.threshold)
    return {"id": alert_id, **alert.model_dump()}


@app.websocket("/api/v1/stream")
async def stream(socket: WebSocket):
    await socket.accept()
    send_lock = asyncio.Lock()
    async def send(payload: dict[str, Any]):
        async with send_lock:
            await socket.send_json(payload)
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    service.clients.add(queue)
    own: set[str] = set()
    await send({"type": "snapshot", "seq": service.sequence, "data": {"quotes": list(service.quotes.values()), "books": list(service.books.values()), "health": {k: v.model_dump(mode="json") for k, v in service.health.items()}}})

    async def sender():
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
                await send(event)
            except TimeoutError:
                await send({"type": "heartbeat", "seq": service.sequence, "time": now().isoformat()})

    async def receiver():
        while True:
            message = await socket.receive_json()
            action = message.get("type")
            if action == "subscribe":
                for value in message.get("instruments", [])[:50]:
                    requested = str(value)
                    venue, symbol = requested.split(":", 1) if ":" in requested else ("yahoo", requested)
                    candidate = instrument_id(symbol.strip(), venue.strip())
                    if candidate in own:
                        await send({"type": "subscribed", "instrument_id": candidate, "data": service.quotes.get(candidate)})
                        continue
                    key = await service.subscribe(requested)
                    own.add(key)
                    await send({"type": "subscribed", "instrument_id": key, "data": service.quotes.get(key)})
            elif action == "unsubscribe":
                for value in message.get("instruments", [])[:50]:
                    requested = str(value)
                    venue, symbol = requested.split(":", 1) if ":" in requested else ("yahoo", requested)
                    key = instrument_id(symbol.strip(), venue.strip())
                    if key in own:
                        own.remove(key)
                        await service.unsubscribe(key)
            elif action == "resync":
                await send({"type": "snapshot", "seq": service.sequence, "data": {"quotes": list(service.quotes.values()), "books": list(service.books.values()), "health": {k: v.model_dump(mode="json") for k, v in service.health.items()}}})

    tasks = [asyncio.create_task(sender()), asyncio.create_task(receiver())]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    except (WebSocketDisconnect, RuntimeError, ValueError, asyncio.CancelledError):
        pass
    finally:
        service.clients.discard(queue)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for key in own:
            await service.unsubscribe(key)
