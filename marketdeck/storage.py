from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Any


SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS kv (kind TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(kind,key));
CREATE TABLE IF NOT EXISTS watchlists (name TEXT PRIMARY KEY, symbols TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS paper_trades (id INTEGER PRIMARY KEY AUTOINCREMENT,
    portfolio TEXT NOT NULL, instrument_id TEXT NOT NULL, side TEXT NOT NULL,
    quantity REAL NOT NULL, quote_price REAL NOT NULL, fill_price REAL NOT NULL,
    fee REAL NOT NULL, slippage_bps REAL NOT NULL, quote_freshness TEXT NOT NULL,
    quote_observed_at TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS alerts (id INTEGER PRIMARY KEY AUTOINCREMENT,
    instrument_id TEXT NOT NULL, direction TEXT NOT NULL, threshold REAL NOT NULL,
    triggered_at TEXT, enabled INTEGER NOT NULL DEFAULT 1);
"""


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version > 1:
            raise RuntimeError(f"MarketDeck database version {version} is newer than this application")
        if version == 0:
            self.conn.executescript(SCHEMA)
            self.conn.execute("PRAGMA user_version=1")
            self.conn.commit()
        else:
            self.conn.execute("PRAGMA journal_mode=WAL")
        self.lock = asyncio.Lock()

    async def put(self, kind: str, key: str, value: Any) -> None:
        async with self.lock:
            self.conn.execute("INSERT INTO kv(kind,key,value) VALUES(?,?,?) ON CONFLICT(kind,key) DO UPDATE SET value=excluded.value,updated_at=CURRENT_TIMESTAMP", (kind, key, json.dumps(value, default=str)))
            self.conn.commit()

    async def get(self, kind: str, key: str, default: Any = None) -> Any:
        async with self.lock:
            row = self.conn.execute("SELECT value FROM kv WHERE kind=? AND key=?", (kind, key)).fetchone()
        return json.loads(row[0]) if row else default

    async def list(self, kind: str) -> dict[str, Any]:
        async with self.lock:
            rows = self.conn.execute("SELECT key,value FROM kv WHERE kind=?", (kind,)).fetchall()
        return {row[0]: json.loads(row[1]) for row in rows}

    async def watchlist(self, name: str = "default") -> list[str]:
        async with self.lock:
            row = self.conn.execute("SELECT symbols FROM watchlists WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else ["AAPL", "MSFT", "NVDA", "SPY", "binance:BTC/USDT", "binance:ETH/USDT", "kraken:BTC/USD", "kraken:ETH/USD"]

    async def set_watchlist(self, name: str, symbols: list[str]) -> None:
        async with self.lock:
            self.conn.execute("INSERT INTO watchlists(name,symbols) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET symbols=excluded.symbols", (name, json.dumps(symbols)))
            self.conn.commit()

    async def paper_trade(self, data: dict[str, Any]) -> int:
        columns = ("portfolio", "instrument_id", "side", "quantity", "quote_price", "fill_price", "fee", "slippage_bps", "quote_freshness", "quote_observed_at")
        async with self.lock:
            cursor = self.conn.execute(f"INSERT INTO paper_trades({','.join(columns)}) VALUES({','.join('?' for _ in columns)})", tuple(data[c] for c in columns))
            self.conn.commit()
            return int(cursor.lastrowid)

    async def trades(self, portfolio: str) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute("SELECT * FROM paper_trades WHERE portfolio=? ORDER BY id DESC", (portfolio,)).fetchall()
        return [dict(row) for row in rows]

    async def add_alert(self, instrument_id: str, direction: str, threshold: float) -> int:
        async with self.lock:
            cursor = self.conn.execute("INSERT INTO alerts(instrument_id,direction,threshold) VALUES(?,?,?)", (instrument_id, direction, threshold))
            self.conn.commit()
            return int(cursor.lastrowid)

    async def alerts(self) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute("SELECT * FROM alerts ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]

    async def trigger_alerts(self, instrument_id: str, price: float) -> list[dict[str, Any]]:
        async with self.lock:
            rows = self.conn.execute("SELECT * FROM alerts WHERE enabled=1 AND triggered_at IS NULL AND instrument_id=? AND ((direction='above' AND ? >= threshold) OR (direction='below' AND ? <= threshold))", (instrument_id, price, price)).fetchall()
            for row in rows:
                self.conn.execute("UPDATE alerts SET triggered_at=CURRENT_TIMESTAMP WHERE id=?", (row["id"],))
            self.conn.commit()
        return [dict(row) for row in rows]

    def close(self) -> None:
        self.conn.close()
