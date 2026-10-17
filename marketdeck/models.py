from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


def now() -> datetime:
    return datetime.now(timezone.utc)


Freshness = Literal["live", "delayed", "reference", "stale", "unavailable"]


class Datum(BaseModel):
    instrument_id: str
    source: str
    venue: str | None = None
    observed_at: datetime
    retrieved_at: datetime = Field(default_factory=now)
    currency: str | None = None
    unit: str | None = None
    update_frequency: str
    freshness: Freshness
    url: str | None = None


class Quote(Datum):
    symbol: str
    price: float
    change_pct: float | None = None
    bid: float | None = None
    ask: float | None = None


class NewsItem(Datum):
    title: str
    summary: str | None = None
    sentiment: dict[str, Any] | None = None


class Health(BaseModel):
    source: str
    state: str
    last_success: datetime | None = None
    last_error: str | None = None
    error_count: int = 0


def instrument_id(symbol: str, venue: str = "yahoo") -> str:
    return f"{venue.lower()}:{symbol.upper()}"
