import asyncio
import concurrent.futures
import tempfile
import unittest
from contextlib import closing, suppress
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch
import httpx

from fastapi.testclient import TestClient

from marketdeck.api import app, service
from marketdeck.models import Quote
from marketdeck.storage import Store
from marketdeck.tui import MarketDeckTUI
from marketdeck.service import MarketService, infer_finbert
from marketdeck.providers import Research, safe_error


class StoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchlist_and_cache_roundtrip(self):
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "test.db")
            await store.set_watchlist("default", ["AAPL", "kraken:BTC/USD"])
            self.assertEqual(await store.watchlist(), ["AAPL", "kraken:BTC/USD"])
            await store.put("news", "AAPL", {"freshness": "reference"})
            self.assertEqual((await store.get("news", "AAPL"))["freshness"], "reference")
            store.close()

    async def test_tui_mount_and_theme(self):
        async def idle(_):
            await asyncio.Event().wait()
        with patch.object(MarketDeckTUI, "_stream", idle), patch.object(MarketDeckTUI, "refresh_focus", new_callable=AsyncMock):
            app = MarketDeckTUI()
            async with app.run_test() as pilot:
                await pilot.press("ctrl+t")
                self.assertEqual(app.deck_theme, "classic")

    async def test_alert_triggers_once_on_quote_replay(self):
        with tempfile.TemporaryDirectory() as folder:
            engine = MarketService(Path(folder) / "test.db")
            await engine.store.add_alert("yahoo:SPY", "above", 100)
            quote = Quote(instrument_id="yahoo:SPY", symbol="SPY", source="fixture", observed_at=datetime.now(timezone.utc), update_frequency="replay", freshness="reference", price=101)
            engine.on_quote(quote)
            await asyncio.sleep(0.05)
            self.assertEqual(len([e for e in engine.events if e["type"] == "alert"]), 1)
            engine.on_quote(quote)
            await asyncio.sleep(0.05)
            self.assertEqual(len([e for e in engine.events if e["type"] == "alert"]), 1)
            await engine.close()

    async def test_cached_record_becomes_stale_on_source_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            engine = MarketService(Path(folder) / "test.db")
            await engine.store.put("macro", "TEST", {"data": {"value": 7}, "freshness": "reference"})
            async def fail():
                raise httpx.ReadTimeout("offline")
            record = await engine.cached("macro", "TEST", 300, fail)
            self.assertEqual(record["freshness"], "stale")
            self.assertEqual(record["data"]["value"], 7)
            await engine.close()

    async def test_offline_startup_marks_cached_quote_stale(self):
        with tempfile.TemporaryDirectory() as folder:
            engine = MarketService(Path(folder) / "test.db")
            quote = Quote(instrument_id="yahoo:SPY", symbol="SPY", source="fixture", observed_at=datetime.now(timezone.utc), update_frequency="replay", freshness="delayed", price=101)
            await engine.store.put("quote", "yahoo:SPY", quote.model_dump(mode="json"))
            with patch.object(engine.yahoo, "stream", new_callable=AsyncMock), patch.object(engine, "_bootstrap_watchlist", new_callable=AsyncMock):
                await engine.start()
                self.assertEqual(engine.quotes["yahoo:SPY"]["freshness"], "stale")
                await engine.close()

    async def test_sec_and_fred_response_parsing(self):
        def transport(request: httpx.Request) -> httpx.Response:
            if "submissions" in str(request.url):
                return httpx.Response(200, json={"name": "Example Inc", "filings": {"recent": {"accessionNumber": ["0001-02-345678"], "primaryDocument": ["report.htm"], "form": ["10-K"], "filingDate": ["2026-10-01"]}}})
            if "browse-edgar" in str(request.url):
                return httpx.Response(200, text='<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>10-K Example</title><updated>2026-10-01T12:00:00Z</updated><link href="https://www.sec.gov/example"/></entry></feed>')
            return httpx.Response(200, json={"observations": [{"date": "2026-10-01", "value": "4.5"}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            adapter = Research(client)
            adapter.sec_agent = "Example example@example.com"
            adapter.fred_key = "test-key"
            filings = await adapter.sec_filings("320193")
            self.assertEqual(filings[0]["form"], "10-K")
            self.assertIn("/320193/000102345678/report.htm", filings[0]["url"])
            feed = await adapter.sec_rss("320193")
            self.assertEqual(feed[0]["title"], "10-K Example")
            series = await adapter.fred_series("TEST")
            self.assertEqual(series["observations"][0]["value"], "4.5")


class APITests(unittest.TestCase):
    def test_fred_key_never_appears_in_error_text(self):
        with patch.dict("os.environ", {"FRED_API_KEY": "secret-key"}):
            self.assertNotIn("secret-key", safe_error("failed https://fred.test/?api_key=secret-key"))

    def test_finbert_probability_contract(self):
        def fake_pipeline(texts, truncation):
            return [[{"label": "positive", "score": 0.7}, {"label": "negative", "score": 0.1}, {"label": "neutral", "score": 0.2}] for _ in texts]
        with patch("marketdeck.service._PIPELINE", fake_pipeline), patch.dict("os.environ", {"FINBERT_REVISION": "a" * 40}):
            result = infer_finbert([("headline-hash", "Strong earnings")])["headline-hash"]
        self.assertEqual(result["label"], "positive")
        self.assertEqual(set(result["probabilities"]), {"positive", "negative", "neutral"})
        self.assertAlmostEqual(sum(result["probabilities"].values()), 1)

    def test_snapshot_and_paper_fill(self):
        with tempfile.TemporaryDirectory() as folder, closing(Store(Path(folder) / "test.db")) as test_store, patch.object(service, "start", new_callable=AsyncMock), patch.object(service, "close", new_callable=AsyncMock), patch.object(service, "store", test_store):
            with TestClient(app) as client:
                quote = Quote(instrument_id="yahoo:SPY", symbol="SPY", source="Yahoo Finance", venue="Yahoo", observed_at=datetime.now(timezone.utc), currency="USD", unit="price", update_frequency="stream", freshness="delayed", price=500)
                service.quotes["yahoo:SPY"] = quote.model_dump(mode="json")
                snapshot_seen = False
                resync_seen = False
                # Starlette's current sync TestClient may propagate cancellation
                # from the close handshake after all assertions have completed.
                with suppress(concurrent.futures.CancelledError):
                    with client.websocket_connect("/api/v1/stream") as ws:
                        message = ws.receive_json()
                        self.assertEqual(message["type"], "snapshot")
                        self.assertIn("yahoo:SPY", [x["instrument_id"] for x in message["data"]["quotes"]])
                        snapshot_seen = True
                        ws.send_json({"type": "resync"})
                        self.assertEqual(ws.receive_json()["type"], "snapshot")
                        resync_seen = True
                self.assertTrue(snapshot_seen and resync_seen)
                result = client.post("/api/v1/paper/test/orders", json={"instrument_id": "yahoo:SPY", "side": "buy", "quantity": 2, "slippage_bps": 10, "fee": 1})
                self.assertEqual(result.status_code, 200, result.text)
                self.assertAlmostEqual(result.json()["fill_price"], 500.5)
                self.assertEqual(result.json()["quote_freshness"], "delayed")
                summary = client.get("/api/v1/paper/test").json()
                self.assertAlmostEqual(summary["cash"], 98_998.0)
                self.assertAlmostEqual(summary["equity"], 99_998.0)
                quote.freshness = "stale"
                service.quotes["yahoo:SPY"] = quote.model_dump(mode="json")
                self.assertEqual(client.post("/api/v1/paper/test/orders", json={"instrument_id": "yahoo:SPY", "side": "buy", "quantity": 1}).status_code, 409)
                quote.freshness = "reference"
                service.quotes["yahoo:SPY"] = quote.model_dump(mode="json")
                self.assertEqual(client.post("/api/v1/paper/test/orders", json={"instrument_id": "yahoo:SPY", "side": "buy", "quantity": 1}).status_code, 409)

    def test_websocket_repeated_subscribe_is_idempotent(self):
        with patch.object(service, "start", new_callable=AsyncMock), patch.object(service, "close", new_callable=AsyncMock), patch.object(service, "subscribe", new_callable=AsyncMock, return_value="yahoo:SPY") as subscribe, patch.object(service, "unsubscribe", new_callable=AsyncMock) as unsubscribe:
            with TestClient(app) as client:
                with suppress(concurrent.futures.CancelledError):
                    with client.websocket_connect("/api/v1/stream") as ws:
                        self.assertEqual(ws.receive_json()["type"], "snapshot")
                        ws.send_json({"type": "subscribe", "instruments": ["SPY", "yahoo:spy"]})
                        self.assertEqual(ws.receive_json()["instrument_id"], "yahoo:SPY")
                        self.assertEqual(ws.receive_json()["instrument_id"], "yahoo:SPY")
                        ws.send_json({"type": "resync"})
                        snapshot = ws.receive_json()
                        self.assertEqual(snapshot["type"], "snapshot")
                        self.assertIn("health", snapshot["data"])
                        ws.send_json({"type": "unsubscribe", "instruments": ["spy"]})
                        ws.send_json({"type": "resync"})
                        self.assertEqual(ws.receive_json()["type"], "snapshot")
                        subscribe.assert_awaited_once_with("SPY")
                        unsubscribe.assert_awaited_once_with("yahoo:SPY")


if __name__ == "__main__":
    unittest.main()
