# MarketDeck

A local, read-only market research workstation with a FastAPI data service, Textual terminal client, and React web client. Quotes from free sources may be delayed, incomplete, or unavailable; this is not an exchange-grade Bloomberg replacement or investment advice. Paper orders never reach a broker.

## Run

Python 3.11+ and Node 20+ are required.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m marketdeck serve
```

In another terminal:

```powershell
.\.venv\Scripts\python.exe -m marketdeck tui
```

For the web client:

```powershell
cd web
npm install
npm run dev
```

Open `http://127.0.0.1:5173`. The web UI and TUI share `http://127.0.0.1:8765` as their single backend. The original `main.py` remains as the prototype and is not the new launch entry point.
To run that preserved prototype in a fresh environment, also install `requirements-prototype.txt`.
After dependencies are installed, `scripts/run.ps1` (Windows) or `scripts/run.sh` (macOS/Linux) starts the backend and web client together; stopping the script stops its backend process. The TUI remains a separate command.

In the web UI, press `Ctrl+K` for instrument search; choose a result to add it to the saved watchlist. Use the Layout slider to persist dashboard column width. In the terminal UI, press `Ctrl+P`, enter a Yahoo symbol or `venue:BASE/QUOTE`, and press Enter to add a live subscription; `Ctrl+T` cycles themes.

## Optional research credentials

Set `SEC_USER_AGENT` to a real name and contact email before using EDGAR. Set `FRED_API_KEY` to your free FRED key for macro data. Store these in your shell or a local secret manager, never in source control. The API does not expose either secret.

FinBERT is opt-in because its first download is large. Install `requirements-sentiment.txt`, then set `MARKETDECK_FINBERT=1` and `FINBERT_REVISION` to an audited 40-character Hugging Face commit hash; the worker refuses an unpinned revision. FinBERT inference runs in a separate process; headlines still appear if the model is unavailable. The `ProsusAI/finbert` model weights use a CC BY-NC-SA 4.0 noncommercial license—review it before use beyond personal research. Sentiment describes text only, not likely price movement.

## Data provenance and limitations

- Yahoo Finance via `yfinance`: streaming quotes where available; quotes are labeled `delayed` because entitlement and exchange delay vary. Charts cache for at least 60 seconds, listed options 5 minutes, and news 2 minutes. Yahoo data is intended for personal use.
- CCXT Pro: public Binance/Kraken trades and order books are kept per venue. Exchange geography or availability may prevent a feed.
- SEC: official EDGAR JSON and Atom filing feed, only when a contact user agent is configured, capped at two requests per second. Ticker/company lookup and recent filings are available; full-text filing search is not yet implemented. Set `MARKETDECK_WATCH_CIKS=320193,789019` for watched-company polling (60 seconds during U.S. business hours; 5 minutes otherwise).
- FRED: official API; requires a key. Requested observations cache for 5 minutes and release calendar daily. Set `MARKETDECK_WATCH_FRED=FEDFUNDS,UNRATE` to poll recent updates and refresh changed tracked series (5 minutes around releases; 30 minutes otherwise).
- ECB: daily FX reference rates, not executable quotes. U.S. Treasury: official daily par yield curve XML, not individual-bond prices. Commodity/futures, FX, and bond-ETF shortcuts use curated Yahoo symbols and must be read as proxies or expiring contracts. Finviz scraping is not enabled; no robots/terms permission was established for this build.
- All UI data includes a source, observation or publication time when supplied, retrieval time, and freshness state. Cached quotes are stale after restart. Free feeds do not guarantee real-time data.

The current web interface provides live quotes, watchlists, candlestick charts, options, Yahoo news and optional sentiment, SEC filing lists and extracted company facts, FRED observations and releases, ECB FX rates, Treasury yields, source health, themes, adjustable saved column width, local alerts, and paper fills/positions with CSV export. The Textual interface provides quote, chart, options, and news panels. The backend also provides a local-quote screener. Fully draggable panel layouts in both clients, richer screeners, full filing-text analysis, and packaged launchers remain future work; do not assume a missing endpoint means zero data.

## API

Open `/docs` on the local backend for the REST contract. `/api/v1/stream` sends a snapshot, sequenced events and heartbeats, and accepts `{ "type": "subscribe", "instruments": ["AAPL", "kraken:BTC/USD"] }`, `unsubscribe`, and `resync` messages. Do not expose the backend publicly: it has no authentication and is bound to loopback.
