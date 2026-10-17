import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { createChart, CandlestickSeries, type CandlestickData, type UTCTimestamp } from 'lightweight-charts';
import { Activity, BarChart3, Newspaper, Search, ShieldCheck, SlidersHorizontal, Wifi, WifiOff } from 'lucide-react';
import './style.css';
import './terminal.css';

type Quote = { instrument_id: string; symbol: string; price: number; change_pct: number | null; source: string; venue?: string; observed_at: string; retrieved_at: string; freshness: string; currency?: string };
type RecordSet<T> = { data: T; retrieved_at: string; freshness: string; source: string; error?: string };
type Candle = { time: number; open: number; high: number; low: number; close: number; volume: number };
type Option = { side: string; strike: number; last: number | null; iv: number | null; volume: number | null };
type News = { id: string; title: string; url?: string; observed_at?: string; source: string; sentiment?: { label: string; probabilities: Record<string, number> } };
type Theme = 'modern' | 'classic' | 'research';
const api = '/api/v1';

async function fetchJSON<T>(path: string): Promise<T> { const response = await fetch(api + path); if (!response.ok) throw new Error(`${response.status}: ${await response.text()}`); return response.json(); }
function prettyTime(value?: string) { return value ? new Date(value).toLocaleString() : '—'; }
function Fresh({ value }: { value?: string }) { return <span className={'fresh ' + (value || 'unavailable')}>{value || 'unavailable'}</span>; }

function Chart({ symbol, data }: { symbol: string; data: Candle[] }) {
  const element = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!element.current || !data.length) return;
    const chart = createChart(element.current, { width: element.current.clientWidth, height: element.current.clientHeight, layout: { background: { color: 'transparent' }, textColor: '#8da5c0', attributionLogo: true }, grid: { vertLines: { color: '#1c2c43' }, horzLines: { color: '#1c2c43' } }, timeScale: { timeVisible: true } });
    const series = chart.addSeries(CandlestickSeries, { upColor: '#24c4a8', downColor: '#f36f79', borderVisible: false, wickUpColor: '#24c4a8', wickDownColor: '#f36f79' });
    series.setData(data.map(x => ({ time: x.time as UTCTimestamp, open: x.open, high: x.high, low: x.low, close: x.close })) as CandlestickData<UTCTimestamp>[]);
    chart.timeScale().fitContent();
    const resize = new ResizeObserver(() => chart.applyOptions({ width: element.current?.clientWidth || 500, height: element.current?.clientHeight || 300 }));
    resize.observe(element.current);
    return () => { resize.disconnect(); chart.remove(); };
  }, [symbol, data]);
  return <div className="chart" ref={element} />;
}

function App() {
  const [quotes, setQuotes] = useState<Record<string, Quote>>({});
  const [focus, setFocus] = useState('SPY');
  const [entry, setEntry] = useState('');
  const [suggestions, setSuggestions] = useState<any[]>([]);
  const searchInput = useRef<HTMLInputElement>(null);
  const [theme, setTheme] = useState<Theme>('modern');
  const [leftPct, setLeftPct] = useState(44);
  useEffect(() => { document.documentElement.style.setProperty('--leftPct', `${leftPct}%`); }, [leftPct]);
  const [tab, setTab] = useState<'terminal' | 'research' | 'paper'>('terminal');
  const [connected, setConnected] = useState(false);
  const [watchlist, setWatchlist] = useState<string[]>([]);
  const [candles, setCandles] = useState<RecordSet<Candle[]> | null>(null);
  const [options, setOptions] = useState<RecordSet<{ expiration: string; rows: Option[] }> | null>(null);
  const [news, setNews] = useState<RecordSet<News[]> | null>(null);
  const [aggregate, setAggregate] = useState<{ sample_size: number; window_hours: number; probabilities: Record<string, number> | null; source: string; retrieved_at: string } | null>(null);
  const [book, setBook] = useState<{ bids: number[][]; asks: number[][]; freshness: string } | null>(null);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [cik, setCik] = useState('320193');
  const [filings, setFilings] = useState<RecordSet<any[]> | null>(null);
  const [fundamentals, setFundamentals] = useState<RecordSet<any> | null>(null);
  const [fred, setFred] = useState('FEDFUNDS');
  const [macro, setMacro] = useState<RecordSet<any> | null>(null);
  const [fx, setFx] = useState<RecordSet<any> | null>(null);
  const [treasury, setTreasury] = useState<RecordSet<any> | null>(null);
  const [releases, setReleases] = useState<RecordSet<any> | null>(null);
  const [trades, setTrades] = useState<any[]>([]);
  const [portfolio, setPortfolio] = useState<any | null>(null);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [threshold, setThreshold] = useState('');
  const [direction, setDirection] = useState<'above' | 'below'>('above');
  const [health, setHealth] = useState<Record<string, any>>({});
  const seq = useRef(0);
  const socket = useRef<WebSocket | null>(null);
  const focusRef = useRef(focus);
  useEffect(() => { focusRef.current = focus; }, [focus]);
  useEffect(() => { const key = (event: KeyboardEvent) => { if (event.ctrlKey && event.key.toLowerCase() === 'k') { event.preventDefault(); searchInput.current?.focus(); } }; window.addEventListener('keydown', key); return () => window.removeEventListener('keydown', key); }, []);
  useEffect(() => { if (entry.trim().length < 2 || entry.includes(':')) { setSuggestions([]); return; } const timer = setTimeout(() => fetchJSON<RecordSet<any[]>>(`/search?q=${encodeURIComponent(entry.trim())}`).then(x => setSuggestions(x.data || [])).catch(() => setSuggestions([])), 650); return () => clearTimeout(timer); }, [entry]);

  useEffect(() => {
    fetchJSON<{ symbols: string[] }>('/watchlists/default').then(x => setWatchlist(x.symbols)).catch(e => setError(String(e)));
    fetchJSON<{ theme: Theme; focus: string; layout?: {leftPct?: number} }>('/workspaces/default').then(x => { setTheme(x.theme); setFocus(x.focus); if (x.layout?.leftPct) setLeftPct(x.layout.leftPct); }).catch(() => {});
    let stop = false; let retry = 1000;
    const connect = () => {
      if (stop) return;
      const ws = new WebSocket(`ws://${location.hostname}:8765/api/v1/stream`); socket.current = ws;
      ws.onopen = () => { setConnected(true); retry = 1000; };
      ws.onmessage = event => {
        const msg = JSON.parse(event.data);
        if (msg.type === 'resync_required') ws.send(JSON.stringify({ type: 'resync' }));
        if (msg.type === 'snapshot') { seq.current = msg.seq; setQuotes(Object.fromEntries((msg.data.quotes || []).map((q: Quote) => [q.instrument_id, q]))); setHealth(msg.data.health || {}); }
        else if (msg.type === 'quote') { if (seq.current && msg.seq !== seq.current + 1) ws.send(JSON.stringify({ type: 'resync' })); seq.current = msg.seq; setQuotes(prev => ({ ...prev, [msg.data.instrument_id]: msg.data })); }
        else if (msg.type === 'book') { seq.current = msg.seq; if (msg.data.instrument_id === focusRef.current) setBook(msg.data); }
        else if (msg.type === 'health') { seq.current = msg.seq; setHealth(prev => ({ ...prev, [msg.data.source]: msg.data })); }
        else if (msg.type === 'alert') { seq.current = msg.seq; setNotice(`Price alert: ${msg.data.instrument_id} ${msg.data.direction} ${msg.data.threshold} · ${msg.data.trigger_price}`); fetchJSON<any[]>('/alerts').then(setAlerts).catch(() => {}); }
        else if (msg.type === 'sentiment') { seq.current = msg.seq; const current = focusRef.current; const symbol = current.includes(':') ? current.split(':')[1].replace('/', '-') : current; fetchJSON<RecordSet<News[]>>(`/news/${symbol}`).then(setNews).catch(() => {}); fetchJSON<any>(`/sentiment/${symbol}?hours=24`).then(setAggregate).catch(() => {}); }
        else if (typeof msg.seq === 'number') seq.current = msg.seq;
      };
      ws.onclose = () => { setConnected(false); if (!stop) { setTimeout(connect, retry); retry = Math.min(30000, retry * 2); } };
      ws.onerror = () => ws.close();
    };
    connect();
    return () => { stop = true; socket.current?.close(); };
  }, []);

  useEffect(() => {
    const symbol = focus.includes(':') ? focus.split(':')[1].replace('/', '-') : focus;
    setCandles(null); setOptions(null); setNews(null); setBook(null);
    fetchJSON<RecordSet<Candle[]>>(`/candles/${symbol}`).then(setCandles).catch(e => setError(String(e)));
    fetchJSON<RecordSet<{ expiration: string; rows: Option[] }>>(`/options/${symbol}`).then(setOptions).catch(() => setOptions({ data: { expiration: '', rows: [] }, freshness: 'unavailable', source: 'Yahoo', retrieved_at: '' }));
    fetchJSON<RecordSet<News[]>>(`/news/${symbol}`).then(setNews).catch(() => setNews({ data: [], freshness: 'unavailable', source: 'Yahoo', retrieved_at: '' }));
    fetchJSON<any>(`/sentiment/${symbol}?hours=24`).then(setAggregate).catch(() => setAggregate(null));
  }, [focus]);

  const persistWorkspace = (nextTheme: Theme, nextFocus: string, nextLeftPct = leftPct) => fetch(api + '/workspaces/default', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ theme: nextTheme, focus: nextFocus, layout: { leftPct: nextLeftPct } }) }).catch(() => {});
  const choose = (value: string) => { setFocus(value); persistWorkspace(theme, value); };
  const addSymbol = (symbol: string) => { const value = symbol.trim().toUpperCase(); if (!value) return; const next = [...new Set([...watchlist, value])]; setWatchlist(next); choose(value); setEntry(''); setSuggestions([]); fetch(api + '/watchlists/default', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ symbols: next }) }); socket.current?.send(JSON.stringify({ type: 'subscribe', instruments: [value] })); };
  const add = () => addSymbol(entry);
  const addPreset = (symbols: string[]) => { const next = [...new Set([...watchlist, ...symbols])]; setWatchlist(next); choose(symbols[0]); fetch(api + '/watchlists/default', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ symbols: next }) }); socket.current?.send(JSON.stringify({ type: 'subscribe', instruments: symbols })); };
  const loadFilings = async () => { try { const target = /^\d+$/.test(cik) ? cik : (await fetchJSON<RecordSet<{cik:string}[]>>(`/companies?q=${encodeURIComponent(cik)}`)).data?.[0]?.cik; if (!target) throw new Error('No matching SEC company'); setCik(target); const [items, facts] = await Promise.all([fetchJSON<RecordSet<any[]>>(`/filings/${target}`), fetchJSON<RecordSet<any>>(`/fundamentals/${target}`)]); setFilings(items); setFundamentals(facts); } catch (e) { setError(String(e)); } };
  const loadMacro = () => fetchJSON<RecordSet<any>>(`/macro/${fred}`).then(setMacro).catch(e => setError(String(e)));
  const loadTrades = () => fetchJSON<any[]>('/paper/default/trades').then(setTrades).catch(e => setError(String(e)));
  const loadPortfolio = () => fetchJSON<any>('/paper/default').then(setPortfolio).catch(e => setError(String(e)));
  const loadAlerts = () => fetchJSON<any[]>('/alerts').then(setAlerts).catch(e => setError(String(e)));
  const addAlert = () => { fetch(api + '/alerts', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ instrument_id: focus.includes(':') ? focus : `yahoo:${focus}`, direction, threshold: Number(threshold) }) }).then(async r => { if (!r.ok) throw new Error(await r.text()); setThreshold(''); loadAlerts(); }).catch(e => setError(String(e))); };
  const paper = (side: string) => { fetch(api + '/paper/default/orders', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ instrument_id: focus.includes(':') ? focus : `yahoo:${focus}`, side, quantity: 1 }) }).then(async r => { if (!r.ok) throw new Error(await r.text()); loadTrades(); loadPortfolio(); }).catch(e => setError(String(e))); };
  useEffect(() => { if (tab === 'paper') { loadTrades(); loadAlerts(); loadPortfolio(); } }, [tab]);
  useEffect(() => { if (tab === 'research') { fetchJSON<RecordSet<any>>('/fx').then(setFx).catch(() => {}); fetchJSON<RecordSet<any>>('/treasury/yields').then(setTreasury).catch(() => {}); fetchJSON<RecordSet<any>>('/releases').then(setReleases).catch(() => {}); } }, [tab]);
  const focused = quotes[focus.includes(':') ? focus : `yahoo:${focus}`];

  return <div className={'app ' + theme}>
    <aside className="rail"><div className="brand"><span className="brand-icon">M</span><span>MARKET<span className="accent">DECK</span><small>LOCAL WORKSTATION</small></span></div><nav><button className={tab === 'terminal' ? 'active' : ''} onClick={() => setTab('terminal')}><Activity size={17}/> Terminal</button><button className={tab === 'research' ? 'active' : ''} onClick={() => setTab('research')}><Newspaper size={17}/> Research</button><button className={tab === 'paper' ? 'active' : ''} onClick={() => setTab('paper')}><BarChart3 size={17}/> Paper trading</button></nav><div className="rail-bottom"><div className="section-label">THEME</div><select value={theme} onChange={e => { const value = e.target.value as Theme; setTheme(value); persistWorkspace(value, focus); }}><option value="modern">Modern dense</option><option value="classic">Classic terminal</option><option value="research">Research dashboard</option></select><div className="connection">{connected ? <Wifi size={15}/> : <WifiOff size={15}/>} {connected ? 'Connected' : 'Reconnecting'}</div><div className="tiny">Personal research · no live orders</div></div></aside>
    <main><header className="topbar"><div className="heading"><span className="eyebrow">MDK / WORKSPACE 01 / {tab.toUpperCase()}</span><h1>{tab === 'terminal' ? 'MARKET MONITOR' : tab === 'research' ? 'RESEARCH DESK' : 'PAPER PORTFOLIO'}</h1></div>{tab === 'terminal' && <label className="layout-control" title="Resize dashboard columns">COLS <input type="range" min="35" max="55" value={leftPct} onChange={e => { const v = Number(e.target.value); setLeftPct(v); persistWorkspace(theme, focus, v); }}/></label>}<div className="search"><Search size={17}/><input ref={searchInput} placeholder="ENTER SYMBOL OR COMMAND  [CTRL+K]" value={entry} onChange={e => setEntry(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') add(); if (e.key === 'Escape') setSuggestions([]); }}/><button onClick={add}>GO</button></div><span className="clock">{new Date().toLocaleDateString()}</span></header>
      <div className="market-strip"><span className="strip-label">MARKET TAPE</span>{Object.values(quotes).slice(0, 6).map(q => <button key={q.instrument_id} onClick={() => { choose(q.instrument_id.startsWith('yahoo:') ? q.symbol : q.instrument_id); setTab('terminal'); }}><strong>{q.symbol}</strong><span>{q.price.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span><em className={q.change_pct != null && q.change_pct >= 0 ? 'up' : 'down'}>{q.change_pct == null ? '—' : `${q.change_pct >= 0 ? '+' : ''}${q.change_pct.toFixed(2)}%`}</em></button>)}{!Object.keys(quotes).length && <span className="strip-empty">AWAITING MARKET FEEDS ...</span>}</div>
      {suggestions.length > 0 && <div className="search-results">{suggestions.slice(0, 8).map((s, i) => <button key={i} onClick={() => addSymbol(s.symbol)}><strong>{s.symbol}</strong><span>{s.shortname || s.longname || s.quoteType || ''}</span></button>)}</div>}
      {error && <div className="error" onClick={() => setError('')}>{error} <span>×</span></div>}
      {notice && <div className="notice" onClick={() => setNotice('')}>{notice} <span>×</span></div>}
      {tab === 'terminal' && <div className="dashboard"><section className="panel watch"><div className="panel-head"><div><span className="eyebrow">01 / MARKETS</span><h2>Watchlist</h2></div><SlidersHorizontal size={16}/></div><div className="table-wrap"><table><thead><tr><th>INSTRUMENT</th><th>LAST</th><th>CHANGE</th><th>STATE</th></tr></thead><tbody>{watchlist.map(id => { const key = id.includes(':') ? id : `yahoo:${id}`; const q = quotes[key]; return <tr key={id} onClick={() => choose(id)} className={focus === id ? 'selected' : ''}><td><strong>{id}</strong><small>{q?.source || 'Awaiting feed'} · Obs {prettyTime(q?.observed_at)} · Got {prettyTime(q?.retrieved_at)}</small></td><td>{q ? q.price.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'}</td><td className={q?.change_pct && q.change_pct >= 0 ? 'up' : 'down'}>{q?.change_pct == null ? '—' : `${q.change_pct >= 0 ? '+' : ''}${q.change_pct.toFixed(2)}%`}</td><td><Fresh value={q?.freshness}/></td></tr>; })}</tbody></table></div></section>
      <section className="panel candles"><div className="panel-head"><div><span className="eyebrow">02 / PRICE ACTION</span><h2>{focus} <span className="muted">· 15-minute candles</span></h2></div><Fresh value={candles?.freshness}/></div><div className="quote-line"><span>{focused ? focused.price.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'}</span><small>{focused?.currency || 'USD'} · {focused?.source || 'Yahoo Finance'} · Observed {prettyTime(focused?.observed_at)} · Retrieved {prettyTime(focused?.retrieved_at)}</small></div>{candles?.data?.length ? <Chart symbol={focus} data={candles.data}/> : <div className="empty">{candles ? 'Chart unavailable' : 'Loading candles…'}</div>}<div className="panel-foot">Retrieved {prettyTime(candles?.retrieved_at)} · Data may be delayed</div></section>
      <section className="panel options"><div className="panel-head"><div><span className="eyebrow">03 / DERIVATIVES</span><h2>Options chain <span className="muted">{options?.data?.expiration}</span></h2></div><Fresh value={options?.freshness}/></div><div className="table-wrap"><table><thead><tr><th>TYPE</th><th>STRIKE</th><th>LAST</th><th>IV</th><th>VOL</th></tr></thead><tbody>{options?.data?.rows?.slice(0, 60).map((o, i) => <tr key={i}><td className={o.side === 'call' ? 'up' : 'down'}>{o.side.toUpperCase()}</td><td>{o.strike}</td><td>{o.last?.toFixed(2) || '—'}</td><td>{o.iv == null ? '—' : `${(o.iv * 100).toFixed(1)}%`}</td><td>{o.volume ?? '—'}</td></tr>)}</tbody></table>{options && !options.data.rows.length && <div className="empty">No listed options for this instrument.</div>}</div><div className="panel-foot">Yahoo Finance · Retrieved {prettyTime(options?.retrieved_at)}</div></section>
      <section className="panel news"><div className="panel-head"><div><span className="eyebrow">04 / INTELLIGENCE</span><h2>Latest headlines</h2></div><Fresh value={news?.freshness}/></div><div className="aggregate">{aggregate?.probabilities ? `24h text sentiment · ${aggregate.sample_size} Yahoo headlines · +${(aggregate.probabilities.positive * 100).toFixed(0)}% / −${(aggregate.probabilities.negative * 100).toFixed(0)}% / neutral ${(aggregate.probabilities.neutral * 100).toFixed(0)}%` : '24h text sentiment · no scored headlines'}</div><div className="news-list">{news?.data?.map(n => <a href={n.url} target="_blank" rel="noreferrer" key={n.id}><div>{n.title}</div><small>{n.source} · {prettyTime(n.observed_at)} {n.sentiment && <b className={'sentiment ' + n.sentiment.label}>{n.sentiment.label}</b>}</small></a>)}{news && !news.data.length && <div className="empty">No headlines available.</div>}</div><div className="panel-foot">Text sentiment is not a price forecast · Retrieved {prettyTime(news?.retrieved_at)}</div></section>
      {focus.includes(':') && <section className="panel orderbook"><div className="panel-head"><div><span className="eyebrow">05 / MICROSTRUCTURE</span><h2>Order book</h2></div><Fresh value={book?.freshness}/></div><div className="book-columns"><div><b>BIDS</b>{book?.bids?.map((x, i) => <div key={i}>{x[0]} <span>{x[1]}</span></div>)}</div><div><b>ASKS</b>{book?.asks?.map((x, i) => <div key={i}>{x[0]} <span>{x[1]}</span></div>)}</div></div></section>}</div>}
      {tab === 'research' && <div className="research-grid"><section className="panel"><div className="panel-head"><div><span className="eyebrow">SEC / EDGAR</span><h2>Company filings</h2></div><ShieldCheck size={18}/></div><div className="form-row"><input value={cik} onChange={e => setCik(e.target.value)} placeholder="Ticker, company or CIK"/><button onClick={loadFilings}>Load filings</button></div><div className="news-list">{filings?.data?.map((f, i) => <a key={i} href={f.url} target="_blank" rel="noreferrer"><div>{f.form} · {f.company}</div><small>Filed {f.filed_at} · {f.accession}</small></a>)}<div className="empty">Set SEC_USER_AGENT in the backend environment for SEC access.</div></div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">FRED / MACRO</span><h2>Economic series</h2></div><BarChart3 size={18}/></div><div className="form-row"><input value={fred} onChange={e => setFred(e.target.value)} placeholder="FRED series ID"/><button onClick={loadMacro}>Load series</button></div><div className="table-wrap"><table><thead><tr><th>DATE</th><th>VALUE</th></tr></thead><tbody>{macro?.data?.observations?.slice(0, 50).map((x: any, i: number) => <tr key={i}><td>{x.date}</td><td>{x.value}</td></tr>)}</tbody></table></div><div className="panel-foot">Set FRED_API_KEY to access official observations.</div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">PROVENANCE</span><h2>Source health</h2></div></div>{Object.values(health).map((h: any) => <div className="health-row" key={h.source}><strong>{h.source}</strong><Fresh value={h.state}/><small>{h.last_error || prettyTime(h.last_success)}</small></div>)}</section></div>}
      {tab === 'paper' && <div className="paper-grid"><section className="panel"><div className="panel-head"><div><span className="eyebrow">SIMULATION ONLY</span><h2>Trade {focus}</h2></div></div><p>Paper fills use the currently displayed quote, freshness, 2 bps slippage and no default fee. Reference and stale quotes are rejected.</p><div className="quote-line">{focused?.price ?? 'No quote'} <Fresh value={focused?.freshness}/></div><div className="form-row"><button onClick={() => paper('buy')}>Paper buy 1</button><button onClick={() => paper('sell')}>Paper sell 1</button></div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">AUDIT TRAIL</span><h2>Trades</h2></div><a href={api + '/paper/default/export.csv'} className="export">Export CSV</a></div><div className="table-wrap"><table><thead><tr><th>INSTRUMENT</th><th>SIDE</th><th>QTY</th><th>FILL</th><th>QUOTE STATE</th></tr></thead><tbody>{trades.map(t => <tr key={t.id}><td>{t.instrument_id}</td><td>{t.side}</td><td>{t.quantity}</td><td>{t.fill_price.toFixed(4)}</td><td><Fresh value={t.quote_freshness}/></td></tr>)}</tbody></table></div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">LOCAL WORKFLOW</span><h2>Price alerts</h2></div></div><div className="form-row"><select value={direction} onChange={e => setDirection(e.target.value as 'above' | 'below')}><option value="above">Above</option><option value="below">Below</option></select><input type="number" min="0" step="any" value={threshold} onChange={e => setThreshold(e.target.value)} placeholder="Price threshold"/><button onClick={addAlert}>Add alert</button></div><div className="table-wrap"><table><thead><tr><th>INSTRUMENT</th><th>WHEN</th><th>PRICE</th><th>TRIGGERED</th></tr></thead><tbody>{alerts.map(a => <tr key={a.id}><td>{a.instrument_id}</td><td>{a.direction}</td><td>{a.threshold}</td><td>{a.triggered_at || '—'}</td></tr>)}</tbody></table></div></section></div>}
      {tab === 'research' && <div className="reference-grid"><section className="panel"><div className="panel-head"><div><span className="eyebrow">ECB / DAILY REFERENCE</span><h2>EUR exchange rates</h2></div><Fresh value={fx?.freshness}/></div><div className="table-wrap"><table><thead><tr><th>CURRENCY</th><th>PER EUR</th></tr></thead><tbody>{fx?.data?.rates?.map((r: any) => <tr key={r.currency}><td>{r.currency}</td><td>{r.rate}</td></tr>)}</tbody></table></div><div className="panel-foot">Observed {fx?.data?.observed_at || '—'} · Retrieved {prettyTime(fx?.retrieved_at)}</div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">U.S. TREASURY / DAILY REFERENCE</span><h2>Par yield curve</h2></div><Fresh value={treasury?.freshness}/></div><div className="table-wrap"><table><thead><tr><th>TENOR</th><th>YIELD %</th></tr></thead><tbody>{Object.entries(treasury?.data?.rates || {}).filter(([k]) => k.startsWith('BC_')).map(([k, v]) => <tr key={k}><td>{k.replace('BC_', '').replaceAll('_', ' ')}</td><td>{String(v)}</td></tr>)}</tbody></table></div><div className="panel-foot">Observed {treasury?.data?.observed_at || '—'} · Retrieved {prettyTime(treasury?.retrieved_at)}</div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">SEC / COMPANY FACTS</span><h2>{fundamentals?.data?.company || 'Financial facts'}</h2></div><Fresh value={fundamentals?.freshness}/></div><div className="table-wrap"><table><thead><tr><th>METRIC</th><th>VALUE</th><th>PERIOD</th></tr></thead><tbody>{fundamentals?.data?.facts?.map((f: any, i: number) => <tr key={i}><td><a href={f.url} target="_blank" rel="noreferrer">{f.label}</a></td><td>{Number(f.value).toLocaleString()} {f.unit}</td><td>{f.end}</td></tr>)}</tbody></table></div><div className="panel-foot">SEC filing-linked XBRL · Retrieved {prettyTime(fundamentals?.retrieved_at)}</div></section><section className="panel"><div className="panel-head"><div><span className="eyebrow">CURATED YAHOO PROXIES</span><h2>Cross-asset shortcuts</h2></div></div><div className="preset-list"><button onClick={() => addPreset(['GC=F', 'SI=F', 'CL=F', 'NG=F'])}>Commodity futures · gold, silver, oil, gas</button><button onClick={() => addPreset(['TLT', 'IEF', 'SHY'])}>Bond ETF proxies · long/intermediate/short</button><button onClick={() => addPreset(['EURUSD=X', 'GBPUSD=X', 'JPY=X'])}>FX market proxies · Yahoo</button></div><div className="panel-foot">Futures contracts expire; ETFs are not individual bond prices.</div></section></div>}
      {tab === 'research' && <section className="panel releases"><div className="panel-head"><div><span className="eyebrow">FRED / RELEASE CALENDAR</span><h2>Upcoming and recent releases</h2></div><Fresh value={releases?.freshness}/></div><div className="table-wrap"><table><thead><tr><th>DATE</th><th>RELEASE</th></tr></thead><tbody>{releases?.data?.release_dates?.slice(0, 50).map((r: any, i: number) => <tr key={i}><td>{r.date}</td><td>{r.release_name}</td></tr>)}</tbody></table></div><div className="panel-foot">FRED · Retrieved {prettyTime(releases?.retrieved_at)}</div></section>}
      {tab === 'paper' && portfolio && <section className="panel portfolio-summary"><div className="panel-head"><div><span className="eyebrow">PAPER PORTFOLIO / DEFAULT</span><h2>Cash {portfolio.cash.toLocaleString(undefined, {maximumFractionDigits: 2})} · Equity {portfolio.equity == null ? 'Unavailable' : portfolio.equity.toLocaleString(undefined, {maximumFractionDigits: 2})}</h2></div></div><div className="table-wrap"><table><thead><tr><th>INSTRUMENT</th><th>QUANTITY</th><th>MARK</th><th>FRESHNESS</th></tr></thead><tbody>{portfolio.positions.map((p: any) => <tr key={p.instrument_id}><td>{p.instrument_id}</td><td>{p.quantity}</td><td>{p.mark_price ?? '—'}</td><td><Fresh value={p.mark_freshness}/></td></tr>)}</tbody></table></div><div className="panel-foot">Starting cash 100,000 · Equity unavailable if any position lacks a mark · Stale marks are not executable</div></section>}
      <footer>MARKETDECK 0.2 · Free-source data may be delayed or incomplete · No investment advice · Charts by TradingView Lightweight Charts</footer>
    </main></div>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>);
