# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fund NAV (Net Asset Value) data collection and visualization system. Pulls fund data from 163 email attachments (Excel files), stores it in SQLite, serves it via a FastAPI REST API, and displays it in a React frontend. Also supports A-share index and financial futures daily/intraday data via akshare (free, no API key required).

## Commands

### Backend

```bash
# Install dependencies
pip install -r requirements.txt

# Pull new fund data from 163 email (incremental on subsequent runs)
python get_163_email.py

# Sync A-share market data (indices + futures, no token required)
python get_market_data.py

# Run data quality check and generate fund_clean.db
python data_quality_check.py

# Export data to Excel
python organize_fund_data.py

# Start API server (also starts the background scheduler)
python api.py
# API available at http://localhost:8000
# Swagger docs at http://localhost:8000/docs
```

### Frontend

```bash
cd web

# Install dependencies
npm install

# Start dev server (http://localhost:5173)
npm run dev

# Production build (outputs to web/dist/)
npm run build

# Preview production build
npm run preview
```

## Deployment (Production — Ubuntu 22.04)

The app is designed to run behind **nginx** on a single Linux server:

- **nginx** listens on port 80, serves `web/dist/` as static files, proxies `/api/` → uvicorn
- **uvicorn** runs on `127.0.0.1:8000` (not exposed publicly), managed by systemd
- **deploy/** directory contains ready-to-use config files:
  - `deploy/nginx-site.conf` — nginx server block (copy to `/etc/nginx/sites-available/`)
  - `deploy/fundata-api.service` — systemd unit (copy to `/etc/systemd/system/`)
- App is installed at `/opt/fundata/`, Python venv at `/opt/fundata/venv/`
- `.env` file at `/opt/fundata/.env` (never committed, contains email credentials)
- `fund_data.db` lives in `/opt/fundata/` and is **not** in git (gitignored)

**systemd service** reads `EnvironmentFile=/opt/fundata/.env` and starts:
```
uvicorn api:app --host 127.0.0.1 --port 8000 --workers 1
```
Use `systemctl status fundata-api` and `journalctl -u fundata-api -f` to monitor.

The system has four layers:

**Data Ingestion — Email** (`get_163_email.py` + `smart_extractor.py`): IMAP client connects to 163 email, fetches attachments incrementally by UID, extracts Excel files in-memory (BytesIO), and delegates to `smart_extractor.py` which detects and parses multiple Excel layouts (table format, key-value format, etc.). Parsed records go into `fund_data.db`. Failed extractions are logged to `extraction_failures`.

**Data Ingestion — Market** (`get_market_data.py`): Fetches A-share index daily data from Tencent QQ Finance API (direct JSONP) and CFFEX financial futures daily data from akshare (official CFFEX website). Resolves active contracts via `ak.match_main_contract`. Also supports 5-minute intraday K-lines via Sina Finance. Runs incrementally using `market_index_last_date` / `market_futures_last_date` keys in `sync_state`. No API token required.

**API Layer** (`api.py`): FastAPI app with 30+ endpoints serving from `fund_data.db`. By default, queries apply a quality filter (`apply_filter=true`) that excludes records with NAV > 5. Manual NAV entry is supported (these records have `source_id = NULL`). Paginates at 1000 rows default, 5000 max. An `APScheduler BackgroundScheduler` runs inside the `lifespan` context, triggering email sync at 12:00 & 18:00 and market sync at 11:30 & 15:15 (Asia/Shanghai). Both sync jobs use `threading.Lock` to prevent concurrent runs. Sync state is stored as key-value rows in the `sync_state` table.

**Data Ingestion — Crude Oil** (`get_crude_data.py` + `crude_api.py`): Fetches WTI (NYMEX via akshare `futures_foreign_hist("CL")`), Brent (ICE via akshare `futures_foreign_hist("OIL")`), and Shanghai SC (INE via akshare `futures_zh_daily_sina("SC0")`) daily prices. Stores in `crude_daily` table in `fund_data.db`. `crude_api.py` is a standalone FastAPI `APIRouter` (prefix `/api/crude`) mounted in `api.py` via `app.include_router()`.

**Frontend** (`web/src/`): Six-page React app with a shared `Layout` component:
- `FundList.jsx`: Dashboard with search, "立即同步" button, per-row data-issue badge, extraction failures badge (orange, shows count, opens modal with failure details), and nav link to Market page.
- `FundDetail.jsx`: Chart.js line chart for a single fund with date range controls, manual NAV entry modal (POST /api/nav), manual record deletion, manual records highlighted as orange dots on chart, and `chartjs-plugin-annotation` annotations for anomalous dates and date gaps.
- `MarketDashboard.jsx`: Index overview cards (9 A-share indices, click to see history chart), financial futures table (latest active contract per symbol), and market sync trigger.
- `FundComparison.jsx` (`/compare`): Multi-fund comparison (up to 10 funds), searchable selector, normalized-to-100 or absolute NAV chart, performance metrics table (period return, annualized return, annualized volatility, max drawdown, Sharpe ratio, monthly win rate). Uses `computeMetrics()` from `web/src/utils/metrics.js`.
- `BasisAnalysis.jsx` (`/basis`): Stock-index futures basis analysis for IF/IC/IH/IM. Shows today's contract snapshot table (当季/下季/隔季, basis, annualized basis%) and historical annualized basis% chart. Calls `/api/market/basis/quarterly` and `/api/market/basis/today`.
- `CrudeOilComparison.jsx` (`/crude`): Crude oil price comparison page — WTI/Brent (USD, left Y-axis) vs Shanghai SC (CNY, right Y-axis) dual-axis chart, latest-price summary cards, sync controls. API calls in `web/src/api/crudeApi.js`.
- `RangeScrubber.jsx`: Dual-handle date range scrubber used on chart pages. Handles are `w-4 h-4 md:w-3.5 md:h-3.5` (slightly larger on mobile for touch). Date labels use `text-[11px]`.

The API base URL is proxied via Vite to `http://127.0.0.1:8000`.

## Databases

Two SQLite databases:

- **`fund_data.db`** — raw data with full lineage. Key tables: `funds` (master, keyed on `产品代码`), `fund_nav_data` (NAV records, UNIQUE on `(产品代码, 净值日期)`), `email_sources` (audit trail), `extraction_failures` (failed attachment parses), `sync_state` (IMAP UID checkpoint + scheduler sync status), `index_daily` (A-share index OHLCV), `futures_daily` (financial futures OHLCV+OI), `index_5min` (index 5-min bars), `futures_5min` (futures 5-min bars), `crude_daily` (WTI/Brent/SC OHLCV).
- **`fund_clean.db`** — generated by `data_quality_check.py`. Filters out NAV > 5, deduplicates, and denormalizes email source fields inline.

Dates are stored as `YYYYMMDD` strings in the DB; the fund API accepts/returns `YYYY-MM-DD`. Market endpoints accept/return `YYYYMMDD` directly.

### `sync_state` keys

| key | purpose |
|-----|---------|
| `last_uid` | Last IMAP UID processed (incremental sync checkpoint) |
| `uidvalidity` | Mailbox UIDVALIDITY; change triggers full rescan |
| `sync_last_time` | ISO timestamp of most recent email sync attempt |
| `sync_last_status` | `running` / `success` / `error` (email sync) |
| `sync_last_added` | Reserved for future: count of records added |
| `sync_last_error` | Error message string on failure, empty on success (email sync) |
| `market_index_last_date` | Last successfully synced date for index data (YYYYMMDD) |
| `market_futures_last_date` | Last successfully synced date for futures data (YYYYMMDD) |
| `market_last_status` | `running` / `success` / `error` (market sync) |
| `market_last_error` | Error message on market sync failure |
| `crude_last_status` | `running` / `success` / `partial_error` / `error` (crude sync) |
| `crude_last_time` | ISO timestamp of most recent crude sync attempt |
| `crude_last_error` | Error message on crude sync failure |
| `crude_last_added` | Count of rows added in last crude sync |

## Configuration

Copy `.env.example` to `.env` and set:

```
DB_PATH=fund_data.db
CLEAN_DB_PATH=fund_clean.db
EMAIL_USER=your_email@163.com
EMAIL_PASSWORD=<IMAP auth code, not login password>
API_HOST=0.0.0.0
API_PORT=8000
MARKET_INTRADAY_MODE=0
```

- The IMAP password must be the 163 email IMAP authorization code, not the account login password.
- `MARKET_INTRADAY_MODE=1` enables 5-minute intraday polling during trading hours (9:30–11:30, 13:00–15:00). Default `0` = twice-daily snapshots only (11:30 and 15:15).
- Market data requires no registration or API token. Data sources: Tencent QQ Finance (index daily), CFFEX official website via akshare (futures daily), Sina Finance (5-min bars).
- **Note**: 中证2000 (932000.CSI) is not available via Tencent/Sina; it is silently skipped in index sync.
- **Proxy note**: If running behind a proxy (e.g., Clash in global mode), Chinese domestic data sources may be inaccessible. The code sets `NO_PROXY` for known domains, but a proxy in global mode may still block API paths. Set Clash/proxy to rule mode or add bypass rules for `proxy.finance.qq.com`, `www.cffex.com.cn`, `finance.sina.com.cn`.

## API Endpoints Summary

### Fund endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/funds` | List all funds with summary stats |
| GET | `/api/funds/search` | Search by name/code |
| GET | `/api/funds/issues` | Data issues for all funds |
| GET | `/api/funds/{fund_id}` | Single fund detail |
| GET | `/api/funds/{fund_id}/nav` | NAV time series |
| GET | `/api/funds/{fund_id}/issues` | Issues for one fund |
| GET | `/api/compare` | Multi-fund NAV comparison |

### Manual NAV CRUD
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/nav` | Create manual NAV record (`source_id=NULL`) |
| GET | `/api/nav/{id}` | Get single NAV record |
| PUT | `/api/nav/{id}` | Update NAV record |
| DELETE | `/api/nav/{id}` | Delete NAV record |

### System endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/stats` | Aggregate stats |
| GET | `/api/failures` | Extraction failure records |
| GET | `/api/sync/status` | Email sync status |
| POST | `/api/sync/trigger` | Trigger email sync |

### Crude oil endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/crude/daily` | Three-symbol combined data (WTI/BRENT/SC) for comparison chart |
| GET | `/api/crude/{ts_code}/daily` | Single symbol history (ts_code: WTI/BRENT/SC) |
| GET | `/api/crude/sync/status` | Crude sync status |
| POST | `/api/crude/sync/trigger` | Trigger crude data sync |

### Market endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/market/indices` | Latest row per index |
| GET | `/api/market/indices/{ts_code}/daily` | Index history (`date_from`, `date_to`, `limit`) |
| GET | `/api/market/futures` | Latest row per futures symbol |
| GET | `/api/market/futures/{ts_code}/daily` | Futures contract history |
| GET | `/api/market/sync/status` | Market sync status |
| POST | `/api/market/sync/trigger` | Trigger market sync |

## Key Design Decisions

- **Incremental sync**: IMAP UID + UIDVALIDITY stored in `sync_state` table; falls back to full scan if mailbox is rebuilt.
- **Scheduled sync**: `APScheduler BackgroundScheduler` integrated into `lifespan`; email sync at 12:00 & 18:00, market sync at 11:30 & 15:15 Asia/Shanghai. Each uses a separate `threading.Lock` to prevent concurrent runs. Manual triggers via POST endpoints.
- **Smart extraction**: `smart_extractor.py` handles 4+ Excel layouts with Chinese field name aliases for normalization.
- **Extraction failures**: Failed attachment parses are recorded in `extraction_failures` table. Frontend shows orange badge with count and modal detail view.
- **Dual DB pattern**: Raw DB preserves all data; clean DB is regenerated on demand from quality checks.
- **`fund_id` as canonical key**: Integer `fund_id` in `funds` table is the stable cross-database identifier; `产品代码` (product code) is the unique business key.
- **Quality filter default**: API defaults `apply_filter=true`, excluding anomalous NAV values from responses.
- **Manual NAV records**: `source_id = NULL` identifies manually entered records. FundDetail highlights them as orange dots on the chart and shows a deletable list below.
- **Issue detection**: `_compute_issues(conn, fund_id)` detects anomalous records (unit_nav > 5) and date gaps. Gap threshold = `max(median_interval × 2.5, 30 days)`. Called by `GET /api/funds/issues` (all funds) and `GET /api/funds/{fund_id}/issues`.
- **Chart annotations**: `chartjs-plugin-annotation` v3 (Chart.js v4 compatible) draws red dashed lines on anomalous dates and semi-transparent grey boxes over gap ranges.
- **Market data — active futures**: `get_active_futures_ak()` calls `ak.match_main_contract(symbol="cffex")` to get nearest active contract per symbol (IF, IC, IH, IM, T, TF, TS). Falls back to current-month contract code if akshare fails.
- **Market data — index source**: Direct Tencent QQ Finance kline API (`proxy.finance.qq.com`) with date-range query. Returns OHLCV + pct_chg + amount. Max 2000 rows per call (~8 years). 中证2000 not available, silently skipped.
- **Market data — conditional import**: `api.py` wraps `from get_market_data import ...` in a try/except so the API starts normally even if akshare is not installed. Market endpoints return empty responses in that case.
- **5-min intraday mode**: Controlled by `MARKET_INTRADAY_MODE=1` env var. Adds 5-minute APScheduler interval job; `is_trading_hours()` guard skips execution outside 9:25–11:35 and 12:55–15:05. Index 5-min uses `ak.stock_zh_a_minute` (Sina), futures 5-min uses `ak.futures_zh_minute_sina`.
- **Crude oil data**: `get_crude_data.py` + `crude_api.py` are an independent module. `crude_api.py` is an `APIRouter` mounted on `/api/crude`. Data stored in `crude_daily` table (UNIQUE on `ts_code, trade_date`). SC uses `futures_zh_daily_sina("SC0")`; WTI/Brent use `futures_foreign_hist("CL"/"OIL")`. Sync state keys: `crude_last_status`, `crude_last_time`, `crude_last_error`, `crude_last_added`. Import wrapped in try/except so API starts even if akshare is missing.
- **Fund comparison**: `/compare` route. Uses `/api/compare` endpoint (max 20 fund_ids). Frontend normalizes series to 100 at start date. Performance metrics computed by `web/src/utils/metrics.js` (period return, annualized return, volatility, max drawdown, Sharpe at 2.5% risk-free, monthly win rate).
- **Basis analysis**: `/basis` route. Calculates stock-index futures basis (spot − futures) and annualizes it (basis / futures / remaining_days × 365 × 100). Covers IF/IC/IH/IM; 当季 and 下季 contracts. Expiry = third Friday of delivery month. Data from `index_daily` joined with `futures_daily`.
- **Mobile responsiveness**: `Layout.jsx` renders a `lg:hidden sticky top-0 z-40 h-14` topbar on mobile (≥375px) instead of the old `fixed` floating hamburger. All page-level sticky headers use `sticky top-14 lg:top-0` so they stick below the 56px topbar on mobile and at y=0 on desktop. Index cards grid is `grid-cols-2 sm:grid-cols-3 lg:grid-cols-5`. Table cells use `px-3 py-3 md:px-6 md:py-4` (reduced on mobile). Content padding is `p-4 md:p-8`. Symbol tabs in BasisAnalysis use `flex-wrap` so they don't overflow at 375px.
