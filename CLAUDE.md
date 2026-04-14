# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 沟通规范（最高优先级）

**必须做**：
- 所有回复、解释、问题均使用**简体中文**，也可以使用少部分英文。
- 代码注释遵循项目现有语言（后端已有大量中文注释，前端以英文为主）

**不能做**：
- 不能在回复正文中使用 emoji 表情符号（⚠️ ✅ ❌ 等）——Windows 终端渲染可能出现乱码
- 不能在文件名、变量名、commit message 中使用非 ASCII 或非中文字符集之外的字符（避免日文/韩文乱码）
- 不能在没有用户明确要求的情况下切换到英文回复

## Project Overview

Fund NAV (Net Asset Value) data collection and visualization system. Pulls fund data from 163 email attachments (Excel files), stores it in SQLite, serves it via a FastAPI REST API, and displays it in a React frontend. Also supports A-share index and financial futures daily/intraday data via akshare (free, no API key required). Includes a Middle East conflict & crude oil news aggregator (RSS from USNI News / OilPrice.com / Al Jazeera, auto-fetched every 2 hours) and cross-validation of WTI/Brent prices against Yahoo Finance (yfinance).

## Commands

### Backend

```bash
# Install dependencies
pip install -r requirements.txt

# Import fund data from Excel files in zxdemo/ (臻选货架.xlsx + ZXdatabase.xlsx)
python get_excel_data.py

# Pull new fund data from 163 email (incremental on subsequent runs)
python get_163_email.py

# Sync A-share market data (indices + futures, no token required)
python get_market_data.py

# Sync crude oil prices (WTI/Brent/SC via akshare + yfinance cross-validation)
python get_crude_data.py

# Fetch Middle East / crude oil news from RSS feeds
python get_news_data.py

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

**Data Ingestion — Crude Oil** (`get_crude_data.py` + `crude_api.py`): Fetches WTI (NYMEX via akshare `futures_foreign_hist("CL")`), Brent (ICE via akshare `futures_foreign_hist("OIL")`), and Shanghai SC (INE via akshare `futures_zh_daily_sina("SC0")`) daily prices. Stores in `crude_daily` table in `fund_data.db`. After each sync, cross-validates WTI and Brent against Yahoo Finance (yfinance tickers `CL=F` / `BZ=F`) for the last 90 days; differences > 3% are flagged `is_verified=0` in `crude_price_cross` table. yfinance import wrapped in try/except — cross-validation skips silently if not installed. `crude_api.py` is a standalone FastAPI `APIRouter` (prefix `/api/crude`) mounted in `api.py` via `app.include_router()`.

**Data Ingestion — News** (`get_news_data.py` + `news_api.py`): Fetches RSS feeds from USNI News (full, naval/Hormuz), OilPrice.com (full, crude oil market), and Al Jazeera (keyword-filtered: Iran/Israel/Hormuz/crude/Houthi/IRGC etc.). Uses `feedparser`; deduplicates by `url UNIQUE`. Stores in `crude_news` table with `category` = `conflict` or `crude`. `news_api.py` is an independent `APIRouter` (prefix `/api/news`) mounted in `api.py`. APScheduler triggers news sync every 2 hours.

**Frontend** (`web/src/`): Six-page React app with a shared `Layout` component:
- `FundList.jsx`: Dashboard with search, three-level strategy filter dropdowns (strategy1/2/3), "立即同步" button, "导入Excel" button, per-row data-issue badge, extraction failures badge (orange), Excel conflicts badge (shows count, opens conflict modal), strategy editor floating panel (click fund name to edit strategy tags), and nav link to Market page.
- `FundDetail.jsx`: Chart.js line chart for a single fund with date range controls, manual NAV entry modal (POST /api/nav), manual record deletion, manual records highlighted as orange dots on chart, and `chartjs-plugin-annotation` annotations for anomalous dates and date gaps.
- `MarketDashboard.jsx`: Index overview cards (9 A-share indices, click to see history chart), financial futures table (latest active contract per symbol), and market sync trigger.
- `FundComparison.jsx` (`/compare`): Multi-fund comparison (up to 10 funds), searchable selector, normalized-to-100 or absolute NAV chart, performance metrics table (period return, annualized return, annualized volatility, max drawdown, Sharpe ratio, monthly win rate). Uses `computeMetrics()` from `web/src/utils/metrics.js`.
- `BasisAnalysis.jsx` (`/basis`): Stock-index futures basis analysis for IF/IC/IH/IM. Shows today's contract snapshot table (当季/下季/隔季, basis, annualized basis%) and historical annualized basis% chart. Calls `/api/market/basis/quarterly` and `/api/market/basis/today`.
- `CrudeOilComparison.jsx` (`/crude`): Crude oil price comparison page — WTI/Brent (USD, left Y-axis) vs Shanghai SC (CNY, right Y-axis) dual-axis chart, latest-price summary cards, sync controls. News section includes: (1) 今日观察 card with 24h/7d/30d stats tab, top5 high-priority headlines (hover shows English original), focus_text; (2) 航运/Hormuz 观察 card (keyword-filtered: Hormuz/tanker/Red Sea/shipping/strait); (3) full news list with 5-category filter + sort toggle (最新时间 / 相关度). "抓取新闻" triggers POST /api/news/sync/trigger. API calls in `web/src/api/crudeApi.js`.
- `RangeScrubber.jsx`: Dual-handle date range scrubber used on chart pages. Handles are `w-4 h-4 md:w-3.5 md:h-3.5` (slightly larger on mobile for touch). Date labels use `text-[11px]`.

The API base URL is proxied via Vite to `http://127.0.0.1:8000`.

## Databases

Two SQLite databases:

- **`fund_data.db`** — raw data with full lineage. Key tables: `funds` (master, keyed on `产品代码`; includes strategy1/strategy2/strategy3/is_show/setup_date/start_date fields), `fund_nav_data` (NAV records, UNIQUE on `(产品代码, 净值日期)`; source_id=NULL manual, source_id=0 Excel import, source_id>0 email), `email_sources` (audit trail; id=0 reserved as Excel import placeholder), `extraction_failures` (failed attachment parses), `excel_conflicts` (same fund+date conflicts between email and Excel data), `sync_state` (IMAP UID checkpoint + scheduler sync status), `index_daily` (A-share index OHLCV), `futures_daily` (financial futures OHLCV+OI), `index_5min` (index 5-min bars), `futures_5min` (futures 5-min bars), `crude_daily` (WTI/Brent/SC OHLCV), `crude_price_cross` (WTI/Brent cross-validation vs yfinance), `crude_news` (RSS news articles).
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
| `news_last_status` | `running` / `success` / `error` (news RSS sync) |
| `news_last_time` | ISO timestamp of most recent news sync attempt |
| `news_last_error` | Error message on news sync failure |
| `news_last_added` | Count of articles added in last news sync |

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

### 完整环境变量清单

| 变量 | 必须 | 说明 |
|------|-----|------|
| `DB_PATH` | ✅ | 主数据库路径，默认 `fund_data.db` |
| `CLEAN_DB_PATH` | ✅ | 清洁库路径，默认 `fund_clean.db` |
| `EMAIL_USER` | ✅ | 163 邮箱地址 |
| `EMAIL_PASSWORD` | ✅ | 163 IMAP 授权码（非登录密码） |
| `API_HOST` | ✅ | uvicorn 监听地址，生产用 `0.0.0.0` |
| `API_PORT` | ✅ | uvicorn 端口，默认 `8000` |
| `MARKET_INTRADAY_MODE` | ⚪ | `1` 启用5分钟内盘轮询，默认 `0` |
| `AISSTREAM_API_KEY` | ⚪ | AISStream.io WebSocket token，缺失时 `/api/hormuz` 同步静默跳过 |

缺少 Required（✅）变量时 API 启动会报错；缺少 Optional（⚪）变量时对应模块降级静默跳过。

## API Endpoints Summary

### Fund endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/funds` | List all funds; supports `strategy1`, `strategy2`, `strategy3`, `is_show` filter params |
| GET | `/api/funds/search` | Search by name/code |
| GET | `/api/funds/issues` | Data issues for all funds |
| GET | `/api/funds/{fund_id}` | Single fund detail (includes strategy1/2/3/is_show/setup_date/start_date) |
| GET | `/api/funds/{fund_id}/nav` | NAV time series |
| GET | `/api/funds/{fund_id}/issues` | Issues for one fund |
| PUT | `/api/funds/{fund_id}/strategy` | Update fund strategy tags (strategy1/2/3) |
| GET | `/api/compare` | Multi-fund NAV comparison |

### Excel import endpoints
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/excel/import` | Trigger Excel import from `zxdemo/` (background task) |
| GET | `/api/excel/conflicts` | List email vs Excel NAV conflicts |

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
| GET | `/api/crude/cross` | Price cross-validation results (akshare vs yfinance, last 60 rows) |
| GET | `/api/crude/sync/status` | Crude sync status |
| POST | `/api/crude/sync/trigger` | Trigger crude data sync |

### News endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/news` | News list (query: `category=conflict\|crude\|shipping\|official_west\|official_iran`, `limit`, `offset`, `sort=time\|relevance`) |
| GET | `/api/news/summary` | 今日观察摘要：last_24h_count, by_category (24h/7d/30d), top5, focus_text |
| GET | `/api/news/hormuz` | Hormuz / 航运观察新闻（关键词：Hormuz/tanker/Red Sea/shipping/strait，按 published_at DESC） |
| GET | `/api/news/sources` | News RSS sync status per source |
| GET | `/api/news/sync/status` | News RSS sync status |
| POST | `/api/news/sync/trigger` | Trigger news RSS fetch |

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
- **Scheduled sync**: `APScheduler BackgroundScheduler` integrated into `lifespan`; email sync at 12:00 & 18:00, market sync at 11:30 & 15:15, crude sync at 15:20, news RSS sync every 2 hours (Asia/Shanghai). Each uses a separate `threading.Lock` to prevent concurrent runs. Manual triggers via POST endpoints.
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
- **Crude oil data**: `get_crude_data.py` + `crude_api.py` are an independent module. `crude_api.py` is an `APIRouter` mounted on `/api/crude`. Data stored in `crude_daily` table (UNIQUE on `ts_code, trade_date`). SC uses `futures_zh_daily_sina("SC0")`; WTI/Brent use `futures_foreign_hist("CL"/"OIL")`. After each sync, cross-validates WTI/Brent against yfinance (`CL=F`/`BZ=F`, 90-day lookback); results in `crude_price_cross` table (`is_verified=0` when diff > 3%). Cross-validation skips silently if yfinance not installed. Sync state keys: `crude_last_status`, `crude_last_time`, `crude_last_error`, `crude_last_added`. Import wrapped in try/except so API starts even if akshare is missing.
- **News aggregation**: `get_news_data.py` + `news_api.py` are an independent module. `news_api.py` is an `APIRouter` mounted on `/api/news`. `feedparser` fetches from 10 RSS sources (USNI News, OilPrice.com, Al Jazeera, IAEA, Iran International, White House, State Dept, The National, Reuters Energy, Shipping & Hormuz). Articles stored in `crude_news` table (UNIQUE on `url`). Category: `conflict` / `shipping` / `crude` / `official_west` / `official_iran`. Priority 1–10 computed from `_source_weight` + `_topic_score`. Titles with priority ≤ 4 are auto-translated to Chinese via `deep-translator` (stored in `title_zh`; NULL=not attempted, ""=failed). APScheduler fires every 2 hours. Key endpoints: `GET /api/news` (supports `sort=time|relevance`), `GET /api/news/summary` (24h/7d/30d category stats + top5), `GET /api/news/hormuz` (keyword-filtered: Hormuz/tanker/Red Sea/shipping/strait). Sync state keys: `news_last_status`, `news_last_time`, `news_last_error`, `news_last_added`. Import wrapped in try/except so API starts if feedparser is missing.
- **Price cross-validation**: `_sync_cross_validate()` in `get_crude_data.py`. Compares akshare `close` vs yfinance `Close` (adj) for WTI/BRENT. Tolerance `CROSS_DIFF_THRESHOLD = 3.0%`. Results in `crude_price_cross` (`close_primary` = akshare, `close_alt` = yfinance). Exposed via `GET /api/crude/cross`.
- **Fund comparison**: `/compare` route. Uses `/api/compare` endpoint (max 20 fund_ids). Frontend normalizes series to 100 at start date. Performance metrics computed by `web/src/utils/metrics.js` (period return, annualized return, volatility, max drawdown, Sharpe at 2.5% risk-free, monthly win rate).
- **Basis analysis**: `/basis` route. Calculates stock-index futures basis (spot − futures) and annualizes it (basis / futures / remaining_days × 365 × 100). Covers IF/IC/IH/IM; 当季 and 下季 contracts. Expiry = third Friday of delivery month. Data from `index_daily` joined with `futures_daily`.
- **Mobile responsiveness**: `Layout.jsx` renders a `lg:hidden sticky top-0 z-40 h-14` topbar on mobile (≥375px) instead of the old `fixed` floating hamburger. All page-level sticky headers use `sticky top-14 lg:top-0` so they stick below the 56px topbar on mobile and at y=0 on desktop. Index cards grid is `grid-cols-2 sm:grid-cols-3 lg:grid-cols-5`. Table cells use `px-3 py-3 md:px-6 md:py-4` (reduced on mobile). Content padding is `p-4 md:p-8`. Symbol tabs in BasisAnalysis use `flex-wrap` so they don't overflow at 375px.
- **Excel import** (`get_excel_data.py`): Reads `zxdemo/臻选货架.xlsx` (fund master data: strategy tags, setup date, is_show) and `zxdemo/ZXdatabase.xlsx` (NAV data, one sheet per fund). Only imports funds whose Code_Id appears in 臻选货架 (whitelist filter). Conflict detection: if same fund+date already has email data (source_id IS NOT NULL) and values differ by >1e-6, records to `excel_conflicts` table and overwrites with Excel value. After import, recomputes adjusted NAV via `compute_adjusted_nav`. Triggered via `POST /api/excel/import` or `python get_excel_data.py`.
- **source_id semantics**: `NULL` = manually entered record; `0` = Excel import (placeholder row with id=0 inserted into `email_sources` on first import); positive integer = email source id. `email_sources.id=0` is a reserved placeholder — never a real email.
- **Strategy tag system**: Three-level fixed classification stored in `funds` table. strategy1/strategy2 are single-select TEXT; strategy3 is comma-separated TEXT (multi-select, 24 fixed values). Frontend enums hardcoded in `FundList.jsx`. SQL filter for strategy3 uses `(',' || strategy3 || ',') LIKE '%,value,%'`. Old free-form tag system (fund_tags/fund_tag_assignments tables) has been removed from schema creation but may still exist in older DBs.
- **Excel conflicts**: `excel_conflicts` table records cases where email-sourced NAV and Excel NAV differ for the same fund+date. Exposed via `GET /api/excel/conflicts`. Frontend shows conflict count badge and modal in FundList.

## Harness 协作协议

本项目配置了一套 AI agent 协作体系，以下约定在每次 session 中均有效。

### Agent 体系

三个项目专属 agent 定义在 `.claude/agents/`（非全局 `~/.claude/agents/`），通过对话意图触发。

| Agent | 触发时机 | 输出文件 |
|-------|---------|---------|
| `backend-evaluator-agent` | `api.py` / `*_api.py` / `get_*.py` 被修改后 | `.claude/evals/latest-backend.md` |
| `ui-evaluator-agent` | `web/src/` 被修改后，或用户明确要求 | `.claude/evals/latest-ui.md` |
| `data-source-agent` | 需要接入新外部数据源时 | `.claude/evals/latest-datasource.md` |

**必须做**：
- 调用 evaluator agent 前，先读取对应的 `latest-*.md`，在报告中注明上次问题是否已解决
- Evaluator agent 必须在回复前将报告写入文件（防止上下文压缩丢失评估结果）

**不能做**：
- 不能因为"只是小改动"就跳过写 evals 文件
- 不能在 evaluator agent 中直接修改业务代码（只评估，不修复）
- 不能为无关数据接入的功能请求调用 `data-source-agent`

### Eval 文件读取协议

每次新 session 开始处理后端或前端任务时：
1. 读取 `.claude/evals/latest-backend.md`（了解当前测试覆盖率和已知问题）
2. 如果涉及前端，读取 `.claude/evals/latest-ui.md`
3. 在实现计划中标注本次会解决哪些"下一轮迭代目标"

### Hook 体系

项目 hook 只有两条（`.claude/settings.json`），均为项目专属：
- **PostToolUse / Python 语法检查**：每次编辑 `.py` 文件后自动运行 `py_compile`
- **Stop / pytest**：每次 response 结束时运行 `pytest tests/ -q`

**不能做**：不能把 ECC 全局 hook 复制到项目 `settings.json`——ECC 已通过 `~/.claude/settings.json` 全局加载，重复会导致每条 hook 执行两次。

## Testing

### 运行测试

```bash
cd fundata_new
python -m pytest tests/ -v --tb=short                        # 运行全部
python -m pytest tests/ --cov=api --cov-report=term-missing  # 带覆盖率
```

### 测试架构

- 框架：pytest + FastAPI TestClient
- 隔离：每个测试使用独立的内存 SQLite（`:memory:`），与 `fund_data.db` 完全隔离
- 注入：通过 `app.dependency_overrides[get_db]` 替换 DB 连接

### Fixtures（`tests/conftest.py`）

| Fixture | 说明 |
|---------|------|
| `mem_db` | 空内存库（含完整 schema） |
| `mem_db_with_data` | 预置 F001/F002 两条基金 |
| `client` | 注入空库的 TestClient |
| `client_with_data` | 注入预置数据的 TestClient |

`conftest.py` 中的 schema 必须与 `api.py` 的 `_init_db_schema()` 保持同步；添加新表时两处同步更新。

### 覆盖率目标与现状

- 目标：≥80%（全局规则要求）
- 当前：41%（`api.py`），主要缺口：`market` / `crude` / `news` 端点未覆盖
- 待补充：`tests/test_market_api.py`、`tests/test_crude_api.py`、`tests/test_news_api.py`

### 必须做 / 不能做

**必须做**：
- 新增 API 端点时在 `tests/` 中同步添加对应测试用例
- 新增数据表时在 `conftest.py` 的 `_create_schema()` 中同步添加 `CREATE TABLE`

**不能做**：
- 不能在测试中直接读写 `fund_data.db`（必须通过 `mem_db` fixture）
- 不能修改测试断言来让失败的测试通过（测试失败时修复实现，不改断言）
