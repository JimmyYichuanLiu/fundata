#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fund NAV Visualization System - Backend API
FastAPI application exposing CRUD + search endpoints for fund_data.db
"""

# =============================================================================
# Section 1: Imports
# =============================================================================
import json
import hashlib
import logging
import math
import os
from pathlib import Path
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date as _date, datetime, timedelta
from typing import Dict, List, Optional

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import BackgroundTasks, Depends, FastAPI, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from admin_auth import allowed_origins, configure_auth, initialize_auth, readonly
from fund_store import initialize_database, normalize_nav_date, recalculate_adj_nav
from sync_service import run_email_sync, get_sync_summary, SyncError
from get_163_email import connect_and_fetch_email
from pydantic import BaseModel, field_validator

# =============================================================================
# Section 2: Configuration
# =============================================================================
load_dotenv()

DB_PATH: str = str(Path(os.getenv("DB_PATH", str(Path(__file__).parent / "fund_data.db"))).resolve())
API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
API_PORT: int = int(os.getenv("API_PORT", "8000"))
_INTRADAY_MODE: bool = os.getenv("MARKET_INTRADAY_MODE", "0").strip() == "1"

# Mapping from stock-index futures symbol to underlying index ts_code
# (bond futures IF/IC/IH/IM are the only ones with a spot index equivalent)
FUTURES_TO_INDEX = {
    "IF": "000300.SH",
    "IC": "000905.SH",
    "IH": "000016.SH",
    "IM": "000852.SH",
}

# Quarterly delivery months for CFFEX stock-index futures
_QUARTERLY_MONTHS = {3, 6, 9, 12}

# Roll "当季" label to the next contract when remaining calendar days < this threshold.
# Near expiry the annualised-basis formula amplifies small moves (factor = 365/remaining_days),
# so we advance the label window ~3 weeks before expiry to keep the chart stable.
BASIS_ROLL_THRESHOLD = 21


def _third_friday(year: int, month: int) -> _date:
    """Third Friday of year/month — CFFEX stock-index futures expiry day."""
    first = _date(year, month, 1)
    days = (4 - first.weekday()) % 7      # days until first Friday (Mon=0, Fri=4)
    return first + timedelta(days=days + 14)  # + 2 weeks = 3rd Friday

# 原油数据模块（独立，失败不影响主 API）
try:
    from crude_api import crude_router as _crude_router, _run_crude_sync
    _CRUDE_ENABLED = True
except ImportError:
    _crude_router = None
    _run_crude_sync = None
    _CRUDE_ENABLED = False

# 新闻模块（独立，失败不影响主 API）
try:
    from news_api import news_router as _news_router, _run_news_sync as _run_news_sync_bg
    _NEWS_ENABLED = True
except ImportError:
    _news_router = None
    _run_news_sync_bg = None
    _NEWS_ENABLED = False

# 霍尔木兹 AIS 模块（独立，失败不影响主 API）
try:
    from hormuz_api import hormuz_router as _hormuz_router, _run_ais_sync as _run_ais_sync_bg
    _HORMUZ_ENABLED = True
except ImportError:
    _hormuz_router = None
    _run_ais_sync_bg = None
    _HORMUZ_ENABLED = False

try:
    from get_market_data import (
        connect_and_fetch_market as _connect_and_fetch_market,
        connect_and_fetch_market_5min as _connect_and_fetch_market_5min,
        connect_and_fetch_realtime as _connect_and_fetch_realtime,
        INDEX_NAMES as _INDEX_NAMES,
    )
    _MARKET_ENABLED = True
except ImportError:
    _MARKET_ENABLED = False
    _connect_and_fetch_market = None
    _connect_and_fetch_market_5min = None
    _connect_and_fetch_realtime = None
    _INDEX_NAMES = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fund_api")

# =============================================================================
# Section 3: FastAPI App + CORS
# =============================================================================

def _init_db_schema():
    """Ensure the post-migration English-column schema exists.
    Creates missing tables/indexes; does NOT rename columns (that is
    db_schema_migrate.py's job).  Safe to call on an already-migrated DB."""
    migrated = initialize_database(DB_PATH)
    migrated.close()
    with _get_raw_conn() as conn:
        initialize_auth(conn)
        # funds — English columns (post-migration schema)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS funds (
                fund_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code       TEXT NOT NULL UNIQUE,
                fund_name       TEXT,
                created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                benchmark_index TEXT DEFAULT NULL,
                strategy_l1     TEXT DEFAULT NULL,
                strategy_l2     TEXT DEFAULT NULL,
                strategy_l3     TEXT DEFAULT NULL,
                manager         TEXT DEFAULT NULL,
                custodian       TEXT DEFAULT NULL,
                inception_date  TEXT DEFAULT NULL,
                start_date      TEXT DEFAULT NULL,
                display         TEXT
            )
        ''')

        # fund_nav_data — English columns (post-migration schema)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS fund_nav_data (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_id     INTEGER,
                fund_code   TEXT,
                fund_name   TEXT,
                nav_date    TEXT NOT NULL,
                unit_nav    REAL NOT NULL,
                accum_nav   REAL,
                "录入时间"  DATETIME DEFAULT CURRENT_TIMESTAMP,
                source_id   INTEGER,
                adj_nav     REAL,
                data_source TEXT,
                UNIQUE(fund_code, nav_date),
                FOREIGN KEY (fund_id) REFERENCES funds(fund_id)
            )
        ''')

        # Tag tables
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fund_tags (
                tag_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                tag_name  TEXT NOT NULL UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fund_tag_assignments (
                fund_id INTEGER NOT NULL REFERENCES funds(fund_id) ON DELETE CASCADE,
                tag_id  INTEGER NOT NULL REFERENCES fund_tags(tag_id) ON DELETE CASCADE,
                PRIMARY KEY (fund_id, tag_id)
            )
        """)

        # Indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_fund_id   ON fund_nav_data(fund_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_fund_code ON fund_nav_data(fund_code)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_nav_date  ON fund_nav_data(nav_date)')

        _ensure_portfolio_tables(conn)

    logger.info("Database schema initialised at %s", DB_PATH)


_scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_db_schema()
    if os.getenv('FUNDATA_SCHEDULER_ENABLED', '1') == '0':
        yield
        return
    _scheduler.add_job(_run_sync, "cron", hour="12,18", minute=0, id='email_sync', replace_existing=True)
    if _MARKET_ENABLED:
        # Daily snapshots: midday (11:30) and post-market close (15:15)
        _scheduler.add_job(_run_market_sync, "cron", hour=11, minute=30)
        _scheduler.add_job(_run_market_sync, "cron", hour=15, minute=15)
        # Real-time snapshot every 5 minutes during trading hours
        _scheduler.add_job(_run_realtime_sync, "interval", minutes=5)
        if _INTRADAY_MODE:
            # Optional 5-minute intraday K-line polling during trading hours
            _scheduler.add_job(_run_market_5min_sync, "interval", minutes=5)
    if _CRUDE_ENABLED:
        # 原油数据：收盘后 15:20 更新
        # _scheduler.add_job(_run_crude_sync, "cron", hour=15, minute=20)
        pass
    if _NEWS_ENABLED:
        # 新闻同步：每 2 小时抓取一次 RSS
        # _scheduler.add_job(_run_news_sync_bg, "interval", hours=2)
        pass
    if _HORMUZ_ENABLED:
        # AIS 采集：每 30 分钟一次
        # _scheduler.add_job(_run_ais_sync_bg, "interval", minutes=30)
        pass
    _scheduler.start()
    logger.info("Scheduler started")
    yield
    _scheduler.shutdown()


app = FastAPI(
    title="Fund NAV Visualization API",
    description="Backend API for fund net asset value visualization system",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
configure_auth(app, lambda: DB_PATH)

# 挂载原油路由（独立模块）
if _CRUDE_ENABLED:
    # app.include_router(_crude_router)
    pass

# 挂载新闻路由（独立模块）
if _NEWS_ENABLED:
    # app.include_router(_news_router)
    pass

# 挂载霍尔木兹路由（独立模块）
if _HORMUZ_ENABLED:
    # app.include_router(_hormuz_router)
    pass

# =============================================================================
# Section 4: Pydantic Models
# =============================================================================

class NavRecord(BaseModel):
    id: int
    fund_id: int
    product_name: Optional[str]
    product_code: str
    nav_date: str                      # YYYY-MM-DD
    unit_nav: float
    accumulated_nav: Optional[float]
    adjusted_nav: Optional[float]
    insert_time: Optional[str]
    source_id: Optional[int]
    data_source: Optional[str] = None
    adj_nav: Optional[float] = None
    adj_nav_reason: Optional[str] = None


class FundSummary(BaseModel):
    fund_id: int
    product_code: str
    product_name: Optional[str]
    first_entry_time: Optional[str]


class FundDetail(FundSummary):
    record_count: int
    earliest_date: Optional[str]       # YYYY-MM-DD
    latest_date: Optional[str]
    latest_nav: Optional[float]
    anomalous_count: int = 0
    benchmark_index: Optional[str] = None
    strategy_l1: Optional[str] = None
    strategy_l2: Optional[str] = None
    sources: List[str] = []
    latest_nav_date: Optional[str] = None


class StrategyUpdateRequest(BaseModel):
    strategy_l1: Optional[str] = None
    strategy_l2: Optional[str] = None


class FundIssue(BaseModel):
    anomalous: List[dict]   # [{nav_date, unit_nav}]
    gaps: List[dict]        # [{from_date, to_date, gap_days}]


class AllIssuesResponse(BaseModel):
    issues: Dict[int, FundIssue]


class FundListResponse(BaseModel):
    total: int
    items: List[FundDetail]


class FundSearchResponse(BaseModel):
    total: int
    items: List[FundSummary]


class NavListResponse(BaseModel):
    total: int
    fund_id: int
    items: List[NavRecord]


class NavCreateRequest(BaseModel):
    product_code: str
    product_name: Optional[str] = None
    nav_date: str
    unit_nav: float
    accumulated_nav: Optional[float] = None

    @field_validator("nav_date")
    @classmethod
    def validate_nav_date(cls, v: str) -> str:
        import re
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError("nav_date must be in YYYY-MM-DD format")
        if normalize_nav_date(v) is None:
            raise ValueError('nav_date must be a real calendar date')
        return v

    @field_validator("unit_nav", "accumulated_nav")
    @classmethod
    def validate_unit_nav(cls, v: float) -> float:
        if v is None:
            return v
        if not math.isfinite(v) or v <= 0:
            raise ValueError("unit_nav must be greater than 0")
        return v


class NavUpdateRequest(BaseModel):
    product_name: Optional[str] = None
    nav_date: Optional[str] = None
    unit_nav: Optional[float] = None
    accumulated_nav: Optional[float] = None

    @field_validator("nav_date", mode="before")
    @classmethod
    def validate_nav_date(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        import re
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError("nav_date must be in YYYY-MM-DD format")
        if normalize_nav_date(v) is None:
            raise ValueError('nav_date must be a real calendar date')
        return v

    @field_validator("unit_nav", "accumulated_nav")
    @classmethod
    def validate_unit_nav(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        if not math.isfinite(v) or v <= 0:
            raise ValueError("unit_nav must be greater than 0")
        return v


class NavDataPoint(BaseModel):
    date: str
    nav: float
    accumulated_nav: Optional[float]
    adj_nav: Optional[float] = None


class FundNavSeries(BaseModel):
    fund_id: int
    product_code: str
    product_name: Optional[str]
    series: List[NavDataPoint]


class CompareResponse(BaseModel):
    funds: Dict[int, FundNavSeries]


class TagCreate(BaseModel):
    tag_name: str


class TagResponse(BaseModel):
    tag_id: int
    tag_name: str


# =============================================================================
# Section 5: Custom Exception
# =============================================================================

class NavAPIError(Exception):
    def __init__(self, status_code: int, detail: str, code: str):
        self.status_code = status_code
        self.detail = detail
        self.code = code
        super().__init__(detail)


# =============================================================================
# Section 6: Exception Handlers
# =============================================================================

@app.exception_handler(NavAPIError)
async def nav_api_error_handler(request: Request, exc: NavAPIError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.code, "detail": exc.detail},
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = exc.errors()
    messages = [f"{'.'.join(str(l) for l in e['loc'])}: {e['msg']}" for e in errors]
    return JSONResponse(
        status_code=422,
        content={"error": "VALIDATION_ERROR", "detail": "; ".join(messages)},
    )


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"error": "INTERNAL_ERROR", "detail": "An unexpected error occurred"},
    )


# =============================================================================
# Section 7: Database Utility Functions
# =============================================================================

@contextmanager
def _get_raw_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_db():
    """FastAPI Depends target."""
    with _get_raw_conn() as conn:
        yield conn


def get_or_create_fund_id(conn, product_code: str, product_name: Optional[str] = None) -> int:
    """Get existing fund_id or create a new funds record."""
    cursor = conn.cursor()
    cursor.execute("SELECT fund_id FROM funds WHERE fund_code = ?", (product_code,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO funds (fund_code, fund_name) VALUES (?, ?)",
        (product_code, product_name),
    )
    conn.commit()
    return cursor.lastrowid


def db_date_to_api(s: Optional[str]) -> Optional[str]:
    """Convert YYYYMMDD → YYYY-MM-DD. Returns None for empty/None input.
    Also handles YYYY年MM月DD日 (Chinese date format) as a fallback.
    """
    if not s:
        return s
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if len(s) == 10 and s[4] == '-':
        return s  # already YYYY-MM-DD
    # Try YYYY年MM月DD日 Chinese format
    try:
        from datetime import datetime
        dt = datetime.strptime(s, "%Y年%m月%d日")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    return s


def api_date_to_db(s: str) -> str:
    """Convert YYYY-MM-DD → YYYYMMDD."""
    return s.replace("-", "")


def nav_row_to_model(row) -> NavRecord:
    try:
        unit_nav = float(row["unit_nav"])
    except (TypeError, ValueError):
        unit_nav = 0.0
    try:
        accumulated_nav = float(row["accum_nav"]) if row["accum_nav"] is not None else None
    except (TypeError, ValueError):
        accumulated_nav = None
    try:
        adjusted_nav = float(row["adj_nav"]) if row["adj_nav"] is not None else None
    except (TypeError, ValueError):
        adjusted_nav = None
    return NavRecord(
        id=row["id"],
        fund_id=row["fund_id"],
        product_name=row["fund_name"],
        product_code=row["fund_code"],
        nav_date=row["nav_date"],  # already YYYY-MM-DD in DB
        unit_nav=unit_nav,
        accumulated_nav=accumulated_nav,
        adjusted_nav=adjusted_nav,
        insert_time=row["录入时间"],
        source_id=row["source_id"],
        data_source=row['data_source'] if 'data_source' in row.keys() else None,
        adj_nav=adjusted_nav,
        adj_nav_reason=row['adj_nav_reason'] if 'adj_nav_reason' in row.keys() else None,
    )


def quality_filter_sql(apply: bool) -> str:
    """Return SQL AND clause for quality filter, or empty string."""
    if not apply:
        return ""
    return " AND unit_nav <= 5 AND (accum_nav IS NULL OR accum_nav <= 5)"


# ── Sync state helpers ──────────────────────────────────────────────────────

def _get_sync_status() -> dict:
    with _get_raw_conn() as conn:
        result = get_sync_summary(conn)
    # The public summary never carries subjects, senders, or internal error text.
    result.pop('last_error', None)
    result.update(sync_last_time=result['last_attempt_time'], sync_last_status=result['last_status'],
                  sync_last_added=result['last_added'], sync_last_error=None)
    job = _scheduler.get_job('email_sync') if _scheduler.running else None
    result['next_scheduled_at'] = job.next_run_time.isoformat() if job and job.next_run_time else None
    return result


def _set_sync_key(key: str, value: str):
    with _get_raw_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO sync_state(key, value) VALUES(?,?)", (key, value))


_sync_lock = threading.Lock()


def _run_sync(trigger='scheduled', retry_failure_id=None):
    if not _sync_lock.acquire(blocking=False):
        return  # already running, skip
    try:
        email_user = os.getenv("EMAIL_USER", "")
        email_pwd  = os.getenv("EMAIL_PASSWORD", "")
        return run_email_sync(email_user, email_pwd, DB_PATH, trigger=trigger, retry_failure_id=retry_failure_id)
    except SyncError as e:
        logger.error('Email sync failed: %s', e)
    finally:
        _sync_lock.release()


_market_sync_lock = threading.Lock()


def _run_market_sync():
    if not _market_sync_lock.acquire(blocking=False):
        return
    try:
        _set_sync_key("market_last_status", "running")
        _set_sync_key("market_last_error", "")
        if not _MARKET_ENABLED:
            raise ImportError("get_market_data module not available (akshare not installed?)")
        _connect_and_fetch_market(DB_PATH)
        _set_sync_key("market_last_status", "success")
        _set_sync_key("market_last_error", "")
    except Exception as e:
        _set_sync_key("market_last_status", "error")
        _set_sync_key("market_last_error", str(e))
    finally:
        _market_sync_lock.release()


_market_5min_lock = threading.Lock()


def _run_market_5min_sync():
    if not _market_5min_lock.acquire(blocking=False):
        return
    try:
        if _MARKET_ENABLED:
            _connect_and_fetch_market_5min(DB_PATH)
    except Exception as e:
        logger.warning("Market 5-min sync error: %s", e)
    finally:
        _market_5min_lock.release()


_realtime_lock = threading.Lock()


def _run_realtime_sync():
    if not _realtime_lock.acquire(blocking=False):
        return
    try:
        if _MARKET_ENABLED:
            _connect_and_fetch_realtime(DB_PATH)
    except Exception as e:
        logger.warning("Realtime sync error: %s", e)
    finally:
        _realtime_lock.release()


def _compute_issues(conn, fund_id: int) -> dict:
    rows = conn.execute(
        """SELECT nav_date, unit_nav FROM valid_fund_nav
           WHERE fund_id=? AND nav_date IS NOT NULL AND LENGTH(nav_date) = 10
           ORDER BY nav_date ASC""",
        (fund_id,)
    ).fetchall()
    anomalous = []
    for r in rows:
        if r[1] is None:
            continue
        try:
            nav_val = float(str(r[1]).replace(",", ""))
        except (ValueError, TypeError):
            continue
        if nav_val > 5:
            anomalous.append({"nav_date": r[0], "unit_nav": nav_val})
    gaps = []
    dates = [r[0] for r in rows]
    if len(dates) >= 3:
        try:
            ivs = [
                (datetime.strptime(dates[i+1], "%Y-%m-%d") - datetime.strptime(dates[i], "%Y-%m-%d")).days
                for i in range(len(dates) - 1)
            ]
            median = sorted(ivs)[len(ivs) // 2]
            threshold = max(median * 2.5, 30)
            for i, gap_days in enumerate(ivs):
                if gap_days > threshold:
                    gaps.append({
                        "from_date": dates[i],
                        "to_date": dates[i + 1],
                        "gap_days": gap_days,
                    })
        except Exception as e:
            logger.warning("Gap detection failed for fund %s: %s", fund_id, e)
    return {"anomalous": anomalous, "gaps": gaps}


# =============================================================================
# Section 8: Route Handlers
# =============================================================================

# --- Health check -----------------------------------------------------------

@app.get("/api/health", tags=["system"])
def health_check():
    try:
        with _get_raw_conn() as conn:
            conn.execute("SELECT 1")
        return {"status": "ok"}
    except Exception as exc:
        logger.error("DB health check failed: %s", exc)
        raise NavAPIError(503, "Database unavailable", "DB_UNAVAILABLE")


# --- Extraction failures -----------------------------------------------------

@app.get("/api/failures", tags=["system"])
def list_failures(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    conn: sqlite3.Connection = Depends(get_db),
):
    total = conn.execute("SELECT COUNT(*) FROM extraction_failures").fetchone()[0]
    rows = conn.execute(
        "SELECT * FROM extraction_failures ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)
    ).fetchall()
    items = [dict(r) for r in rows]
    for item in items:
        item['retryable'] = bool(item.get('mailbox_uid') and item.get('uidvalidity') and item.get('status') != 'resolved')
        item['retry_reason'] = None if item['retryable'] else ('已恢复' if item.get('status') == 'resolved' else '历史记录缺少邮箱 UID，无法自动定位；需重新导入原始邮件')
    return {"total": total, "items": items}


# --- Global stats -----------------------------------------------------------

@app.get("/api/stats", tags=["system"])
def get_stats(conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        "SELECT COUNT(*) AS total_records FROM valid_fund_nav"
    ).fetchone()
    total_records: int = row["total_records"]

    row2 = conn.execute("SELECT COUNT(DISTINCT fund_id) AS total_funds FROM valid_fund_nav").fetchone()
    total_funds: int = row2["total_funds"]

    row3 = conn.execute(
        "SELECT COUNT(*) AS manual_records FROM valid_fund_nav WHERE data_source = 'manual'"
    ).fetchone()
    manual_records: int = row3["manual_records"]

    return {
        "total_records": total_records,
        "total_funds": total_funds,
        "manual_records": manual_records,
        "valid_funds": total_funds,
        "latest_nav_date": conn.execute('SELECT MAX(nav_date) FROM valid_fund_nav').fetchone()[0],
        "quarantined_records": conn.execute("SELECT COUNT(*) FROM fund_nav_data WHERE quality_status != 'valid'").fetchone()[0],
    }


# --- Fund list --------------------------------------------------------------

@app.get("/api/funds", response_model=FundListResponse, tags=["funds"])
def list_funds(
    strategy_l1: Optional[str] = Query(None),
    strategy_l2: Optional[str] = Query(None),
    apply_filter: bool = Query(True, description="Exclude unit_nav > 5 from record_count"),
    source: str = Query('all', pattern='^(all|email|zx_excel|manual)$'),
    conn: sqlite3.Connection = Depends(get_db),
):
    conditions = []
    params: list = []
    if source != 'all':
        conditions.append('EXISTS (SELECT 1 FROM valid_fund_nav sn WHERE sn.fund_id=f.fund_id AND sn.data_source=?)')
        params.append(source)
    conditions.append('EXISTS (SELECT 1 FROM valid_fund_nav vn WHERE vn.fund_id=f.fund_id)')
    if strategy_l1 is not None:
        conditions.append("f.strategy_l1 = ?")
        params.append(strategy_l1)
    if strategy_l2 is not None:
        conditions.append("f.strategy_l2 = ?")
        params.append(strategy_l2)

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    nav_filter = "AND n.unit_nav <= 5" if apply_filter else ""

    rows = conn.execute(
        f"""
        SELECT
            f.fund_id,
            f.fund_code,
            f.fund_name,
            f.created_at,
            f.strategy_l1,
            f.strategy_l2,
            COUNT(n.id)                                AS record_count,
            MIN(n.nav_date)                            AS earliest_date,
            MAX(n.nav_date)                            AS latest_date,
            COUNT(CASE WHEN n.unit_nav > 5 THEN 1 END) AS anomalous_count
        FROM funds f
        LEFT JOIN valid_fund_nav n ON f.fund_id = n.fund_id {nav_filter}
        {where_clause}
        GROUP BY f.fund_id
        ORDER BY f.fund_id
        """,
        params,
    ).fetchall()

    items: List[FundDetail] = []
    for r in rows:
        latest_nav: Optional[float] = None
        if r["latest_date"]:
            nav_row = conn.execute(
                "SELECT unit_nav FROM valid_fund_nav WHERE fund_id = ? AND nav_date = ? LIMIT 1",
                (r["fund_id"], r["latest_date"]),
            ).fetchone()
            if nav_row:
                try:
                    latest_nav = float(nav_row[0])
                except (TypeError, ValueError):
                    latest_nav = None

        fd = FundDetail(
            fund_id=r["fund_id"],
            product_code=r["fund_code"],
            product_name=r["fund_name"],
            first_entry_time=r["created_at"],
            record_count=r["record_count"] or 0,
            earliest_date=r["earliest_date"],
            latest_date=r["latest_date"],
            latest_nav=latest_nav,
            anomalous_count=r["anomalous_count"] or 0,
            strategy_l1=r["strategy_l1"],
            strategy_l2=r["strategy_l2"],
            sources=[s[0] for s in conn.execute('SELECT DISTINCT data_source FROM valid_fund_nav WHERE fund_id=? AND data_source IS NOT NULL ORDER BY data_source', (r['fund_id'],))],
            latest_nav_date=r['latest_date'],
        )
        items.append(fd.model_dump())

    return {"total": len(items), "items": items}


# --- Fund search (must be before /{fund_id}) --------------------------------

@app.get("/api/funds/search", response_model=FundSearchResponse, tags=["funds"])
def search_funds(
    q: str = Query(..., min_length=1, description="Search keyword (name or code)"),
    limit: int = Query(50, ge=1, le=200),
    conn: sqlite3.Connection = Depends(get_db),
):
    pattern = f"%{q}%"
    rows = conn.execute(
        """
        SELECT fund_id, fund_code, fund_name, created_at
        FROM funds
        WHERE fund_code LIKE ? OR fund_name LIKE ?
        ORDER BY fund_id
        LIMIT ?
        """,
        (pattern, pattern, limit),
    ).fetchall()

    items = [
        FundSummary(
            fund_id=r["fund_id"],
            product_code=r["fund_code"],
            product_name=r["fund_name"],
            first_entry_time=r["created_at"],
        )
        for r in rows
    ]
    return FundSearchResponse(total=len(items), items=items)


# --- All-funds issues summary (must be before /{fund_id}) -------------------

@app.get("/api/funds/issues", tags=["funds"])
def get_all_issues(conn: sqlite3.Connection = Depends(get_db)):
    fund_ids = [r[0] for r in conn.execute("SELECT fund_id FROM funds ORDER BY fund_id").fetchall()]
    issues = {fid: _compute_issues(conn, fid) for fid in fund_ids}
    return {"issues": issues}


# --- Batch fund returns (solves N+1 on FundList) ---------------------------

@app.get("/api/funds/returns", tags=["funds"])
def get_fund_returns(
    periods: str = Query("1w,1m,3m,6m", description="Comma-separated period codes: 1w,1m,3m,6m,1y,ytd,inception"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Return multi-period returns + sparkline for all funds in a single query."""
    period_list = [p.strip() for p in periods.split(",") if p.strip()]

    # Determine max lookback needed (180 days covers 6m; 365 for 1y)
    max_days = 180
    for p in period_list:
        if p == "1y" or p == "ytd" or p == "inception":
            max_days = 99999
            break

    # Compute cutoff date
    import datetime as _dt
    today = _dt.date.today()
    if max_days < 99999:
        cutoff = (today - _dt.timedelta(days=max_days + 30)).strftime("%Y-%m-%d")
    else:
        cutoff = "1900-01-01"

    rows = conn.execute(
        """
        SELECT n.fund_id, n.nav_date, n.unit_nav
        FROM valid_fund_nav n
        WHERE n.nav_date >= ?
          AND n.unit_nav IS NOT NULL
          AND n.unit_nav > 0
          AND n.unit_nav <= 5
        ORDER BY n.fund_id, n.nav_date
        """,
        [cutoff],
    ).fetchall()

    # Group by fund_id
    from collections import defaultdict
    fund_data: dict = defaultdict(list)
    for r in rows:
        fund_data[r["fund_id"]].append((r["nav_date"], float(r["unit_nav"])))

    def _period_return(series, period_code):
        """Compute return for a period code from a sorted (date, nav) series."""
        if len(series) < 2:
            return None
        last_date_str, last_nav = series[-1]
        if period_code == "inception":
            first_nav = series[0][1]
            return (last_nav - first_nav) / first_nav * 100 if first_nav > 0 else None

        if period_code == "ytd":
            year_start = last_date_str[:4] + "-01-01"
            for d, v in series:
                if d >= year_start and v > 0:
                    return (last_nav - v) / v * 100
            return None

        days_map = {"1w": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365}
        days = days_map.get(period_code)
        if days is None:
            return None

        target = (today - _dt.timedelta(days=days)).strftime("%Y-%m-%d")
        # Find nearest data point on or after target
        for d, v in series:
            if d >= target and v > 0:
                return (last_nav - v) / v * 100
        return None

    items = {}
    for fid, series in fund_data.items():
        entry = {}
        for p in period_list:
            entry[p] = _period_return(series, p)
        # Sparkline: sample up to 30 points from recent 90 days
        cutoff_90 = (today - _dt.timedelta(days=90)).strftime("%Y-%m-%d")
        recent = [(d, v) for d, v in series if d >= cutoff_90]
        if len(recent) > 30:
            step = len(recent) / 30
            sampled = [recent[int(i * step)] for i in range(30)]
        else:
            sampled = recent
        if sampled and sampled[0][1] > 0:
            base = sampled[0][1]
            entry["sparkline"] = [round(v / base, 4) for _, v in sampled]
        else:
            entry["sparkline"] = []
        items[fid] = entry

    return {"items": items}


# --- Fund metrics summary ---------------------------------------------------

def _compute_fund_metrics(series: list) -> dict:
    """Compute key performance metrics for a NAV series.

    series: sorted list of (yyyymmdd_str, nav_float) tuples.
    Returns a dict with annualized_return, annualized_vol, max_drawdown, sharpe, monthly_win_rate.
    """
    import math as _math
    import datetime as _dt_module

    if len(series) < 2:
        return {}

    dates = [s[0] for s in series]
    vals = [s[1] for s in series]
    if vals[0] <= 0:
        return {}

    try:
        d0 = _dt_module.date.fromisoformat(dates[0])
        d1 = _dt_module.date.fromisoformat(dates[-1])
        total_days = max(1, (d1 - d0).days)
    except Exception:
        total_days = len(series)

    # Annualized return (compound)
    period_ret = (vals[-1] - vals[0]) / vals[0]
    ann_ret = None
    if total_days >= 30:
        ann_ret = (_math.pow(1 + period_ret, 365 / total_days) - 1) * 100

    # Daily returns
    daily_rets = [
        (vals[i] - vals[i - 1]) / vals[i - 1]
        for i in range(1, len(vals))
        if vals[i - 1] > 0
    ]

    # Annualized volatility (std dev of daily returns × √250)
    ann_vol = None
    if len(daily_rets) > 1:
        n = len(daily_rets)
        mu = sum(daily_rets) / n
        var = sum((r - mu) ** 2 for r in daily_rets) / (n - 1)
        ann_vol = _math.sqrt(var * 250) * 100

    # Max drawdown (peak-to-trough, %)
    peak = vals[0]
    max_dd = 0.0
    for v in vals[1:]:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (v - peak) / peak
            if dd < max_dd:
                max_dd = dd
    max_dd_pct = max_dd * 100  # negative number

    # Sharpe (risk-free = 2.5% p.a.)
    sharpe = None
    if ann_ret is not None and ann_vol is not None and ann_vol > 0:
        sharpe = (ann_ret - 2.5) / ann_vol

    # Monthly win rate: count months with positive first→last return
    monthly_rets = []
    month_start_idx = 0
    for i in range(1, len(dates)):
        if dates[i][:7] != dates[i - 1][:7]:   # YYYY-MM changed
            start_nav = vals[month_start_idx]
            end_nav = vals[i - 1]
            if start_nav > 0:
                monthly_rets.append((end_nav - start_nav) / start_nav)
            month_start_idx = i
    # Include last (possibly incomplete) month
    if month_start_idx < len(vals) - 1:
        start_nav = vals[month_start_idx]
        end_nav = vals[-1]
        if start_nav > 0:
            monthly_rets.append((end_nav - start_nav) / start_nav)

    monthly_win_rate = None
    if monthly_rets:
        monthly_win_rate = sum(1 for r in monthly_rets if r > 0) / len(monthly_rets) * 100

    return {
        "annualized_return": round(ann_ret, 4) if ann_ret is not None else None,
        "annualized_vol": round(ann_vol, 4) if ann_vol is not None else None,
        "max_drawdown": round(max_dd_pct, 4),
        "sharpe": round(sharpe, 4) if sharpe is not None else None,
        "monthly_win_rate": round(monthly_win_rate, 2) if monthly_win_rate is not None else None,
        "period_days": total_days,
    }


@app.get("/api/funds/metrics/summary", tags=["funds"])
def get_fund_metrics_summary(
    period: str = Query("all", description="Lookback period: all | 1y | 3y"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Per-fund performance metrics: annualised return/vol, max drawdown, Sharpe, monthly win rate."""
    import datetime as _dt_mod

    days_map = {"1y": 365, "3y": 1095}
    max_days = days_map.get(period, 99999)

    today = _dt_mod.date.today()
    if max_days < 99999:
        cutoff = (today - _dt_mod.timedelta(days=max_days + 30)).strftime("%Y-%m-%d")
    else:
        cutoff = "1900-01-01"

    rows = conn.execute(
        """
        SELECT n.fund_id, n.nav_date, n.unit_nav
        FROM valid_fund_nav n
        WHERE n.nav_date >= ?
          AND n.unit_nav IS NOT NULL
          AND n.unit_nav > 0
          AND n.unit_nav <= 5
        ORDER BY n.fund_id, n.nav_date
        """,
        [cutoff],
    ).fetchall()

    from collections import defaultdict as _dd
    fund_data: dict = _dd(list)
    for r in rows:
        fund_data[r["fund_id"]].append((r["nav_date"], float(r["unit_nav"])))

    result = {}
    for fid, series in fund_data.items():
        result[fid] = _compute_fund_metrics(series)

    return {"period": period, "items": result}


# --- Set fund benchmark ----------------------------------------------------

class BenchmarkUpdateRequest(BaseModel):
    benchmark_index: Optional[str] = None


@app.put("/api/funds/{fund_id}/benchmark", tags=["funds"])
def set_fund_benchmark(
    fund_id: int,
    body: BenchmarkUpdateRequest,
    conn: sqlite3.Connection = Depends(get_db),
):
    row = conn.execute("SELECT fund_id FROM funds WHERE fund_id = ?", (fund_id,)).fetchone()
    if not row:
        raise NavAPIError(404, f"Fund {fund_id} not found", "NOT_FOUND")
    conn.execute(
        "UPDATE funds SET benchmark_index = ? WHERE fund_id = ?",
        (body.benchmark_index, fund_id),
    )
    return {"fund_id": fund_id, "benchmark_index": body.benchmark_index}


@app.patch("/api/funds/{fund_id}/strategy", tags=["funds"])
def set_fund_strategy(
    fund_id: int,
    body: StrategyUpdateRequest,
    conn: sqlite3.Connection = Depends(get_db),
):
    row = conn.execute("SELECT fund_id FROM funds WHERE fund_id = ?", (fund_id,)).fetchone()
    if not row:
        raise NavAPIError(404, f"Fund {fund_id} not found", "NOT_FOUND")
    conn.execute(
        "UPDATE funds SET strategy_l1 = ?, strategy_l2 = ? WHERE fund_id = ?",
        (body.strategy_l1, body.strategy_l2, fund_id),
    )
    return {"fund_id": fund_id, "strategy_l1": body.strategy_l1, "strategy_l2": body.strategy_l2}


# --- Single fund detail -----------------------------------------------------

@app.get("/api/funds/{fund_id}", response_model=FundDetail, tags=["funds"])
def get_fund(fund_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        """
        SELECT
            f.fund_id, f.fund_code, f.fund_name, f.created_at,
            f.benchmark_index, f.strategy_l1, f.strategy_l2,
            COUNT(n.id) AS record_count,
            MIN(n.nav_date) AS earliest_date,
            MAX(n.nav_date) AS latest_date
        FROM funds f
        LEFT JOIN valid_fund_nav n ON f.fund_id = n.fund_id
        WHERE f.fund_id = ?
        GROUP BY f.fund_id
        """,
        (fund_id,),
    ).fetchone()

    if not row:
        raise NavAPIError(404, f"Fund {fund_id} not found", "NOT_FOUND")

    latest_nav: Optional[float] = None
    if row["latest_date"]:
        nav_row = conn.execute(
            "SELECT unit_nav FROM valid_fund_nav WHERE fund_id = ? AND nav_date = ? LIMIT 1",
            (fund_id, row["latest_date"]),
        ).fetchone()
        if nav_row:
            try:
                latest_nav = float(nav_row[0])
            except (TypeError, ValueError):
                latest_nav = None

    return FundDetail(
        fund_id=row["fund_id"],
        product_code=row["fund_code"],
        product_name=row["fund_name"],
        first_entry_time=row["created_at"],
        record_count=row["record_count"] or 0,
        earliest_date=row["earliest_date"],
        latest_date=row["latest_date"],
        latest_nav=latest_nav,
        benchmark_index=row["benchmark_index"],
        strategy_l1=row["strategy_l1"],
        strategy_l2=row["strategy_l2"],
        sources=[s[0] for s in conn.execute('SELECT DISTINCT data_source FROM valid_fund_nav WHERE fund_id=? AND data_source IS NOT NULL ORDER BY data_source', (fund_id,))],
        latest_nav_date=row['latest_date'],
    )


# --- Fund NAV time series ---------------------------------------------------

@app.get("/api/funds/{fund_id}/nav", response_model=NavListResponse, tags=["funds"])
def get_fund_nav(
    fund_id: int,
    date_from: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    apply_filter: bool = Query(True, description="Apply data quality filter"),
    limit: int = Query(1000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    conn: sqlite3.Connection = Depends(get_db),
):
    # Validate fund exists
    fund_row = conn.execute("SELECT fund_id FROM funds WHERE fund_id = ?", (fund_id,)).fetchone()
    if not fund_row:
        raise NavAPIError(404, f"Fund {fund_id} not found", "NOT_FOUND")

    # Validate date range
    if date_from and date_to:
        if date_from > date_to:
            raise NavAPIError(400, "date_from must not be after date_to", "BAD_REQUEST")

    # Build WHERE clause dynamically
    conditions = ["fund_id = ?"]
    params: list = [fund_id]

    conditions.append("1=1" + quality_filter_sql(apply_filter))

    if date_from:
        conditions.append("nav_date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("nav_date <= ?")
        params.append(date_to)

    where_clause = " AND ".join(conditions)

    count_row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM valid_fund_nav WHERE {where_clause}", params
    ).fetchone()
    total: int = count_row["cnt"]

    rows = conn.execute(
        f"""
        SELECT *
        FROM valid_fund_nav
        WHERE {where_clause}
        ORDER BY nav_date ASC
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()

    return NavListResponse(
        total=total,
        fund_id=fund_id,
        items=[nav_row_to_model(r) for r in rows],
    )


# --- Create NAV record ------------------------------------------------------

@app.post("/api/nav", response_model=NavRecord, status_code=status.HTTP_201_CREATED, tags=["nav"])
def create_nav(body: NavCreateRequest, conn: sqlite3.Connection = Depends(get_db)):
    fund_id = get_or_create_fund_id(conn, body.product_code, body.product_name)

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO fund_nav_data
            (fund_id, fund_name, fund_code, nav_date, unit_nav, accum_nav, source_id, data_source)
        VALUES (?, ?, ?, ?, ?, ?, NULL, 'manual')
        """,
        (fund_id, body.product_name, body.product_code,
         body.nav_date, body.unit_nav, body.accumulated_nav),
    )

    if cursor.rowcount == 0:
        raise NavAPIError(
            409,
            f"A record for product_code={body.product_code} on {body.nav_date} already exists",
            "DUPLICATE_RECORD",
        )

    new_id = cursor.lastrowid
    recalculate_adj_nav(conn, body.product_code)
    conn.commit()

    row = conn.execute(
        'SELECT * '
        "FROM valid_fund_nav WHERE id = ?",
        (new_id,),
    ).fetchone()
    return nav_row_to_model(row)


# --- Get single NAV record --------------------------------------------------

@app.get("/api/nav/{nav_id}", response_model=NavRecord, tags=["nav"])
def get_nav(nav_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        'SELECT * '
        "FROM valid_fund_nav WHERE id = ?",
        (nav_id,),
    ).fetchone()
    if not row:
        raise NavAPIError(404, f"NAV record {nav_id} not found", "NOT_FOUND")
    return nav_row_to_model(row)


# --- Update NAV record (partial) --------------------------------------------

@app.put("/api/nav/{nav_id}", response_model=NavRecord, tags=["nav"])
def update_nav(nav_id: int, body: NavUpdateRequest, conn: sqlite3.Connection = Depends(get_db)):
    existing = conn.execute(
        'SELECT * '
        "FROM valid_fund_nav WHERE id = ?",
        (nav_id,),
    ).fetchone()
    if not existing:
        raise NavAPIError(404, f"NAV record {nav_id} not found", "NOT_FOUND")

    # Determine effective values after update
    new_product_name = body.product_name if body.product_name is not None else existing["fund_name"]
    new_nav_date = body.nav_date if body.nav_date is not None else existing["nav_date"]
    new_unit_nav = body.unit_nav if body.unit_nav is not None else existing["unit_nav"]
    new_accumulated_nav = body.accumulated_nav if body.accumulated_nav is not None else existing["accum_nav"]
    new_fund_code = existing["fund_code"]  # fund_code is immutable via this endpoint

    # Check for uniqueness conflict when date changes
    if body.nav_date is not None and new_nav_date != existing["nav_date"]:
        conflict = conn.execute(
            "SELECT id FROM valid_fund_nav WHERE fund_code = ? AND nav_date = ? AND id != ?",
            (new_fund_code, new_nav_date, nav_id),
        ).fetchone()
        if conflict:
            raise NavAPIError(
                409,
                f"A record for product_code={new_fund_code} on {body.nav_date} already exists",
                "DUPLICATE_RECORD",
            )

    set_clauses = []
    params = []

    if body.product_name is not None:
        set_clauses.append("fund_name = ?")
        params.append(new_product_name)
    if body.nav_date is not None:
        set_clauses.append("nav_date = ?")
        params.append(new_nav_date)
    if body.unit_nav is not None:
        set_clauses.append("unit_nav = ?")
        params.append(new_unit_nav)
    if body.accumulated_nav is not None:
        set_clauses.append("accum_nav = ?")
        params.append(new_accumulated_nav)

    if not set_clauses:
        return nav_row_to_model(existing)

    params.append(nav_id)
    conn.execute(
        f"UPDATE fund_nav_data SET {', '.join(set_clauses)} WHERE id = ?",
        params,
    )
    recalculate_adj_nav(conn, new_fund_code)
    conn.commit()

    updated = conn.execute(
        'SELECT * '
        "FROM valid_fund_nav WHERE id = ?",
        (nav_id,),
    ).fetchone()
    return nav_row_to_model(updated)


# --- Delete NAV record ------------------------------------------------------

@app.delete("/api/nav/{nav_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["nav"])
def delete_nav(nav_id: int, conn: sqlite3.Connection = Depends(get_db)):
    existing = conn.execute(
        "SELECT id, fund_code FROM valid_fund_nav WHERE id = ?", (nav_id,)
    ).fetchone()
    if not existing:
        raise NavAPIError(404, f"NAV record {nav_id} not found", "NOT_FOUND")

    conn.execute("DELETE FROM fund_nav_data WHERE id = ?", (nav_id,))
    recalculate_adj_nav(conn, existing['fund_code'])
    conn.commit()
    # 204 No Content — FastAPI returns empty body automatically


# --- Multi-fund compare -----------------------------------------------------

@app.get("/api/compare", response_model=CompareResponse, tags=["funds"])
def compare_funds(
    fund_ids: List[int] = Query(..., description="Fund IDs to compare (repeat param for multiple)"),
    date_from: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    apply_filter: bool = Query(True),
    conn: sqlite3.Connection = Depends(get_db),
):
    unique_ids = list(dict.fromkeys(fund_ids))  # deduplicate, preserve order
    if len(unique_ids) > 8:
        raise NavAPIError(400, "At most 8 fund_ids are allowed per compare request", "BAD_REQUEST")

    if date_from and date_to:
        if date_from > date_to:
            raise NavAPIError(400, "date_from must not be after date_to", "BAD_REQUEST")

    result: Dict[int, FundNavSeries] = {}

    for fid in unique_ids:
        fund_row = conn.execute(
            "SELECT fund_id, fund_code, fund_name FROM funds WHERE fund_id = ?", (fid,)
        ).fetchone()
        if not fund_row:
            raise NavAPIError(404, f"Fund {fid} not found", "NOT_FOUND")

        conditions = ["fund_id = ?"]
        params: list = [fid]
        conditions.append("1=1" + quality_filter_sql(apply_filter))

        if date_from:
            conditions.append("nav_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("nav_date <= ?")
            params.append(date_to)

        where_clause = " AND ".join(conditions)
        rows = conn.execute(
            f"""
            SELECT nav_date, unit_nav, accum_nav, adj_nav
            FROM valid_fund_nav
            WHERE {where_clause}
            ORDER BY nav_date ASC
            """,
            params,
        ).fetchall()

        series = [
            NavDataPoint(
                date=r["nav_date"],
                nav=r["unit_nav"],
                accumulated_nav=r["accum_nav"],
                adj_nav=r['adj_nav'],
            )
            for r in rows
        ]

        result[fid] = FundNavSeries(
            fund_id=fund_row["fund_id"],
            product_code=fund_row["fund_code"],
            product_name=fund_row["fund_name"],
            series=series,
        )

    return CompareResponse(funds=result)


# --- Per-fund issues --------------------------------------------------------

@app.get("/api/funds/{fund_id}/issues", tags=["funds"])
def get_fund_issues(fund_id: int, conn: sqlite3.Connection = Depends(get_db)):
    fund_row = conn.execute("SELECT fund_id FROM funds WHERE fund_id = ?", (fund_id,)).fetchone()
    if not fund_row:
        raise NavAPIError(404, f"Fund {fund_id} not found", "NOT_FOUND")
    return _compute_issues(conn, fund_id)


# --- Sync status and trigger ------------------------------------------------

@app.get("/api/sync/status", tags=["system"])
def get_sync_status():
    return _get_sync_status()


@app.post("/api/sync/trigger", tags=["system"])
def trigger_sync(background_tasks: BackgroundTasks):
    if _get_sync_status()['is_running'] or _sync_lock.locked():
        raise NavAPIError(409, '同步正在运行', 'SYNC_RUNNING')
    background_tasks.add_task(_run_sync, 'manual')
    return {"message": "sync queued", "status": "queued"}


@app.get('/api/sync/history', tags=['system'])
def sync_history(limit: int = Query(50, ge=1, le=200), conn: sqlite3.Connection = Depends(get_db)):
    return {'items': [dict(r) for r in conn.execute('SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?', (limit,))]}


@app.post('/api/failures/{failure_id}/retry', tags=['system'])
def retry_failure(failure_id: int, background_tasks: BackgroundTasks, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute('SELECT * FROM extraction_failures WHERE id=?', (failure_id,)).fetchone()
    if not row:
        raise NavAPIError(404, '失败记录不存在', 'NOT_FOUND')
    if not row['mailbox_uid'] or not row['uidvalidity']:
        raise NavAPIError(409, '历史记录缺少邮箱 UID，无法自动定位原邮件', 'RETRY_UNAVAILABLE')
    if row['status'] == 'resolved':
        raise NavAPIError(409, '此记录已恢复', 'ALREADY_RESOLVED')
    if _get_sync_status()['is_running'] or _sync_lock.locked():
        raise NavAPIError(409, '同步正在运行', 'SYNC_RUNNING')
    background_tasks.add_task(_run_sync, 'retry', failure_id)
    return {'status': 'queued', 'failure_id': failure_id}


@app.get('/api/export/email.xlsx', tags=['system'])
def export_email_excel(background_tasks: BackgroundTasks):
    from organize_fund_data import organize_fund_data
    from uuid import uuid4
    output = Path(__file__).parent / 'exports' / f'email-nav-{uuid4().hex}.xlsx'
    output.parent.mkdir(exist_ok=True)
    try:
        organize_fund_data(DB_PATH, str(output), source='email')
    except ValueError as exc:
        raise NavAPIError(409, str(exc), 'NO_EXPORT_DATA') from exc
    background_tasks.add_task(output.unlink, missing_ok=True)
    return FileResponse(output, filename='fund_email_nav.xlsx', background=background_tasks,
                        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# --- Market data helpers -----------------------------------------------------

def _market_table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    ).fetchone()
    return row is not None


# --- Market: indices ---------------------------------------------------------

@app.get("/api/market/indices", tags=["market"])
def get_market_indices(conn: sqlite3.Connection = Depends(get_db)):
    if not _market_table_exists(conn, "index_daily"):
        return {"items": []}
    rows = conn.execute(
        """
        SELECT i.ts_code, i.trade_date, i.close, i.open, i.high, i.low,
               i.vol, i.amount, i.pct_chg
        FROM index_daily i
        INNER JOIN (
            SELECT ts_code, MAX(trade_date) AS max_date
            FROM index_daily GROUP BY ts_code
        ) latest ON i.ts_code = latest.ts_code AND i.trade_date = latest.max_date
        ORDER BY i.ts_code
        """
    ).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        d["name"] = _INDEX_NAMES.get(d["ts_code"], d["ts_code"])
        items.append(d)
    return {"items": items}


@app.get("/api/market/indices/{ts_code}/daily", tags=["market"])
def get_index_daily(
    ts_code: str,
    date_from: Optional[str] = Query(None, description="Start date YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="End date YYYYMMDD"),
    limit: int = Query(250, ge=1, le=2000),
    conn: sqlite3.Connection = Depends(get_db),
):
    if not _market_table_exists(conn, "index_daily"):
        return {"ts_code": ts_code, "name": _INDEX_NAMES.get(ts_code, ts_code), "items": []}

    if date_from or date_to:
        conditions = ["ts_code = ?"]
        params: list = [ts_code]
        if date_from:
            conditions.append("trade_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("trade_date <= ?")
            params.append(date_to)
        params.append(limit)
        rows = conn.execute(
            f"""SELECT ts_code, trade_date, close, open, high, low, vol, amount, pct_chg
                FROM index_daily WHERE {' AND '.join(conditions)}
                ORDER BY trade_date ASC LIMIT ?""",
            params,
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT ts_code, trade_date, close, open, high, low, vol, amount, pct_chg
               FROM (SELECT ts_code, trade_date, close, open, high, low, vol, amount, pct_chg
                     FROM index_daily WHERE ts_code = ?
                     ORDER BY trade_date DESC LIMIT ?)
               ORDER BY trade_date ASC""",
            (ts_code, limit),
        ).fetchall()

    return {
        "ts_code": ts_code,
        "name": _INDEX_NAMES.get(ts_code, ts_code),
        "items": [dict(r) for r in rows],
    }


# --- Market: futures ---------------------------------------------------------

@app.get("/api/market/futures", tags=["market"])
def get_market_futures(conn: sqlite3.Connection = Depends(get_db)):
    if not _market_table_exists(conn, "futures_daily"):
        return {"items": []}
    rows = conn.execute(
        """
        SELECT f.ts_code, f.symbol, f.trade_date, f.close, f.open, f.high, f.low,
               f.vol, f.amount, f.oi
        FROM futures_daily f
        INNER JOIN (
            SELECT symbol, MAX(trade_date) AS max_date
            FROM futures_daily GROUP BY symbol
        ) latest ON f.symbol = latest.symbol AND f.trade_date = latest.max_date
        ORDER BY f.symbol
        """
    ).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/market/futures/{ts_code}/daily", tags=["market"])
def get_futures_daily(
    ts_code: str,
    date_from: Optional[str] = Query(None, description="Start date YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="End date YYYYMMDD"),
    limit: int = Query(250, ge=1, le=2000),
    conn: sqlite3.Connection = Depends(get_db),
):
    if not _market_table_exists(conn, "futures_daily"):
        return {"ts_code": ts_code, "items": []}

    if date_from or date_to:
        conditions = ["ts_code = ?"]
        params: list = [ts_code]
        if date_from:
            conditions.append("trade_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("trade_date <= ?")
            params.append(date_to)
        params.append(limit)
        rows = conn.execute(
            f"""SELECT ts_code, symbol, trade_date, close, open, high, low, vol, amount, oi
                FROM futures_daily WHERE {' AND '.join(conditions)}
                ORDER BY trade_date ASC LIMIT ?""",
            params,
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT ts_code, symbol, trade_date, close, open, high, low, vol, amount, oi
               FROM (SELECT ts_code, symbol, trade_date, close, open, high, low, vol, amount, oi
                     FROM futures_daily WHERE ts_code = ?
                     ORDER BY trade_date DESC LIMIT ?)
               ORDER BY trade_date ASC""",
            (ts_code, limit),
        ).fetchall()

    return {"ts_code": ts_code, "items": [dict(r) for r in rows]}


@app.get("/api/market/basis/{symbol}/daily", tags=["market"])
def get_basis_daily(
    symbol: str,
    date_from: Optional[str] = Query(None, description="Start date YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="End date YYYYMMDD"),
    limit: int = Query(250, ge=1, le=2000),
    conn: sqlite3.Connection = Depends(get_db),
):
    symbol = symbol.upper()
    if symbol not in FUTURES_TO_INDEX:
        raise NavAPIError(404, f"No index mapping for symbol {symbol}", "NOT_FOUND")
    index_ts_code = FUTURES_TO_INDEX[symbol]
    if not _market_table_exists(conn, "futures_daily") or not _market_table_exists(conn, "index_daily"):
        return {"symbol": symbol, "index_ts_code": index_ts_code, "items": []}
    params: list = [symbol, index_ts_code]
    date_where = ""
    if date_from:
        date_where += " AND f.trade_date >= ?"
        params.append(date_from)
    if date_to:
        date_where += " AND f.trade_date <= ?"
        params.append(date_to)
    params.append(limit)
    sql = f"""
        SELECT f.trade_date,
               f.ts_code   AS futures_code,
               f.close     AS futures_close,
               i.close     AS index_close,
               (i.close - f.close) AS basis,
               CASE WHEN f.close IS NOT NULL AND f.close != 0
                    THEN ROUND((i.close - f.close) / f.close * 100, 4)
                    ELSE NULL END AS basis_pct
        FROM (
            SELECT trade_date, ts_code, close, oi, vol,
                   ROW_NUMBER() OVER (PARTITION BY trade_date
                                      ORDER BY COALESCE(oi, vol, 0) DESC) AS rn
            FROM futures_daily WHERE symbol = ?
        ) f
        JOIN index_daily i ON i.trade_date = f.trade_date AND i.ts_code = ?
        WHERE f.rn = 1{date_where}
        ORDER BY f.trade_date DESC LIMIT ?
    """
    rows = conn.execute(sql, params).fetchall()
    items = [dict(r) for r in reversed(rows)]
    return {"symbol": symbol, "index_ts_code": index_ts_code, "items": items}


@app.get("/api/market/basis/{symbol}/quarterly", tags=["market"])
def get_basis_quarterly(
    symbol: str,
    date_from: Optional[str] = Query(None, description="Start date YYYYMMDD"),
    date_to: Optional[str] = Query(None, description="End date YYYYMMDD"),
    conn: sqlite3.Connection = Depends(get_db),
):
    """Per-trade-date basis for 当季 and 下季 quarterly contracts.

    Basis = spot_close − futures_close  (positive → spot premium / 贴水)
    Annualised basis % = basis / futures_close / remaining_days × 365 × 100
    """
    symbol = symbol.upper()
    if symbol not in FUTURES_TO_INDEX:
        raise NavAPIError(404, f"No index mapping for symbol {symbol}", "NOT_FOUND")
    index_ts_code = FUTURES_TO_INDEX[symbol]
    if not _market_table_exists(conn, "futures_daily") or not _market_table_exists(conn, "index_daily"):
        return {"symbol": symbol, "index_ts_code": index_ts_code, "items": []}

    params: list = [index_ts_code, symbol]
    date_where = ""
    if date_from:
        date_where += " AND f.trade_date >= ?"
        params.append(date_from)
    if date_to:
        date_where += " AND f.trade_date <= ?"
        params.append(date_to)

    # Fetch all quarterly contracts for this symbol joined with spot index
    sql = f"""
        SELECT f.trade_date,
               f.ts_code   AS ts_code,
               f.close     AS futures_close,
               i.close     AS index_close
        FROM futures_daily f
        JOIN index_daily i ON i.trade_date = f.trade_date AND i.ts_code = ?
        WHERE f.symbol = ?
          AND CAST(SUBSTR(f.ts_code, -2) AS INTEGER) IN (3, 6, 9, 12)
          {date_where}
        ORDER BY f.trade_date ASC, f.ts_code ASC
    """
    rows = conn.execute(sql, params).fetchall()

    # Group by trade_date and classify 当季 / 下季
    from collections import defaultdict
    by_date: dict = defaultdict(list)
    for row in rows:
        by_date[row["trade_date"]].append(dict(row))

    items = []
    for trade_date in sorted(by_date.keys()):
        trade_date_obj = _date(
            int(trade_date[:4]), int(trade_date[4:6]), int(trade_date[6:8])
        )
        index_close = by_date[trade_date][0]["index_close"]

        # Build list of unexpired contracts for this date, sorted by expiry
        contracts = []
        for r in by_date[trade_date]:
            ts = r["ts_code"]
            try:
                year = 2000 + int(ts[-4:-2])
                month = int(ts[-2:])
                expiry = _third_friday(year, month)
            except (ValueError, IndexError):
                continue
            if expiry <= trade_date_obj:
                continue   # already expired
            remaining = (expiry - trade_date_obj).days
            contracts.append({
                "ts_code": ts,
                "futures_close": r["futures_close"],
                "expiry": expiry,
                "remaining_days": remaining,
            })
        contracts.sort(key=lambda x: x["expiry"])

        # Roll threshold: if nearest contract is too close to expiry, advance label window
        # to avoid large annualised-basis spikes (e.g. the recurring ~10th-of-month anomaly).
        label_start = 0
        if len(contracts) >= 2 and contracts[0]["remaining_days"] < BASIS_ROLL_THRESHOLD:
            label_start = 1
        for i, label in enumerate(["当季", "下季"]):
            idx = label_start + i
            if idx >= len(contracts):
                break
            c = contracts[idx]
            fc = c["futures_close"]
            ic = index_close
            basis = round(ic - fc, 4) if ic is not None and fc is not None else None
            ann_basis = None
            if basis is not None and fc and c["remaining_days"] >= 10:
                ann_basis = round(basis / fc / c["remaining_days"] * 365 * 100, 4)
            items.append({
                "trade_date": trade_date,
                "contract_type": label,
                "ts_code": c["ts_code"],
                "futures_close": fc,
                "index_close": ic,
                "basis": basis,
                "annualized_basis_pct": ann_basis,
                "remaining_days": c["remaining_days"],
                "expiry_date": c["expiry"].strftime("%Y%m%d"),
            })

    return {"symbol": symbol, "index_ts_code": index_ts_code, "items": items}


@app.get("/api/market/basis/{symbol}/today", tags=["market"])
def get_basis_today(
    symbol: str,
    conn: sqlite3.Connection = Depends(get_db),
):
    """All active contracts on the latest available trading day."""
    symbol = symbol.upper()
    if symbol not in FUTURES_TO_INDEX:
        raise NavAPIError(404, f"No index mapping for symbol {symbol}", "NOT_FOUND")
    index_ts_code = FUTURES_TO_INDEX[symbol]
    if not _market_table_exists(conn, "futures_daily"):
        return {"symbol": symbol, "trade_date": None, "items": []}

    row = conn.execute(
        "SELECT MAX(trade_date) FROM futures_daily WHERE symbol=?", (symbol,)
    ).fetchone()
    latest_date = row[0] if row else None
    if not latest_date:
        return {"symbol": symbol, "trade_date": None, "items": []}

    rows = conn.execute("""
        SELECT f.ts_code, f.close AS futures_close, i.close AS index_close
        FROM futures_daily f
        LEFT JOIN index_daily i ON i.trade_date = f.trade_date AND i.ts_code = ?
        WHERE f.symbol = ? AND f.trade_date = ?
          AND CAST(SUBSTR(f.ts_code, -2) AS INTEGER) IN (3, 6, 9, 12)
        ORDER BY f.ts_code
    """, (index_ts_code, symbol, latest_date)).fetchall()

    today_obj = _date(int(latest_date[:4]), int(latest_date[4:6]), int(latest_date[6:8]))

    # Collect all active contracts first, then apply roll-threshold labelling
    all_contracts = []
    for r in rows:
        ts = r["ts_code"]
        try:
            year = 2000 + int(ts[-4:-2])
            month = int(ts[-2:])
            expiry = _third_friday(year, month)
        except (ValueError, IndexError):
            continue
        if expiry <= today_obj:
            continue
        remaining = (expiry - today_obj).days
        fc = r["futures_close"]
        ic = r["index_close"]
        basis = round(ic - fc, 4) if ic is not None and fc is not None else None
        ann_basis = None
        if basis is not None and fc and remaining >= 10:
            ann_basis = round(basis / fc / remaining * 365 * 100, 4)
        all_contracts.append({
            "ts_code": ts,
            "expiry": expiry,
            "remaining_days": remaining,
            "futures_close": fc,
            "index_close": ic,
            "basis": basis,
            "annualized_basis_pct": ann_basis,
        })

    # Apply roll threshold: if nearest contract is within BASIS_ROLL_THRESHOLD days,
    # advance label window (下季→当季) to suppress amplified near-expiry basis spikes.
    label_start = 0
    if len(all_contracts) >= 2 and all_contracts[0]["remaining_days"] < BASIS_ROLL_THRESHOLD:
        label_start = 1

    items = []
    for i, label in enumerate(["当季", "下季", "隔季"]):
        idx = label_start + i
        if idx >= len(all_contracts):
            break
        c = all_contracts[idx]
        items.append({
            "ts_code": c["ts_code"],
            "contract_type": label,
            "expiry_date": c["expiry"].strftime("%Y%m%d"),
            "remaining_days": c["remaining_days"],
            "futures_close": c["futures_close"],
            "index_close": c["index_close"],
            "basis": c["basis"],
            "annualized_basis_pct": c["annualized_basis_pct"],
        })

    return {"symbol": symbol, "trade_date": latest_date, "items": items}


# --- Real-time market snapshots ---------------------------------------------

@app.get("/api/market/realtime/indices", tags=["market"])
def get_realtime_indices(conn: sqlite3.Connection = Depends(get_db)):
    """Latest real-time snapshot for all indices."""
    if not _market_table_exists(conn, "market_realtime"):
        return {"items": [], "updated_at": None}
    rows = conn.execute(
        """SELECT ts_code, name, price, open, high, low, prev_close,
                  pct_chg, volume, amount, updated_at
           FROM market_realtime WHERE category='index'
           ORDER BY ts_code"""
    ).fetchall()
    items = [dict(r) for r in rows]
    updated_at = items[0]["updated_at"] if items else None
    return {"items": items, "updated_at": updated_at}


@app.get("/api/market/realtime/futures", tags=["market"])
def get_realtime_futures(conn: sqlite3.Connection = Depends(get_db)):
    """Latest real-time snapshot for all futures contracts."""
    if not _market_table_exists(conn, "market_realtime"):
        return {"items": [], "updated_at": None}
    rows = conn.execute(
        """SELECT ts_code, name, price, open, high, low, prev_close,
                  pct_chg, volume, amount, extra_json, updated_at
           FROM market_realtime WHERE category='futures'
           ORDER BY ts_code"""
    ).fetchall()
    items = []
    for r in rows:
        d = dict(r)
        extra = d.pop("extra_json", None)
        if extra:
            try:
                d.update(json.loads(extra))
            except (json.JSONDecodeError, TypeError):
                pass
        items.append(d)
    updated_at = items[0]["updated_at"] if items else None
    return {"items": items, "updated_at": updated_at}


@app.post("/api/market/realtime/trigger", tags=["market"])
def trigger_realtime_sync():
    """Manually trigger a real-time snapshot sync."""
    if not _MARKET_ENABLED:
        raise NavAPIError(503, "Market module not available", "SERVICE_UNAVAILABLE")
    threading.Thread(target=_run_realtime_sync, daemon=True).start()
    return {"status": "triggered"}


# --- Tags CRUD --------------------------------------------------------------

@app.get("/api/tags", tags=["tags"])
def list_tags(conn: sqlite3.Connection = Depends(get_db)):
    rows = conn.execute("SELECT tag_id, tag_name FROM fund_tags ORDER BY tag_id").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.post("/api/tags", status_code=201, tags=["tags"])
def create_tag(body: TagCreate, conn: sqlite3.Connection = Depends(get_db)):
    try:
        cur = conn.execute(
            "INSERT INTO fund_tags(tag_name) VALUES(?)", (body.tag_name.strip(),)
        )
        conn.commit()
        return {"tag_id": cur.lastrowid, "tag_name": body.tag_name.strip()}
    except sqlite3.IntegrityError:
        raise NavAPIError(409, f"Tag '{body.tag_name}' already exists", "CONFLICT")


@app.delete("/api/tags/{tag_id}", status_code=204, tags=["tags"])
def delete_tag(tag_id: int, conn: sqlite3.Connection = Depends(get_db)):
    conn.execute("DELETE FROM fund_tags WHERE tag_id=?", (tag_id,))
    conn.commit()


@app.post("/api/funds/{fund_id}/tags/{tag_id}", status_code=201, tags=["tags"])
def assign_tag(fund_id: int, tag_id: int, conn: sqlite3.Connection = Depends(get_db)):
    conn.execute(
        "INSERT OR IGNORE INTO fund_tag_assignments(fund_id, tag_id) VALUES(?,?)",
        (fund_id, tag_id),
    )
    conn.commit()
    return {"fund_id": fund_id, "tag_id": tag_id}


@app.delete("/api/funds/{fund_id}/tags/{tag_id}", status_code=204, tags=["tags"])
def remove_tag(fund_id: int, tag_id: int, conn: sqlite3.Connection = Depends(get_db)):
    conn.execute(
        "DELETE FROM fund_tag_assignments WHERE fund_id=? AND tag_id=?",
        (fund_id, tag_id),
    )
    conn.commit()


# --- Market: sync status and trigger -----------------------------------------

@app.get("/api/market/sync/status", tags=["market"])
def get_market_sync_status():
    keys = ["market_last_status", "market_index_last_date", "market_futures_last_date"]
    with _get_raw_conn() as conn:
        row = {k: conn.execute("SELECT value FROM sync_state WHERE key=?", (k,)).fetchone() for k in keys}
    return {k: (row[k][0] if row[k] else None) for k in keys}


@app.post("/api/market/sync/trigger", tags=["market"])
def trigger_market_sync(background_tasks: BackgroundTasks):
    if not _MARKET_ENABLED:
        raise NavAPIError(503, "Market data module not available (akshare not installed)", "NOT_AVAILABLE")
    background_tasks.add_task(_run_market_sync)
    return {"message": "market sync started"}


# --- Portfolio MVP -----------------------------------------------------------

class PortfolioConstituentIn(BaseModel):
    fund_id: int
    target_amount: Optional[float] = None
    target_weight: Optional[float] = None
    effective_date: str


class PortfolioCreateIn(BaseModel):
    portfolio_name: str
    build_method: str
    description: Optional[str] = None
    constituents: List[PortfolioConstituentIn]


class PortfolioUpdateIn(PortfolioCreateIn):
    pass


def _validate_portfolio(body):
    if not body.portfolio_name.strip() or len(body.portfolio_name) > 200:
        raise NavAPIError(422, '组合名称不能为空且不能超过 200 字', 'BAD_REQUEST')
    if body.build_method not in ('BATCH_INCLUDE', 'UNIFIED_START'):
        raise NavAPIError(422, '无效构建方式', 'BAD_REQUEST')
    if not 2 <= len(body.constituents) <= 100 or len({c.fund_id for c in body.constituents}) != len(body.constituents):
        raise NavAPIError(422, '组合需要 2–100 只不重复基金', 'BAD_REQUEST')
    for c in body.constituents:
        if normalize_nav_date(c.effective_date) != c.effective_date:
            raise NavAPIError(422, '生效日期必须是有效 YYYY-MM-DD 日期', 'BAD_REQUEST')
        value = c.target_weight if body.build_method == 'UNIFIED_START' else c.target_amount
        if value is None or not math.isfinite(value) or value <= 0:
            raise NavAPIError(422, '权重或金额必须为有限正数', 'BAD_REQUEST')
    if body.build_method == 'UNIFIED_START' and not math.isclose(sum(c.target_weight for c in body.constituents), 1, abs_tol=1e-6):
        raise NavAPIError(422, '组合权重合计必须等于 100%', 'BAD_REQUEST')


def _ensure_portfolio_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS portfolio_master (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_code TEXT UNIQUE NOT NULL,
            portfolio_name TEXT NOT NULL,
            description TEXT,
            build_method TEXT NOT NULL CHECK (build_method IN ('BATCH_INCLUDE','UNIFIED_START')),
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS portfolio_constituents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            fund_id INTEGER NOT NULL,
            fund_code TEXT NOT NULL,
            target_amount REAL,
            target_weight REAL,
            effective_date TEXT NOT NULL,
            include_order INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(portfolio_id, fund_id)
        );

        CREATE TABLE IF NOT EXISTS portfolio_nav_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            portfolio_id INTEGER NOT NULL,
            nav_date TEXT NOT NULL,
            portfolio_nav REAL NOT NULL,
            total_asset REAL NOT NULL,
            is_rebalance_day INTEGER NOT NULL DEFAULT 0,
            included_fund_count INTEGER NOT NULL,
            calc_version INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(portfolio_id, nav_date, calc_version)
        );
        CREATE TABLE IF NOT EXISTS portfolio_calculation_state (
            portfolio_id INTEGER PRIMARY KEY,
            input_signature TEXT NOT NULL
        );
        """
    )


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _portfolio_input_signature(conn, portfolio_id):
    definition = conn.execute('SELECT build_method FROM portfolio_master WHERE id=?', (portfolio_id,)).fetchone()
    members = conn.execute('SELECT fund_id,target_amount,target_weight,effective_date FROM portfolio_constituents WHERE portfolio_id=? ORDER BY include_order', (portfolio_id,)).fetchall()
    digest = hashlib.sha256(json.dumps([list(definition) if definition else None, [list(row) for row in members]]).encode())
    for member in members:
        rows = conn.execute('SELECT nav_date,adj_nav FROM valid_fund_nav WHERE fund_id=? ORDER BY nav_date', (member['fund_id'],)).fetchall()
        digest.update(json.dumps([list(row) for row in rows]).encode())
    return digest.hexdigest()


def _portfolio_series_by_fund(conn: sqlite3.Connection, fund_id: int) -> dict:
    missing = conn.execute('SELECT COUNT(*) FROM valid_fund_nav WHERE fund_id=? AND adj_nav IS NULL', (fund_id,)).fetchone()[0]
    if missing:
        raise NavAPIError(409, f'基金 {fund_id} 缺少完整复权净值，无法计算组合', 'MISSING_ADJUSTED_NAV')
    rows = conn.execute(
        """
        SELECT nav_date, adj_nav AS nav
        FROM valid_fund_nav
        WHERE fund_id = ? AND adj_nav IS NOT NULL
        ORDER BY nav_date ASC
        """,
        (fund_id,),
    ).fetchall()
    return {r["nav_date"]: float(r["nav"]) for r in rows}


def _calculate_unified_start(constituents: list, nav_maps: dict) -> list:
    # t0 = latest of each fund's first available NAV date (>= its effective_date)
    fund_starts = []
    for c in constituents:
        fid = c["fund_id"]
        eff = c["effective_date"]
        first = next((d for d in sorted(nav_maps[fid].keys()) if d >= eff), None)
        if first is None:
            raise NavAPIError(400, f"Fund {fid} has no nav on or after effective_date={eff}", "BAD_REQUEST")
        fund_starts.append(first)
    t0 = max(fund_starts)
    if any(max(values, default='') < t0 for values in nav_maps.values()):
        raise NavAPIError(409, '基金数据没有公共区间', 'NO_COMMON_RANGE')
    # Keep full portfolio weights: only dates with an observed NAV for every fund.
    common_dates = set.intersection(*(set(values) for values in nav_maps.values()))
    all_dates = sorted(d for d in common_dates if d >= t0)
    if not all_dates:
        raise NavAPIError(409, '基金没有共同净值日期，无法按统一起始方式计算', 'NO_COMMON_RANGE')
    t0 = all_dates[0]
    base = {}
    for c in constituents:
        fid = c["fund_id"]
        # Use the closest available NAV on or after t0 as the base
        base_nav = next((nav_maps[fid][d] for d in sorted(nav_maps[fid].keys()) if d >= t0), None)
        if base_nav is None:
            raise NavAPIError(400, f"Fund {fid} has no nav on or after t0={t0}", "BAD_REQUEST")
        base[fid] = base_nav
    items = []
    for d in all_dates:
        val = 0.0
        included = 0
        for c in constituents:
            fid = c["fund_id"]
            w = float(c["target_weight"] or 0.0)
            if d in nav_maps[fid]:
                included += 1
                val += w * (nav_maps[fid][d] / base[fid])
        if included > 0:
            items.append({"nav_date": d, "portfolio_nav": val, "total_asset": val, "is_rebalance_day": 0, "included_fund_count": included})
    return items


def _calculate_batch_include(constituents: list, nav_maps: dict) -> list:
    constituents = sorted(constituents, key=lambda x: x["effective_date"])
    t0 = min(c["effective_date"] for c in constituents)
    all_dates = sorted({d for m in nav_maps.values() for d in m.keys() if d >= t0})
    shares = {}
    total_asset = 1.0
    base_asset = None
    items = []

    last_nav: dict[int, float] = {}  # LOCF cache: fid -> last known nav

    for d in all_dates:
        # Update LOCF cache for all funds that have data on this date
        for fid, nmap in nav_maps.items():
            if d in nmap:
                last_nav[fid] = nmap[d]

        # Existing positions remain held across missing observations (legacy LOCF policy).
        active = [c for c in constituents if c["effective_date"] <= d
                  and (d in nav_maps[c["fund_id"]] or c['fund_id'] in shares)]
        if not active:
            continue
        # Trigger rebalance if first day, or any new fund joins that wasn't in shares yet
        new_fund_joined = any(c["fund_id"] not in shares for c in active)
        rebalance = (len(items) == 0) or new_fund_joined
        if rebalance:
            # Revalue existing shares before reallocating; never discard today's return.
            if shares:
                total_asset = sum(amount * last_nav[fid] for fid, amount in shares.items())
            total_target = sum(float(c["target_amount"] or 0.0) for c in active)
            if total_target <= 0:
                raise NavAPIError(400, "target_amount must be > 0 for BATCH_INCLUDE", "BAD_REQUEST")
            new_shares = {}
            for c in active:
                fid = c["fund_id"]
                w = float(c["target_amount"] or 0.0) / total_target
                alloc = total_asset * w
                new_shares[fid] = alloc / last_nav[fid]
            shares = new_shares
        total_asset = 0.0
        for c in active:
            fid = c["fund_id"]
            if fid not in shares:
                continue  # fund active but never rebalanced in — skip
            nav_val = nav_maps[fid].get(d, last_nav.get(fid))
            if nav_val is None:
                continue  # no NAV available at all — skip
            total_asset += shares[fid] * nav_val
        if base_asset is None:
            base_asset = total_asset
        items.append({
            "nav_date": d,
            "portfolio_nav": total_asset / base_asset,
            "total_asset": total_asset,
            "is_rebalance_day": 1 if rebalance else 0,
            "included_fund_count": len(active),
        })
    return items


def _get_portfolio_constituents(conn: sqlite3.Connection, portfolio_id: int) -> list:
    rows = conn.execute(
        """
        SELECT portfolio_id, fund_id, fund_code, target_amount, target_weight, effective_date, include_order,
            (SELECT fund_name FROM funds WHERE funds.fund_id=portfolio_constituents.fund_id) AS fund_name
        FROM portfolio_constituents WHERE portfolio_id=? ORDER BY include_order ASC
        """,
        (portfolio_id,),
    ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/portfolios", tags=["portfolio"])
def create_portfolio(body: PortfolioCreateIn, conn: sqlite3.Connection = Depends(get_db)):
    _validate_portfolio(body)
    _ensure_portfolio_tables(conn)
    now = _now_iso()
    code = datetime.now().strftime("PF%Y%m%d%H%M%S%f")
    cur = conn.execute(
        """
        INSERT INTO portfolio_master(portfolio_code, portfolio_name, description, build_method, status, created_at, updated_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (code, body.portfolio_name, body.description, body.build_method, "ACTIVE", now, now),
    )
    pid = cur.lastrowid
    for idx, c in enumerate(body.constituents, start=1):
        f = conn.execute("SELECT fund_code FROM funds WHERE fund_id=?", (c.fund_id,)).fetchone()
        if not f:
            raise NavAPIError(404, f"Fund {c.fund_id} not found", "NOT_FOUND")
        conn.execute(
            """
            INSERT INTO portfolio_constituents(portfolio_id, fund_id, fund_code, target_amount, target_weight, effective_date, include_order, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (pid, c.fund_id, f["fund_code"], c.target_amount, c.target_weight, c.effective_date, idx, now, now),
        )
    return {"id": pid, "portfolio_code": code, "portfolio_name": body.portfolio_name}


@app.get("/api/portfolios", tags=["portfolio"])
def list_portfolios(conn: sqlite3.Connection = Depends(get_db)):
    _ensure_portfolio_tables(conn)
    rows = conn.execute("SELECT id, portfolio_code, portfolio_name, build_method, updated_at FROM portfolio_master WHERE status='ACTIVE' ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/portfolios/{portfolio_id}", tags=["portfolio"])
def get_portfolio(portfolio_id: int, conn: sqlite3.Connection = Depends(get_db)):
    _ensure_portfolio_tables(conn)
    row = conn.execute("SELECT * FROM portfolio_master WHERE id=?", (portfolio_id,)).fetchone()
    if not row:
        raise NavAPIError(404, f"Portfolio {portfolio_id} not found", "NOT_FOUND")
    data = dict(row)
    data["constituents"] = _get_portfolio_constituents(conn, portfolio_id)
    return data


@app.put("/api/portfolios/{portfolio_id}", tags=["portfolio"])
def update_portfolio(portfolio_id: int, body: PortfolioUpdateIn, conn: sqlite3.Connection = Depends(get_db)):
    _validate_portfolio(body)
    _ensure_portfolio_tables(conn)
    row = conn.execute("SELECT id FROM portfolio_master WHERE id=?", (portfolio_id,)).fetchone()
    if not row:
        raise NavAPIError(404, f"Portfolio {portfolio_id} not found", "NOT_FOUND")
    now = _now_iso()
    conn.execute("UPDATE portfolio_master SET portfolio_name=?, description=?, build_method=?, updated_at=? WHERE id=?", (body.portfolio_name, body.description, body.build_method, now, portfolio_id))
    conn.execute("DELETE FROM portfolio_constituents WHERE portfolio_id=?", (portfolio_id,))
    for idx, c in enumerate(body.constituents, start=1):
        f = conn.execute("SELECT fund_code FROM funds WHERE fund_id=?", (c.fund_id,)).fetchone()
        if not f:
            raise NavAPIError(404, f"Fund {c.fund_id} not found", "NOT_FOUND")
        conn.execute(
            """
            INSERT INTO portfolio_constituents(portfolio_id, fund_id, fund_code, target_amount, target_weight, effective_date, include_order, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (portfolio_id, c.fund_id, f["fund_code"], c.target_amount, c.target_weight, c.effective_date, idx, now, now),
        )
    return get_portfolio(portfolio_id, conn)


@app.delete("/api/portfolios/{portfolio_id}", tags=["portfolio"])
def delete_portfolio(portfolio_id: int, conn: sqlite3.Connection = Depends(get_db)):
    _ensure_portfolio_tables(conn)
    conn.execute("UPDATE portfolio_master SET status='ARCHIVED', updated_at=? WHERE id=?", (_now_iso(), portfolio_id))
    return {"ok": True}


@app.post("/api/portfolios/{portfolio_id}/calculate", tags=["portfolio"])
def calculate_portfolio(portfolio_id: int, conn: sqlite3.Connection = Depends(get_db)):
    _ensure_portfolio_tables(conn)
    p = conn.execute("SELECT * FROM portfolio_master WHERE id=?", (portfolio_id,)).fetchone()
    if not p:
        raise NavAPIError(404, f"Portfolio {portfolio_id} not found", "NOT_FOUND")
    constituents = _get_portfolio_constituents(conn, portfolio_id)
    if len(constituents) < 2:
        raise NavAPIError(400, "Portfolio requires at least 2 constituents", "BAD_REQUEST")
    nav_maps = {c["fund_id"]: _portfolio_series_by_fund(conn, c["fund_id"]) for c in constituents}

    if p["build_method"] == "UNIFIED_START":
        items = _calculate_unified_start(constituents, nav_maps)
    else:
        items = _calculate_batch_include(constituents, nav_maps)
    if not items:
        raise NavAPIError(409, '当前日期范围没有可计算的组合数据', 'NO_PORTFOLIO_DATA')

    last_ver = conn.execute("SELECT COALESCE(MAX(calc_version),0) FROM portfolio_nav_cache WHERE portfolio_id=?", (portfolio_id,)).fetchone()[0]
    ver = int(last_ver) + 1
    now = _now_iso()
    for it in items:
        conn.execute(
            """
            INSERT INTO portfolio_nav_cache(portfolio_id, nav_date, portfolio_nav, total_asset, is_rebalance_day, included_fund_count, calc_version, created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (portfolio_id, it["nav_date"], it["portfolio_nav"], it["total_asset"], it["is_rebalance_day"], it["included_fund_count"], ver, now),
        )
    conn.execute('INSERT INTO portfolio_calculation_state VALUES (?,?) ON CONFLICT(portfolio_id) DO UPDATE SET input_signature=excluded.input_signature',
                 (portfolio_id, _portfolio_input_signature(conn, portfolio_id)))
    return {"portfolio_id": portfolio_id, "calc_version": ver, "rows": len(items)}


@app.get("/api/portfolios/{portfolio_id}/nav", tags=["portfolio"])
def get_portfolio_nav(portfolio_id: int, conn: sqlite3.Connection = Depends(get_db)):
    _ensure_portfolio_tables(conn)
    ver = conn.execute("SELECT MAX(calc_version) FROM portfolio_nav_cache WHERE portfolio_id=?", (portfolio_id,)).fetchone()[0]
    if ver is None:
        return {"items": [], "stale": False, "reason": "组合尚未计算，请管理员生成净值"}
    state = conn.execute('SELECT input_signature FROM portfolio_calculation_state WHERE portfolio_id=?', (portfolio_id,)).fetchone()
    if not state or state[0] != _portfolio_input_signature(conn, portfolio_id):
        return {'items': [], 'stale': True, 'reason': '基金净值或组合配置已变化，请管理员重新计算；旧结果已保留但不再展示'}
    rows = conn.execute(
        """
        SELECT nav_date, portfolio_nav, total_asset, is_rebalance_day, included_fund_count
        FROM portfolio_nav_cache
        WHERE portfolio_id=? AND calc_version=?
        ORDER BY nav_date ASC
        """,
        (portfolio_id, ver),
    ).fetchall()
    return {"items": [dict(r) for r in rows], "calc_version": ver}


@app.get("/api/portfolios/{portfolio_id}/metrics", tags=["portfolio"])
def get_portfolio_metrics(portfolio_id: int, conn: sqlite3.Connection = Depends(get_db)):
    nav = get_portfolio_nav(portfolio_id, conn)
    items = nav.get("items", [])
    series = [(i["nav_date"], float(i["portfolio_nav"])) for i in items]
    m = _compute_fund_metrics(series)
    return {
        "annualized_return": m.get("annualized_return"),
        "annualized_vol": m.get("annualized_vol"),
        "max_drawdown": m.get("max_drawdown"),
        "sharpe": m.get("sharpe"),
        "monthly_win_rate": m.get("monthly_win_rate"),
    }


# =============================================================================
# Section 9: Entry Point
# =============================================================================

if __name__ == "__main__":
    logger.info("Starting Fund NAV API on %s:%s", API_HOST, API_PORT)
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False)
