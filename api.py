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
import logging
import os
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
from get_163_email import connect_and_fetch_email
from pydantic import BaseModel, field_validator

# =============================================================================
# Section 2: Configuration
# =============================================================================
load_dotenv()

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")
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
    """Ensure funds table exists and fund_id column is present in fund_nav_data.
    Mirrors the migration logic from get_163_email.py init_database(), but
    only for the tables/columns the API depends on."""
    with _get_raw_conn() as conn:
        # Create funds table if not present
        conn.execute('''
            CREATE TABLE IF NOT EXISTS funds (
                fund_id INTEGER PRIMARY KEY AUTOINCREMENT,
                产品代码 TEXT NOT NULL UNIQUE,
                产品名称 TEXT,
                首次录入时间 DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Add fund_id column to fund_nav_data if missing
        try:
            conn.execute(
                'ALTER TABLE fund_nav_data ADD COLUMN fund_id INTEGER REFERENCES funds(fund_id)'
            )
        except Exception:
            pass  # column already exists

        # Populate funds from fund_nav_data if empty
        empty = conn.execute('SELECT COUNT(*) FROM funds').fetchone()[0] == 0
        if empty:
            conn.execute('''
                INSERT OR IGNORE INTO funds (产品代码, 产品名称, 首次录入时间)
                SELECT 产品代码, MIN(产品名称), MIN(插入时间)
                FROM fund_nav_data
                WHERE 产品代码 IS NOT NULL
                GROUP BY 产品代码
                ORDER BY MIN(插入时间)
            ''')

        # Backfill fund_id in fund_nav_data for existing rows
        conn.execute('''
            UPDATE fund_nav_data
            SET fund_id = (SELECT fund_id FROM funds WHERE funds.产品代码 = fund_nav_data.产品代码)
            WHERE fund_id IS NULL AND 产品代码 IS NOT NULL
        ''')

        # Ensure indexes exist
        conn.execute('CREATE INDEX IF NOT EXISTS idx_fund_id ON fund_nav_data(fund_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_product_code ON fund_nav_data(产品代码)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_nav_date ON fund_nav_data(净值日期)')

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

        # Add benchmark_index column to funds if missing
        try:
            conn.execute('ALTER TABLE funds ADD COLUMN benchmark_index TEXT DEFAULT NULL')
        except Exception:
            pass  # column already exists

    logger.info("Database schema initialised at %s", DB_PATH)


_scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_db_schema()
    _scheduler.add_job(_run_sync, "cron", hour="12,18", minute=0)
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
        _scheduler.add_job(_run_crude_sync, "cron", hour=15, minute=20)
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
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载原油路由（独立模块）
if _CRUDE_ENABLED:
    app.include_router(_crude_router)

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
    insert_time: Optional[str]
    source_id: Optional[int]           # NULL = manually entered


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
        return v

    @field_validator("unit_nav")
    @classmethod
    def validate_unit_nav(cls, v: float) -> float:
        if v <= 0:
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
        return v

    @field_validator("unit_nav", mode="before")
    @classmethod
    def validate_unit_nav(cls, v: Optional[float]) -> Optional[float]:
        if v is None:
            return v
        if v <= 0:
            raise ValueError("unit_nav must be greater than 0")
        return v


class NavDataPoint(BaseModel):
    date: str
    nav: float
    accumulated_nav: Optional[float]


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
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
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
    """Get existing fund_id or create a new funds record. Copied from get_163_email.py."""
    cursor = conn.cursor()
    cursor.execute("SELECT fund_id FROM funds WHERE 产品代码 = ?", (product_code,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO funds (产品代码, 产品名称) VALUES (?, ?)",
        (product_code, product_name),
    )
    conn.commit()
    return cursor.lastrowid


def db_date_to_api(s: Optional[str]) -> Optional[str]:
    """Convert YYYYMMDD → YYYY-MM-DD. Returns None for empty/None input."""
    if not s or len(s) != 8:
        return s
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}"


def api_date_to_db(s: str) -> str:
    """Convert YYYY-MM-DD → YYYYMMDD."""
    return s.replace("-", "")


def nav_row_to_model(row) -> NavRecord:
    try:
        unit_nav = float(row["单位净值"])
    except (TypeError, ValueError):
        unit_nav = 0.0
    try:
        accumulated_nav = float(row["累计单位净值"]) if row["累计单位净值"] is not None else None
    except (TypeError, ValueError):
        accumulated_nav = None
    return NavRecord(
        id=row["id"],
        fund_id=row["fund_id"],
        product_name=row["产品名称"],
        product_code=row["产品代码"],
        nav_date=db_date_to_api(row["净值日期"]),
        unit_nav=unit_nav,
        accumulated_nav=accumulated_nav,
        insert_time=row["插入时间"],
        source_id=row["source_id"],
    )


def quality_filter_sql(apply: bool) -> str:
    """Return SQL AND clause for quality filter, or empty string."""
    if not apply:
        return ""
    return " AND 单位净值 <= 5 AND (累计单位净值 IS NULL OR 累计单位净值 <= 5)"


# ── Sync state helpers ──────────────────────────────────────────────────────

def _get_sync_status() -> dict:
    with _get_raw_conn() as conn:
        keys = ["sync_last_time", "sync_last_status", "sync_last_added", "sync_last_error"]
        row = {k: conn.execute("SELECT value FROM sync_state WHERE key=?", (k,)).fetchone() for k in keys}
        return {k: (row[k][0] if row[k] else None) for k in keys}


def _set_sync_key(key: str, value: str):
    with _get_raw_conn() as conn:
        conn.execute("INSERT OR REPLACE INTO sync_state(key, value) VALUES(?,?)", (key, value))


_sync_lock = threading.Lock()


def _run_sync():
    if not _sync_lock.acquire(blocking=False):
        return  # already running, skip
    try:
        _set_sync_key("sync_last_time", datetime.now().isoformat())
        _set_sync_key("sync_last_status", "running")
        _set_sync_key("sync_last_error", "")
        email_user = os.getenv("EMAIL_USER", "")
        email_pwd  = os.getenv("EMAIL_PASSWORD", "")
        connect_and_fetch_email(email_user, email_pwd, DB_PATH)
        _set_sync_key("sync_last_status", "success")
        _set_sync_key("sync_last_added", "")
    except Exception as e:
        _set_sync_key("sync_last_status", "error")
        _set_sync_key("sync_last_error", str(e))
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
        """SELECT 净值日期, 单位净值 FROM fund_nav_data
           WHERE fund_id=? AND 净值日期 IS NOT NULL AND LENGTH(净值日期) = 8
           ORDER BY 净值日期 ASC""",
        (fund_id,)
    ).fetchall()
    anomalous = [
        {"nav_date": db_date_to_api(r[0]), "unit_nav": float(r[1])}
        for r in rows if r[1] is not None and float(r[1]) > 5
    ]
    gaps = []
    dates = [r[0] for r in rows]
    if len(dates) >= 3:
        try:
            ivs = [
                (datetime.strptime(dates[i+1], "%Y%m%d") - datetime.strptime(dates[i], "%Y%m%d")).days
                for i in range(len(dates) - 1)
            ]
            median = sorted(ivs)[len(ivs) // 2]
            threshold = max(median * 2.5, 30)
            for i, gap_days in enumerate(ivs):
                if gap_days > threshold:
                    gaps.append({
                        "from_date": db_date_to_api(dates[i]),
                        "to_date": db_date_to_api(dates[i + 1]),
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
    try:
        total = conn.execute("SELECT COUNT(*) FROM extraction_failures").fetchone()[0]
        rows = conn.execute(
            """SELECT id, 失败时间, 邮件主题, 邮件发件人, 邮件日期, 附件文件名, sheet名称, 失败原因
               FROM extraction_failures ORDER BY 失败时间 DESC LIMIT ? OFFSET ?""",
            (limit, offset),
        ).fetchall()
        items = [dict(r) for r in rows]
    except Exception:
        # Table may not exist in older DBs
        total = 0
        items = []
    return {"total": total, "items": items}


# --- Global stats -----------------------------------------------------------

@app.get("/api/stats", tags=["system"])
def get_stats(conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        "SELECT COUNT(*) AS total_records FROM fund_nav_data"
    ).fetchone()
    total_records: int = row["total_records"]

    row2 = conn.execute("SELECT COUNT(*) AS total_funds FROM funds").fetchone()
    total_funds: int = row2["total_funds"]

    row3 = conn.execute(
        "SELECT COUNT(*) AS manual_records FROM fund_nav_data WHERE source_id IS NULL"
    ).fetchone()
    manual_records: int = row3["manual_records"]

    return {
        "total_records": total_records,
        "total_funds": total_funds,
        "manual_records": manual_records,
    }


# --- Fund list --------------------------------------------------------------

@app.get("/api/funds", response_model=FundListResponse, tags=["funds"])
def list_funds(
    tag_id: Optional[int] = Query(None),
    conn: sqlite3.Connection = Depends(get_db),
):
    tag_filter = ""
    tag_params: list = []
    if tag_id is not None:
        tag_filter = " WHERE f.fund_id IN (SELECT fund_id FROM fund_tag_assignments WHERE tag_id=?)"
        tag_params = [tag_id]

    rows = conn.execute(
        f"""
        SELECT
            f.fund_id,
            f.产品代码,
            f.产品名称,
            f.首次录入时间,
            COUNT(n.id)                                  AS record_count,
            MIN(n.净值日期)                               AS earliest_date,
            MAX(n.净值日期)                               AS latest_date,
            COUNT(CASE WHEN n.单位净值 > 5 THEN 1 END)   AS anomalous_count
        FROM funds f
        LEFT JOIN fund_nav_data n ON f.fund_id = n.fund_id
        {tag_filter}
        GROUP BY f.fund_id
        ORDER BY f.fund_id
        """,
        tag_params,
    ).fetchall()

    # Build a tags map: fund_id -> list of {tag_id, tag_name}
    tag_rows = conn.execute(
        """
        SELECT a.fund_id, t.tag_id, t.tag_name
        FROM fund_tag_assignments a
        JOIN fund_tags t ON t.tag_id = a.tag_id
        ORDER BY a.fund_id, t.tag_id
        """
    ).fetchall()
    tags_map: dict = {}
    for tr in tag_rows:
        fid = tr["fund_id"]
        if fid not in tags_map:
            tags_map[fid] = []
        tags_map[fid].append({"tag_id": tr["tag_id"], "tag_name": tr["tag_name"]})

    items: List[FundDetail] = []
    for r in rows:
        latest_nav: Optional[float] = None
        if r["latest_date"]:
            nav_row = conn.execute(
                "SELECT 单位净值 FROM fund_nav_data WHERE fund_id = ? AND 净值日期 = ? LIMIT 1",
                (r["fund_id"], r["latest_date"]),
            ).fetchone()
            if nav_row:
                try:
                    latest_nav = float(nav_row[0])
                except (TypeError, ValueError):
                    latest_nav = None

        fd = FundDetail(
            fund_id=r["fund_id"],
            product_code=r["产品代码"],
            product_name=r["产品名称"],
            first_entry_time=r["首次录入时间"],
            record_count=r["record_count"] or 0,
            earliest_date=db_date_to_api(r["earliest_date"]),
            latest_date=db_date_to_api(r["latest_date"]),
            latest_nav=latest_nav,
            anomalous_count=r["anomalous_count"] or 0,
        )
        d = fd.model_dump()
        d["tags"] = tags_map.get(r["fund_id"], [])
        items.append(d)

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
        SELECT fund_id, 产品代码, 产品名称, 首次录入时间
        FROM funds
        WHERE 产品代码 LIKE ? OR 产品名称 LIKE ?
        ORDER BY fund_id
        LIMIT ?
        """,
        (pattern, pattern, limit),
    ).fetchall()

    items = [
        FundSummary(
            fund_id=r["fund_id"],
            product_code=r["产品代码"],
            product_name=r["产品名称"],
            first_entry_time=r["首次录入时间"],
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
    tag_id: Optional[int] = Query(None),
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

    tag_filter = ""
    tag_params: list = []
    if tag_id is not None:
        tag_filter = " AND n.fund_id IN (SELECT fund_id FROM fund_tag_assignments WHERE tag_id=?)"
        tag_params = [tag_id]

    # Compute cutoff date
    import datetime as _dt
    today = _dt.date.today()
    if max_days < 99999:
        cutoff = (today - _dt.timedelta(days=max_days + 30)).strftime("%Y%m%d")
    else:
        cutoff = "19000101"

    rows = conn.execute(
        f"""
        SELECT n.fund_id, n.净值日期 AS nav_date, n.单位净值 AS unit_nav
        FROM fund_nav_data n
        WHERE n.净值日期 >= ?
          AND n.单位净值 IS NOT NULL
          AND n.单位净值 > 0
          AND n.单位净值 <= 5
          {tag_filter}
        ORDER BY n.fund_id, n.净值日期
        """,
        [cutoff] + tag_params,
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
            year_start = last_date_str[:4] + "0101"
            for d, v in series:
                if d >= year_start and v > 0:
                    return (last_nav - v) / v * 100
            return None

        days_map = {"1w": 7, "1m": 30, "3m": 90, "6m": 180, "1y": 365}
        days = days_map.get(period_code)
        if days is None:
            return None

        target = (today - _dt.timedelta(days=days)).strftime("%Y%m%d")
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
        cutoff_90 = (today - _dt.timedelta(days=90)).strftime("%Y%m%d")
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


# --- Single fund detail -----------------------------------------------------

@app.get("/api/funds/{fund_id}", response_model=FundDetail, tags=["funds"])
def get_fund(fund_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        """
        SELECT
            f.fund_id, f.产品代码, f.产品名称, f.首次录入时间,
            f.benchmark_index,
            COUNT(n.id) AS record_count,
            MIN(n.净值日期) AS earliest_date,
            MAX(n.净值日期) AS latest_date
        FROM funds f
        LEFT JOIN fund_nav_data n ON f.fund_id = n.fund_id
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
            "SELECT 单位净值 FROM fund_nav_data WHERE fund_id = ? AND 净值日期 = ? LIMIT 1",
            (fund_id, row["latest_date"]),
        ).fetchone()
        if nav_row:
            try:
                latest_nav = float(nav_row[0])
            except (TypeError, ValueError):
                latest_nav = None

    return FundDetail(
        fund_id=row["fund_id"],
        product_code=row["产品代码"],
        product_name=row["产品名称"],
        first_entry_time=row["首次录入时间"],
        record_count=row["record_count"] or 0,
        earliest_date=db_date_to_api(row["earliest_date"]),
        latest_date=db_date_to_api(row["latest_date"]),
        latest_nav=latest_nav,
        benchmark_index=row["benchmark_index"],
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
        if api_date_to_db(date_from) > api_date_to_db(date_to):
            raise NavAPIError(400, "date_from must not be after date_to", "BAD_REQUEST")

    # Build WHERE clause dynamically
    conditions = ["fund_id = ?"]
    params: list = [fund_id]

    conditions.append("1=1" + quality_filter_sql(apply_filter))

    if date_from:
        conditions.append("净值日期 >= ?")
        params.append(api_date_to_db(date_from))
    if date_to:
        conditions.append("净值日期 <= ?")
        params.append(api_date_to_db(date_to))

    where_clause = " AND ".join(conditions)

    count_row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM fund_nav_data WHERE {where_clause}", params
    ).fetchone()
    total: int = count_row["cnt"]

    rows = conn.execute(
        f"""
        SELECT id, fund_id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值, 插入时间, source_id
        FROM fund_nav_data
        WHERE {where_clause}
        ORDER BY 净值日期 ASC
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
    db_date = api_date_to_db(body.nav_date)

    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO fund_nav_data
            (fund_id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值, source_id)
        VALUES (?, ?, ?, ?, ?, ?, NULL)
        """,
        (fund_id, body.product_name, body.product_code, db_date, body.unit_nav, body.accumulated_nav),
    )

    if cursor.rowcount == 0:
        raise NavAPIError(
            409,
            f"A record for product_code={body.product_code} on {body.nav_date} already exists",
            "DUPLICATE_RECORD",
        )

    new_id = cursor.lastrowid
    conn.commit()

    row = conn.execute(
        "SELECT id, fund_id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值, 插入时间, source_id "
        "FROM fund_nav_data WHERE id = ?",
        (new_id,),
    ).fetchone()
    return nav_row_to_model(row)


# --- Get single NAV record --------------------------------------------------

@app.get("/api/nav/{nav_id}", response_model=NavRecord, tags=["nav"])
def get_nav(nav_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        "SELECT id, fund_id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值, 插入时间, source_id "
        "FROM fund_nav_data WHERE id = ?",
        (nav_id,),
    ).fetchone()
    if not row:
        raise NavAPIError(404, f"NAV record {nav_id} not found", "NOT_FOUND")
    return nav_row_to_model(row)


# --- Update NAV record (partial) --------------------------------------------

@app.put("/api/nav/{nav_id}", response_model=NavRecord, tags=["nav"])
def update_nav(nav_id: int, body: NavUpdateRequest, conn: sqlite3.Connection = Depends(get_db)):
    existing = conn.execute(
        "SELECT id, fund_id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值, 插入时间, source_id "
        "FROM fund_nav_data WHERE id = ?",
        (nav_id,),
    ).fetchone()
    if not existing:
        raise NavAPIError(404, f"NAV record {nav_id} not found", "NOT_FOUND")

    # Determine effective values after update
    new_product_name = body.product_name if body.product_name is not None else existing["产品名称"]
    new_nav_date = api_date_to_db(body.nav_date) if body.nav_date is not None else existing["净值日期"]
    new_unit_nav = body.unit_nav if body.unit_nav is not None else existing["单位净值"]
    new_accumulated_nav = body.accumulated_nav if body.accumulated_nav is not None else existing["累计单位净值"]
    new_product_code = existing["产品代码"]  # product_code is immutable via this endpoint

    # Check for uniqueness conflict when date changes
    if body.nav_date is not None and new_nav_date != existing["净值日期"]:
        conflict = conn.execute(
            "SELECT id FROM fund_nav_data WHERE 产品代码 = ? AND 净值日期 = ? AND id != ?",
            (new_product_code, new_nav_date, nav_id),
        ).fetchone()
        if conflict:
            raise NavAPIError(
                409,
                f"A record for product_code={new_product_code} on {body.nav_date} already exists",
                "DUPLICATE_RECORD",
            )

    set_clauses = []
    params = []

    if body.product_name is not None:
        set_clauses.append("产品名称 = ?")
        params.append(new_product_name)
    if body.nav_date is not None:
        set_clauses.append("净值日期 = ?")
        params.append(new_nav_date)
    if body.unit_nav is not None:
        set_clauses.append("单位净值 = ?")
        params.append(new_unit_nav)
    if body.accumulated_nav is not None:
        set_clauses.append("累计单位净值 = ?")
        params.append(new_accumulated_nav)

    if not set_clauses:
        # Nothing to update — return existing record as-is
        return nav_row_to_model(existing)

    params.append(nav_id)
    conn.execute(
        f"UPDATE fund_nav_data SET {', '.join(set_clauses)} WHERE id = ?",
        params,
    )
    conn.commit()

    updated = conn.execute(
        "SELECT id, fund_id, 产品名称, 产品代码, 净值日期, 单位净值, 累计单位净值, 插入时间, source_id "
        "FROM fund_nav_data WHERE id = ?",
        (nav_id,),
    ).fetchone()
    return nav_row_to_model(updated)


# --- Delete NAV record ------------------------------------------------------

@app.delete("/api/nav/{nav_id}", status_code=status.HTTP_204_NO_CONTENT, tags=["nav"])
def delete_nav(nav_id: int, conn: sqlite3.Connection = Depends(get_db)):
    existing = conn.execute(
        "SELECT id FROM fund_nav_data WHERE id = ?", (nav_id,)
    ).fetchone()
    if not existing:
        raise NavAPIError(404, f"NAV record {nav_id} not found", "NOT_FOUND")

    conn.execute("DELETE FROM fund_nav_data WHERE id = ?", (nav_id,))
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
    if len(unique_ids) > 20:
        raise NavAPIError(400, "At most 20 fund_ids are allowed per compare request", "BAD_REQUEST")

    if date_from and date_to:
        if api_date_to_db(date_from) > api_date_to_db(date_to):
            raise NavAPIError(400, "date_from must not be after date_to", "BAD_REQUEST")

    result: Dict[int, FundNavSeries] = {}

    for fid in unique_ids:
        fund_row = conn.execute(
            "SELECT fund_id, 产品代码, 产品名称 FROM funds WHERE fund_id = ?", (fid,)
        ).fetchone()
        if not fund_row:
            raise NavAPIError(404, f"Fund {fid} not found", "NOT_FOUND")

        conditions = ["fund_id = ?"]
        params: list = [fid]
        conditions.append("1=1" + quality_filter_sql(apply_filter))

        if date_from:
            conditions.append("净值日期 >= ?")
            params.append(api_date_to_db(date_from))
        if date_to:
            conditions.append("净值日期 <= ?")
            params.append(api_date_to_db(date_to))

        where_clause = " AND ".join(conditions)
        rows = conn.execute(
            f"""
            SELECT 净值日期, 单位净值, 累计单位净值
            FROM fund_nav_data
            WHERE {where_clause}
            ORDER BY 净值日期 ASC
            """,
            params,
        ).fetchall()

        series = [
            NavDataPoint(
                date=db_date_to_api(r["净值日期"]),
                nav=r["单位净值"],
                accumulated_nav=r["累计单位净值"],
            )
            for r in rows
        ]

        result[fid] = FundNavSeries(
            fund_id=fund_row["fund_id"],
            product_code=fund_row["产品代码"],
            product_name=fund_row["产品名称"],
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
    background_tasks.add_task(_run_sync)
    return {"message": "sync started"}


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

        for i, label in enumerate(["当季", "下季"]):
            if i >= len(contracts):
                break
            c = contracts[i]
            fc = c["futures_close"]
            ic = index_close
            basis = round(ic - fc, 4) if ic is not None and fc is not None else None
            ann_basis = None
            if basis is not None and fc and c["remaining_days"] >= 7:
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
    items = []
    active_count = 0
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
        if basis is not None and fc and remaining >= 7:
            ann_basis = round(basis / fc / remaining * 365 * 100, 4)
        active_count += 1
        label = ["当季", "下季", "隔季"][min(active_count - 1, 2)]
        items.append({
            "ts_code": ts,
            "contract_type": label,
            "expiry_date": expiry.strftime("%Y%m%d"),
            "remaining_days": remaining,
            "futures_close": fc,
            "index_close": ic,
            "basis": basis,
            "annualized_basis_pct": ann_basis,
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
    keys = ["market_last_status", "market_last_error", "market_index_last_date", "market_futures_last_date"]
    with _get_raw_conn() as conn:
        row = {k: conn.execute("SELECT value FROM sync_state WHERE key=?", (k,)).fetchone() for k in keys}
    return {k: (row[k][0] if row[k] else None) for k in keys}


@app.post("/api/market/sync/trigger", tags=["market"])
def trigger_market_sync(background_tasks: BackgroundTasks):
    if not _MARKET_ENABLED:
        raise NavAPIError(503, "Market data module not available (akshare not installed)", "NOT_AVAILABLE")
    background_tasks.add_task(_run_market_sync)
    return {"message": "market sync started"}


# =============================================================================
# Section 9: Entry Point
# =============================================================================

if __name__ == "__main__":
    logger.info("Starting Fund NAV API on %s:%s", API_HOST, API_PORT)
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False)
