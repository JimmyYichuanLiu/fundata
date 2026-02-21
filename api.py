#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fund NAV Visualization System - Backend API
FastAPI application exposing CRUD + search endpoints for fund_data.db
"""

# =============================================================================
# Section 1: Imports
# =============================================================================
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
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
TUSHARE_TOKEN: str = os.getenv("TUSHARE_TOKEN", "")

try:
    from get_market_data import connect_and_fetch_market as _connect_and_fetch_market, INDEX_NAMES as _INDEX_NAMES
    _MARKET_ENABLED = True
except ImportError:
    _MARKET_ENABLED = False
    _connect_and_fetch_market = None
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

    logger.info("Database schema initialised at %s", DB_PATH)


_scheduler = BackgroundScheduler(timezone="Asia/Shanghai")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _init_db_schema()
    _scheduler.add_job(_run_sync, "cron", hour="12,18", minute=0)
    if _MARKET_ENABLED and TUSHARE_TOKEN:
        _scheduler.add_job(_run_market_sync, "cron", hour=17, minute=0)
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
        if not TUSHARE_TOKEN:
            raise ValueError("TUSHARE_TOKEN not configured")
        if not _MARKET_ENABLED:
            raise ImportError("get_market_data module not available (tushare not installed?)")
        _connect_and_fetch_market(TUSHARE_TOKEN, DB_PATH)
        _set_sync_key("market_last_status", "success")
        _set_sync_key("market_last_error", "")
    except Exception as e:
        _set_sync_key("market_last_status", "error")
        _set_sync_key("market_last_error", str(e))
    finally:
        _market_sync_lock.release()


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
def list_funds(conn: sqlite3.Connection = Depends(get_db)):
    rows = conn.execute(
        """
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
        GROUP BY f.fund_id
        ORDER BY f.fund_id
        """
    ).fetchall()

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

        items.append(
            FundDetail(
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
        )

    return FundListResponse(total=len(items), items=items)


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


# --- Single fund detail -----------------------------------------------------

@app.get("/api/funds/{fund_id}", response_model=FundDetail, tags=["funds"])
def get_fund(fund_id: int, conn: sqlite3.Connection = Depends(get_db)):
    row = conn.execute(
        """
        SELECT
            f.fund_id, f.产品代码, f.产品名称, f.首次录入时间,
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
        raise NavAPIError(503, "Market data module not available (tushare not installed)", "NOT_AVAILABLE")
    if not TUSHARE_TOKEN:
        raise NavAPIError(400, "TUSHARE_TOKEN not configured", "NO_TOKEN")
    background_tasks.add_task(_run_market_sync)
    return {"message": "market sync started"}


# =============================================================================
# Section 9: Entry Point
# =============================================================================

if __name__ == "__main__":
    logger.info("Starting Fund NAV API on %s:%s", API_HOST, API_PORT)
    uvicorn.run("api:app", host=API_HOST, port=API_PORT, reload=False)
