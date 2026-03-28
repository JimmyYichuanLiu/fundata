#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油价格对比 API 路由 — 独立 FastAPI router

挂载到主 api.py：app.include_router(crude_router)

端点：
  GET  /api/crude/daily              — 三品种联合查询（对比折线图用）
  GET  /api/crude/{ts_code}/daily    — 单品种历史数据
  GET  /api/crude/sync/status        — 同步状态
  POST /api/crude/sync/trigger       — 手动触发同步
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query
from fastapi.responses import JSONResponse

from get_crude_data import (
    CRUDE_SYMBOLS,
    CROSS_SYMBOLS,
    CROSS_DIFF_THRESHOLD,
    connect_and_fetch_crude,
    init_crude_db,
    init_cross_db,
)

logger = logging.getLogger(__name__)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

crude_router = APIRouter(prefix="/api/crude", tags=["crude"])

# 防止并发同步
_crude_sync_lock = threading.Lock()


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _get_sync_key(key: str) -> str:
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT value FROM sync_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else ""
    finally:
        conn.close()


def _run_crude_sync():
    """后台线程安全同步任务。"""
    if not _crude_sync_lock.acquire(blocking=False):
        logger.info("原油同步已在运行，跳过")
        return
    try:
        connect_and_fetch_crude(DB_PATH)
    except Exception as e:
        logger.error("原油后台同步失败: %s", e, exc_info=True)
    finally:
        _crude_sync_lock.release()


# ---------------------------------------------------------------------------
# 确保表存在（首次请求时兜底初始化）
# ---------------------------------------------------------------------------

def _ensure_table():
    conn = sqlite3.connect(DB_PATH)
    try:
        init_crude_db(conn)
        init_cross_db(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/crude/sync/status
# ---------------------------------------------------------------------------

@crude_router.get("/sync/status", summary="原油同步状态")
def get_crude_sync_status():
    """
    返回最近一次原油数据同步的状态信息。
    字段与 market sync status 格式一致，方便前端统一处理。
    """
    return {
        "last_status": _get_sync_key("crude_last_status") or "never",
        "last_time":   _get_sync_key("crude_last_time"),
        "last_error":  _get_sync_key("crude_last_error"),
        "last_added":  _get_sync_key("crude_last_added"),
        "symbols": list(CRUDE_SYMBOLS.keys()),
    }


# ---------------------------------------------------------------------------
# POST /api/crude/sync/trigger
# ---------------------------------------------------------------------------

@crude_router.post("/sync/trigger", summary="手动触发原油同步")
def trigger_crude_sync(background_tasks: BackgroundTasks):
    """在后台启动一次原油数据同步，立即返回。"""
    _ensure_table()
    background_tasks.add_task(_run_crude_sync)
    return {"message": "原油数据同步已启动"}


# ---------------------------------------------------------------------------
# GET /api/crude/daily  — 三品种联合对比数据
# ---------------------------------------------------------------------------

@crude_router.get("/daily", summary="三品种原油对比数据")
def get_crude_daily_compare(
    date_from: Optional[str] = Query(None, description="起始日期 YYYYMMDD"),
    date_to:   Optional[str] = Query(None, description="截止日期 YYYYMMDD"),
    limit:     int           = Query(500, ge=1, le=2000, description="最多返回行数（按日期去重后）"),
):
    """
    返回三个品种在相同日期范围内的收盘价，合并成按日期对齐的格式。

    响应格式：
    {
      "items": [
        {"trade_date": "20250301", "WTI": 72.5, "BRENT": 75.1, "SC": 530.2},
        ...
      ],
      "latest_date": "20250308",
      "symbols": {"WTI": "USD", "BRENT": "USD", "SC": "CNY"}
    }

    注意：SC 单位为 CNY/桶，WTI/BRENT 单位为 USD/桶，前端需双Y轴处理。
    """
    _ensure_table()
    conn = _get_db()
    try:
        # 构建查询条件
        conditions = []
        params: list = []
        if date_from:
            conditions.append("trade_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("trade_date <= ?")
            params.append(date_to)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # 拉取所有品种的数据
        rows = conn.execute(
            f"SELECT ts_code, trade_date, close, data_source, is_reference FROM crude_daily {where} "
            f"ORDER BY trade_date DESC",
            params,
        ).fetchall()

        # 以日期为 key 聚合（取最新 limit 个日期）
        date_map: dict[str, dict] = {}
        for r in rows:
            d = r["trade_date"]
            if d not in date_map:
                date_map[d] = {"trade_date": d}
            date_map[d][r["ts_code"]] = r["close"]
            # 标注参考数据
            if r["is_reference"]:
                date_map[d][f"{r['ts_code']}_is_reference"] = True
                date_map[d][f"{r['ts_code']}_data_source"] = r["data_source"]

        # 按日期降序排列，截取 limit 条
        sorted_dates = sorted(date_map.keys(), reverse=True)[:limit]
        items = [date_map[d] for d in sorted(sorted_dates)]  # 升序返回给前端

        return {
            "items": items,
            "latest_date": sorted_dates[0] if sorted_dates else None,
            "symbols": {k: v["currency"] for k, v in CRUDE_SYMBOLS.items()},
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/crude/{ts_code}/daily  — 单品种历史数据
# ---------------------------------------------------------------------------

@crude_router.get("/{ts_code}/daily", summary="单品种原油历史数据")
def get_crude_symbol_daily(
    ts_code:   str,
    date_from: Optional[str] = Query(None, description="起始日期 YYYYMMDD"),
    date_to:   Optional[str] = Query(None, description="截止日期 YYYYMMDD"),
    limit:     int           = Query(500, ge=1, le=2000),
):
    """
    返回单个品种（WTI / BRENT / SC）的 OHLCV 日频数据。
    ts_code 不区分大小写。
    """
    ts_code_upper = ts_code.upper()
    if ts_code_upper not in CRUDE_SYMBOLS:
        return JSONResponse(
            status_code=404,
            content={"detail": f"未知品种 {ts_code}，支持: {list(CRUDE_SYMBOLS.keys())}"},
        )

    _ensure_table()
    conn = _get_db()
    try:
        conditions = ["ts_code = ?"]
        params: list = [ts_code_upper]
        if date_from:
            conditions.append("trade_date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("trade_date <= ?")
            params.append(date_to)

        where = "WHERE " + " AND ".join(conditions)
        rows = conn.execute(
            f"SELECT trade_date, open, high, low, close, volume, currency, data_source, is_reference "
            f"FROM crude_daily {where} "
            f"ORDER BY trade_date DESC LIMIT ?",
            params + [limit],
        ).fetchall()

        items = [dict(r) for r in rows]
        items.reverse()  # 升序返回

        cfg = CRUDE_SYMBOLS[ts_code_upper]
        return {
            "ts_code":     ts_code_upper,
            "description": cfg["description"],
            "currency":    cfg["currency"],
            "items":       items,
            "count":       len(items),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/crude/cross  — 价格交叉验证结果
# ---------------------------------------------------------------------------

@crude_router.get("/cross", summary="价格交叉验证结果（akshare vs yfinance）")
def get_crude_cross(
    ts_code: Optional[str] = Query(None, description="WTI 或 BRENT，不传则全部"),
    limit:   int           = Query(60, ge=1, le=365),
):
    """
    返回最近 N 天的 WTI/BRENT 交叉验证数据。

    响应格式：
    {
      "threshold_pct": 3.0,
      "items": [
        {
          "ts_code": "WTI",
          "trade_date": "20250320",
          "close_primary": 68.5,   // akshare
          "close_alt": 68.3,       // yfinance
          "diff_pct": 0.29,
          "is_verified": 1
        },
        ...
      ]
    }
    """
    _ensure_table()
    conn = _get_db()
    try:
        conditions: list = []
        params: list = []
        if ts_code and ts_code.upper() in CROSS_SYMBOLS:
            conditions.append("ts_code = ?")
            params.append(ts_code.upper())

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = conn.execute(
            f"""
            SELECT ts_code, trade_date, close_primary, close_alt,
                   diff_pct, is_verified, verified_at
            FROM crude_price_cross {where}
            ORDER BY trade_date DESC, ts_code
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()

        return {
            "threshold_pct": CROSS_DIFF_THRESHOLD,
            "symbols":       list(CROSS_SYMBOLS.keys()),
            "items":         [dict(r) for r in rows],
        }
    finally:
        conn.close()
