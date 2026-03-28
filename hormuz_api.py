#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
霍尔木兹海峡船舶监测 API 路由

挂载到主 api.py：app.include_router(hormuz_router)

端点：
  GET  /api/hormuz/current   — 最新快照（船只数、油轮数）+ 完整船只列表
  GET  /api/hormuz/history   — 最近24条快照（用于趋势图）
  GET  /api/hormuz/status    — AIS 同步状态
  POST /api/hormuz/trigger   — 手动触发 AIS 采集
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks

logger = logging.getLogger(__name__)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

hormuz_router = APIRouter(prefix="/api/hormuz", tags=["hormuz"])

_ais_sync_lock = threading.Lock()


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
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        row = conn.execute("SELECT value FROM sync_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# 后台同步任务
# ---------------------------------------------------------------------------

def _run_ais_sync():
    if not _ais_sync_lock.acquire(blocking=False):
        logger.info("AIS 采集已在运行，跳过")
        return
    try:
        from get_ais_data import collect_hormuz_ais
        collect_hormuz_ais(DB_PATH)
    except Exception as e:
        logger.error("AIS 后台采集失败: %s", e, exc_info=True)
    finally:
        _ais_sync_lock.release()


# ---------------------------------------------------------------------------
# GET /api/hormuz/status
# ---------------------------------------------------------------------------

@hormuz_router.get("/status", summary="AIS 同步状态")
def get_hormuz_status():
    api_key_configured = bool(os.getenv("AISSTREAM_API_KEY", "").strip())
    return {
        "api_key_configured": api_key_configured,
        "last_status": _get_sync_key("hormuz_last_status") or "never",
        "last_time":   _get_sync_key("hormuz_last_time"),
        "last_error":  _get_sync_key("hormuz_last_error"),
        "bbox": [[25.5, 56.0], [26.8, 57.5]],
        "note": "数据来源：AISStream.io，覆盖可能不完整，仅供参考",
    }


# ---------------------------------------------------------------------------
# POST /api/hormuz/trigger
# ---------------------------------------------------------------------------

@hormuz_router.post("/trigger", summary="手动触发 AIS 采集")
def trigger_hormuz_sync(background_tasks: BackgroundTasks):
    if not os.getenv("AISSTREAM_API_KEY", "").strip():
        return {"message": "AISSTREAM_API_KEY 未配置，无法采集", "ok": False}
    background_tasks.add_task(_run_ais_sync)
    return {"message": "AIS 采集已启动", "ok": True}


# ---------------------------------------------------------------------------
# GET /api/hormuz/current
# ---------------------------------------------------------------------------

@hormuz_router.get("/current", summary="当前区域船舶快照")
def get_hormuz_current():
    """
    返回最新一次采集的快照统计 + 完整船只列表。

    响应格式：
    {
      "snapshot": {
        "snapshot_at": "2026-03-26T10:00:00+00:00",
        "vessel_count": 15,
        "tanker_count": 9,
        "data_quality": "partial"
      },
      "vessels": [
        {"mmsi", "ship_name", "vessel_type", "lat", "lon", "speed", "course", "nav_status", "fetched_at"},
        ...
      ],
      "api_key_configured": true,
      "note": "数据来源：AISStream.io，覆盖可能不完整，仅供参考"
    }
    """
    conn = _get_db()
    try:
        api_key_configured = bool(os.getenv("AISSTREAM_API_KEY", "").strip())

        if not _table_exists(conn, "hormuz_snapshot"):
            return {
                "snapshot": None,
                "vessels": [],
                "api_key_configured": api_key_configured,
                "note": "数据来源：AISStream.io，覆盖可能不完整，仅供参考",
            }

        snap_row = conn.execute(
            "SELECT snapshot_at, vessel_count, tanker_count, data_quality "
            "FROM hormuz_snapshot ORDER BY snapshot_at DESC LIMIT 1"
        ).fetchone()

        vessels = []
        if _table_exists(conn, "hormuz_vessels"):
            rows = conn.execute(
                """SELECT mmsi, ship_name, vessel_type, lat, lon,
                          speed, course, nav_status, fetched_at
                   FROM hormuz_vessels
                   ORDER BY vessel_type, ship_name"""
            ).fetchall()
            vessels = [dict(r) for r in rows]

        return {
            "snapshot": dict(snap_row) if snap_row else None,
            "vessels": vessels,
            "api_key_configured": api_key_configured,
            "note": "数据来源：AISStream.io，覆盖可能不完整，仅供参考",
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/hormuz/history
# ---------------------------------------------------------------------------

@hormuz_router.get("/history", summary="近24条快照趋势")
def get_hormuz_history():
    """
    返回最近24条快照记录，用于前端趋势折线图。

    响应格式：
    {
      "items": [
        {"snapshot_at", "vessel_count", "tanker_count", "data_quality"},
        ...
      ]
    }
    """
    conn = _get_db()
    try:
        if not _table_exists(conn, "hormuz_snapshot"):
            return {"items": []}
        rows = conn.execute(
            """SELECT snapshot_at, vessel_count, tanker_count, data_quality
               FROM hormuz_snapshot
               ORDER BY snapshot_at DESC
               LIMIT 24"""
        ).fetchall()
        items = list(reversed([dict(r) for r in rows]))  # 升序返回给前端
        return {"items": items}
    finally:
        conn.close()
