#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油与中东冲突新闻 API 路由

挂载到主 api.py：app.include_router(news_router)

端点：
  GET  /api/news                 — 新闻列表（支持 category / limit / offset）
  GET  /api/news/sync/status     — 同步状态
  POST /api/news/sync/trigger    — 手动触发同步
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query

from get_news_data import connect_and_fetch_news, init_news_db

logger = logging.getLogger(__name__)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

news_router = APIRouter(prefix="/api/news", tags=["news"])

_news_sync_lock = threading.Lock()


# ---------------------------------------------------------------------------
# 内部工具
# ---------------------------------------------------------------------------

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _get_sync_key(key: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT value FROM sync_state WHERE key=?", (key,)).fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def _set_sync_key(key: str, value: str):
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO sync_state(key, value) VALUES(?, ?)",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_table():
    conn = sqlite3.connect(DB_PATH)
    try:
        init_news_db(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 后台同步任务（线程安全）
# ---------------------------------------------------------------------------

def _run_news_sync():
    if not _news_sync_lock.acquire(blocking=False):
        logger.info("新闻同步已在运行，跳过")
        return
    try:
        _set_sync_key("news_last_status", "running")
        _set_sync_key("news_last_time", datetime.now().isoformat())
        added = connect_and_fetch_news(DB_PATH)
        _set_sync_key("news_last_status", "success")
        _set_sync_key("news_last_added", str(added))
        _set_sync_key("news_last_error", "")
        logger.info("新闻同步完毕，新增 %d 条", added)
    except Exception as e:
        logger.error("新闻后台同步失败: %s", e, exc_info=True)
        _set_sync_key("news_last_status", "error")
        _set_sync_key("news_last_error", str(e))
    finally:
        _news_sync_lock.release()


# ---------------------------------------------------------------------------
# GET /api/news/sync/status
# ---------------------------------------------------------------------------

@news_router.get("/sync/status", summary="新闻同步状态")
def get_news_sync_status():
    return {
        "last_status": _get_sync_key("news_last_status") or "never",
        "last_time":   _get_sync_key("news_last_time"),
        "last_error":  _get_sync_key("news_last_error"),
        "last_added":  _get_sync_key("news_last_added"),
        "sources":     ["USNI News", "OilPrice.com", "Al Jazeera"],
    }


# ---------------------------------------------------------------------------
# POST /api/news/sync/trigger
# ---------------------------------------------------------------------------

@news_router.post("/sync/trigger", summary="手动触发新闻同步")
def trigger_news_sync(background_tasks: BackgroundTasks):
    _ensure_table()
    background_tasks.add_task(_run_news_sync)
    return {"message": "新闻同步已启动"}


# ---------------------------------------------------------------------------
# GET /api/news
# ---------------------------------------------------------------------------

@news_router.get("", summary="新闻列表")
def list_news(
    category: Optional[str] = Query(None, description="'conflict' 或 'crude'，不传则全部"),
    limit:    int            = Query(50, ge=1, le=200),
    offset:   int            = Query(0,  ge=0),
):
    """
    返回最新新闻列表，按 published_at 降序排列。

    响应格式：
    {
      "total": 120,
      "offset": 0,
      "limit": 50,
      "items": [
        {
          "id": 1,
          "title": "...",
          "url": "https://...",
          "source_name": "USNI News",
          "published_at": "2025-03-21T12:00:00+00:00",
          "summary": "...",
          "category": "conflict"
        },
        ...
      ]
    }
    """
    _ensure_table()
    conn = _get_db()
    try:
        conditions = []
        params: list = []
        if category in ("conflict", "crude"):
            conditions.append("category = ?")
            params.append(category)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        total = conn.execute(
            f"SELECT COUNT(*) FROM crude_news {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""
            SELECT id, title, url, source_name, published_at, summary, category
            FROM crude_news {where}
            ORDER BY published_at DESC
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()

        return {
            "total":  total,
            "offset": offset,
            "limit":  limit,
            "items":  [dict(r) for r in rows],
        }
    finally:
        conn.close()
