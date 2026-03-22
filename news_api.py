#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油与中东冲突新闻 API 路由

挂载到主 api.py：app.include_router(news_router)

端点：
  GET  /api/news                 — 新闻列表（支持 category / limit / offset）
  GET  /api/news/summary         — 今日观察摘要（最近24小时统计 + top3）
  GET  /api/news/sync/status     — 同步状态
  POST /api/news/sync/trigger    — 手动触发同步

category 合法值：conflict / shipping / crude / official_west / official_iran
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Query

from get_news_data import connect_and_fetch_news, init_news_db, RSS_FEEDS

logger = logging.getLogger(__name__)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

news_router = APIRouter(prefix="/api/news", tags=["news"])

_news_sync_lock = threading.Lock()

# 合法的 category 值
VALID_CATEGORIES = {"conflict", "shipping", "crude", "official_west", "official_iran"}


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
    source_names = [feed["name"] for feed in RSS_FEEDS]
    return {
        "last_status": _get_sync_key("news_last_status") or "never",
        "last_time":   _get_sync_key("news_last_time"),
        "last_error":  _get_sync_key("news_last_error"),
        "last_added":  _get_sync_key("news_last_added"),
        "sources":     source_names,
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
# GET /api/news/summary  — 今日观察摘要
# ---------------------------------------------------------------------------

@news_router.get("/summary", summary="今日观察摘要（最近24小时）")
def get_news_summary():
    """
    返回最近 24 小时的新闻统计与高优先级摘要。

    响应格式：
    {
      "last_24h_count": 23,
      "by_category": {
        "conflict": 8,
        "shipping": 3,
        "crude": 7,
        "official_west": 3,
        "official_iran": 2
      },
      "top3": [
        {
          "id": 1,
          "title": "...",
          "url": "...",
          "source_name": "IAEA",
          "published_at": "2025-03-21T12:00:00+00:00",
          "category": "official_west",
          "priority": 2
        }
      ]
    }
    """
    _ensure_table()
    conn = _get_db()
    try:
        # 最近 24 小时的时间阈值（ISO 8601）
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        # 总数
        total_row = conn.execute(
            "SELECT COUNT(*) FROM crude_news WHERE fetched_at >= ?",
            (cutoff,),
        ).fetchone()
        last_24h_count = total_row[0] if total_row else 0

        # 按分类统计
        by_category = {cat: 0 for cat in VALID_CATEGORIES}
        cat_rows = conn.execute(
            """
            SELECT category, COUNT(*) as cnt
            FROM crude_news
            WHERE fetched_at >= ?
            GROUP BY category
            """,
            (cutoff,),
        ).fetchall()
        for row in cat_rows:
            cat = row["category"]
            if cat in by_category:
                by_category[cat] = row["cnt"]

        # top3：优先级最高（priority 最小）的前 3 条
        top3_rows = conn.execute(
            """
            SELECT id, title, url, source_name, published_at, category, priority
            FROM crude_news
            WHERE fetched_at >= ?
            ORDER BY priority ASC, published_at DESC
            LIMIT 3
            """,
            (cutoff,),
        ).fetchall()
        top3 = [dict(r) for r in top3_rows]

        return {
            "last_24h_count": last_24h_count,
            "by_category": by_category,
            "top3": top3,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/news
# ---------------------------------------------------------------------------

@news_router.get("", summary="新闻列表")
def list_news(
    category: Optional[str] = Query(
        None,
        description="category 过滤：conflict / shipping / crude / official_west / official_iran，不传则全部",
    ),
    limit:    int            = Query(50, ge=1, le=200),
    offset:   int            = Query(0,  ge=0),
):
    """
    返回新闻列表，按 priority ASC, published_at DESC 排序。

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
          "category": "conflict",
          "priority": 3
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
        if category in VALID_CATEGORIES:
            conditions.append("category = ?")
            params.append(category)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        total = conn.execute(
            f"SELECT COUNT(*) FROM crude_news {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""
            SELECT id, title, url, source_name, published_at, summary, category, priority
            FROM crude_news {where}
            ORDER BY priority ASC, published_at DESC
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
