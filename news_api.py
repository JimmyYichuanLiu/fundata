#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油与中东冲突新闻 API 路由

挂载到主 api.py：app.include_router(news_router)

端点：
  GET  /api/news                 — 新闻列表（支持 category / limit / offset）
  GET  /api/news/summary         — 今日观察摘要（最近24小时统计 + top3 + focus_text）
  GET  /api/news/sources         — 各新闻源最近一次抓取状态
  GET  /api/news/sync/status     — 同步状态
  POST /api/news/sync/trigger    — 手动触发同步

category 合法值：conflict / shipping / crude / official_west / official_iran
"""

import json
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

# 分类中文名（用于 focus_text 生成）
_CATEGORY_ZH = {
    "conflict":      "冲突动态",
    "shipping":      "航运运输",
    "crude":         "原油市场",
    "official_west": "欧美官方",
    "official_iran": "伊朗官方",
}


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


def _make_focus_text(last_24h_count: int, by_category: dict) -> str:
    """根据分类统计生成规则性摘要文字（最多2个分类）。"""
    if last_24h_count == 0:
        return "最近24小时暂无新闻"
    top_cats = sorted(
        [(k, v) for k, v in by_category.items() if v > 0],
        key=lambda x: x[1],
        reverse=True,
    )[:2]
    if not top_cats:
        return f"最近24小时共 {last_24h_count} 条新闻"
    focus = "、".join(_CATEGORY_ZH.get(k, k) for k, _ in top_cats)
    return f"最近24小时共 {last_24h_count} 条新闻，焦点集中在{focus}"


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
        result = connect_and_fetch_news(DB_PATH)
        total = result["total"]
        per_source = result["per_source"]

        _set_sync_key("news_last_status", "success")
        _set_sync_key("news_last_added", str(total))
        _set_sync_key("news_last_error", "")
        # 持久化逐源状态（JSON 字符串）
        _set_sync_key("news_source_status", json.dumps(per_source, ensure_ascii=False))
        logger.info("新闻同步完毕，新增 %d 条", total)
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
# GET /api/news/sources  — 各新闻源最近一次抓取状态
# ---------------------------------------------------------------------------

@news_router.get("/sources", summary="各新闻源抓取状态")
def get_news_sources():
    """
    返回各新闻源最近一次抓取状态列表。
    首次同步前返回空数组。

    响应格式：
    [
      {
        "source_name": "USNI News",
        "latest_fetch_time": "2026-03-22T22:34:00+00:00",
        "latest_added_count": 0,
        "latest_status": "ok",
        "latest_error": ""
      },
      ...
    ]
    """
    raw = _get_sync_key("news_source_status")
    if not raw:
        return []
    try:
        per_source = json.loads(raw)
    except Exception:
        return []

    result = []
    for name, info in per_source.items():
        result.append({
            "source_name":         name,
            "latest_fetch_time":   info.get("time", ""),
            "latest_added_count":  info.get("added", 0),
            "latest_status":       info.get("status", ""),
            "latest_error":        info.get("error", ""),
        })
    return result


# ---------------------------------------------------------------------------
# GET /api/news/summary  — 今日观察摘要
# ---------------------------------------------------------------------------

@news_router.get("/summary", summary="今日观察摘要（最近24小时 + 7天 + 30天统计）")
def get_news_summary():
    """
    返回最近 24 小时的新闻统计与高优先级摘要，同时附带 7 天和 30 天的分类统计。
    时间口径：优先使用 published_at，缺失时回退 fetched_at。

    响应格式：
    {
      "last_24h_count": 23,
      "by_category":    { "conflict": 8, "shipping": 3, ... },   # 最近24小时
      "by_category_7d": { "conflict": 45, "shipping": 12, ... }, # 最近7天
      "by_category_30d":{ "conflict": 180,"shipping": 50, ... }, # 最近30天
      "top5": [
        { "id", "title", "title_zh", "url", "source_name", "published_at", "category", "priority" }
      ],
      "focus_text": "最近24小时共23条新闻，焦点集中在航运运输、欧美官方"
    }
    """
    _ensure_table()
    conn = _get_db()
    try:
        now = datetime.now(timezone.utc)
        # 优先使用 published_at，缺失时回退 fetched_at
        time_expr = "COALESCE(published_at, fetched_at)"

        cutoff_24h = (now - timedelta(hours=24)).isoformat()
        cutoff_7d  = (now - timedelta(days=7)).isoformat()
        cutoff_30d = (now - timedelta(days=30)).isoformat()

        def _query_by_category(cutoff: str) -> dict:
            result = {cat: 0 for cat in VALID_CATEGORIES}
            rows = conn.execute(
                f"""
                SELECT category, COUNT(*) as cnt
                FROM crude_news
                WHERE {time_expr} >= ?
                GROUP BY category
                """,
                (cutoff,),
            ).fetchall()
            for row in rows:
                cat = row["category"]
                if cat in result:
                    result[cat] = row["cnt"]
            return result

        # 24小时总数
        total_row = conn.execute(
            f"SELECT COUNT(*) FROM crude_news WHERE {time_expr} >= ?",
            (cutoff_24h,),
        ).fetchone()
        last_24h_count = total_row[0] if total_row else 0

        by_category     = _query_by_category(cutoff_24h)
        by_category_7d  = _query_by_category(cutoff_7d)
        by_category_30d = _query_by_category(cutoff_30d)

        # top5：优先级最高（priority 最小）的前 5 条
        top5_rows = conn.execute(
            f"""
            SELECT id, title, title_zh, url, source_name, published_at, category, priority
            FROM crude_news
            WHERE {time_expr} >= ?
            ORDER BY priority ASC, published_at DESC
            LIMIT 5
            """,
            (cutoff_24h,),
        ).fetchall()
        top5 = [dict(r) for r in top5_rows]

        focus_text = _make_focus_text(last_24h_count, by_category)

        return {
            "last_24h_count":  last_24h_count,
            "by_category":     by_category,
            "by_category_7d":  by_category_7d,
            "by_category_30d": by_category_30d,
            "top5":            top5,
            "focus_text":      focus_text,
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
    sort:     str            = Query(
        "time",
        description="排序方式：time=最新时间优先，relevance=相关度（priority）优先",
    ),
    limit:    int            = Query(50, ge=1, le=200),
    offset:   int            = Query(0,  ge=0),
):
    """
    返回新闻列表。

    sort=time      → ORDER BY published_at DESC
    sort=relevance → ORDER BY priority ASC, published_at DESC

    响应格式：
    {
      "total": 120,
      "offset": 0,
      "limit": 50,
      "sort": "time",
      "items": [
        {
          "id", "title", "title_zh", "url", "source_name",
          "published_at", "summary", "category", "priority"
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

        order = "published_at DESC" if sort != "relevance" else "priority ASC, published_at DESC"

        total = conn.execute(
            f"SELECT COUNT(*) FROM crude_news {where}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""
            SELECT id, title, title_zh, url, source_name, published_at, summary, category, priority
            FROM crude_news {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()

        return {
            "total":  total,
            "offset": offset,
            "limit":  limit,
            "sort":   sort if sort in ("time", "relevance") else "time",
            "items":  [dict(r) for r in rows],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# GET /api/news/hormuz  — Hormuz / 航运相关新闻
# ---------------------------------------------------------------------------

_HORMUZ_KEYWORDS = ["Hormuz", "tanker", "Red Sea", "shipping", "strait"]

@news_router.get("/hormuz", summary="Hormuz / 航运观察新闻")
def get_hormuz_news(
    limit: int = Query(10, ge=1, le=50),
):
    """
    返回最近与 Hormuz / 航运相关的新闻（按 published_at DESC）。
    关键词匹配（title 字段）：Hormuz / tanker / Red Sea / shipping / strait

    响应格式：
    [
      { "id", "title", "title_zh", "url", "source_name", "published_at", "category", "priority" }
    ]
    """
    _ensure_table()
    conn = _get_db()
    try:
        conditions = " OR ".join(
            f"title LIKE ?" for _ in _HORMUZ_KEYWORDS
        )
        params = [f"%{kw}%" for kw in _HORMUZ_KEYWORDS]
        rows = conn.execute(
            f"""
            SELECT id, title, title_zh, url, source_name, published_at, category, priority
            FROM crude_news
            WHERE {conditions}
            ORDER BY published_at DESC
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
