#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油与中东冲突新闻 RSS 抓取模块

数据源（三个，免费、无需 token）：
  - USNI News     https://news.usni.org/feed          — 美海军/霍尔木兹专项
  - OilPrice.com  https://oilprice.com/rss/main        — 原油市场（全量收录）
  - Al Jazeera    https://www.aljazeera.com/xml/rss/all.xml — 中东冲突（关键词过滤）

数据库表：crude_news（写入 fund_data.db）
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import feedparser
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

DB_PATH: str = os.getenv("DB_PATH", "fund_data.db")

# ---------------------------------------------------------------------------
# RSS 源配置
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    {
        "name": "USNI News",
        "url": "https://news.usni.org/feed",
        "default_category": "conflict",
        "keywords": None,          # 全量收录（每条都与美国海军/中东行动高度相关）
    },
    {
        "name": "OilPrice.com",
        "url": "https://oilprice.com/rss/main",
        "default_category": "crude",
        "keywords": None,          # 全量收录（专属原油能源媒体）
    },
    {
        "name": "Al Jazeera",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "default_category": "conflict",
        "keywords": [              # 仅保留命中关键词的条目
            "Iran", "Israel", "Hormuz", "crude", "oil price",
            "Houthi", "Yemen", "IRGC", "Gaza", "Lebanon",
            "sanctions", "nuclear", "Brent", "WTI", "OPEC",
            "Red Sea", "tanker", "petroleum",
        ],
    },
]

# ---------------------------------------------------------------------------
# 数据库初始化
# ---------------------------------------------------------------------------

def init_news_db(conn: sqlite3.Connection):
    """创建 crude_news 表（若不存在）。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crude_news (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT    NOT NULL,
            url          TEXT    UNIQUE,
            source_name  TEXT    NOT NULL,
            published_at TEXT,
            summary      TEXT,
            category     TEXT    DEFAULT 'crude',
            fetched_at   TEXT    NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_news_published
        ON crude_news(published_at DESC)
    """)
    conn.commit()
    logger.info("crude_news 表初始化完毕")


# ---------------------------------------------------------------------------
# 工具：解析 feedparser 时间为 ISO 8601 字符串
# ---------------------------------------------------------------------------

def _parse_time(entry) -> str:
    """从 feedparser entry 提取发布时间，返回 ISO 8601 字符串。"""
    t = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if t:
        try:
            return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 关键词过滤
# ---------------------------------------------------------------------------

def _matches_keywords(title: str, summary: str, keywords: list) -> bool:
    text = (title + " " + summary).lower()
    return any(kw.lower() in text for kw in keywords)


# ---------------------------------------------------------------------------
# 单源抓取
# ---------------------------------------------------------------------------

def _fetch_feed(conn: sqlite3.Connection, feed_cfg: dict) -> int:
    """抓取单个 RSS 源，返回新增条数。"""
    name = feed_cfg["name"]
    url = feed_cfg["url"]
    keywords = feed_cfg.get("keywords")
    category = feed_cfg["default_category"]

    logger.info("[%s] 开始抓取: %s", name, url)
    try:
        parsed = feedparser.parse(url, request_headers={"User-Agent": "Mozilla/5.0"})
    except Exception as e:
        logger.error("[%s] feedparser 失败: %s", name, e)
        return 0

    entries = parsed.get("entries", [])
    if not entries:
        logger.warning("[%s] 返回 0 条，可能被封锁或 RSS 格式变化", name)
        return 0

    fetched_at = datetime.now(timezone.utc).isoformat()
    added = 0

    for entry in entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = entry.get("summary", "").strip()[:500]

        if not title or not link:
            continue

        # 关键词过滤（仅对 keywords 非 None 的源生效）
        if keywords and not _matches_keywords(title, summary, keywords):
            continue

        pub_at = _parse_time(entry)

        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO crude_news
                    (title, url, source_name, published_at, summary, category, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (title, link, name, pub_at, summary, category, fetched_at),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                added += 1
        except Exception as e:
            logger.warning("[%s] 插入失败: %s — %s", name, title[:40], e)

    conn.commit()
    logger.info("[%s] 新增 %d 条", name, added)
    return added


# ---------------------------------------------------------------------------
# 主入口：供 api.py 调度器调用
# ---------------------------------------------------------------------------

def connect_and_fetch_news(db_path: str = DB_PATH) -> int:
    """抓取全部 RSS 源，返回总新增条数。"""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        init_news_db(conn)
        total = sum(_fetch_feed(conn, cfg) for cfg in RSS_FEEDS)
        logger.info("新闻抓取完毕，共新增 %d 条", total)
        return total
    except Exception as e:
        logger.error("新闻抓取整体失败: %s", e, exc_info=True)
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 直接运行入口
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== 原油新闻 RSS 抓取（直接运行）===")
    n = connect_and_fetch_news()
    print(f"完成，新增 {n} 条")

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT title, source_name, category, published_at "
        "FROM crude_news ORDER BY published_at DESC LIMIT 10"
    ).fetchall()
    print("\n--- 最新 10 条 ---")
    for r in rows:
        print(r)
    conn.close()
