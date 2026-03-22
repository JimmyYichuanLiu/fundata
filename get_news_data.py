#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
原油与中东冲突新闻 RSS 抓取模块

数据源（10个，免费、无需 token）：
  - USNI News          https://news.usni.org/feed               — 美海军/霍尔木兹专项
  - OilPrice.com       https://oilprice.com/rss/main            — 原油市场（全量收录）
  - Al Jazeera         https://www.aljazeera.com/xml/rss/all.xml — 中东冲突（关键词过滤）
  - IAEA               https://www.iaea.org/feeds/topnews       — 国际原子能机构（全量）
  - Iran International Google News RSS                          — 伊朗/波斯湾局势
  - White House        Google News RSS site:whitehouse.gov       — 美国官方立场（关键词过滤）
  - State Dept         Google News RSS site:state.gov           — 国务院声明（关键词过滤）
  - The National       Google News RSS site:thenationalnews.com — 阿联酋视角（关键词过滤）
  - Reuters Energy     Google News RSS site:reuters.com         — 路透能源（关键词过滤）
  - Hormuz/Shipping    Google News RSS 关键词聚合               — 航运/油轮动态

数据库表：crude_news（写入 fund_data.db）

category 合法值：conflict / shipping / crude / official_west / official_iran
priority：1~10，数值越小越重要（官方源从 2 开始，其余从 5 开始，关键词命中可扣分）
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote_plus

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
# Google News RSS URL 构造
# ---------------------------------------------------------------------------

def _google_news_rss(query: str) -> str:
    """生成 Google News RSS URL（英文，美国区域）。"""
    encoded = quote_plus(query)
    return f"https://news.google.com/rss/search?q={encoded}&hl=en-US&gl=US&ceid=US:en"


# ---------------------------------------------------------------------------
# RSS 源配置
# ---------------------------------------------------------------------------
RSS_FEEDS = [
    # ---- 原有来源（全量收录，高信噪比） ----
    {
        "name": "USNI News",
        "url": "https://news.usni.org/feed",
        "default_category": "conflict",
        "keywords": None,           # 全量收录（美国海军/中东行动专项媒体）
        "base_priority": 3,
    },
    {
        "name": "OilPrice.com",
        "url": "https://oilprice.com/rss/main",
        "default_category": "crude",
        "keywords": None,           # 全量收录（专属原油能源媒体）
        "base_priority": 5,
    },
    {
        "name": "Al Jazeera",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
        "default_category": "conflict",
        "keywords": [               # 仅保留命中关键词的条目
            "Iran", "Israel", "Hormuz", "crude", "oil price",
            "Houthi", "Yemen", "IRGC", "Gaza", "Lebanon",
            "sanctions", "nuclear", "Brent", "WTI", "OPEC",
            "Red Sea", "tanker", "petroleum",
        ],
        "base_priority": 5,
    },

    # ---- 新增：欧美官方来源 ----
    {
        "name": "IAEA",
        "url": "https://www.iaea.org/feeds/topnews",
        "default_category": "official_west",
        "keywords": None,           # 发布频率低但信噪比极高，全量收录
        "base_priority": 2,
    },
    {
        "name": "White House",
        "url": _google_news_rss("site:whitehouse.gov Iran OR sanctions OR oil OR energy OR Gulf when:3d"),
        "default_category": "official_west",
        "keywords": [
            "Iran", "sanctions", "oil", "energy", "Gulf",
            "nuclear", "OPEC", "Middle East", "Hormuz",
        ],
        "base_priority": 2,
    },
    {
        "name": "State Dept",
        "url": _google_news_rss("site:state.gov Iran OR sanctions OR oil OR energy OR Gulf when:3d"),
        "default_category": "official_west",
        "keywords": [
            "Iran", "sanctions", "oil", "energy", "Gulf",
            "nuclear", "JCPOA", "Middle East", "Hormuz",
        ],
        "base_priority": 2,
    },

    # ---- 新增：伊朗官方/反对视角 ----
    {
        "name": "Iran International",
        "url": _google_news_rss("site:iranintl.com when:2d"),
        "default_category": "official_iran",
        "keywords": None,           # 专门报道伊朗的英文媒体，全量收录
        "base_priority": 2,
    },

    # ---- 新增：中东地区视角 ----
    {
        "name": "The National",
        "url": _google_news_rss(
            "site:thenationalnews.com Iran OR Israel OR Houthi OR Hormuz OR oil when:2d"
        ),
        "default_category": "conflict",
        "keywords": [
            "Iran", "Israel", "Houthi", "Hormuz", "oil",
            "IRGC", "Yemen", "Red Sea", "tanker", "sanctions",
        ],
        "base_priority": 4,
    },

    # ---- 新增：原油能源专项 ----
    {
        "name": "Reuters Energy",
        "url": _google_news_rss(
            "site:reuters.com (oil OR crude OR Brent OR WTI OR OPEC OR energy) when:3d"
        ),
        "default_category": "crude",
        "keywords": [
            "oil", "crude", "Brent", "WTI", "OPEC",
            "energy", "barrel", "petroleum", "LNG", "refinery",
        ],
        "base_priority": 4,
    },

    # ---- 新增：航运/霍尔木兹专项聚合 ----
    {
        "name": "Shipping & Hormuz",
        "url": _google_news_rss(
            'Hormuz OR tanker OR "oil shipment" OR "Red Sea shipping" OR "Strait of Hormuz" when:3d'
        ),
        "default_category": "shipping",
        "keywords": [
            "Hormuz", "tanker", "shipping", "shipment",
            "Red Sea", "Suez", "strait", "vessel", "oil cargo",
        ],
        "base_priority": 3,
    },
]

# ---------------------------------------------------------------------------
# priority 计算关键词规则
# ---------------------------------------------------------------------------

# 命中任意词 → 扣分（累加，最终 priority = max(1, base + 所有扣分之和)，注意扣分为负数）
_PRIORITY_RULES = [
    # 格式：(关键词列表, 分值调整量)
    (["Hormuz", "tanker", "shipping", "strait", "Red Sea"], -2),
    (["missile", "strike", "airstrike", "drone strike", "attack", "sanctions", "nuclear"], -2),
    (["oil", "Brent", "WTI", "OPEC", "crude", "barrel"], -1),
]


def _calc_priority(base: int, title: str, summary: str) -> int:
    """根据标题+摘要关键词调整 priority，返回 1~10 之间的整数。"""
    text = (title + " " + summary).lower()
    score = base
    for keywords, delta in _PRIORITY_RULES:
        if any(kw.lower() in text for kw in keywords):
            score += delta
    return max(1, min(10, score))


# ---------------------------------------------------------------------------
# 数据库初始化
# ---------------------------------------------------------------------------

def init_news_db(conn: sqlite3.Connection):
    """创建 crude_news 表（若不存在），并向后兼容地添加 priority 列。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crude_news (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT    NOT NULL,
            url          TEXT    UNIQUE,
            source_name  TEXT    NOT NULL,
            published_at TEXT,
            summary      TEXT,
            category     TEXT    DEFAULT 'crude',
            priority     INTEGER DEFAULT 5,
            fetched_at   TEXT    NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_news_published
        ON crude_news(published_at DESC)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_crude_news_priority
        ON crude_news(priority ASC, published_at DESC)
    """)
    conn.commit()

    # 向后兼容：若旧库中 priority 列不存在，通过 ALTER TABLE 添加
    try:
        conn.execute("ALTER TABLE crude_news ADD COLUMN priority INTEGER DEFAULT 5")
        conn.commit()
        logger.info("crude_news 表：已添加 priority 列（旧库兼容）")
    except Exception:
        # 列已存在，忽略
        pass

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
    base_priority = feed_cfg.get("base_priority", 5)

    logger.info("[%s] 开始抓取: %s", name, url[:80])
    try:
        parsed = feedparser.parse(
            url,
            request_headers={"User-Agent": "Mozilla/5.0"},
        )
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
        priority = _calc_priority(base_priority, title, summary)

        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO crude_news
                    (title, url, source_name, published_at, summary, category, priority, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (title, link, name, pub_at, summary, category, priority, fetched_at),
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
        "SELECT priority, source_name, category, title "
        "FROM crude_news ORDER BY priority ASC, published_at DESC LIMIT 15"
    ).fetchall()
    print("\n--- 最高优先级 15 条 ---")
    for r in rows:
        print(r)

    print("\n--- 各分类统计 ---")
    cats = conn.execute(
        "SELECT category, COUNT(*) FROM crude_news GROUP BY category"
    ).fetchall()
    for c in cats:
        print(c)
    conn.close()
