#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
db_schema_migrate.py — Schema migration for fund_data.db and zx_fund.db

Unifies both databases to a shared column naming convention:
  - English column names everywhere
  - Consistent nav table structure (fund_id INTEGER FK, fund_code/fund_name redundant,
    nav_date as YYYY-MM-DD, unit_nav/accum_nav/adj_nav, source_id, 录入时间, data_source)
  - funds / zx_fund_product both gain: strategy_l3, manager, custodian,
    inception_date, start_date, display
  - zx_fund_product: id → fund_id, benchmark → benchmark_index

Usage (one-time, operates in-place on the real DBs):
    python db_schema_migrate.py
    python db_schema_migrate.py --fund-db path/to/fund_data.db --zx-db path/to/zx_fund.db
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# fund_data.db migrations
# ---------------------------------------------------------------------------

def _migrate_funds_table(conn: sqlite3.Connection) -> None:
    """
    Rename Chinese columns to English, add 6 new columns.

    Renames:
      产品代码    → fund_code
      产品名称    → fund_name
      首次录入时间 → created_at

    Adds (all NULL for existing rows except display = '展示'):
      strategy_l3, manager, custodian, inception_date, start_date, display
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(funds)").fetchall()}
    if "fund_code" in cols and "产品代码" not in cols:
        logger.info("funds table already migrated — skipping")
        return
    logger.info("Migrating funds table …")

    conn.execute("""
        CREATE TABLE funds_new (
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
    """)

    conn.execute("""
        INSERT INTO funds_new
            (fund_id, fund_code, fund_name, created_at,
             benchmark_index, strategy_l1, strategy_l2,
             strategy_l3, manager, custodian, inception_date, start_date, display)
        SELECT
            fund_id,
            "产品代码",
            "产品名称",
            "首次录入时间",
            benchmark_index,
            strategy_l1,
            strategy_l2,
            NULL, NULL, NULL, NULL, NULL,
            '展示'
        FROM funds
    """)

    conn.execute("DROP TABLE funds")
    conn.execute("ALTER TABLE funds_new RENAME TO funds")
    logger.info("funds table migrated.")


def _migrate_fund_nav_data_table(conn: sqlite3.Connection) -> None:
    """
    Rename Chinese columns to English, convert nav_date YYYYMMDD → YYYY-MM-DD,
    add data_source column.

    Renames:
      产品代码      → fund_code
      产品名称      → fund_name
      净值日期      → nav_date  (also converts YYYYMMDD → YYYY-MM-DD)
      单位净值      → unit_nav
      累计单位净值  → accum_nav
      adjusted_nav  → adj_nav

    Keeps: id, fund_id, source_id, 录入时间 (unchanged)

    Adds:
      data_source TEXT  ('email' if source_id IS NOT NULL, else 'manual')
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(fund_nav_data)").fetchall()}
    if "nav_date" in cols and "净值日期" not in cols:
        logger.info("fund_nav_data table already migrated — skipping")
        return
    logger.info("Migrating fund_nav_data table …")

    conn.execute("""
        CREATE TABLE fund_nav_data_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id     INTEGER,
            fund_code   TEXT,
            fund_name   TEXT NOT NULL,
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
    """)

    conn.execute("""
        INSERT INTO fund_nav_data_new
            (id, fund_id, fund_code, fund_name, nav_date,
             unit_nav, accum_nav, "录入时间", source_id, adj_nav, data_source)
        SELECT
            id,
            fund_id,
            "产品代码",
            "产品名称",
            substr("净值日期", 1, 4) || '-' ||
            substr("净值日期", 5, 2) || '-' ||
            substr("净值日期", 7, 2),
            "单位净值",
            "累计单位净值",
            "录入时间",
            source_id,
            adjusted_nav,
            CASE WHEN source_id IS NOT NULL THEN 'email' ELSE 'manual' END
        FROM fund_nav_data
    """)

    conn.execute("DROP TABLE fund_nav_data")
    conn.execute("ALTER TABLE fund_nav_data_new RENAME TO fund_nav_data")

    # Recreate indexes
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_fund_id "
        "ON fund_nav_data(fund_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_nav_date "
        "ON fund_nav_data(nav_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_product_code "
        "ON fund_nav_data(fund_code)"
    )
    logger.info("fund_nav_data table migrated.")


def migrate_fund_data_db(db_path: str) -> None:
    """Use the shared backed-up migration used by ingestion and API startup."""
    from fund_store import initialize_database
    conn = initialize_database(db_path)
    conn.close()
    logger.info("fund_data.db shared migration complete.")


# ---------------------------------------------------------------------------
# zx_fund.db migrations
# ---------------------------------------------------------------------------

def _migrate_zx_fund_product_table(conn: sqlite3.Connection) -> None:
    """
    Rename id → fund_id, benchmark → benchmark_index.
    All other columns are already in the target schema.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(zx_fund_product)").fetchall()}
    if "fund_id" in cols and "id" not in cols:
        logger.info("zx_fund_product table already migrated — skipping")
        return
    logger.info("Migrating zx_fund_product table …")

    conn.execute("""
        CREATE TABLE zx_fund_product_new (
            fund_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code      TEXT NOT NULL UNIQUE,
            fund_name      TEXT,
            strategy_l1    TEXT,
            strategy_l2    TEXT,
            strategy_l3    TEXT,
            manager        TEXT,
            custodian      TEXT,
            inception_date TEXT,
            start_date     TEXT,
            benchmark_index TEXT,
            display        TEXT,
            created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        INSERT INTO zx_fund_product_new
            (fund_id, fund_code, fund_name,
             strategy_l1, strategy_l2, strategy_l3,
             manager, custodian, inception_date, start_date,
             benchmark_index, display, created_at)
        SELECT
            id, fund_code, fund_name,
            strategy_l1, strategy_l2, strategy_l3,
            manager, custodian, inception_date, start_date,
            benchmark, display, created_at
        FROM zx_fund_product
    """)

    conn.execute("DROP TABLE zx_fund_product")
    conn.execute("ALTER TABLE zx_fund_product_new RENAME TO zx_fund_product")
    logger.info("zx_fund_product table migrated.")


def _migrate_zx_fund_nav_table(conn: sqlite3.Connection) -> None:
    """
    Replace TEXT fund_code FK with INTEGER fund_id FK.
    Add redundant fund_code / fund_name columns (joined from zx_fund_product).
    Add source_id (NULL), 录入时间 (NULL), data_source ('zx_excel').

    zx_fund_product MUST already be migrated (fund_id column exists) before
    calling this function.
    """
    col_info = conn.execute("PRAGMA table_info(zx_fund_nav)").fetchall()
    cols = {r[1] for r in col_info}
    col_types = {r[1]: r[2] for r in col_info}
    if ("fund_id" in cols and "INT" in col_types.get("fund_id", "").upper()
            and "data_source" in cols):
        logger.info("zx_fund_nav table already migrated — skipping")
        return
    logger.info("Migrating zx_fund_nav table …")

    conn.execute("""
        CREATE TABLE zx_fund_nav_new (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id     INTEGER NOT NULL,
            fund_code   TEXT,
            fund_name   TEXT,
            nav_date    TEXT NOT NULL,
            unit_nav    REAL,
            accum_nav   REAL,
            adj_nav     REAL,
            source_id   INTEGER,
            "录入时间"  DATETIME,
            data_source TEXT,
            UNIQUE(fund_id, nav_date),
            FOREIGN KEY (fund_id) REFERENCES zx_fund_product(fund_id)
        )
    """)

    # Join on fund_code to resolve fund_id (and pull fund_name) from the
    # already-migrated zx_fund_product table.
    conn.execute("""
        INSERT INTO zx_fund_nav_new
            (id, fund_id, fund_code, fund_name,
             nav_date, unit_nav, accum_nav, adj_nav,
             source_id, "录入时间", data_source)
        SELECT
            n.id,
            p.fund_id,
            p.fund_code,
            p.fund_name,
            n.nav_date,
            n.unit_nav,
            n.accum_nav,
            n.adj_nav,
            NULL,
            NULL,
            'zx_excel'
        FROM zx_fund_nav n
        JOIN zx_fund_product p ON n.fund_code = p.fund_code
    """)

    conn.execute("DROP TABLE zx_fund_nav")
    conn.execute("ALTER TABLE zx_fund_nav_new RENAME TO zx_fund_nav")

    # Recreate indexes
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_zx_nav_fund_id_date "
        "ON zx_fund_nav(fund_id, nav_date)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_zx_nav_fund_code_date "
        "ON zx_fund_nav(fund_code, nav_date)"
    )
    logger.info("zx_fund_nav table migrated.")


def migrate_zx_fund_db(db_path: str) -> None:
    """
    Migrate zx_fund.db in-place.
    zx_fund_product must be migrated before zx_fund_nav (fund_id dependency).
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("BEGIN")
        _migrate_zx_fund_product_table(conn)
        _migrate_zx_fund_nav_table(conn)
        conn.execute("COMMIT")
        logger.info("zx_fund.db migration complete.")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Migrate fund_data.db and zx_fund.db schemas")
    parser.add_argument("--fund-db", default="fund_data.db", help="Path to fund_data.db")
    parser.add_argument("--zx-db",   default="zx_fund.db",   help="Path to zx_fund.db")
    args = parser.parse_args()

    import pathlib
    for path, label, fn in [
        (args.fund_db, "fund_data.db", migrate_fund_data_db),
        (args.zx_db,   "zx_fund.db",   migrate_zx_fund_db),
    ]:
        if not pathlib.Path(path).exists():
            print(f"SKIP {label} — file not found: {path}")
            continue
        print(f"Migrating {label} at {path} …")
        fn(path)
        print(f"  Done.")
