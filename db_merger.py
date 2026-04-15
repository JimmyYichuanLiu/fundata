#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
db_merger.py — Migrate zx_fund.db data into fund_data.db

Merges zx_fund_product → funds and zx_fund_nav → fund_nav_data:
  - fund_code is the universal business key linking both databases
  - zx data wins on all conflicts
  - field-level differences are logged to migration_conflicts
  - adj_nav is recalculated for every fund whose NAV series is changed
  - zx_fund.db is opened read-only and never modified

Usage (one-time, operates in-place on the real DBs):
    python db_merger.py
    python db_merger.py --fund-db path/to/fund_data.db --zx-db path/to/zx_fund.db
"""

import logging
import sqlite3
from typing import Optional

logger = logging.getLogger(__name__)

# Metadata fields to sync from zx_fund_product → funds
# (excludes fund_id, fund_code, created_at — created_at is preserved from email)
_FUND_FIELDS = [
    "fund_name", "strategy_l1", "strategy_l2", "strategy_l3",
    "manager", "custodian", "inception_date", "start_date",
    "benchmark_index", "display",
]


# ---------------------------------------------------------------------------
# Conflict table
# ---------------------------------------------------------------------------

def init_conflict_table(fd_conn: sqlite3.Connection) -> None:
    """
    Drop and recreate migration_conflicts in fund_data.db.

    Called at the start of every run so re-runs produce a fresh conflict log
    (idempotency: on second run the table will be empty because values already
    match zx).
    """
    fd_conn.execute("DROP TABLE IF EXISTS migration_conflicts")
    fd_conn.execute("""
        CREATE TABLE migration_conflicts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            migrated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            table_name  TEXT NOT NULL,
            fund_code   TEXT NOT NULL,
            nav_date    TEXT,
            column_name TEXT NOT NULL,
            old_value   TEXT,
            new_value   TEXT,
            resolution  TEXT NOT NULL DEFAULT 'zx_wins'
        )
    """)
    logger.info("migration_conflicts table initialised.")


def _log_conflict(
    fd_conn: sqlite3.Connection,
    table_name: str,
    fund_code: str,
    column_name: str,
    old_value,
    new_value,
    nav_date: Optional[str] = None,
) -> None:
    fd_conn.execute(
        """
        INSERT INTO migration_conflicts
            (table_name, fund_code, nav_date, column_name,
             old_value, new_value, resolution)
        VALUES (?, ?, ?, ?, ?, ?, 'zx_wins')
        """,
        (
            table_name, fund_code, nav_date, column_name,
            str(old_value) if old_value is not None else None,
            str(new_value) if new_value is not None else None,
        ),
    )


# ---------------------------------------------------------------------------
# Fund product migration
# ---------------------------------------------------------------------------

def merge_funds(
    fd_conn: sqlite3.Connection,
    zx_conn: sqlite3.Connection,
) -> dict:
    """
    Merge zx_fund_product → funds.

    - New funds (not in funds by fund_code): INSERT with full zx metadata.
    - Existing funds: UPDATE all metadata fields with zx values (zx wins);
      preserve the original created_at; log any field-level differences.

    Returns:
        {"new": int, "updated": int}
    """
    zx_funds = zx_conn.execute(
        "SELECT fund_code, fund_name, strategy_l1, strategy_l2, strategy_l3, "
        "       manager, custodian, inception_date, start_date, benchmark_index, display "
        "FROM zx_fund_product"
    ).fetchall()

    new_count = 0
    updated_count = 0

    for row in zx_funds:
        fund_code = row[0]
        zx_meta = dict(zip(
            ["fund_code"] + _FUND_FIELDS,
            row,
        ))

        existing = fd_conn.execute(
            "SELECT fund_name, strategy_l1, strategy_l2, strategy_l3, "
            "       manager, custodian, inception_date, start_date, "
            "       benchmark_index, display "
            "FROM funds WHERE fund_code=?",
            (fund_code,),
        ).fetchone()

        if existing is None:
            # ── New fund ──────────────────────────────────────────────────
            fd_conn.execute(
                """
                INSERT INTO funds
                    (fund_code, fund_name, strategy_l1, strategy_l2, strategy_l3,
                     manager, custodian, inception_date, start_date,
                     benchmark_index, display)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fund_code,
                    zx_meta["fund_name"], zx_meta["strategy_l1"], zx_meta["strategy_l2"],
                    zx_meta["strategy_l3"], zx_meta["manager"], zx_meta["custodian"],
                    zx_meta["inception_date"], zx_meta["start_date"],
                    zx_meta["benchmark_index"], zx_meta["display"],
                ),
            )
            new_count += 1
            logger.debug("Inserted new fund: %s", fund_code)

        else:
            # ── Existing fund — log conflicts, then overwrite ─────────────
            fd_meta = dict(zip(_FUND_FIELDS, existing))

            for field in _FUND_FIELDS:
                old_val = fd_meta[field]
                new_val = zx_meta[field]
                if old_val != new_val:
                    _log_conflict(fd_conn, "funds", fund_code, field, old_val, new_val)

            # Always apply zx values; created_at is NOT in the SET clause
            fd_conn.execute(
                """
                UPDATE funds SET
                    fund_name       = ?,
                    strategy_l1     = ?,
                    strategy_l2     = ?,
                    strategy_l3     = ?,
                    manager         = ?,
                    custodian       = ?,
                    inception_date  = ?,
                    start_date      = ?,
                    benchmark_index = ?,
                    display         = ?
                WHERE fund_code = ?
                """,
                (
                    zx_meta["fund_name"], zx_meta["strategy_l1"], zx_meta["strategy_l2"],
                    zx_meta["strategy_l3"], zx_meta["manager"], zx_meta["custodian"],
                    zx_meta["inception_date"], zx_meta["start_date"],
                    zx_meta["benchmark_index"], zx_meta["display"],
                    fund_code,
                ),
            )
            updated_count += 1
            logger.debug("Updated fund: %s", fund_code)

    logger.info("merge_funds: new=%d, updated=%d", new_count, updated_count)
    return {"new": new_count, "updated": updated_count}


# ---------------------------------------------------------------------------
# adj_nav recalculation
# ---------------------------------------------------------------------------

def _recalculate_adj_nav(fd_conn: sqlite3.Connection, fund_code: str) -> None:
    """
    Recompute the full adj_nav series for fund_code.

    Formula (identical to zx_importer.compute_adj_nav):
        adj[0] = 1.0
        adj[i] = adj[i-1] * (unit[i-1] + accum[i] - accum[i-1]) / unit[i-1]
        When unit[i-1] == 0: carry adj[i-1] forward (avoid division by zero).

    All rows are updated in a single pass ordered by nav_date ASC.
    """
    rows = fd_conn.execute(
        "SELECT id, unit_nav, accum_nav FROM fund_nav_data "
        "WHERE fund_code=? ORDER BY nav_date",
        (fund_code,),
    ).fetchall()

    if not rows:
        return

    ids = [r[0] for r in rows]
    unit_navs = [r[1] for r in rows]
    accum_navs = [r[2] if r[2] is not None else r[1] for r in rows]

    n = len(rows)
    adj = [0.0] * n
    adj[0] = 1.0

    for i in range(1, n):
        prev_unit = unit_navs[i - 1]
        if prev_unit == 0:
            adj[i] = adj[i - 1]
        else:
            adj[i] = (
                adj[i - 1]
                * (prev_unit + accum_navs[i] - accum_navs[i - 1])
                / prev_unit
            )

    for row_id, adj_val in zip(ids, adj):
        fd_conn.execute(
            "UPDATE fund_nav_data SET adj_nav=? WHERE id=?",
            (adj_val, row_id),
        )


# ---------------------------------------------------------------------------
# NAV migration
# ---------------------------------------------------------------------------

def merge_nav_data(
    fd_conn: sqlite3.Connection,
    zx_conn: sqlite3.Connection,
) -> dict:
    """
    Merge zx_fund_nav → fund_nav_data.

    For each row in zx_fund_nav:
    - Look up fund_id in fund_data.db's funds table by fund_code (do NOT copy
      zx's fund_id — the two databases have independent AUTOINCREMENT sequences).
    - INSERT if (fund_code, nav_date) is new; count as "inserted".
    - UPDATE if exists: overwrite unit_nav / accum_nav, set source_id=NULL and
      data_source='zx_excel' (Option A); count as "updated".
      - If unit_nav differs from the stored value: log a conflict.

    After all inserts/updates, recalculate adj_nav for every affected fund.

    Returns:
        {"inserted": int, "updated": int, "value_conflicts": int}
    """
    zx_rows = zx_conn.execute(
        "SELECT fund_code, fund_name, nav_date, unit_nav, accum_nav "
        "FROM zx_fund_nav "
        "ORDER BY fund_code, nav_date"
    ).fetchall()

    inserted = 0
    updated = 0
    value_conflicts = 0
    affected_funds: set = set()

    for fund_code, zx_fund_name, nav_date, unit_nav, accum_nav in zx_rows:
        # Resolve fund_id from fund_data.db (re-mapped, not from zx)
        fund_row = fd_conn.execute(
            "SELECT fund_id, fund_name FROM funds WHERE fund_code=?",
            (fund_code,),
        ).fetchone()
        if fund_row is None:
            logger.warning("fund_code %s not found in funds — skipping NAV row %s",
                           fund_code, nav_date)
            continue
        fd_fund_id, fd_fund_name = fund_row

        existing = fd_conn.execute(
            "SELECT id, unit_nav FROM fund_nav_data "
            "WHERE fund_code=? AND nav_date=?",
            (fund_code, nav_date),
        ).fetchone()

        if existing is None:
            # ── New row ───────────────────────────────────────────────────
            fd_conn.execute(
                """
                INSERT INTO fund_nav_data
                    (fund_id, fund_code, fund_name, nav_date,
                     unit_nav, accum_nav, source_id, data_source)
                VALUES (?, ?, ?, ?, ?, ?, NULL, 'zx_excel')
                """,
                (fd_fund_id, fund_code, fd_fund_name, nav_date, unit_nav, accum_nav),
            )
            inserted += 1
            affected_funds.add(fund_code)

        else:
            existing_id, existing_unit_nav = existing

            # Detect value conflict (unit_nav differs)
            if abs(existing_unit_nav - unit_nav) > 1e-9:
                _log_conflict(
                    fd_conn, "fund_nav_data", fund_code,
                    "unit_nav", existing_unit_nav, unit_nav,
                    nav_date=nav_date,
                )
                value_conflicts += 1

            # Overwrite with zx values; clear source provenance (Option A)
            fd_conn.execute(
                """
                UPDATE fund_nav_data SET
                    fund_id     = ?,
                    fund_name   = ?,
                    unit_nav    = ?,
                    accum_nav   = ?,
                    source_id   = NULL,
                    data_source = 'zx_excel'
                WHERE id = ?
                """,
                (fd_fund_id, fd_fund_name, unit_nav, accum_nav, existing_id),
            )
            updated += 1
            affected_funds.add(fund_code)

    logger.info("merge_nav_data: inserted=%d, updated=%d, value_conflicts=%d",
                inserted, updated, value_conflicts)

    # Recalculate adj_nav for every fund whose NAV series changed
    for fund_code in affected_funds:
        _recalculate_adj_nav(fd_conn, fund_code)
        logger.debug("Recalculated adj_nav for %s", fund_code)

    return {"inserted": inserted, "updated": updated, "value_conflicts": value_conflicts}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_migration(fund_data_db: str, zx_fund_db: str) -> dict:
    """
    Migrate all data from zx_fund.db into fund_data.db.

    zx_fund.db is opened read-only (URI mode, never written to).
    fund_data.db is modified in-place within a single transaction; rolled back
    completely on any error.

    Args:
        fund_data_db: Path to fund_data.db (target, modified in-place).
        zx_fund_db:   Path to zx_fund.db (source, read-only).

    Returns:
        {
            "funds": {"new": int, "updated": int},
            "nav":   {"inserted": int, "updated": int, "value_conflicts": int},
        }
    """
    zx_conn = sqlite3.connect(f"file:{zx_fund_db}?mode=ro", uri=True)
    fd_conn = sqlite3.connect(fund_data_db)
    try:
        fd_conn.execute("PRAGMA journal_mode=WAL")
        fd_conn.execute("BEGIN")

        init_conflict_table(fd_conn)
        fund_stats = merge_funds(fd_conn, zx_conn)
        nav_stats  = merge_nav_data(fd_conn, zx_conn)

        fd_conn.execute("COMMIT")
        logger.info("Migration complete: funds=%s  nav=%s", fund_stats, nav_stats)

    except Exception:
        fd_conn.execute("ROLLBACK")
        raise
    finally:
        zx_conn.close()
        fd_conn.close()

    return {"funds": fund_stats, "nav": nav_stats}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import pathlib

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Migrate zx_fund.db data into fund_data.db"
    )
    parser.add_argument("--fund-db", default="fund_data.db", help="Path to fund_data.db")
    parser.add_argument("--zx-db",   default="zx_fund.db",   help="Path to zx_fund.db")
    args = parser.parse_args()

    for label, path in [("fund_data.db", args.fund_db), ("zx_fund.db", args.zx_db)]:
        if not pathlib.Path(path).exists():
            print(f"ERROR: {label} not found at {path}")
            raise SystemExit(1)

    print(f"Migrating {args.zx_db} → {args.fund_db} …")
    result = run_migration(args.fund_db, args.zx_db)
    print("Done.")
    print(f"  Funds : new={result['funds']['new']}, updated={result['funds']['updated']}")
    print(f"  NAV   : inserted={result['nav']['inserted']}, "
          f"updated={result['nav']['updated']}, "
          f"value_conflicts={result['nav']['value_conflicts']}")
