"""
tests/test_db_merger.py

TDD tests for db_merger.py

All test databases are built from scratch using known values — no real DB files
are copied.  Run BEFORE the implementation (all should ERROR / FAIL initially).

Scenarios covered:
  OVERLAP_CODE  — fund that exists in BOTH databases, with metadata and NAV conflicts
  EMAIL_ONLY    — fund only in fund_data.db  (must remain untouched)
  ZX_ONLY       — fund only in zx_fund.db   (must be inserted)

Usage:
    pytest tests/test_db_merger.py -v
"""

import sqlite3
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Test-data constants
# ---------------------------------------------------------------------------

OVERLAP_CODE   = "T_OVERLAP1"
EMAIL_ONLY     = "T_EMAIL_ONLY1"
ZX_ONLY        = "T_ZX_ONLY1"

# fund_data.db —— email-sourced records
FD_OVERLAP_META = dict(
    fund_id=1, fund_code=OVERLAP_CODE, fund_name="重叠测试基金",
    strategy_l1=None, strategy_l2=None, display="展示",
    created_at="2024-01-01 00:00:00",
)
FD_EMAIL_ONLY_META = dict(
    fund_id=2, fund_code=EMAIL_ONLY, fund_name="邮件专属基金",
    strategy_l1=None, strategy_l2=None, display="展示",
    created_at="2024-01-15 00:00:00",
)

# zx_fund.db —— zx-sourced records
ZX_OVERLAP_META = dict(
    fund_id=5,   # different PK in zx world
    fund_code=OVERLAP_CODE, fund_name="重叠测试基金（完整版)",
    strategy_l1="量化股票", strategy_l2="量化选时",
    display="展示", created_at="2023-06-01 00:00:00",
)
ZX_ZX_ONLY_META = dict(
    fund_id=6,
    fund_code=ZX_ONLY, fund_name="ZX专属测试基金",
    strategy_l1="固收", strategy_l2=None,
    display="展示", created_at="2023-09-01 00:00:00",
)

# NAV rows — (nav_date, unit_nav, accum_nav, adj_nav)
# fund_data.db — OVERLAP email nav
#   2024-03-01 : same unit_nav as zx  → no value conflict
#   2024-03-02 : unit_nav 1.0400 vs zx 1.0450  → VALUE CONFLICT
FD_OVERLAP_NAV = [
    ("2024-03-01", 1.0300, 1.0300, 1.0000),
    ("2024-03-02", 1.0400, 1.0400, 1.0097),   # ← will conflict with zx
]
FD_EMAIL_ONLY_NAV = [
    ("2024-03-01", 1.1500, 1.1500, 1.0000),
]

# zx_fund.db — OVERLAP nav
#   2024-02-15 : new date (not in email)
#   2024-03-01 : same unit_nav as email
#   2024-03-02 : unit_nav 1.0450 ≠ email 1.0400  → conflict
ZX_OVERLAP_NAV = [
    ("2024-02-15", 1.0100, 1.0100, 1.0000),
    ("2024-03-01", 1.0300, 1.0300, 1.0198),
    ("2024-03-02", 1.0450, 1.0450, 1.0347),
]
ZX_ZX_ONLY_NAV = [
    ("2024-03-01", 2.0000, 2.0000, 1.0000),
    ("2024-03-02", 2.0500, 2.0500, 1.0250),
]

# Expected adj_nav values after recalculation (no-dividend funds)
# OVERLAP1 full series after merge: [2024-02-15, 2024-03-01, 2024-03-02]
# unit_navs = [1.01, 1.03, 1.045]
# adj[0]=1.0, adj[1]=1.03/1.01≈1.019802, adj[2]=1.045/1.01≈1.034653
OVERLAP_RECALC_ADJ = [1.0, 1.03 / 1.01, 1.045 / 1.01]   # in nav_date order

# ZX_ONLY full series: [2024-03-01, 2024-03-02]
# unit_navs = [2.00, 2.05]
# adj[0]=1.0, adj[1]=2.05/2.00=1.025
ZX_ONLY_RECALC_ADJ = [1.0, 2.05 / 2.00]


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _create_fund_data_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE funds (
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
        );
        CREATE TABLE fund_nav_data (
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
        );
    """)
    conn.commit()


def _create_zx_schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE zx_fund_product (
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
        );
        CREATE TABLE zx_fund_nav (
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
        );
    """)
    conn.commit()


def _populate_fund_data_db(conn: sqlite3.Connection) -> None:
    """Insert the two pre-existing email-sourced funds and their nav data."""
    for m in (FD_OVERLAP_META, FD_EMAIL_ONLY_META):
        conn.execute(
            "INSERT INTO funds "
            "(fund_id, fund_code, fund_name, strategy_l1, strategy_l2, display, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (m["fund_id"], m["fund_code"], m["fund_name"],
             m["strategy_l1"], m["strategy_l2"], m["display"], m["created_at"]),
        )

    for date, unit, accum, adj in FD_OVERLAP_NAV:
        conn.execute(
            "INSERT INTO fund_nav_data "
            "(fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav, "
            " source_id, data_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (FD_OVERLAP_META["fund_id"], OVERLAP_CODE, FD_OVERLAP_META["fund_name"],
             date, unit, accum, adj, 1, "email"),
        )
    for date, unit, accum, adj in FD_EMAIL_ONLY_NAV:
        conn.execute(
            "INSERT INTO fund_nav_data "
            "(fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav, "
            " source_id, data_source) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (FD_EMAIL_ONLY_META["fund_id"], EMAIL_ONLY, FD_EMAIL_ONLY_META["fund_name"],
             date, unit, accum, adj, 2, "email"),
        )
    conn.commit()


def _populate_zx_fund_db(conn: sqlite3.Connection) -> None:
    """Insert the two zx-sourced funds and their nav data."""
    for m in (ZX_OVERLAP_META, ZX_ZX_ONLY_META):
        conn.execute(
            "INSERT INTO zx_fund_product "
            "(fund_id, fund_code, fund_name, strategy_l1, strategy_l2, display, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (m["fund_id"], m["fund_code"], m["fund_name"],
             m["strategy_l1"], m["strategy_l2"], m["display"], m["created_at"]),
        )

    for meta, nav_rows in (
        (ZX_OVERLAP_META, ZX_OVERLAP_NAV),
        (ZX_ZX_ONLY_META, ZX_ZX_ONLY_NAV),
    ):
        for date, unit, accum, adj in nav_rows:
            conn.execute(
                "INSERT INTO zx_fund_nav "
                "(fund_id, fund_code, fund_name, nav_date, unit_nav, accum_nav, adj_nav, "
                " data_source) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (meta["fund_id"], meta["fund_code"], meta["fund_name"],
                 date, unit, accum, adj, "zx_excel"),
            )
    conn.commit()


def _build_fund_data_db(path: str) -> None:
    conn = sqlite3.connect(path)
    _create_fund_data_schema(conn)
    _populate_fund_data_db(conn)
    conn.close()


def _build_zx_fund_db(path: str) -> None:
    conn = sqlite3.connect(path)
    _create_zx_schema(conn)
    _populate_zx_fund_db(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def zx_db(tmp_path_factory):
    """Build a zx_fund.db once for the whole module (shared read-only source)."""
    p = tmp_path_factory.mktemp("zx") / "zx_fund.db"
    _build_zx_fund_db(str(p))
    return str(p)


@pytest.fixture(scope="module")
def merged(tmp_path_factory, zx_db):
    """
    Build a fresh fund_data.db, run the migration once, return
    (fd_path, zx_path, stats).
    """
    from db_merger import run_migration
    fd = tmp_path_factory.mktemp("fd") / "fund_data.db"
    _build_fund_data_db(str(fd))
    stats = run_migration(str(fd), zx_db)
    return str(fd), zx_db, stats


@pytest.fixture(scope="module")
def fd_path(merged):
    return merged[0]


@pytest.fixture(scope="module")
def zx_path(merged):
    return merged[1]


@pytest.fixture(scope="module")
def stats(merged):
    return merged[2]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def qrow(conn, sql, *args):
    return conn.execute(sql, args).fetchone()


def qval(conn, sql, *args):
    row = conn.execute(sql, args).fetchone()
    return row[0] if row else None


def get_columns(conn, table):
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


# ===========================================================================
# 1. Conflict table initialisation
# ===========================================================================

class TestInitConflictTable:

    def test_conflict_table_exists(self, fd_path):
        conn = sqlite3.connect(fd_path)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        conn.close()
        assert "migration_conflicts" in tables

    def test_conflict_table_has_required_columns(self, fd_path):
        conn = sqlite3.connect(fd_path)
        cols = get_columns(conn, "migration_conflicts")
        conn.close()
        required = {
            "id", "migrated_at", "table_name", "fund_code",
            "nav_date", "column_name", "old_value", "new_value", "resolution",
        }
        assert required.issubset(cols), f"Missing: {required - cols}"


# ===========================================================================
# 2. Fund product migration
# ===========================================================================

class TestMergeFunds:

    def test_total_fund_count(self, fd_path):
        """After merge: OVERLAP1 + EMAIL_ONLY1 + ZX_ONLY1 = 3 funds."""
        conn = sqlite3.connect(fd_path)
        n = qval(conn, "SELECT COUNT(*) FROM funds")
        conn.close()
        assert n == 3

    def test_new_fund_inserted(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn, "SELECT COUNT(*) FROM funds WHERE fund_code=?", ZX_ONLY)
        conn.close()
        assert n == 1, f"{ZX_ONLY} should be inserted into funds"

    def test_new_fund_carries_zx_metadata(self, fd_path):
        conn = sqlite3.connect(fd_path)
        row = qrow(conn,
            "SELECT fund_name, strategy_l1, display FROM funds WHERE fund_code=?",
            ZX_ONLY)
        conn.close()
        assert row[0] == ZX_ZX_ONLY_META["fund_name"]
        assert row[1] == ZX_ZX_ONLY_META["strategy_l1"]

    def test_overlap_fund_updated_with_zx_name(self, fd_path):
        conn = sqlite3.connect(fd_path)
        name = qval(conn, "SELECT fund_name FROM funds WHERE fund_code=?", OVERLAP_CODE)
        conn.close()
        assert name == ZX_OVERLAP_META["fund_name"], \
            f"Expected zx fund_name, got {name!r}"

    def test_overlap_fund_updated_with_zx_strategy(self, fd_path):
        conn = sqlite3.connect(fd_path)
        row = qrow(conn,
            "SELECT strategy_l1, strategy_l2 FROM funds WHERE fund_code=?",
            OVERLAP_CODE)
        conn.close()
        assert row[0] == ZX_OVERLAP_META["strategy_l1"]
        assert row[1] == ZX_OVERLAP_META["strategy_l2"]

    def test_overlap_fund_created_at_preserved(self, fd_path):
        """created_at must stay as the original email date, not the zx date."""
        conn = sqlite3.connect(fd_path)
        ca = qval(conn, "SELECT created_at FROM funds WHERE fund_code=?", OVERLAP_CODE)
        conn.close()
        # email created_at = "2024-01-01 00:00:00"; zx = "2023-06-01 ..."
        assert ca.startswith("2024-01-01"), \
            f"created_at should be preserved as email date, got {ca!r}"

    def test_email_only_fund_untouched(self, fd_path):
        conn = sqlite3.connect(fd_path)
        row = qrow(conn,
            "SELECT fund_name, strategy_l1, created_at FROM funds WHERE fund_code=?",
            EMAIL_ONLY)
        conn.close()
        assert row[0] == FD_EMAIL_ONLY_META["fund_name"]
        assert row[1] is None
        assert row[2].startswith("2024-01-15")

    def test_fund_conflict_logged_for_fund_name(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='funds' AND fund_code=? AND column_name='fund_name'",
            OVERLAP_CODE)
        conn.close()
        assert n == 1, "fund_name conflict should be logged for OVERLAP fund"

    def test_fund_conflict_old_and_new_values_correct(self, fd_path):
        conn = sqlite3.connect(fd_path)
        row = qrow(conn,
            "SELECT old_value, new_value, resolution FROM migration_conflicts "
            "WHERE table_name='funds' AND fund_code=? AND column_name='fund_name'",
            OVERLAP_CODE)
        conn.close()
        assert row[0] == FD_OVERLAP_META["fund_name"]
        assert row[1] == ZX_OVERLAP_META["fund_name"]
        assert row[2] == "zx_wins"

    def test_fund_conflict_logged_for_strategy_l1(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='funds' AND fund_code=? AND column_name='strategy_l1'",
            OVERLAP_CODE)
        conn.close()
        assert n == 1

    def test_fund_conflict_logged_for_strategy_l2(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='funds' AND fund_code=? AND column_name='strategy_l2'",
            OVERLAP_CODE)
        conn.close()
        assert n == 1

    def test_no_conflict_logged_for_email_only_fund(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='funds' AND fund_code=?",
            EMAIL_ONLY)
        conn.close()
        assert n == 0, "email-only fund should generate no fund-level conflicts"

    def test_no_fund_conflict_logged_for_new_fund(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='funds' AND fund_code=?",
            ZX_ONLY)
        conn.close()
        assert n == 0, "newly inserted fund should generate no conflicts"


# ===========================================================================
# 3. NAV data migration
# ===========================================================================

class TestMergeNavData:

    def test_total_nav_count(self, fd_path):
        """
        OVERLAP (3 rows) + EMAIL_ONLY (1 row) + ZX_ONLY (2 rows) = 6 rows total.
        """
        conn = sqlite3.connect(fd_path)
        n = qval(conn, "SELECT COUNT(*) FROM fund_nav_data")
        conn.close()
        assert n == 6

    def test_earlier_zx_date_inserted_for_overlap(self, fd_path):
        """2024-02-15 from zx must now exist in fund_nav_data for OVERLAP1."""
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-02-15")
        conn.close()
        assert n == 1

    def test_zx_only_nav_inserted(self, fd_path):
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM fund_nav_data WHERE fund_code=?",
            ZX_ONLY)
        conn.close()
        assert n == 2

    def test_zx_only_nav_fund_id_from_funds_table(self, fd_path):
        """
        ZX_ONLY's fund_id in zx_fund.db is 6, but in fund_data.db it must
        be the newly-assigned fund_id from the funds table (3, after 1 and 2).
        """
        conn = sqlite3.connect(fd_path)
        nav_fid  = qval(conn,
            "SELECT fund_id FROM fund_nav_data WHERE fund_code=? LIMIT 1",
            ZX_ONLY)
        fund_fid = qval(conn,
            "SELECT fund_id FROM funds WHERE fund_code=?",
            ZX_ONLY)
        conn.close()
        assert nav_fid is not None
        assert nav_fid == fund_fid, \
            f"nav.fund_id ({nav_fid}) must equal funds.fund_id ({fund_fid}), not zx's id 6"
        assert nav_fid != 6, "must NOT use zx's fund_id directly"

    def test_new_nav_data_source_is_zx_excel(self, fd_path):
        """Newly inserted zx rows must have data_source='zx_excel'."""
        conn = sqlite3.connect(fd_path)
        # The 2024-02-15 row for OVERLAP1 is new
        ds = qval(conn,
            "SELECT data_source FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-02-15")
        conn.close()
        assert ds == "zx_excel"

    def test_new_nav_source_id_is_null(self, fd_path):
        conn = sqlite3.connect(fd_path)
        sid = qval(conn,
            "SELECT source_id FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-02-15")
        conn.close()
        assert sid is None

    def test_conflicting_nav_zx_unit_nav_wins(self, fd_path):
        """2024-03-02 unit_nav must be 1.0450 (zx), not 1.0400 (email)."""
        conn = sqlite3.connect(fd_path)
        unit = qval(conn,
            "SELECT unit_nav FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-03-02")
        conn.close()
        assert abs(unit - 1.0450) < 1e-9, f"Expected 1.0450, got {unit}"

    def test_conflicting_nav_source_id_cleared(self, fd_path):
        """After zx overwrites, source_id must be NULL (Option A)."""
        conn = sqlite3.connect(fd_path)
        sid = qval(conn,
            "SELECT source_id FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-03-02")
        conn.close()
        assert sid is None, f"source_id should be NULL, got {sid}"

    def test_conflicting_nav_data_source_updated(self, fd_path):
        conn = sqlite3.connect(fd_path)
        ds = qval(conn,
            "SELECT data_source FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-03-02")
        conn.close()
        assert ds == "zx_excel"

    def test_non_conflicting_overlap_row_source_id_cleared(self, fd_path):
        """2024-03-01 has same unit_nav but is still overwritten by zx → source_id NULL."""
        conn = sqlite3.connect(fd_path)
        sid = qval(conn,
            "SELECT source_id FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-03-01")
        conn.close()
        assert sid is None

    def test_nav_value_conflict_logged(self, fd_path):
        """unit_nav difference on 2024-03-02 must appear in migration_conflicts."""
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='fund_nav_data' AND fund_code=? "
            "AND nav_date='2024-03-02' AND column_name='unit_nav'",
            OVERLAP_CODE)
        conn.close()
        assert n == 1

    def test_nav_conflict_old_and_new_values_correct(self, fd_path):
        conn = sqlite3.connect(fd_path)
        row = qrow(conn,
            "SELECT old_value, new_value, resolution FROM migration_conflicts "
            "WHERE table_name='fund_nav_data' AND fund_code=? "
            "AND nav_date='2024-03-02' AND column_name='unit_nav'",
            OVERLAP_CODE)
        conn.close()
        assert abs(float(row[0]) - 1.0400) < 1e-9, f"old_value wrong: {row[0]}"
        assert abs(float(row[1]) - 1.0450) < 1e-9, f"new_value wrong: {row[1]}"
        assert row[2] == "zx_wins"

    def test_no_nav_conflict_logged_for_same_values(self, fd_path):
        """2024-03-01 has same unit_nav → no conflict entry."""
        conn = sqlite3.connect(fd_path)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts "
            "WHERE table_name='fund_nav_data' AND fund_code=? AND nav_date='2024-03-01'",
            OVERLAP_CODE)
        conn.close()
        assert n == 0, "Identical values should not be logged as conflicts"

    def test_email_only_nav_unit_nav_unchanged(self, fd_path):
        conn = sqlite3.connect(fd_path)
        unit = qval(conn,
            "SELECT unit_nav FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            EMAIL_ONLY, "2024-03-01")
        conn.close()
        assert abs(unit - 1.1500) < 1e-9

    def test_email_only_nav_source_id_preserved(self, fd_path):
        conn = sqlite3.connect(fd_path)
        sid = qval(conn,
            "SELECT source_id FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            EMAIL_ONLY, "2024-03-01")
        conn.close()
        assert sid == 2, f"email-only source_id must be preserved, got {sid}"

    def test_email_only_nav_data_source_preserved(self, fd_path):
        conn = sqlite3.connect(fd_path)
        ds = qval(conn,
            "SELECT data_source FROM fund_nav_data WHERE fund_code=? AND nav_date=?",
            EMAIL_ONLY, "2024-03-01")
        conn.close()
        assert ds == "email"

    def test_no_duplicate_nav_rows(self, fd_path):
        conn = sqlite3.connect(fd_path)
        dupes = qval(conn, """
            SELECT COUNT(*) FROM (
                SELECT fund_code, nav_date, COUNT(*) cnt
                FROM fund_nav_data GROUP BY fund_code, nav_date HAVING cnt > 1
            )
        """)
        conn.close()
        assert dupes == 0


# ===========================================================================
# 4. adj_nav recalculation
# ===========================================================================

class TestAdjNavRecalculation:

    def _get_adj_series(self, conn, fund_code):
        rows = conn.execute(
            "SELECT adj_nav FROM fund_nav_data "
            "WHERE fund_code=? ORDER BY nav_date",
            (fund_code,)
        ).fetchall()
        return [r[0] for r in rows]

    def test_overlap_first_adj_nav_is_one(self, fd_path):
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, OVERLAP_CODE)
        conn.close()
        assert abs(adj[0] - 1.0) < 1e-9, f"First adj_nav must be 1.0, got {adj[0]}"

    def test_overlap_adj_nav_middle_row(self, fd_path):
        """2024-03-01: adj = 1.03/1.01 ≈ 1.019802."""
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, OVERLAP_CODE)
        conn.close()
        expected = OVERLAP_RECALC_ADJ[1]
        assert abs(adj[1] - expected) < 1e-6, \
            f"adj[1] expected {expected:.8f}, got {adj[1]:.8f}"

    def test_overlap_adj_nav_last_row(self, fd_path):
        """2024-03-02: adj = 1.045/1.01 ≈ 1.034653."""
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, OVERLAP_CODE)
        conn.close()
        expected = OVERLAP_RECALC_ADJ[2]
        assert abs(adj[2] - expected) < 1e-6, \
            f"adj[2] expected {expected:.8f}, got {adj[2]:.8f}"

    def test_overlap_adj_nav_series_length(self, fd_path):
        """OVERLAP1 must have 3 nav rows after merge."""
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, OVERLAP_CODE)
        conn.close()
        assert len(adj) == 3

    def test_email_only_adj_nav_unchanged(self, fd_path):
        """EMAIL_ONLY1 has no zx data, adj_nav must remain 1.0000."""
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, EMAIL_ONLY)
        conn.close()
        assert len(adj) == 1
        assert abs(adj[0] - 1.0) < 1e-9

    def test_zx_only_adj_nav_first_row_is_one(self, fd_path):
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, ZX_ONLY)
        conn.close()
        assert abs(adj[0] - 1.0) < 1e-9

    def test_zx_only_adj_nav_second_row(self, fd_path):
        """ZX_ONLY1 2024-03-02: adj = 2.05/2.00 = 1.025."""
        conn = sqlite3.connect(fd_path)
        adj = self._get_adj_series(conn, ZX_ONLY)
        conn.close()
        expected = ZX_ONLY_RECALC_ADJ[1]
        assert abs(adj[1] - expected) < 1e-6

    def test_all_adj_nav_positive(self, fd_path):
        conn = sqlite3.connect(fd_path)
        bad = qval(conn, "SELECT COUNT(*) FROM fund_nav_data WHERE adj_nav <= 0")
        conn.close()
        assert bad == 0


# ===========================================================================
# 5. zx_fund.db read-only guarantee
# ===========================================================================

class TestZxDatabaseReadOnly:

    def test_zx_fund_product_row_count_unchanged(self, zx_path):
        conn = sqlite3.connect(zx_path)
        n = qval(conn, "SELECT COUNT(*) FROM zx_fund_product")
        conn.close()
        assert n == 2

    def test_zx_fund_nav_row_count_unchanged(self, zx_path):
        conn = sqlite3.connect(zx_path)
        n = qval(conn, "SELECT COUNT(*) FROM zx_fund_nav")
        conn.close()
        assert n == len(ZX_OVERLAP_NAV) + len(ZX_ZX_ONLY_NAV)

    def test_zx_overlap_meta_unchanged(self, zx_path):
        conn = sqlite3.connect(zx_path)
        row = qrow(conn,
            "SELECT fund_name, strategy_l1, created_at FROM zx_fund_product WHERE fund_code=?",
            OVERLAP_CODE)
        conn.close()
        assert row[0] == ZX_OVERLAP_META["fund_name"]
        assert row[1] == ZX_OVERLAP_META["strategy_l1"]
        assert row[2].startswith("2023-06-01")

    def test_zx_overlap_nav_values_unchanged(self, zx_path):
        """The conflict row 2024-03-02 in zx must still have unit_nav=1.0450."""
        conn = sqlite3.connect(zx_path)
        unit = qval(conn,
            "SELECT unit_nav FROM zx_fund_nav WHERE fund_code=? AND nav_date=?",
            OVERLAP_CODE, "2024-03-02")
        conn.close()
        assert abs(unit - 1.0450) < 1e-9


# ===========================================================================
# 6. Migration stats
# ===========================================================================

class TestMigrationStats:

    def test_stats_has_required_keys(self, stats):
        assert "funds" in stats
        assert "nav" in stats
        for key in ("new", "updated"):
            assert key in stats["funds"], f"stats.funds missing key: {key}"
        for key in ("inserted", "updated", "value_conflicts"):
            assert key in stats["nav"], f"stats.nav missing key: {key}"

    def test_stats_new_funds_count(self, stats):
        assert stats["funds"]["new"] == 1, \
            f"Expected 1 new fund (ZX_ONLY), got {stats['funds']['new']}"

    def test_stats_updated_funds_count(self, stats):
        assert stats["funds"]["updated"] == 1, \
            f"Expected 1 updated fund (OVERLAP), got {stats['funds']['updated']}"

    def test_stats_nav_inserted(self, stats):
        """2024-02-15 (overlap early), 2024-03-01 and 2024-03-02 (ZX_ONLY) = 3 new rows."""
        assert stats["nav"]["inserted"] == 3, \
            f"Expected 3 inserted nav rows, got {stats['nav']['inserted']}"

    def test_stats_nav_updated(self, stats):
        """2024-03-01 and 2024-03-02 for OVERLAP1 = 2 updated rows."""
        assert stats["nav"]["updated"] == 2, \
            f"Expected 2 updated nav rows, got {stats['nav']['updated']}"

    def test_stats_nav_value_conflicts(self, stats):
        """Only 2024-03-02 for OVERLAP1 has a value difference."""
        assert stats["nav"]["value_conflicts"] == 1, \
            f"Expected 1 value conflict, got {stats['nav']['value_conflicts']}"


# ===========================================================================
# 7. Idempotency — second run on the same fund_data.db
# ===========================================================================

@pytest.fixture(scope="module")
def merged_twice(tmp_path_factory, zx_db):
    """Build a fresh fund_data.db, run migration TWICE, return (fd_path, stats2)."""
    from db_merger import run_migration
    fd = tmp_path_factory.mktemp("fd2") / "fund_data.db"
    _build_fund_data_db(str(fd))
    run_migration(str(fd), zx_db)        # first run
    stats2 = run_migration(str(fd), zx_db)  # second run
    return str(fd), stats2


class TestIdempotency:

    def test_second_run_fund_count_unchanged(self, merged_twice):
        fd, _ = merged_twice
        conn = sqlite3.connect(fd)
        n = qval(conn, "SELECT COUNT(*) FROM funds")
        conn.close()
        assert n == 3

    def test_second_run_nav_count_unchanged(self, merged_twice):
        fd, _ = merged_twice
        conn = sqlite3.connect(fd)
        n = qval(conn, "SELECT COUNT(*) FROM fund_nav_data")
        conn.close()
        assert n == 6

    def test_second_run_no_value_conflicts(self, merged_twice):
        """After first run, all values are zx values; second run finds no differences."""
        _, stats2 = merged_twice
        assert stats2["nav"]["value_conflicts"] == 0, \
            "Second run should find no value conflicts (zx already won)"

    def test_second_run_no_fund_conflicts_in_table(self, merged_twice):
        """Conflict table is cleared and regenerated; second run = 0 fund conflicts."""
        fd, _ = merged_twice
        conn = sqlite3.connect(fd)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts WHERE table_name='funds'")
        conn.close()
        assert n == 0, "No fund conflicts expected on second run"

    def test_second_run_no_nav_conflicts_in_table(self, merged_twice):
        fd, _ = merged_twice
        conn = sqlite3.connect(fd)
        n = qval(conn,
            "SELECT COUNT(*) FROM migration_conflicts WHERE table_name='fund_nav_data'")
        conn.close()
        assert n == 0
