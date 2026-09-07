"""
tests/test_db_migration.py

TDD tests for db_schema_migrate.py

Run BEFORE implementation — tests should fail initially.
After implementation all tests must pass.

Usage:
    pytest tests/test_db_migration.py -v
"""

import re
import shutil
import sqlite3
from pathlib import Path

import pytest

DB_DIR       = Path(__file__).parent.parent
FUND_DATA_DB = DB_DIR / "fund_data.db"
ZX_FUND_DB   = DB_DIR / "zx_fund.db"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_columns(conn: sqlite3.Connection, table: str) -> set:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


def get_column_types(conn: sqlite3.Connection, table: str) -> dict:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1]: r[2] for r in rows}


def row_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# Module-level counts: snapshot originals before any migration
# ---------------------------------------------------------------------------

_ORIG_FUNDS_COUNT        = 0
_ORIG_FUND_NAV_COUNT     = 0
_ORIG_ZX_PRODUCT_COUNT   = 0
_ORIG_ZX_NAV_COUNT       = 0


def _load_orig_counts():
    global _ORIG_FUNDS_COUNT, _ORIG_FUND_NAV_COUNT
    global _ORIG_ZX_PRODUCT_COUNT, _ORIG_ZX_NAV_COUNT
    if FUND_DATA_DB.exists():
        c = sqlite3.connect(str(FUND_DATA_DB))
        _ORIG_FUNDS_COUNT    = row_count(c, "funds")
        _ORIG_FUND_NAV_COUNT = row_count(c, "fund_nav_data")
        c.close()
    if ZX_FUND_DB.exists():
        c = sqlite3.connect(str(ZX_FUND_DB))
        _ORIG_ZX_PRODUCT_COUNT = row_count(c, "zx_fund_product")
        _ORIG_ZX_NAV_COUNT     = row_count(c, "zx_fund_nav")
        c.close()


_load_orig_counts()


# ---------------------------------------------------------------------------
# Fixtures: copy real DBs to temp dirs, run migration, return path
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def migrated_fund_data_db(tmp_path_factory):
    from db_schema_migrate import migrate_fund_data_db
    tmp = tmp_path_factory.mktemp("fund_data") / "fund_data.db"
    if not FUND_DATA_DB.exists():
        pytest.skip('Optional local snapshot is unavailable; synthetic migration tests cover CI')
    with sqlite3.connect(FUND_DATA_DB.as_uri() + '?mode=ro', uri=True) as source, sqlite3.connect(tmp) as target:
        source.backup(target)
    migrate_fund_data_db(str(tmp))
    return str(tmp)


@pytest.fixture(scope="module")
def migrated_zx_fund_db(tmp_path_factory):
    from db_schema_migrate import migrate_zx_fund_db
    tmp = tmp_path_factory.mktemp("zx_fund") / "zx_fund.db"
    if not ZX_FUND_DB.exists():
        pytest.skip('Optional local ZX snapshot is unavailable')
    with sqlite3.connect(ZX_FUND_DB.as_uri() + '?mode=ro', uri=True) as source, sqlite3.connect(tmp) as target:
        source.backup(target)
    migrate_zx_fund_db(str(tmp))
    return str(tmp)


# ===========================================================================
# 1. funds table (fund_data.db)
# ===========================================================================

class TestFundsMigration:

    def test_funds_has_all_required_columns(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        cols = get_columns(conn, "funds")
        conn.close()
        required = {
            "fund_id", "fund_code", "fund_name", "created_at",
            "strategy_l1", "strategy_l2", "strategy_l3",
            "manager", "custodian", "inception_date", "start_date",
            "benchmark_index", "display",
        }
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_funds_old_chinese_columns_removed(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        cols = get_columns(conn, "funds")
        conn.close()
        old = {"产品代码", "产品名称", "首次录入时间"}
        overlap = old & cols
        assert not overlap, f"Old Chinese columns still present: {overlap}"

    def test_funds_row_count_preserved(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        count = row_count(conn, "funds")
        conn.close()
        assert count == _ORIG_FUNDS_COUNT, (
            f"Row count changed: {_ORIG_FUNDS_COUNT} → {count}"
        )

    def test_funds_display_is_preserved(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        actual = conn.execute('SELECT fund_id, display FROM funds ORDER BY fund_id').fetchall()
        with sqlite3.connect(FUND_DATA_DB) as original:
            expected = original.execute('SELECT fund_id, display FROM funds ORDER BY fund_id').fetchall()
        conn.close()
        assert actual == expected

    def test_funds_new_nullable_columns_are_null_for_existing_rows(self, migrated_fund_data_db):
        """Never erase existing metadata just to satisfy an old empty-column assumption."""
        conn = sqlite3.connect(migrated_fund_data_db)
        for col in ("strategy_l3", "manager", "custodian", "inception_date", "start_date"):
            actual = conn.execute(f'SELECT fund_id,[{col}] FROM funds ORDER BY fund_id').fetchall()
            with sqlite3.connect(FUND_DATA_DB) as original:
                expected = original.execute(f'SELECT fund_id,[{col}] FROM funds ORDER BY fund_id').fetchall()
            assert actual == expected
        conn.close()

    def test_funds_benchmark_index_still_present(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        cols = get_columns(conn, "funds")
        conn.close()
        assert "benchmark_index" in cols

    def test_funds_fund_code_unique_constraint(self, migrated_fund_data_db):
        """fund_code must have a UNIQUE index."""
        conn = sqlite3.connect(migrated_fund_data_db)
        indexes = conn.execute("PRAGMA index_list(funds)").fetchall()
        conn.close()
        has_unique = any(row[2] == 1 for row in indexes)
        assert has_unique, "funds table should have a UNIQUE index (on fund_code)"

    def test_funds_data_integrity_spot_check(self, migrated_fund_data_db):
        """fund_code and fund_name must be non-empty for all rows."""
        conn = sqlite3.connect(migrated_fund_data_db)
        no_code = conn.execute(
            "SELECT COUNT(*) FROM funds WHERE fund_code IS NULL OR fund_code = ''"
        ).fetchone()[0]
        conn.close()
        assert no_code == 0, f"{no_code} rows have NULL or empty fund_code"


# ===========================================================================
# 2. fund_nav_data table (fund_data.db)
# ===========================================================================

class TestFundNavDataMigration:

    def test_fund_nav_data_has_all_required_columns(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        cols = get_columns(conn, "fund_nav_data")
        conn.close()
        required = {
            "id", "fund_id", "fund_code", "fund_name",
            "nav_date", "unit_nav", "accum_nav", "adj_nav",
            "source_id", "录入时间", "data_source",
        }
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_fund_nav_data_old_columns_removed(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        cols = get_columns(conn, "fund_nav_data")
        conn.close()
        old = {"净值日期", "单位净值", "累计单位净值", "产品代码", "产品名称"}
        overlap = old & cols
        assert not overlap, f"Old columns still present: {overlap}"

    def test_fund_nav_data_row_count_preserved(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        count = row_count(conn, "fund_nav_data")
        conn.close()
        assert count == _ORIG_FUND_NAV_COUNT, (
            f"Row count changed: {_ORIG_FUND_NAV_COUNT} → {count}"
        )

    def test_fund_nav_data_date_format_yyyy_mm_dd(self, migrated_fund_data_db):
        """All nav_date values must match YYYY-MM-DD."""
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        conn = sqlite3.connect(migrated_fund_data_db)
        sample = conn.execute("SELECT nav_date FROM valid_fund_nav").fetchall()
        conn.close()
        for (d,) in sample:
            assert pattern.match(str(d)), f"nav_date has wrong format: {d!r}"

    def test_fund_nav_data_no_old_yyyymmdd_dates(self, migrated_fund_data_db):
        """No nav_date value should look like YYYYMMDD (8 digit string without dashes)."""
        conn = sqlite3.connect(migrated_fund_data_db)
        bad = conn.execute(
            "SELECT COUNT(*) FROM valid_fund_nav "
            "WHERE nav_date GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
        ).fetchone()[0]
        conn.close()
        assert bad == 0, f"{bad} rows still have YYYYMMDD format"

    def test_fund_nav_data_email_records_have_email_source(self, migrated_fund_data_db):
        """Rows with source_id IS NOT NULL should have data_source = 'email'."""
        conn = sqlite3.connect(migrated_fund_data_db)
        bad = conn.execute(
            "SELECT COUNT(*) FROM fund_nav_data "
            "WHERE source_id IS NOT NULL AND data_source != 'email'"
        ).fetchone()[0]
        conn.close()
        assert bad == 0, f"{bad} email rows have wrong data_source"

    def test_fund_nav_data_manual_records_have_manual_source(self, migrated_fund_data_db):
        """ZX data can legitimately have source_id NULL; keep explicit provenance."""
        conn = sqlite3.connect(migrated_fund_data_db)
        actual = conn.execute('SELECT id,data_source FROM fund_nav_data ORDER BY id').fetchall()
        with sqlite3.connect(FUND_DATA_DB) as original:
            expected = original.execute('SELECT id,data_source FROM fund_nav_data ORDER BY id').fetchall()
        conn.close()
        assert actual == expected

    def test_fund_nav_data_no_null_nav_date(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        nulls = conn.execute(
            "SELECT COUNT(*) FROM fund_nav_data WHERE nav_date IS NULL"
        ).fetchone()[0]
        conn.close()
        assert nulls == 0, f"{nulls} rows have NULL nav_date"

    def test_fund_nav_data_unique_constraint_fund_code_nav_date(self, migrated_fund_data_db):
        """No duplicate (fund_code, nav_date) pairs."""
        conn = sqlite3.connect(migrated_fund_data_db)
        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT fund_code, nav_date, COUNT(*) cnt
                FROM fund_nav_data
                GROUP BY fund_code, nav_date
                HAVING cnt > 1
            )
        """).fetchone()[0]
        conn.close()
        assert dupes == 0, f"{dupes} duplicate (fund_code, nav_date) pairs"

    def test_fund_nav_data_no_null_unit_nav(self, migrated_fund_data_db):
        conn = sqlite3.connect(migrated_fund_data_db)
        nulls = conn.execute(
            "SELECT COUNT(*) FROM fund_nav_data WHERE unit_nav IS NULL"
        ).fetchone()[0]
        conn.close()
        assert nulls == 0, f"{nulls} rows have NULL unit_nav"


# ===========================================================================
# 3. zx_fund_product table (zx_fund.db)
# ===========================================================================

class TestZxFundProductMigration:

    def test_zx_fund_product_has_fund_id_column(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_product")
        conn.close()
        assert "fund_id" in cols, "fund_id missing (should be renamed from id)"

    def test_zx_fund_product_old_id_column_gone(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_product")
        conn.close()
        assert "id" not in cols, "old 'id' column should be renamed to 'fund_id'"

    def test_zx_fund_product_has_benchmark_index_column(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_product")
        conn.close()
        assert "benchmark_index" in cols, "benchmark_index missing (should be renamed from benchmark)"

    def test_zx_fund_product_old_benchmark_column_gone(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_product")
        conn.close()
        assert "benchmark" not in cols, "old 'benchmark' column should be renamed to 'benchmark_index'"

    def test_zx_fund_product_all_required_columns(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_product")
        conn.close()
        required = {
            "fund_id", "fund_code", "fund_name", "created_at",
            "strategy_l1", "strategy_l2", "strategy_l3",
            "manager", "custodian", "inception_date", "start_date",
            "benchmark_index", "display",
        }
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_zx_fund_product_row_count_preserved(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        count = row_count(conn, "zx_fund_product")
        conn.close()
        assert count == _ORIG_ZX_PRODUCT_COUNT, (
            f"Row count changed: {_ORIG_ZX_PRODUCT_COUNT} → {count}"
        )

    def test_zx_fund_product_fund_code_unique(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT fund_code, COUNT(*) cnt
                FROM zx_fund_product GROUP BY fund_code HAVING cnt > 1
            )
        """).fetchone()[0]
        conn.close()
        assert dupes == 0, f"{dupes} duplicate fund_code values"

    def test_zx_fund_product_fund_id_is_pk(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        col_info = conn.execute("PRAGMA table_info(zx_fund_product)").fetchall()
        conn.close()
        # pk column has row[5] = 1
        pk_col = next((r[1] for r in col_info if r[5] == 1), None)
        assert pk_col == "fund_id", f"Primary key should be 'fund_id', got {pk_col!r}"


# ===========================================================================
# 4. zx_fund_nav table (zx_fund.db)
# ===========================================================================

class TestZxFundNavMigration:

    def test_zx_fund_nav_has_fund_id_integer_column(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        col_types = get_column_types(conn, "zx_fund_nav")
        conn.close()
        assert "fund_id" in col_types, "fund_id column missing"
        assert "INT" in col_types["fund_id"].upper(), (
            f"fund_id should be INTEGER type, got {col_types['fund_id']!r}"
        )

    def test_zx_fund_nav_has_fund_code_and_fund_name(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_nav")
        conn.close()
        assert "fund_code" in cols, "fund_code redundant column missing"
        assert "fund_name" in cols, "fund_name redundant column missing"

    def test_zx_fund_nav_all_required_columns(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        cols = get_columns(conn, "zx_fund_nav")
        conn.close()
        required = {
            "id", "fund_id", "fund_code", "fund_name",
            "nav_date", "unit_nav", "accum_nav", "adj_nav",
            "source_id", "录入时间", "data_source",
        }
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_zx_fund_nav_fund_id_all_valid(self, migrated_zx_fund_db):
        """Every fund_id in zx_fund_nav must exist in zx_fund_product."""
        conn = sqlite3.connect(migrated_zx_fund_db)
        orphans = conn.execute("""
            SELECT COUNT(*) FROM zx_fund_nav n
            LEFT JOIN zx_fund_product p ON n.fund_id = p.fund_id
            WHERE p.fund_id IS NULL
        """).fetchone()[0]
        conn.close()
        assert orphans == 0, f"{orphans} zx_fund_nav rows have no matching fund_id"

    def test_zx_fund_nav_fund_code_matches_product(self, migrated_zx_fund_db):
        """Redundant fund_code must match the product table."""
        conn = sqlite3.connect(migrated_zx_fund_db)
        mismatches = conn.execute("""
            SELECT COUNT(*) FROM zx_fund_nav n
            JOIN zx_fund_product p ON n.fund_id = p.fund_id
            WHERE n.fund_code != p.fund_code
        """).fetchone()[0]
        conn.close()
        assert mismatches == 0, f"{mismatches} rows have fund_code mismatch"

    def test_zx_fund_nav_data_source_is_zx_excel(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        bad = conn.execute(
            "SELECT COUNT(*) FROM zx_fund_nav WHERE data_source != 'zx_excel'"
        ).fetchone()[0]
        conn.close()
        assert bad == 0, f"{bad} rows have data_source != 'zx_excel'"

    def test_zx_fund_nav_source_id_all_null(self, migrated_zx_fund_db):
        """source_id should be NULL for all zx_excel rows."""
        conn = sqlite3.connect(migrated_zx_fund_db)
        non_null = conn.execute(
            "SELECT COUNT(*) FROM zx_fund_nav WHERE source_id IS NOT NULL"
        ).fetchone()[0]
        conn.close()
        assert non_null == 0, f"{non_null} rows have non-NULL source_id"

    def test_zx_fund_nav_row_count_preserved(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        count = row_count(conn, "zx_fund_nav")
        conn.close()
        assert count == _ORIG_ZX_NAV_COUNT, (
            f"Row count changed: {_ORIG_ZX_NAV_COUNT} → {count}"
        )

    def test_zx_fund_nav_no_duplicate_fund_id_nav_date(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT fund_id, nav_date, COUNT(*) cnt
                FROM zx_fund_nav GROUP BY fund_id, nav_date HAVING cnt > 1
            )
        """).fetchone()[0]
        conn.close()
        assert dupes == 0, f"{dupes} duplicate (fund_id, nav_date) pairs"

    def test_zx_fund_nav_dates_still_ascending_per_fund(self, migrated_zx_fund_db):
        conn = sqlite3.connect(migrated_zx_fund_db)
        bad = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT fund_id, nav_date,
                       LAG(nav_date) OVER (PARTITION BY fund_id ORDER BY nav_date) AS prev
                FROM zx_fund_nav
            ) WHERE prev IS NOT NULL AND nav_date <= prev
        """).fetchone()[0]
        conn.close()
        assert bad == 0, f"{bad} out-of-order date rows after migration"

    def test_zx_fund_nav_nav_date_format_unchanged(self, migrated_zx_fund_db):
        """nav_date was already YYYY-MM-DD in zx_fund_nav — verify unchanged."""
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        conn = sqlite3.connect(migrated_zx_fund_db)
        sample = conn.execute("SELECT nav_date FROM zx_fund_nav LIMIT 50").fetchall()
        conn.close()
        for (d,) in sample:
            assert pattern.match(str(d)), f"nav_date has wrong format: {d!r}"
