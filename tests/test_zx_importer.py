"""
Tests for zx_importer.py

TDD approach: these tests define the expected contract for
  - compute_adj_nav()   (pure math, no I/O)
  - read_shelf()        (reads 臻选货架.xlsx)
  - import_zx_excel()  (reads both Excels, writes zx_fund.db)

Run from the project root:
    pytest tests/test_zx_importer.py -v
"""

import os
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

# ── Paths to demo fixtures ────────────────────────────────────────────────────
DEMO_DIR = Path(__file__).parent.parent / "demo"
SHELF_PATH = DEMO_DIR / "臻选货架.xlsx"
ZXDB_PATH = DEMO_DIR / "ZXdatabase.xlsx"


# =============================================================================
# Pure-unit tests — compute_adj_nav (no I/O)
# =============================================================================

class TestComputeAdjNav:

    def test_empty_input_returns_empty(self):
        from zx_importer import compute_adj_nav
        assert compute_adj_nav([], []) == []

    def test_single_row_is_one(self):
        from zx_importer import compute_adj_nav
        result = compute_adj_nav([1.0], [1.0])
        assert len(result) == 1
        assert abs(result[0] - 1.0) < 1e-9

    def test_no_dividend_tracks_unit_nav(self):
        """With no dividends (unit == accum), adj_nav mirrors unit_nav normalised to 1."""
        from zx_importer import compute_adj_nav
        unit  = [1.0, 1.05, 1.10, 1.15]
        accum = [1.0, 1.05, 1.10, 1.15]
        result = compute_adj_nav(unit, accum)
        expected = [1.0, 1.05, 1.10, 1.15]
        assert len(result) == 4
        for r, e in zip(result, expected):
            assert abs(r - e) < 1e-9, f"got {r}, expected {e}"

    def test_with_dividend_reflects_total_return(self):
        """
        Between day 1 and day 2 the fund pays a dividend of 0.3
        (unit drops 1.1 → 0.9, accum rises 1.1 → 1.2).
        Total return day 1→2 = (0.9 + 0.3) / 1.1 = 9.09 %
        So adj_nav should end up at 1.2.
        """
        from zx_importer import compute_adj_nav
        unit  = [1.0, 1.1, 0.9]
        accum = [1.0, 1.1, 1.2]
        result = compute_adj_nav(unit, accum)
        assert abs(result[0] - 1.0) < 1e-9
        assert abs(result[1] - 1.1) < 1e-9
        assert abs(result[2] - 1.2) < 1e-9, f"Expected 1.2, got {result[2]}"

    def test_zero_prev_unit_carries_forward(self):
        """If prev unit_nav is 0 we carry forward to avoid division by zero."""
        from zx_importer import compute_adj_nav
        unit  = [0.0, 1.0, 1.1]
        accum = [0.0, 1.0, 1.1]
        result = compute_adj_nav(unit, accum)
        assert abs(result[0] - 1.0) < 1e-9   # always starts at 1
        # day 1: prev=0 → carry forward → still 1.0
        assert abs(result[1] - 1.0) < 1e-9
        # day 2: prev=1.0 → normal calculation
        assert abs(result[2] - 1.1) < 1e-9

    def test_multiple_dividends_compound_correctly(self):
        """Two dividends compound multiplicatively."""
        from zx_importer import compute_adj_nav
        # Each period: fund pays 0.1 dividend, unit stays 1.0
        # Period 0: unit=1.0, accum=1.0  → adj=1.0
        # Period 1: unit=1.0, accum=1.1  → delta_div=0.1, pct=(1.0+0.1)/1.0-1=10%, adj=1.1
        # Period 2: unit=1.0, accum=1.2  → delta_div=0.1, pct=(1.0+0.1)/1.0-1=10%, adj=1.21
        unit  = [1.0, 1.0, 1.0]
        accum = [1.0, 1.1, 1.2]
        result = compute_adj_nav(unit, accum)
        assert abs(result[0] - 1.0)  < 1e-9
        assert abs(result[1] - 1.1)  < 1e-9
        assert abs(result[2] - 1.21) < 1e-9, f"Expected 1.21, got {result[2]}"


# =============================================================================
# read_shelf() tests — uses real 臻选货架.xlsx
# =============================================================================

@pytest.fixture(scope="module")
def shelf():
    from zx_importer import read_shelf
    return read_shelf(str(SHELF_PATH))


class TestReadShelf:

    def test_returns_dict(self, shelf):
        assert isinstance(shelf, dict)

    def test_count_in_expected_range(self, shelf):
        """About 276 rows in 臻选货架.xlsx; allow a small margin for empty Code_Id rows."""
        assert 260 <= len(shelf) <= 280, f"Got {len(shelf)} products"

    def test_all_keys_are_nonempty_strings(self, shelf):
        for code_id in shelf:
            assert isinstance(code_id, str), f"Key is not str: {code_id!r}"
            assert len(code_id.strip()) > 0, f"Empty Code_Id key found"

    def test_required_fields_present(self, shelf):
        required = {"fund_name", "strategy_l1", "manager", "custodian",
                    "inception_date", "start_date", "display"}
        for code_id, info in shelf.items():
            for field in required:
                assert field in info, f"Field '{field}' missing for {code_id}"

    def test_start_date_is_never_none(self, shelf):
        """start_date falls back to 2000-01-01 if everything else is missing."""
        for code_id, info in shelf.items():
            assert info["start_date"] is not None, \
                f"start_date is None for {code_id}"
            assert isinstance(info["start_date"], str), \
                f"start_date is not a string for {code_id}: {info['start_date']!r}"

    def test_start_date_format(self, shelf):
        """start_date should be YYYY-MM-DD."""
        import re
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for code_id, info in shelf.items():
            assert pattern.match(info["start_date"]), \
                f"start_date has wrong format for {code_id}: {info['start_date']!r}"

    def test_start_date_prefers_col21_over_inception(self, shelf):
        """
        If Start_date (col 21) is available it should be used.
        Spot-check: at least some funds should have start_date != inception_date,
        which confirms the priority logic is active rather than just copying inception_date.
        """
        diffs = sum(
            1 for info in shelf.values()
            if info.get("start_date") != info.get("inception_date")
            and info.get("inception_date") is not None
        )
        # There should be at least several funds where start_date ≠ inception_date
        assert diffs >= 5, (
            f"Expected some start_date != inception_date cases; only found {diffs}. "
            "This suggests Start_date priority is not being applied."
        )

    def test_known_nonexistent_codes_absent(self, shelf):
        """The 7 codes that are in ZXdatabase but NOT in 臻选货架 should not appear."""
        # These were identified by the initial analysis as "skipped" funds
        # (not whitelisted). Verify none snuck into the shelf dict.
        non_whitelist = {"03854A", "AAT36B", "ALA25B"}
        for code in non_whitelist:
            # They shouldn't be keys in shelf (they're not in 臻选货架 Code_Id column)
            assert code not in shelf, f"{code} should not appear in shelf whitelist"
        assert len(shelf) <= 280


# =============================================================================
# import_zx_excel() integration tests — uses real Excel files → temp DB
# =============================================================================

# Module-level dict to capture import stats from the fixture for use in tests
_IMPORT_STATS: dict = {}


@pytest.fixture(scope="module")
def db_path(tmp_path_factory):
    """
    Import once into a temp database for the entire test module.
    This is the only place actual DB I/O happens.
    """
    from zx_importer import import_zx_excel
    tmp = tmp_path_factory.mktemp("zxdb") / "test_zx_fund.db"
    stats = import_zx_excel(str(tmp), str(ZXDB_PATH), str(SHELF_PATH))
    _IMPORT_STATS.update(stats)
    return str(tmp)


class TestImportZxExcel:

    # ── Sanity / count checks ────────────────────────────────────────────────

    def test_returns_stats_dict(self, db_path):
        for key in ("imported_funds", "skipped_funds", "total_nav_records"):
            assert key in _IMPORT_STATS, f"stats missing key: {key}"

    def test_imported_fund_count(self, db_path):
        """Expect ~273 funds (the intersection of ZXdatabase and 臻选货架)."""
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM zx_fund_product").fetchone()[0]
        conn.close()
        assert 260 <= count <= 280, f"Expected ~273 imported funds, got {count}"

    def test_nav_records_total_in_expected_range(self, db_path):
        """280 sheets × avg 264 rows ≈ 73 000 records; allow generous bounds."""
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM zx_fund_nav").fetchone()[0]
        conn.close()
        assert count > 40_000, f"Expected >40k NAV records, got {count}"
        assert count < 200_000, f"NAV count seems too large: {count}"

    def test_nonwhitelist_funds_not_imported(self, db_path):
        """Sheets in ZXdatabase that are NOT in 臻选货架 must be absent from DB."""
        from zx_importer import read_shelf
        whitelist = read_shelf(str(SHELF_PATH))
        xl = pd.ExcelFile(str(ZXDB_PATH))
        skipped = [s for s in xl.sheet_names if s not in whitelist]
        assert len(skipped) > 0, "Expected at least a few skipped sheets"
        conn = sqlite3.connect(db_path)
        for code in skipped:
            n = conn.execute(
                "SELECT COUNT(*) FROM zx_fund_product WHERE fund_code=?", (code,)
            ).fetchone()[0]
            assert n == 0, f"Fund {code} should have been skipped but was imported"
        conn.close()

    # ── Schema / column checks ───────────────────────────────────────────────

    def test_fund_product_has_expected_columns(self, db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.execute("PRAGMA table_info(zx_fund_product)")
        cols = {row[1] for row in cur.fetchall()}
        conn.close()
        required = {"fund_id", "fund_code", "fund_name", "strategy_l1", "strategy_l2",
                    "strategy_l3", "manager", "custodian", "inception_date",
                    "start_date", "benchmark_index", "display"}
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    def test_fund_nav_has_expected_columns(self, db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.execute("PRAGMA table_info(zx_fund_nav)")
        cols = {row[1] for row in cur.fetchall()}
        conn.close()
        required = {"id", "fund_id", "fund_code", "fund_name",
                    "nav_date", "unit_nav", "accum_nav", "adj_nav",
                    "source_id", "data_source"}
        assert required.issubset(cols), f"Missing columns: {required - cols}"

    # ── adj_nav correctness ──────────────────────────────────────────────────

    def test_first_adj_nav_is_one_for_every_fund(self, db_path):
        """The first adj_nav in each fund's time series must equal 1.0."""
        conn = sqlite3.connect(db_path)
        rows = conn.execute("""
            SELECT n.fund_code, n.adj_nav
            FROM zx_fund_nav n
            INNER JOIN (
                SELECT fund_code, MIN(nav_date) AS first_date
                FROM zx_fund_nav
                GROUP BY fund_code
            ) earliest ON n.fund_code = earliest.fund_code
                       AND n.nav_date = earliest.first_date
        """).fetchall()
        conn.close()
        assert len(rows) > 0
        for code, adj in rows:
            assert abs(adj - 1.0) < 1e-9, \
                f"Fund {code}: first adj_nav={adj}, expected 1.0"

    def test_adj_nav_always_positive(self, db_path):
        """adj_nav must be > 0 for all records."""
        conn = sqlite3.connect(db_path)
        bad = conn.execute(
            "SELECT COUNT(*) FROM zx_fund_nav WHERE adj_nav <= 0"
        ).fetchone()[0]
        conn.close()
        assert bad == 0, f"{bad} records have adj_nav <= 0"

    # ── Data accuracy: spot-check NAV rows against the source Excel ──────────

    def test_nav_matches_excel_first_fund(self, db_path):
        """
        For the first fund code (alphabetically) that exists in both the
        whitelist and ZXdatabase, compare the first 5 DB rows against the
        values read directly from the Excel sheet.
        """
        from zx_importer import read_shelf

        whitelist = read_shelf(str(SHELF_PATH))
        xl = pd.ExcelFile(str(ZXDB_PATH))
        valid = sorted(s for s in xl.sheet_names if s in whitelist)
        assert len(valid) > 0, "No valid sheet found"
        code_id = valid[0]

        # Read directly from Excel
        df = pd.read_excel(str(ZXDB_PATH), sheet_name=code_id, header=0)
        df = df.iloc[:, :3].copy()
        df.columns = ["date", "unit_value", "accumulated_value"]
        df = df.dropna(subset=["date", "unit_value"])
        df["unit_value"] = pd.to_numeric(df["unit_value"], errors="coerce")
        df["accumulated_value"] = pd.to_numeric(df["accumulated_value"], errors="coerce")
        df = df.dropna(subset=["unit_value"])
        df["date_str"] = df["date"].apply(
            lambda v: v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v)[:10]
        )
        df = df.drop_duplicates("date_str", keep="last").sort_values("date_str")
        df = df.reset_index(drop=True)
        n_check = min(5, len(df))

        # Query DB
        conn = sqlite3.connect(db_path)
        db_rows = conn.execute(
            "SELECT nav_date, unit_nav, accum_nav "
            "FROM zx_fund_nav WHERE fund_code=? ORDER BY nav_date LIMIT ?",
            (code_id, n_check)
        ).fetchall()
        conn.close()

        assert len(db_rows) == n_check, \
            f"Expected {n_check} rows for {code_id}, got {len(db_rows)}"

        for i, (db_date, db_unit, db_accum) in enumerate(db_rows):
            excel_date = df.iloc[i]["date_str"]
            excel_unit = float(df.iloc[i]["unit_value"])
            excel_accum = df.iloc[i]["accumulated_value"]
            excel_accum = float(excel_accum) if pd.notna(excel_accum) else excel_unit

            assert db_date == excel_date, \
                f"Row {i} date mismatch for {code_id}: DB={db_date!r} Excel={excel_date!r}"
            assert abs(db_unit - excel_unit) < 1e-6, \
                f"Row {i} unit_nav mismatch for {code_id}: DB={db_unit} Excel={excel_unit}"
            assert abs(db_accum - excel_accum) < 1e-6, \
                f"Row {i} accum_nav mismatch for {code_id}: DB={db_accum} Excel={excel_accum}"

    def test_nav_matches_excel_last_fund(self, db_path):
        """Same accuracy check for the last fund (alphabetically) — coverage diversity."""
        from zx_importer import read_shelf

        whitelist = read_shelf(str(SHELF_PATH))
        xl = pd.ExcelFile(str(ZXDB_PATH))
        valid = sorted(s for s in xl.sheet_names if s in whitelist)
        code_id = valid[-1]

        df = pd.read_excel(str(ZXDB_PATH), sheet_name=code_id, header=0)
        df = df.iloc[:, :3].copy()
        df.columns = ["date", "unit_value", "accumulated_value"]
        df = df.dropna(subset=["date", "unit_value"])
        df["unit_value"] = pd.to_numeric(df["unit_value"], errors="coerce")
        df["accumulated_value"] = pd.to_numeric(df["accumulated_value"], errors="coerce")
        df = df.dropna(subset=["unit_value"])
        df["date_str"] = df["date"].apply(
            lambda v: v.strftime("%Y-%m-%d") if hasattr(v, "strftime") else str(v)[:10]
        )
        df = df.drop_duplicates("date_str", keep="last").sort_values("date_str")
        df = df.reset_index(drop=True)
        n_check = min(5, len(df))

        conn = sqlite3.connect(db_path)
        db_rows = conn.execute(
            "SELECT nav_date, unit_nav, accum_nav "
            "FROM zx_fund_nav WHERE fund_code=? ORDER BY nav_date LIMIT ?",
            (code_id, n_check)
        ).fetchall()
        conn.close()

        assert len(db_rows) == n_check
        for i, (db_date, db_unit, db_accum) in enumerate(db_rows):
            excel_date = df.iloc[i]["date_str"]
            excel_unit = float(df.iloc[i]["unit_value"])
            excel_accum = df.iloc[i]["accumulated_value"]
            excel_accum = float(excel_accum) if pd.notna(excel_accum) else excel_unit

            assert db_date == excel_date
            assert abs(db_unit - excel_unit) < 1e-6
            assert abs(db_accum - excel_accum) < 1e-6

    def test_adj_nav_manual_crosscheck_no_dividend_fund(self, db_path):
        """
        For a fund whose accum_nav always equals unit_nav (no dividends),
        adj_nav at row i should equal unit_nav[i] / unit_nav[0].
        Pick any such fund and verify the first 10 rows.
        """
        conn = sqlite3.connect(db_path)
        # Find a fund with no dividends (accum ≈ unit throughout)
        candidate = conn.execute("""
            SELECT fund_code FROM (
                SELECT fund_code,
                       MAX(ABS(accum_nav - unit_nav)) AS max_diff
                FROM zx_fund_nav
                GROUP BY fund_code
            )
            WHERE max_diff < 0.001
            LIMIT 1
        """).fetchone()

        if candidate is None:
            conn.close()
            pytest.skip("No zero-dividend fund found in DB — skipping crosscheck")

        code = candidate[0]
        rows = conn.execute(
            "SELECT nav_date, unit_nav, adj_nav FROM zx_fund_nav "
            "WHERE fund_code=? ORDER BY nav_date LIMIT 10",
            (code,)
        ).fetchall()
        conn.close()

        first_unit = rows[0][1]
        assert abs(first_unit) > 1e-9, "First unit_nav is zero — cannot normalise"
        for nav_date, unit, adj in rows:
            expected_adj = unit / first_unit
            assert abs(adj - expected_adj) < 1e-6, (
                f"adj_nav mismatch for {code} on {nav_date}: "
                f"got {adj:.8f}, expected {expected_adj:.8f}"
            )

    # ── Uniqueness / integrity ────────────────────────────────────────────────

    def test_no_duplicate_nav_rows(self, db_path):
        """(fund_code, nav_date) must be unique."""
        conn = sqlite3.connect(db_path)
        dupes = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT fund_code, nav_date, COUNT(*) AS cnt
                FROM zx_fund_nav
                GROUP BY fund_code, nav_date
                HAVING cnt > 1
            )
        """).fetchone()[0]
        conn.close()
        assert dupes == 0, f"{dupes} duplicate (fund_code, nav_date) pairs found"

    def test_dates_ascending_per_fund(self, db_path):
        """For each fund, nav_date must be strictly ascending (no out-of-order rows)."""
        conn = sqlite3.connect(db_path)
        # Find any fund where a row's date <= the previous row's date
        bad = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT fund_code, nav_date,
                       LAG(nav_date) OVER (PARTITION BY fund_code ORDER BY nav_date) AS prev_date
                FROM zx_fund_nav
            )
            WHERE prev_date IS NOT NULL AND nav_date <= prev_date
        """).fetchone()[0]
        conn.close()
        assert bad == 0, f"{bad} out-of-order date rows found"
