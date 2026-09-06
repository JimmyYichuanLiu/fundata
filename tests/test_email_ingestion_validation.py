"""Regression tests for validating email-extracted NAV rows before insertion."""

from __future__ import annotations

import sqlite3
from contextlib import closing

import pandas as pd
import pytest

from get_163_email import insert_data_to_db


def _connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE funds (
            fund_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL UNIQUE,
            fund_name TEXT
        );
        CREATE TABLE fund_nav_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id INTEGER,
            fund_name TEXT,
            fund_code TEXT NOT NULL,
            nav_date TEXT NOT NULL,
            unit_nav REAL NOT NULL,
            accum_nav REAL,
            source_id INTEGER,
            data_source TEXT,
            UNIQUE(fund_code, nav_date)
        );
        """
    )
    return connection


def test_shifted_fields_with_fund_name_in_date_column_are_rejected() -> None:
    extracted = pd.DataFrame(
        [
            {
                "产品代码": "0.9929",
                "产品名称": "20260629",
                "净值日期": "中邮永安鑫享成长FOF二号私募证券投资基金",
                "单位净值": 0.9929,
                "累计单位净值": 993595.52,
            }
        ]
    )
    failures: list[dict] = []

    with closing(_connection()) as connection:
        inserted, skipped = insert_data_to_db(connection, extracted, failures)
        nav_count = connection.execute("SELECT COUNT(*) FROM fund_nav_data").fetchone()[0]
        fund_count = connection.execute("SELECT COUNT(*) FROM funds").fetchone()[0]

    assert (inserted, skipped) == (0, 1)
    assert nav_count == 0
    assert fund_count == 0
    assert "净值日期格式无效" in failures[0]["reason"]


@pytest.mark.parametrize("nav_date", ["20260801", "2026-08-01", " 20260801 "])
def test_supported_nav_date_formats_are_stored_canonically(nav_date: str) -> None:
    extracted = pd.DataFrame(
        [
            {
                "产品代码": "SAFE01",
                "产品名称": "有效基金",
                "净值日期": nav_date,
                "单位净值": 1.0,
                "累计单位净值": 1.0,
            }
        ]
    )
    failures: list[dict] = []

    with closing(_connection()) as connection:
        inserted, skipped = insert_data_to_db(connection, extracted, failures)
        stored_date = connection.execute(
            "SELECT nav_date FROM fund_nav_data"
        ).fetchone()[0]

    assert (inserted, skipped) == (1, 0)
    assert stored_date == "2026-08-01"
    assert failures == []


@pytest.mark.parametrize(
    "nav_date",
    ["2026-8-1", "202681", "2026", "20260229", "2026-02-29"],
)
def test_noncanonical_or_impossible_nav_dates_are_rejected(nav_date: str) -> None:
    extracted = pd.DataFrame(
        [
            {
                "产品代码": "STRICT01",
                "产品名称": "严格日期基金",
                "净值日期": nav_date,
                "单位净值": 1.0,
                "累计单位净值": 1.0,
            }
        ]
    )
    failures: list[dict] = []

    with closing(_connection()) as connection:
        inserted, skipped = insert_data_to_db(connection, extracted, failures)
        nav_count = connection.execute("SELECT COUNT(*) FROM fund_nav_data").fetchone()[0]

    assert (inserted, skipped) == (0, 1)
    assert nav_count == 0
    assert "净值日期格式无效" in failures[0]["reason"]


def test_equivalent_nav_date_formats_cannot_create_logical_duplicates() -> None:
    extracted = pd.DataFrame(
        [
            {
                "产品代码": "DEDUP01",
                "产品名称": "去重基金",
                "净值日期": nav_date,
                "单位净值": 1.0,
                "累计单位净值": 1.0,
            }
            for nav_date in ("20260801", "2026-08-01")
        ]
    )
    failures: list[dict] = []

    with closing(_connection()) as connection:
        inserted, skipped = insert_data_to_db(connection, extracted, failures)
        stored_dates = connection.execute(
            "SELECT nav_date FROM fund_nav_data"
        ).fetchall()

    assert (inserted, skipped) == (1, 1)
    assert stored_dates == [("2026-08-01",)]
    assert failures == []


@pytest.mark.parametrize("incoming_date", ["20260801", "2026-08-01"])
def test_existing_legacy_compact_date_blocks_equivalent_insert(
    incoming_date: str,
) -> None:
    extracted = pd.DataFrame(
        [
            {
                "产品代码": "LEGACY01",
                "产品名称": "历史日期基金",
                "净值日期": incoming_date,
                "单位净值": 1.0,
                "累计单位净值": 1.0,
            }
        ]
    )
    failures: list[dict] = []

    with closing(_connection()) as connection:
        connection.execute(
            """
            INSERT INTO fund_nav_data
                (fund_name, fund_code, nav_date, unit_nav, accum_nav, data_source)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("历史日期基金", "LEGACY01", "20260801", 1.0, 1.0, "email"),
        )
        connection.commit()

        inserted, skipped = insert_data_to_db(connection, extracted, failures)
        stored_rows = connection.execute(
            "SELECT nav_date, unit_nav FROM fund_nav_data WHERE fund_code = ?",
            ("LEGACY01",),
        ).fetchall()

    assert (inserted, skipped) == (0, 1)
    assert stored_rows == [("20260801", 1.0)]
    assert failures == []
