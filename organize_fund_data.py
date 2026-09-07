#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Export fund NAV records from SQLite into an organized Excel workbook.

The command defaults to email-sourced records because this is the output used
by the mailbox ingestion workflow. Use ``--source all`` to export the merged
email/ZX/manual dataset instead.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from unicodedata import east_asian_width
from uuid import uuid4

import pandas as pd
from dotenv import load_dotenv
from openpyxl.styles import Alignment, Font, PatternFill


DETAIL_COLUMNS = ["产品名称", "产品代码", "净值日期", "单位净值", "累计单位净值"]
INVALID_SHEET_CHARS = re.compile(r"[\[\]:*?/\\]")
ILLEGAL_XML_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F\uFFFE\uFFFF]")
SOURCE_LABELS = {"email": "邮件", "all": "全部"}
FORMULA_PREFIXES = ("=", "+", "-", "@")


def _parse_nav_date(value) -> pd.Timestamp:
    """Parse the date formats currently present in ``fund_nav_data``."""
    if value is None or pd.isna(value):
        return pd.NaT
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return pd.Timestamp(value)

    text = str(value).strip()
    if not text:
        return pd.NaT
    if re.fullmatch(r"\d{8}\.0", text):
        text = text[:-2]

    strict_formats = (
        (r"\d{8}", "%Y%m%d"),
        (r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"),
        (r"\d{4}/\d{2}/\d{2}", "%Y/%m/%d"),
        (r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "%Y-%m-%d %H:%M:%S"),
    )
    for pattern, date_format in strict_formats:
        if not re.fullmatch(pattern, text):
            continue
        try:
            return pd.Timestamp(datetime.strptime(text, date_format))
        except ValueError:
            return pd.NaT
    return pd.NaT


def _prepare_nav_data(data: pd.DataFrame) -> pd.DataFrame:
    """Normalize dates and return rows in fund/date order."""
    prepared = data.copy()
    prepared["产品代码"] = prepared["产品代码"].astype(str).str.strip()
    prepared["产品名称"] = prepared["产品名称"].fillna("").astype(str).str.strip()
    prepared["_原始日期"] = prepared["净值日期"].fillna("").astype(str).str.strip()
    prepared["_排序日期"] = prepared["净值日期"].map(_parse_nav_date)
    prepared["净值日期"] = prepared["_原始日期"]

    valid_dates = prepared["_排序日期"].notna()
    prepared.loc[valid_dates, "净值日期"] = prepared.loc[
        valid_dates, "_排序日期"
    ].dt.strftime("%Y-%m-%d")

    return prepared.sort_values(
        ["产品代码", "_排序日期", "_原始日期"],
        ascending=[True, True, True],
        na_position="last",
        kind="stable",
    ).reset_index(drop=True)


def _build_summary(data: pd.DataFrame) -> pd.DataFrame:
    """Build one summary row per fund using chronological first/last rows."""
    summary_rows = []
    for fund_code, fund_rows in data.groupby("产品代码", sort=False):
        valid_rows = fund_rows[fund_rows["_排序日期"].notna()]
        chronological = valid_rows if not valid_rows.empty else fund_rows
        first_row = chronological.iloc[0]
        last_row = chronological.iloc[-1]
        names = fund_rows.loc[fund_rows["产品名称"] != "", "产品名称"]

        summary_rows.append(
            {
                "产品代码": fund_code,
                "产品名称": names.iloc[0] if not names.empty else "",
                "记录数": len(fund_rows),
                "最早日期": first_row["净值日期"],
                "最新日期": last_row["净值日期"],
                "最早单位净值": first_row["单位净值"],
                "最新单位净值": last_row["单位净值"],
            }
        )
    return pd.DataFrame(summary_rows)


def _unique_sheet_name(raw_name: str, used_names: set[str]) -> str:
    """Return an Excel-safe, case-insensitively unique sheet name."""
    cleaned = ILLEGAL_XML_CHARS.sub("", str(raw_name))
    cleaned = INVALID_SHEET_CHARS.sub("_", cleaned).strip().strip("'")
    base = cleaned or "未命名基金"
    candidate = base[:31]
    sequence = 2

    while candidate.casefold() in used_names:
        suffix = f"_{sequence}"
        candidate = f"{base[: 31 - len(suffix)]}{suffix}"
        sequence += 1

    used_names.add(candidate.casefold())
    return candidate


def _excel_safe_value(value):
    """Prevent mailbox-controlled text from becoming an Excel formula."""
    if isinstance(value, str):
        cleaned = ILLEGAL_XML_CHARS.sub("", value)
        if cleaned.lstrip().startswith(FORMULA_PREFIXES):
            return f"'{cleaned}"
        return cleaned
    return value


def _excel_safe_frame(data: pd.DataFrame) -> pd.DataFrame:
    """Escape formula-like strings without changing numeric NAV cells."""
    safe = data.copy()
    for column in safe.columns:
        safe[column] = safe[column].map(_excel_safe_value)
    return safe


def _excel_text_width(value: str) -> int:
    """Approximate Excel width, counting full-width CJK characters twice."""
    return sum(
        2 if east_asian_width(character) in {"W", "F"} else 1
        for character in value
    )


def _paths_refer_to_same_file(left: Path, right: Path) -> bool:
    """Compare paths safely on Windows, including aliases and hard links."""
    left_resolved = left.expanduser().resolve()
    right_resolved = right.expanduser().resolve()
    if os.path.normcase(str(left_resolved)) == os.path.normcase(str(right_resolved)):
        return True
    if left_resolved.exists() and right_resolved.exists():
        try:
            return os.path.samefile(left_resolved, right_resolved)
        except OSError:
            return False
    return False


def _format_worksheet(worksheet) -> None:
    """Apply compact formatting that keeps large exports easy to navigate."""
    header_fill = PatternFill("solid", fgColor="1F4E78")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions

    for column_cells in worksheet.columns:
        values = (str(cell.value) if cell.value is not None else "" for cell in column_cells)
        max_length = max(
            (_excel_text_width(value) for value in values),
            default=0,
        )
        worksheet.column_dimensions[column_cells[0].column_letter].width = min(
            max(max_length + 4, 10), 48
        )


def _load_nav_data(db_path: Path, source: str) -> pd.DataFrame:
    """Read the migrated English-column NAV schema without mutating the DB."""
    if source not in SOURCE_LABELS:
        raise ValueError("source 只能是 'email' 或 'all'")
    if not db_path.is_file():
        raise FileNotFoundError(f"数据库不存在: {db_path}")

    database_uri = f"{db_path.resolve().as_uri()}?mode=ro"
    with closing(sqlite3.connect(database_uri, uri=True)) as connection:
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(fund_nav_data)")
        }
        required = {
            "fund_name",
            "fund_code",
            "nav_date",
            "unit_nav",
            "accum_nav",
            "data_source",
        }
        missing = sorted(required - columns)
        if missing:
            raise RuntimeError(
                "fund_nav_data 缺少当前导出所需字段: " + ", ".join(missing)
            )

        where_clause = "WHERE data_source = ?" if source == "email" else ""
        if 'quality_status' in columns:
            where_clause += (" AND " if where_clause else "WHERE ") + "quality_status = 'valid'"
        parameters = ("email",) if source == "email" else ()
        query = f"""
            SELECT
                fund_name AS 产品名称,
                fund_code AS 产品代码,
                nav_date AS 净值日期,
                unit_nav AS 单位净值,
                accum_nav AS 累计单位净值
            FROM fund_nav_data
            {where_clause}
        """
        return pd.read_sql_query(query, connection, params=parameters)


def organize_fund_data(
    db_path: str | Path | None = None,
    output_path: str | Path | None = None,
    *,
    source: str = "email",
) -> Path:
    """Export NAV data and return the absolute path of the created workbook."""
    resolved_db_path = (
        Path(db_path or os.getenv("DB_PATH", "fund_data.db")).expanduser().resolve()
    )
    default_output = "fund_email_nav.xlsx" if source == "email" else "fund_data_organized.xlsx"
    resolved_output_path = Path(output_path or default_output).expanduser().resolve()
    if _paths_refer_to_same_file(resolved_db_path, resolved_output_path):
        raise ValueError("输出文件不能与数据库指向同一个文件")

    print(f"正在读取数据库: {resolved_db_path}")
    raw_data = _load_nav_data(resolved_db_path, source)
    if raw_data.empty:
        raise ValueError(f"没有找到来源为“{SOURCE_LABELS[source]}”的基金净值数据")

    data = _prepare_nav_data(raw_data)
    invalid_date_count = int(data["_排序日期"].isna().sum())
    if invalid_date_count:
        print(f"警告: 跳过 {invalid_date_count} 条日期无法识别的记录")
        data = data[data["_排序日期"].notna()].reset_index(drop=True)
    if data.empty:
        raise ValueError("没有可导出的有效基金净值数据")

    summary = _build_summary(data)
    fund_count = len(summary)
    print(f"共读取 {len(data)} 条记录，涉及 {fund_count} 个基金")

    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = resolved_output_path.with_name(
        f".{resolved_output_path.stem}.{uuid4().hex}.tmp.xlsx"
    )
    used_sheet_names = {"汇总".casefold()}

    try:
        with pd.ExcelWriter(temporary_path, engine="openpyxl") as writer:
            _excel_safe_frame(summary).to_excel(writer, sheet_name="汇总", index=False)
            _format_worksheet(writer.sheets["汇总"])

            for fund_code, fund_rows in data.groupby("产品代码", sort=False):
                sheet_name = _unique_sheet_name(fund_code, used_sheet_names)
                _excel_safe_frame(fund_rows[DETAIL_COLUMNS]).to_excel(
                    writer, sheet_name=sheet_name, index=False
                )
                _format_worksheet(writer.sheets[sheet_name])

        os.replace(temporary_path, resolved_output_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    print(f"[OK] 数据已保存到 {resolved_output_path}")
    print("  - 汇总工作表: 每个基金的记录数和首尾净值")
    print(f"  - 基金工作表: {fund_count} 个")
    return resolved_output_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="将基金净值数据库导出为 Excel")
    parser.add_argument(
        "--db",
        default=os.getenv("DB_PATH", "fund_data.db"),
        help="SQLite 数据库路径（默认读取 DB_PATH 或 fund_data.db）",
    )
    parser.add_argument(
        "--output",
        help="输出 Excel 路径（邮件默认 fund_email_nav.xlsx）",
    )
    parser.add_argument(
        "--source",
        choices=tuple(SOURCE_LABELS),
        default="email",
        help="email 仅导出邮件数据；all 导出全部来源（默认 email）",
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    args = _parse_args()
    try:
        organize_fund_data(args.db, args.output, source=args.source)
    except Exception as exc:
        print(f"错误: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
