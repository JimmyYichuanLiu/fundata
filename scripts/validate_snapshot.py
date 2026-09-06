"""Rehearse migration on an in-project SQLite backup; print aggregate evidence only."""
import argparse
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from fund_store import initialize_database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True)
    parser.add_argument('--snapshot', required=True)
    args = parser.parse_args()
    source_path, destination = Path(args.db).resolve(), Path(args.snapshot).resolve()
    for path in (source_path, destination):
        if ROOT not in path.parents:
            raise SystemExit('All data and snapshots must remain inside this project')
    if destination.exists() or not source_path.is_file():
        raise SystemExit('Source must exist and snapshot must not already exist')
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source_path.as_uri() + '?mode=ro', uri=True) as source:
        before = source.execute('SELECT COUNT(*) FROM fund_nav_data').fetchone()[0]
        with sqlite3.connect(destination) as target:
            source.backup(target)
    conn = initialize_database(destination)
    try:
        report = {
            'raw_before': before,
            'raw_after': conn.execute('SELECT COUNT(*) FROM fund_nav_data').fetchone()[0],
            'valid_records': conn.execute('SELECT COUNT(*) FROM valid_fund_nav').fetchone()[0],
            'valid_funds': conn.execute('SELECT COUNT(DISTINCT fund_id) FROM valid_fund_nav').fetchone()[0],
            'latest_nav_date': conn.execute('SELECT MAX(nav_date) FROM valid_fund_nav').fetchone()[0],
            'quality': dict(conn.execute('SELECT quality_status,COUNT(*) FROM fund_nav_data GROUP BY quality_status')),
            'sources': dict(conn.execute('SELECT data_source,COUNT(*) FROM valid_fund_nav GROUP BY data_source')),
            'adjusted_missing': conn.execute('SELECT COUNT(*) FROM valid_fund_nav WHERE adj_nav IS NULL').fetchone()[0],
            'audit_records': conn.execute('SELECT COUNT(*) FROM nav_quality_audit').fetchone()[0],
            'integrity': conn.execute('PRAGMA quick_check').fetchone()[0],
        }
        assert report['raw_before'] == report['raw_after'], 'Raw evidence count changed'
        assert report['integrity'] == 'ok'
    finally:
        conn.close()
    # Idempotent reopen must keep the same raw record count.
    again = initialize_database(destination)
    try:
        assert again.execute('SELECT COUNT(*) FROM fund_nav_data').fetchone()[0] == before
    finally:
        again.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
