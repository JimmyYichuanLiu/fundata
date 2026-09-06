"""Versioned fund schema, evidence-preserving quality migration and NAV math.

Invalid/conflicting source rows remain in the raw table; all public consumers use
``valid_fund_nav``. Migration never recreates fund identities or source metadata.
"""
import json
import hashlib
import math
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 2


def normalize_nav_date(value):
    text = str(value).strip() if value is not None else ''
    fmt = '%Y%m%d' if re.fullmatch(r'[0-9]{8}', text) else '%Y-%m-%d'
    if fmt == '%Y-%m-%d' and not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', text):
        return None
    try:
        parsed = datetime.strptime(text, fmt)
        if parsed.date() > datetime.now().date():
            return None
        return parsed.strftime('%Y-%m-%d')
    except ValueError:
        return None


def positive_number(value):
    try:
        return math.isfinite(float(value)) and float(value) > 0
    except (ValueError, TypeError, OverflowError):
        return False


def _columns(conn, table):
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}


def _add_columns(conn, table, columns):
    existing = _columns(conn, table)
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {definition}')


def _rename(conn, table, mapping):
    cols = _columns(conn, table)
    for old, new in mapping.items():
        if old in cols and new not in cols:
            conn.execute(f'ALTER TABLE "{table}" RENAME COLUMN "{old}" TO "{new}"')


def _audit(conn, row, reason, normalized=None):
    conn.execute('''INSERT OR IGNORE INTO nav_quality_audit
        (nav_id,reason,normalized_date,original_json) VALUES(?,?,?,?)''',
        (row['id'], reason, normalized, json.dumps(row, ensure_ascii=False, default=str)))


def record_ingestion_conflict(conn, nav_id, incoming, source_id):
    """Retain a conflicting incoming value without replacing the accepted row."""
    evidence = dict(incoming, id=nav_id, incoming_source_id=source_id)
    digest = hashlib.sha256(json.dumps(evidence, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()
    _audit(conn,evidence,'incoming_value_conflict:'+digest,normalize_nav_date(incoming.get('净值日期')))


def refresh_quality(conn):
    """Normalize admissible dates and quarantine bad/conflicting source rows."""
    cur = conn.execute('SELECT * FROM fund_nav_data ORDER BY id')
    names = [d[0] for d in cur.description]
    rows = [dict(zip(names, row)) for row in cur.fetchall()]
    occupied = {(r['fund_code'], r['nav_date']): r for r in rows}
    for row in rows:
        if row['quality_status'] in ('conflict', 'duplicate'):
            continue
        normalized = normalize_nav_date(row['nav_date'])
        reason = None
        if not normalized:
            reason = 'invalid_date'
        elif not positive_number(row['unit_nav']):
            reason = 'invalid_unit_nav'
        elif row['accum_nav'] is not None and not positive_number(row['accum_nav']):
            reason = 'invalid_accum_nav'
        elif not row['fund_code'] or not str(row['fund_code']).strip():
            reason = 'invalid_fund_code'
        if reason:
            _audit(conn, row, reason, normalized)
            conn.execute('UPDATE fund_nav_data SET quality_status=?, quality_reason=?, adj_nav=NULL WHERE id=?',
                         ('invalid', reason, row['id']))
            continue
        other = occupied.get((row['fund_code'], normalized))
        if normalized != row['nav_date'] and other and other['id'] != row['id']:
            status = 'duplicate' if (row['unit_nav'], row['accum_nav']) == (other['unit_nav'], other['accum_nav']) else 'conflict'
            _audit(conn, row, status, normalized)
            conn.execute('UPDATE fund_nav_data SET quality_status=?, quality_reason=?, adj_nav=NULL WHERE id=?',
                         (status, f'canonical row {other["id"]}', row['id']))
            continue
        if normalized != row['nav_date']:
            _audit(conn, row, 'date_normalized', normalized)
            occupied.pop((row['fund_code'], row['nav_date']), None)
            occupied[(row['fund_code'], normalized)] = row
        conn.execute("UPDATE fund_nav_data SET nav_date=?, quality_status='valid', quality_reason=NULL WHERE id=?",
                     (normalized, row['id']))


def recalculate_adj_nav(conn, fund_code=None):
    """Full-series dividend reinvestment, equivalent to legacy email/ZX math.

    Missing accumulated NAV makes the entire adjusted series unavailable; mixing
    unit and accumulated NAV across individual rows would invent dividends.
    """
    codes = [fund_code] if fund_code else [r[0] for r in conn.execute('SELECT DISTINCT fund_code FROM fund_nav_data')]
    changed = 0
    for code in codes:
        conn.execute('UPDATE fund_nav_data SET adj_nav=NULL WHERE fund_code=?', (code,))
        rows = conn.execute('SELECT id,unit_nav,accum_nav FROM valid_fund_nav WHERE fund_code=? ORDER BY nav_date', (code,)).fetchall()
        if not rows:
            continue
        if any(not positive_number(r[1]) or not positive_number(r[2]) for r in rows):
            conn.execute("UPDATE fund_nav_data SET adj_nav_reason='累计净值缺失，无法计算完整复权序列' WHERE fund_code=?", (code,))
            continue
        adjusted = 1.0
        updates = [(adjusted, rows[0][0])]
        for previous, current in zip(rows, rows[1:]):
            adjusted *= (float(previous[1]) + float(current[2]) - float(previous[2])) / float(previous[1])
            if not math.isfinite(adjusted) or adjusted <= 0:
                updates = []
                break
            updates.append((adjusted, current[0]))
        if not updates:
            conn.execute("UPDATE fund_nav_data SET adj_nav_reason='累计净值变化无法形成有效复权序列' WHERE fund_code=?", (code,))
            continue
        conn.executemany('UPDATE fund_nav_data SET adj_nav=?, adj_nav_reason=NULL WHERE id=?', updates)
        changed += len(updates)
    return changed


def initialize_connection(conn):
    """Transactional migration. Caller owns connection; no import-time IO."""
    has_versions = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'").fetchone()
    if has_versions and conn.execute('SELECT 1 FROM schema_migrations WHERE version=?', (SCHEMA_VERSION,)).fetchone():
        return conn
    # SQLite DDL needs an explicit transaction; SAVEPOINT also preserves caller transactions.
    conn.execute('SAVEPOINT fundata_schema_migration')
    try:
        conn.execute('CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)')
        _rename(conn, 'funds', {'产品代码':'fund_code','产品名称':'fund_name','首次录入时间':'created_at'})
        _rename(conn, 'fund_nav_data', {'产品代码':'fund_code','产品名称':'fund_name','净值日期':'nav_date','单位净值':'unit_nav','累计单位净值':'accum_nav'})
        conn.execute('''CREATE TABLE IF NOT EXISTS funds(fund_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL UNIQUE,fund_name TEXT,created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
        _add_columns(conn, 'funds', {n:'TEXT' for n in ('benchmark_index','strategy_l1','strategy_l2','strategy_l3','manager','custodian','inception_date','start_date','display','created_at')})
        conn.execute('''CREATE TABLE IF NOT EXISTS email_sources(id INTEGER PRIMARY KEY AUTOINCREMENT,
            邮件主题 TEXT,邮件发件人 TEXT,邮件日期 TEXT,附件文件名 TEXT,sheet名称 TEXT,记录时间 TEXT DEFAULT CURRENT_TIMESTAMP)''')
        conn.execute('''CREATE TABLE IF NOT EXISTS fund_nav_data(id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id INTEGER REFERENCES funds(fund_id),fund_code TEXT NOT NULL,fund_name TEXT,
            nav_date TEXT NOT NULL,unit_nav REAL NOT NULL,accum_nav REAL,source_id INTEGER,
            UNIQUE(fund_code,nav_date))''')
        _add_columns(conn, 'fund_nav_data', {'fund_id':'INTEGER','fund_name':'TEXT','source_id':'INTEGER','adj_nav':'REAL',
            'data_source':'TEXT', '录入时间':'TEXT', 'inserted_at':'TEXT', 'quality_status':"TEXT NOT NULL DEFAULT 'valid'",'quality_reason':'TEXT','adj_nav_reason':'TEXT'})
        conn.execute('''UPDATE fund_nav_data SET data_source=CASE WHEN source_id IS NOT NULL THEN 'email' ELSE 'manual' END
            WHERE data_source IS NULL OR data_source='' ''')
        conn.execute('UPDATE fund_nav_data SET 录入时间=COALESCE(录入时间,inserted_at,CURRENT_TIMESTAMP)')
        conn.execute('''INSERT OR IGNORE INTO funds(fund_code,fund_name)
            SELECT fund_code,MIN(fund_name) FROM fund_nav_data WHERE fund_code IS NOT NULL GROUP BY fund_code''')
        conn.execute('''UPDATE fund_nav_data SET fund_id=(SELECT fund_id FROM funds f WHERE f.fund_code=fund_nav_data.fund_code)
            WHERE fund_id IS NULL''')
        conn.execute('CREATE TABLE IF NOT EXISTS sync_state(key TEXT PRIMARY KEY,value TEXT)')
        conn.execute('''CREATE TABLE IF NOT EXISTS extraction_failures(id INTEGER PRIMARY KEY AUTOINCREMENT,
            失败时间 TEXT DEFAULT CURRENT_TIMESTAMP,邮件主题 TEXT,邮件发件人 TEXT,邮件日期 TEXT,
            附件文件名 TEXT,sheet名称 TEXT,失败原因 TEXT)''')
        _add_columns(conn, 'extraction_failures', {'mailbox_uid':'INTEGER','uidvalidity':'TEXT','status':"TEXT NOT NULL DEFAULT 'pending'",
            'retry_count':'INTEGER NOT NULL DEFAULT 0','last_retry_at':'TEXT','resolved_at':'TEXT','raw_json':'TEXT'})
        _add_columns(conn, 'email_sources', {'mailbox_uid':'INTEGER','uidvalidity':'TEXT'})
        conn.execute('''CREATE TABLE IF NOT EXISTS nav_quality_audit(id INTEGER PRIMARY KEY AUTOINCREMENT,
            nav_id INTEGER NOT NULL,reason TEXT NOT NULL,normalized_date TEXT,original_json TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,UNIQUE(nav_id,reason))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS sync_runs(id INTEGER PRIMARY KEY AUTOINCREMENT,
            trigger TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT,status TEXT NOT NULL,
            processed INTEGER NOT NULL DEFAULT 0,added INTEGER NOT NULL DEFAULT 0,
            duplicates INTEGER NOT NULL DEFAULT 0,failed INTEGER NOT NULL DEFAULT 0,error TEXT)''')
        _add_columns(conn,'sync_runs',{'heartbeat_at':'TEXT'})
        conn.execute('''CREATE VIEW IF NOT EXISTS valid_fund_nav AS SELECT * FROM fund_nav_data WHERE quality_status='valid' ''')
        refresh_quality(conn)
        # Preserve pre-recalculation values, including the old adjusted_nav column.
        cols = _columns(conn, 'fund_nav_data')
        cur = conn.execute('SELECT * FROM fund_nav_data WHERE adj_nav IS NOT NULL' + (' OR adjusted_nav IS NOT NULL' if 'adjusted_nav' in cols else ''))
        names = [d[0] for d in cur.description]
        for row in cur.fetchall():
            _audit(conn, dict(zip(names,row)), 'adjusted_nav_recalculated')
        recalculate_adj_nav(conn)
        conn.execute('CREATE INDEX IF NOT EXISTS idx_quality_fund_date ON fund_nav_data(quality_status,fund_id,nav_date)')
        conn.execute('INSERT INTO schema_migrations VALUES(?,?)',(SCHEMA_VERSION,datetime.now(timezone.utc).isoformat()))
        conn.execute('RELEASE SAVEPOINT fundata_schema_migration')
    except BaseException:
        conn.execute('ROLLBACK TO SAVEPOINT fundata_schema_migration')
        conn.execute('RELEASE SAVEPOINT fundata_schema_migration')
        raise
    return conn


def initialize_database(db_path, backup=True):
    path = Path(db_path).resolve()
    conn = sqlite3.connect(str(path), timeout=30)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    current = 'schema_migrations' in tables and conn.execute('SELECT 1 FROM schema_migrations WHERE version=?',(SCHEMA_VERSION,)).fetchone()
    if current:
        return conn
    try:
        if tables and backup:
            directory = path.parent / 'backups'
            directory.mkdir(exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
            backup_path = directory / f'{path.stem}-pre-v{SCHEMA_VERSION}-{stamp}.db'
            with sqlite3.connect(backup_path) as destination:
                conn.backup(destination)
            # Validate the exact input in memory before touching the source.
            trial = sqlite3.connect(':memory:')
            try:
                conn.backup(trial)
                initialize_connection(trial)
                if trial.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                    raise RuntimeError('Migration validation failed')
            finally:
                trial.close()
        initialize_connection(conn)
        return conn
    except BaseException:
        conn.close()
        raise
