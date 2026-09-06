from pathlib import Path
import sqlite3

import pytest

from deploy.manage_release import ReleaseError, backup_database, contained, exact_commit, fingerprint, restore_database


def test_path_validation_rejects_escape_and_root(tmp_path):
    with pytest.raises(ReleaseError):
        contained(tmp_path,tmp_path)
    with pytest.raises(ReleaseError):
        contained(tmp_path,tmp_path/'..'/'outside',exists=False)
    assert contained(tmp_path,'backups/data.db',exists=False) == tmp_path/'backups'/'data.db'


@pytest.mark.parametrize('value',['main','HEAD','abcdef','a'*39,'A'*40,'a'*40+';echo'])
def test_release_requires_exact_revision(value):
    with pytest.raises(ReleaseError):
        exact_commit(value)
    assert exact_commit('a'*40) == 'a'*40


def test_backup_and_restore_roundtrip_preserves_pre_restore_evidence(tmp_path):
    current = tmp_path/'fund.db'
    saved = tmp_path/'before.db'
    recovery = tmp_path/'recovery.db'
    with sqlite3.connect(current) as conn:
        conn.execute('CREATE TABLE nav(value REAL)')
        conn.execute('INSERT INTO nav VALUES(1)')
    digest = backup_database(current,saved)
    assert digest == fingerprint(saved)
    with sqlite3.connect(current) as conn:
        conn.execute('INSERT INTO nav VALUES(2)')
    backup_database(current,recovery)
    restore_database(saved,current)
    with sqlite3.connect(current) as conn:
        assert conn.execute('SELECT value FROM nav').fetchall() == [(1.0,)]
    with sqlite3.connect(recovery) as conn:
        assert conn.execute('SELECT COUNT(*) FROM nav').fetchone()[0] == 2
    with pytest.raises(ReleaseError):
        backup_database(current,saved)
def test_refuses_legacy_unprotected_rollback(tmp_path, monkeypatch):
    import deploy.manage_release as release
    monkeypatch.setattr(release, 'run', lambda *args, **kwargs: 'app = FastAPI()')
    with pytest.raises(ReleaseError, match='unprotected'):
        release.require_protected_revision(tmp_path, 'a' * 40)
