from email.message import EmailMessage
import sqlite3

import pytest

from sync_service import SyncError, run_email_sync


class Mailbox:
    untagged_responses = {'UIDVALIDITY': [b'10']}
    def __init__(self, *args, **kwargs):
        pass
    def login(self, *args):
        return 'OK', []
    def _simple_command(self, *args):
        return 'OK', []
    def select(self, *args, **kwargs):
        return 'OK', [b'0']
    def uid(self, *args):
        return 'OK', [b'']
    def logout(self):
        pass


def test_no_new_mail_is_success_with_durable_run(tmp_path):
    path = tmp_path / 'data.db'
    result = run_email_sync('u','p',path,imap_factory=Mailbox)
    assert result['status'] == 'success'
    assert result['processed'] == result['added'] == 0
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT status FROM sync_runs').fetchone()[0] == 'success'
        assert conn.execute("SELECT value FROM sync_state WHERE key='sync_last_success_time'").fetchone()[0]


@pytest.mark.parametrize('stage', ['connect','login','search'])
def test_transport_failures_raise_and_record_error(tmp_path, stage):
    class Broken(Mailbox):
        def __init__(self,*args,**kwargs):
            if stage == 'connect':
                raise OSError('offline')
        def login(self,*args):
            return ('NO', []) if stage == 'login' else ('OK',[])
        def uid(self,*args):
            return 'NO', []
    path = tmp_path / 'data.db'
    with pytest.raises(SyncError):
        run_email_sync('u','p',path,imap_factory=Broken)
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT status FROM sync_runs').fetchone()[0] == 'error'
        assert not conn.execute("SELECT 1 FROM sync_state WHERE key='sync_last_success_time'").fetchone()


def test_incremental_range_does_not_reprocess_last_uid(tmp_path):
    path = tmp_path / 'data.db'
    run_email_sync('u','p',path,imap_factory=Mailbox)
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT OR REPLACE INTO sync_state VALUES('last_uid','20')")
    class OldUID(Mailbox):
        def uid(self,*args):
            assert args[0] == 'search'
            return 'OK',[b'20']
    assert run_email_sync('u','p',path,imap_factory=OldUID)['processed'] == 0


def test_partial_failure_has_replay_coordinates_and_resolves_idempotently(tmp_path, monkeypatch):
    import pandas as pd
    import get_163_email
    path = tmp_path / 'data.db'
    msg = EmailMessage()
    msg['Subject'] = 'NAV'
    msg.set_content('report')
    class OneMail(Mailbox):
        def uid(self, command, *args):
            return ('OK',[b'21']) if command == 'search' else ('OK',[(b'21',msg.as_bytes())])
    monkeypatch.setattr(get_163_email,'extract_excel_attachments',lambda msg,failures:
                        (failures.append(dict(filename='nav.xlsx',sheet_name='NAV',reason='unrecognized')) or [], True))
    result = run_email_sync('u','p',path,imap_factory=OneMail)
    assert result['status'] == 'partial_success'
    with sqlite3.connect(path) as conn:
        fail = conn.execute('SELECT id,mailbox_uid,uidvalidity FROM extraction_failures').fetchone()
        assert fail[1:] == (21,'10')
        assert conn.execute("SELECT value FROM sync_state WHERE key='last_uid'").fetchone()[0] == '21'
    frame = pd.DataFrame([{'产品代码':'A','产品名称':'Alpha','净值日期':'20260101','单位净值':1.,'累计单位净值':1.}])
    monkeypatch.setattr(get_163_email,'extract_excel_attachments',lambda msg,failures:
                        ([dict(filename='nav.xlsx',sheet_name='NAV',data=frame)],True))
    result = run_email_sync('u','p',path,retry_failure_id=fail[0],imap_factory=OneMail)
    assert result['added'] == 1
    assert result['status'] == 'success'
    assert run_email_sync('u','p',path,retry_uid=21,imap_factory=OneMail)['duplicates'] == 1
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT status FROM extraction_failures').fetchone()[0] == 'resolved'
        assert conn.execute('SELECT count(*) FROM fund_nav_data').fetchone()[0] == 1
        assert conn.execute('SELECT count(*) FROM email_sources').fetchone()[0] == 1


def test_fetch_failure_keeps_last_committed_uid(tmp_path):
    path = tmp_path / 'data.db'
    msg = EmailMessage()
    msg.set_content('no attachment')
    class Interrupted(Mailbox):
        def uid(self,command,*args):
            if command == 'search':
                return 'OK',[b'1 2 3']
            if args[0] == '2':
                return 'NO',[]
            return 'OK',[(b'1',msg.as_bytes())]
    with pytest.raises(SyncError):
        run_email_sync('u','p',path,imap_factory=Interrupted)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT value FROM sync_state WHERE key='last_uid'").fetchone()[0] == '1'
        assert conn.execute('SELECT processed FROM sync_runs').fetchone()[0] == 1


def test_historical_failure_without_uid_cannot_be_replayed(tmp_path):
    from fund_store import initialize_database
    path = tmp_path / 'data.db'
    with initialize_database(path) as conn:
        conn.execute("INSERT INTO extraction_failures(失败原因) VALUES('old')")
    with pytest.raises(SyncError,match='Historical record'):
        run_email_sync('u','p',path,retry_failure_id=1,imap_factory=Mailbox)


def test_conflicting_retry_preserves_value_evidence_and_stays_unresolved(tmp_path,monkeypatch):
    import pandas as pd
    import get_163_email
    from fund_store import initialize_database
    path = tmp_path/'data.db'
    with initialize_database(path) as conn:
        conn.execute("INSERT INTO funds(fund_code) VALUES('A')")
        conn.execute("INSERT INTO fund_nav_data(fund_id,fund_code,nav_date,unit_nav,accum_nav) VALUES(1,'A','2026-01-01',1,1)")
        conn.execute("INSERT INTO extraction_failures(mailbox_uid,uidvalidity,附件文件名,sheet名称) VALUES(21,'10','nav.xlsx','NAV')")
    msg = EmailMessage()
    msg.set_content('NAV')
    class OneMail(Mailbox):
        def uid(self,*args):
            return 'OK',[(b'21',msg.as_bytes())]
    frame = pd.DataFrame([{'产品代码':'A','产品名称':'Alpha','净值日期':'20260101','单位净值':1.5,'累计单位净值':1.5}])
    monkeypatch.setattr(get_163_email,'extract_excel_attachments',lambda msg,failures:([dict(filename='nav.xlsx',sheet_name='NAV',data=frame)],True))
    result = run_email_sync('u','p',path,retry_failure_id=1,imap_factory=OneMail)
    assert result['status'] == 'partial_success'
    assert result['duplicates'] == 0
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT unit_nav FROM fund_nav_data').fetchone()[0] == 1
        assert conn.execute('SELECT status FROM extraction_failures').fetchone()[0] == 'pending'
        assert conn.execute("SELECT original_json FROM nav_quality_audit WHERE reason LIKE 'incoming_value_conflict:%'").fetchone()


def test_recalculation_failure_rolls_back_message_and_checkpoint(tmp_path,monkeypatch):
    import pandas as pd
    import get_163_email
    import sync_service
    path = tmp_path/'data.db'
    msg = EmailMessage()
    msg.set_content('NAV')
    class OneMail(Mailbox):
        def uid(self,command,*args):
            return ('OK',[b'21']) if command=='search' else ('OK',[(b'21',msg.as_bytes())])
    frame = pd.DataFrame([{'产品代码':'A','产品名称':'Alpha','净值日期':'20260101','单位净值':1.,'累计单位净值':1.}])
    monkeypatch.setattr(get_163_email,'extract_excel_attachments',lambda msg,failures:([dict(filename='nav.xlsx',sheet_name='NAV',data=frame)],True))
    def broken(*args):
        raise RuntimeError('recalc failed')
    monkeypatch.setattr(sync_service,'recalculate_adj_nav',broken)
    with pytest.raises(SyncError):
        run_email_sync('u','p',path,imap_factory=OneMail)
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT count(*) FROM fund_nav_data').fetchone()[0] == 0
        assert conn.execute('SELECT count(*) FROM email_sources').fetchone()[0] == 0
        assert conn.execute('SELECT count(*) FROM funds').fetchone()[0] == 0
        assert not conn.execute("SELECT value FROM sync_state WHERE key='last_uid'").fetchone()
        assert conn.execute('SELECT added,processed,status FROM sync_runs').fetchone() == (0,0,'error')


def test_recent_heartbeat_blocks_stealing_old_run_lease(tmp_path):
    from fund_store import initialize_database
    from datetime import datetime,timezone
    path = tmp_path/'data.db'
    with initialize_database(path) as conn:
        conn.execute("INSERT INTO sync_runs(trigger,started_at,heartbeat_at,status) VALUES('scheduled','2020-01-01',?,'running')",(datetime.now(timezone.utc).isoformat(),))
    with pytest.raises(SyncError,match='already running'):
        run_email_sync('u','p',path,imap_factory=Mailbox)
def test_expired_sync_summary_allows_manual_recovery(tmp_path):
    from sync_service import get_sync_summary
    from fund_store import initialize_database
    conn = initialize_database(tmp_path / 'expired.db')
    conn.execute("INSERT INTO sync_state VALUES('sync_last_status','running')")
    conn.execute("INSERT INTO sync_runs(trigger,started_at,status) VALUES('manual','2020-01-01T00:00:00+00:00','running')")
    summary = get_sync_summary(conn)
    assert summary['is_running'] is False
    assert summary['last_status'] == 'interrupted'
    conn.close()
