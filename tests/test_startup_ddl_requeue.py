"""Deferred startup DDL survives an unreachable PostgreSQL (#144) and its
direct connections carry a connect_timeout (#145).
"""

from contextlib import contextmanager
from psycopg.conninfo import conninfo_to_dict
from zodb_pgjsonb import storage as storage_module
from zodb_pgjsonb.storage import _ddl_dsn

import psycopg
import pytest


def _timeout(dsn):
    return conninfo_to_dict(dsn).get("connect_timeout")


# ── _ddl_dsn (#145) ──────────────────────────────────────────────────


def test_ddl_dsn_adds_pool_timeout_as_connect_timeout(monkeypatch):
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    assert _timeout(_ddl_dsn("dbname=x host=h", 30.0)) == "30"


def test_ddl_dsn_rounds_up_and_respects_libpq_minimum(monkeypatch):
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    assert _timeout(_ddl_dsn("dbname=x host=h", 7.2)) == "8"
    assert _timeout(_ddl_dsn("dbname=x host=h", 0.5)) == "2"


def test_ddl_dsn_keeps_connect_timeout_from_dsn(monkeypatch):
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    dsn = "dbname=x host=h connect_timeout=3"
    assert _ddl_dsn(dsn, 30.0) == dsn
    url = "postgresql://u:p@h:5432/x?connect_timeout=3"
    assert _ddl_dsn(url, 30.0) == url


def test_ddl_dsn_keeps_pgconnect_timeout_from_env(monkeypatch):
    monkeypatch.setenv("PGCONNECT_TIMEOUT", "4")
    assert _ddl_dsn("dbname=x host=h", 30.0) == "dbname=x host=h"


def test_ddl_dsn_without_pool_timeout_is_unchanged(monkeypatch):
    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    assert _ddl_dsn("dbname=x host=h", None) == "dbname=x host=h"


# ── DDL path uses the DDL DSN (#145) ─────────────────────────────────


@pytest.mark.db
def test_storage_derives_ddl_dsn_from_pool_timeout(monkeypatch):
    from tests.conftest import clean_db
    from tests.conftest import DSN
    from zodb_pgjsonb.storage import PGJsonbStorage

    monkeypatch.delenv("PGCONNECT_TIMEOUT", raising=False)
    clean_db()
    s = PGJsonbStorage(DSN, pool_timeout=5)
    try:
        assert _timeout(s._ddl_dsn) == "5"
    finally:
        s.close()


@pytest.mark.db
def test_ddl_path_connects_with_connect_timeout(storage, monkeypatch):
    """Probe, lock session, DDL connection and callables all get the
    DDL DSN."""
    seen = []
    real_connect = psycopg.connect
    real_lock = storage_module.startup_ddl_lock

    def spy_connect(dsn, *args, **kwargs):
        seen.append(("connect", dsn))
        return real_connect(dsn, *args, **kwargs)

    def spy_lock(dsn, *args, **kwargs):
        seen.append(("lock", dsn))
        return real_lock(dsn, *args, **kwargs)

    monkeypatch.setattr(psycopg, "connect", spy_connect)
    monkeypatch.setattr(storage_module, "startup_ddl_lock", spy_lock)

    storage._pending_ddl.append(
        ("CREATE TABLE IF NOT EXISTS ddl_dsn_probe (x int)", "probe-ddl", "v1")
    )
    storage.defer_startup_action(
        lambda dsn: seen.append(("action", dsn)), "probe-action"
    )
    storage._apply_pending_ddl()

    kinds = [kind for kind, _ in seen]
    assert "lock" in kinds
    assert "action" in kinds
    assert kinds.count("connect") >= 2  # pre-lock probe + DDL connection
    assert all(_timeout(dsn) is not None for _, dsn in seen), seen


# ── Requeue while PostgreSQL is unreachable (#144) ───────────────────


@contextmanager
def _unreachable_lock(dsn, timeout=None):
    raise psycopg.OperationalError("connection refused")
    yield  # pragma: no cover


@pytest.mark.db
def test_requeued_when_lock_connection_fails(storage, monkeypatch):
    calls = []
    storage.defer_startup_action(lambda dsn: calls.append(1), "counter")

    monkeypatch.setattr(storage_module, "startup_ddl_lock", _unreachable_lock)
    storage._apply_pending_ddl()  # must not raise
    assert calls == []
    assert [e[1] for e in storage._pending_ddl] == ["counter"]

    monkeypatch.undo()
    storage._apply_pending_ddl()
    assert calls == [1]
    assert storage._pending_ddl == []


@pytest.mark.db
def test_requeued_when_in_lock_recheck_fails(storage, monkeypatch):
    storage.defer_startup_action(lambda dsn: None, "counter", version="v1")
    real_filter = storage._filter_stale

    def failing_recheck(pending, conn=None):
        if conn is not None:
            raise psycopg.OperationalError("server closed the connection")
        return real_filter(pending)

    monkeypatch.setattr(storage, "_filter_stale", failing_recheck)
    storage._apply_pending_ddl()
    assert [e[1] for e in storage._pending_ddl] == ["counter"]


@pytest.mark.db
def test_requeued_and_raised_on_unexpected_lock_phase_error(storage, monkeypatch):
    storage.defer_startup_action(lambda dsn: None, "counter", version="v1")
    real_filter = storage._filter_stale

    def broken_recheck(pending, conn=None):
        if conn is not None:
            raise RuntimeError("bug")
        return real_filter(pending)

    monkeypatch.setattr(storage, "_filter_stale", broken_recheck)
    with pytest.raises(RuntimeError):
        storage._apply_pending_ddl()
    assert [e[1] for e in storage._pending_ddl] == ["counter"]


@pytest.mark.db
def test_entry_requeued_on_operational_error_dropped_otherwise(storage):
    applied = []

    def lost_connection(dsn):
        raise psycopg.OperationalError("connection lost")

    def broken(dsn):
        raise psycopg.ProgrammingError("syntax error")

    storage.defer_startup_action(lost_connection, "lost")
    storage.defer_startup_action(broken, "broken")
    storage.defer_startup_action(lambda dsn: applied.append(1), "ok")
    storage._apply_pending_ddl()

    assert applied == [1]
    assert [e[1] for e in storage._pending_ddl] == ["lost"]
