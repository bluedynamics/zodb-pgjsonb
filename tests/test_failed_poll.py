"""#142: a poll that fails because PostgreSQL is unreachable must not raise.

ZODB polls from Connection.open() (via DB.open()) and from
Connection.afterCompletion() (via transaction.abort()/commit()).  An
exception there strands the ZODB Connection: DB.open() never returns it, and
Zope's publisher skips close() when abort() raises.  A Connection that is
never closed is never freed, so each failover leaked a full object cache.
"""

from persistent.mapping import PersistentMapping
from psycopg_pool import PoolTimeout
from tests.conftest import clean_db
from tests.conftest import DSN
from ZODB.utils import u64

import psycopg
import pytest
import transaction
import ZODB


pytestmark = pytest.mark.db


def _storage(**kw):
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    params = {"cache_warm_pct": 0, "pool_timeout": 2.0}
    params.update(kw)
    return PGJsonbStorage(DSN, **params)


def _unreachable(monkeypatch):
    """Make every poll fail as it does while the primary is switching."""
    import zodb_pgjsonb.instance as instance_mod

    def boom(conn):
        raise psycopg.OperationalError("simulated: server closed the connection")

    monkeypatch.setattr(instance_mod, "_read_max_tid", boom)


def _reachable(monkeypatch):
    monkeypatch.undo()


def _store_object(db):
    conn = db.open()
    conn.root()["obj"] = PersistentMapping(value=1)
    transaction.commit()
    oid = conn.root()["obj"]._p_oid
    conn.close()
    return oid


def test_failed_poll_returns_none_and_frees_slot(monkeypatch):
    storage = _storage(pool_size=1, pool_max_size=1)
    try:
        inst = storage.new_instance()
        _unreachable(monkeypatch)
        assert inst.poll_invalidations() is None
        assert inst._conn is None
        pool = storage._instance_pool
        conn = pool.getconn(timeout=0.5)
        pool.putconn(conn)
        inst.release()
    finally:
        storage.close()


def test_pool_timeout_during_poll_returns_none(monkeypatch):
    storage = _storage()
    try:
        inst = storage.new_instance()

        def timeout(*args, **kwargs):
            raise PoolTimeout("simulated: couldn't get a connection")

        monkeypatch.setattr(storage._instance_pool, "getconn", timeout)
        assert inst.poll_invalidations() is None
        monkeypatch.undo()
        inst.release()
    finally:
        storage.close()


def test_failed_poll_clears_load_cache(monkeypatch):
    storage = _storage()
    db = ZODB.DB(storage)
    try:
        oid = _store_object(db)
        inst = storage.new_instance()
        inst.poll_invalidations()
        inst.load(oid)
        assert inst._load_cache.get(u64(oid)) is not None
        _unreachable(monkeypatch)
        inst.poll_invalidations()
        assert inst._load_cache.get(u64(oid)) is None
        inst.release()
    finally:
        db.close()


def test_other_errors_still_raise(monkeypatch):
    import zodb_pgjsonb.instance as instance_mod

    storage = _storage()
    try:
        inst = storage.new_instance()

        def bug(conn):
            raise ValueError("not a connection problem")

        monkeypatch.setattr(instance_mod, "_read_max_tid", bug)
        with pytest.raises(ValueError):
            inst.poll_invalidations()
        assert inst._conn is None
        inst.release()
    finally:
        storage.close()


def test_load_after_failed_poll_begins_snapshot(monkeypatch):
    storage = _storage()
    db = ZODB.DB(storage)
    try:
        oid = _store_object(db)
        inst = storage.new_instance()
        _unreachable(monkeypatch)
        assert inst.poll_invalidations() is None
        _reachable(monkeypatch)
        assert inst._in_read_txn is False
        inst.load(oid)
        assert inst._in_read_txn is True
        assert inst._polled_tid == u64(storage.lastTransaction())
        inst.release()
    finally:
        db.close()


@pytest.mark.parametrize(
    "read",
    [
        pytest.param(lambda inst, oid: inst.load(oid), id="load"),
        pytest.param(lambda inst, oid: inst.load_multiple([oid]), id="load_multiple"),
        pytest.param(lambda inst, oid: inst.pg_connection, id="pg_connection"),
    ],
)
def test_reads_raise_while_still_unreachable(monkeypatch, read):
    storage = _storage()
    db = ZODB.DB(storage)
    try:
        oid = _store_object(db)
        inst = storage.new_instance()
        _unreachable(monkeypatch)
        assert inst.poll_invalidations() is None
        with pytest.raises(psycopg.OperationalError):
            read(inst, oid)
        _reachable(monkeypatch)
        read(inst, oid)
        assert inst._in_read_txn is True
        inst.release()
    finally:
        db.close()


def test_abort_during_outage_does_not_raise(monkeypatch):
    """transaction.abort() must not raise, so the publisher reaches close()."""
    storage = _storage()
    db = ZODB.DB(storage, pool_size=3)
    try:
        _store_object(db)
        tm = transaction.TransactionManager()
        conn = db.open(tm)
        tm.begin()
        conn.root()["obj"]["value"]
        _unreachable(monkeypatch)
        tm.abort()
        conn.close()
        _reachable(monkeypatch)
        assert conn.opened is None
        assert len(db.pool.available) == 1
    finally:
        db.close()


def test_db_open_during_outage_does_not_raise(monkeypatch):
    storage = _storage()
    db = ZODB.DB(storage, pool_size=3)
    try:
        _store_object(db)
        _unreachable(monkeypatch)
        conn = db.open()
        conn.close()
        _reachable(monkeypatch)
        conn = db.open()
        assert conn.root()["obj"]["value"] == 1
        conn.close()
        assert all(c.opened is None for c in db.pool.all)
    finally:
        db.close()


@pytest.mark.parametrize(
    "read",
    [
        pytest.param(lambda inst, oid: inst.load(oid), id="load"),
        pytest.param(
            lambda inst, oid: inst.load_multiple([oid])[oid], id="load_multiple"
        ),
    ],
)
def test_read_after_failed_poll_sees_commit_from_outage(monkeypatch, read):
    """The repeated poll must advance the shared-cache gate.

    Another process commits during the outage, so this process's shared
    cache still holds the state from before it.  Read with the stale
    ``_polled_tid``, the shared cache would return that state instead of the
    one committed meanwhile.
    """
    from zodb_pgjsonb.storage import PGJsonbStorage

    storage = _storage()
    db = ZODB.DB(storage)
    other_db = None
    try:
        oid = _store_object(db)
        inst = storage.new_instance()
        inst.poll_invalidations()
        _, old_tid = inst.load(oid)  # now in the shared cache
        _unreachable(monkeypatch)
        assert inst.poll_invalidations() is None
        _reachable(monkeypatch)

        other_db = ZODB.DB(PGJsonbStorage(DSN, cache_warm_pct=0))
        tm = transaction.TransactionManager()
        conn = other_db.open(tm)
        conn.root()["obj"]["value"] = 2
        tm.commit()
        new_tid = conn.root()["obj"]._p_serial
        conn.close()
        assert new_tid != old_tid

        _, tid = read(inst, oid)
        assert tid == new_tid
        inst.release()
    finally:
        if other_db is not None:
            other_db.close()
        db.close()


def test_undo_after_failed_poll_repeats_poll(monkeypatch):
    """Readers that go straight to the connection get a snapshot too."""
    storage = _storage()
    try:
        inst = storage.new_instance()
        _unreachable(monkeypatch)
        assert inst.poll_invalidations() is None
        _reachable(monkeypatch)
        inst._ensure_conn()
        assert inst._poll_failed is False
        assert inst._in_read_txn is True
        inst.release()
    finally:
        storage.close()


def test_released_instance_does_not_repeat_poll(monkeypatch):
    """A late read on a released instance must not check out a slot that
    release() never returns (#126)."""
    storage = _storage(pool_size=1, pool_max_size=1)
    try:
        inst = storage.new_instance()
        _unreachable(monkeypatch)
        assert inst.poll_invalidations() is None
        _reachable(monkeypatch)
        inst.release()
        inst._repeat_failed_poll()
        assert inst._conn is None
        pool = storage._instance_pool
        conn = pool.getconn(timeout=0.5)
        pool.putconn(conn)
    finally:
        storage.close()


def test_traceback_logged_once_per_outage(monkeypatch, caplog):
    storage = _storage()
    try:
        inst = storage.new_instance()
        _unreachable(monkeypatch)
        with caplog.at_level("WARNING", logger="zodb_pgjsonb.instance"):
            inst.poll_invalidations()
            inst.poll_invalidations()
            inst.poll_invalidations()
        polls = [r for r in caplog.records if "poll_invalidations" in r.getMessage()]
        assert len(polls) == 3
        assert [bool(r.exc_info) for r in polls] == [True, False, False]
        _reachable(monkeypatch)
        inst.release()
    finally:
        storage.close()
