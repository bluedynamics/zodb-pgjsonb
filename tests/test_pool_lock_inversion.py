"""#126: the instance must never block on the pool while ZODB holds DB._lock.

ZODB's DB.open() creates new Connections under DB._lock, and
Connection.__init__ calls storage.new_instance().  If new_instance() blocks
in getconn(), every Connection.close() (which needs DB._lock to return its
connection) waits for pool_timeout.  Under load the convoy never drains.
"""

from tests.conftest import clean_db
from tests.conftest import DSN

import pytest
import threading
import time


pytestmark = pytest.mark.db


def _small_storage(**kw):
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    params = {
        "cache_warm_pct": 0,
        "pool_size": 1,
        "pool_max_size": 1,
        "pool_timeout": 5.0,
    }
    params.update(kw)
    return PGJsonbStorage(DSN, **params)


def _wait_for_pool_waiter(pool, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pool.get_stats().get("requests_waiting", 0) >= 1:
            return
        time.sleep(0.02)
    raise AssertionError("no client started waiting on the pool")


def test_new_instance_does_not_check_out():
    storage = _small_storage()
    try:
        pool = storage._instance_pool
        calls = []
        orig = pool.getconn

        def counting_getconn(*a, **k):
            calls.append(1)
            return orig(*a, **k)

        pool.getconn = counting_getconn
        inst = storage.new_instance()
        try:
            assert calls == []
            assert inst._conn is None
            inst.poll_invalidations()
            assert len(calls) == 1
            assert inst._conn is not None
        finally:
            inst.release()
    finally:
        storage.close()


def test_close_not_blocked_by_open_waiting_for_slot():
    import ZODB

    storage = _small_storage(pool_max_size=2)
    db = ZODB.DB(storage)
    pool = storage._instance_pool
    foreign = None
    try:
        c1 = db.open()  # pops the Connection DB() parked: holds slot 1
        c1.root()
        foreign = pool.getconn()  # e.g. a catalog query: holds slot 2
        errors = []

        def opener():
            try:
                c2 = db.open()  # nothing parked: new Connection, needs a slot
                c2.root()
                c2.close()
            except Exception as e:
                errors.append(repr(e))

        t = threading.Thread(target=opener, daemon=True)
        t.start()
        _wait_for_pool_waiter(pool)

        t0 = time.monotonic()
        c1.close()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0, f"close() waited {elapsed:.1f}s for DB._lock"

        pool.putconn(foreign)  # the waiting opener gets this slot
        foreign = None
        t.join(10)
        assert not t.is_alive()
        assert errors == []
    finally:
        if foreign is not None:
            pool.putconn(foreign)
        db.close()
        storage.close()


def test_pg_connection_checks_out_lazily():
    storage = _small_storage()
    try:
        inst = storage.new_instance()
        try:
            conn = inst.pg_connection
            assert conn is not None
            assert conn.execute("SELECT 1 AS x").fetchone()["x"] == 1
        finally:
            inst.release()
    finally:
        storage.close()


def test_load_before_any_poll_works():
    """Explicit-transaction mode can load before poll_invalidations()."""
    from ZODB.utils import z64

    import ZODB

    # pool_max_size=2: the connection DB() parks after creating the root
    # still pins one slot.
    storage = _small_storage(pool_max_size=2)
    db = ZODB.DB(storage)  # creates the root object
    try:
        inst = storage.new_instance()
        try:
            data, tid = inst.load(z64)
            assert data and tid
        finally:
            inst.release()
    finally:
        db.close()
        storage.close()


def test_tpc_abort_without_connection_is_quiet(caplog):
    storage = _small_storage()
    try:
        inst = storage.new_instance()
        try:
            inst.tpc_abort(None)
            assert "Error during rollback" not in caplog.text
        finally:
            inst.release()
    finally:
        storage.close()


def test_failed_poll_returns_slot(monkeypatch):
    """An exception after checkout must not escape Connection.open() with
    the slot still held; ZODB strands that Connection (#126)."""
    import psycopg
    import zodb_pgjsonb.instance as instance_mod

    storage = _small_storage(pool_timeout=2.0)
    try:
        inst = storage.new_instance()

        def boom(conn):
            raise psycopg.OperationalError("simulated failure after BEGIN")

        monkeypatch.setattr(instance_mod, "_read_max_tid", boom)
        with pytest.raises(psycopg.OperationalError):
            inst.poll_invalidations()
        assert inst._conn is None
        # pool_max_size=1: the only slot must be free again
        pool = storage._instance_pool
        conn = pool.getconn(timeout=0.5)
        pool.putconn(conn)
        inst.release()
    finally:
        storage.close()


def test_instance_recovers_after_failed_poll(monkeypatch):
    """A parked instance without a connection must work on its next use."""
    import psycopg
    import zodb_pgjsonb.instance as instance_mod

    storage = _small_storage(pool_timeout=2.0)
    try:
        inst = storage.new_instance()
        real = instance_mod._read_max_tid

        def boom(conn):
            raise psycopg.OperationalError("simulated")

        monkeypatch.setattr(instance_mod, "_read_max_tid", boom)
        with pytest.raises(psycopg.OperationalError):
            inst.poll_invalidations()
        monkeypatch.setattr(instance_mod, "_read_max_tid", real)
        assert inst.poll_invalidations() == []
        assert inst._in_read_txn is True
        inst.release()
    finally:
        storage.close()
