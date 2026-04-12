"""Integration tests for startup DDL advisory lock serialization.

Requires a running PostgreSQL on localhost:5433 (see conftest.py).
"""

from tests.conftest import clean_db
from tests.conftest import DSN

import pytest
import threading
import time


pytestmark = pytest.mark.db


def test_apply_pending_ddl_serializes_across_threads():
    """Two concurrent `_apply_pending_ddl` calls run sequentially."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        started = []
        finished = []
        barrier = threading.Barrier(2)
        lock = threading.Lock()

        def slow_action(dsn):
            with lock:
                started.append(time.monotonic())
            time.sleep(0.5)
            with lock:
                finished.append(time.monotonic())

        def worker():
            storage._pending_ddl.append((slow_action, "slow"))
            barrier.wait()
            storage._apply_pending_ddl()

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert len(started) == 2
        assert len(finished) == 2
        # Second action must start AFTER first action finishes
        started.sort()
        finished.sort()
        assert started[1] >= finished[0] - 0.05  # 50ms scheduling slack
    finally:
        storage.close()


def test_apply_pending_ddl_requeues_on_lock_timeout():
    """When lock is held externally, pending work is requeued."""
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_KEY
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_NS
    from zodb_pgjsonb.storage import PGJsonbStorage

    import os
    import psycopg

    clean_db()
    # Acquire the lock from a separate connection, hold it.
    holder = psycopg.connect(DSN, autocommit=True)
    holder.execute(
        "SELECT pg_advisory_lock(%s, %s)",
        (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY),
    )
    try:
        storage = PGJsonbStorage(DSN, cache_warm_pct=0)
        try:
            # Queue an action; expect it to be requeued after timeout.
            def noop_action(dsn):
                pass

            storage._pending_ddl.append((noop_action, "noop"))

            # Force a very short timeout via env var.
            old = os.environ.get("ZODB_PGJSONB_DDL_LOCK_TIMEOUT")
            os.environ["ZODB_PGJSONB_DDL_LOCK_TIMEOUT"] = "500ms"
            try:
                storage._apply_pending_ddl()
            finally:
                if old is None:
                    del os.environ["ZODB_PGJSONB_DDL_LOCK_TIMEOUT"]
                else:
                    os.environ["ZODB_PGJSONB_DDL_LOCK_TIMEOUT"] = old

            # Pending work was requeued, not lost
            assert len(storage._pending_ddl) == 1
            assert storage._pending_ddl[0][1] == "noop"
        finally:
            storage.close()
    finally:
        holder.execute(
            "SELECT pg_advisory_unlock(%s, %s)",
            (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY),
        )
        holder.close()


def test_lock_released_when_holder_disconnects():
    """PG auto-releases the advisory lock on session disconnect."""
    from zodb_pgjsonb.startup_locks import startup_ddl_lock
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_KEY
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_NS

    import psycopg

    clean_db()
    # Session 1: acquire then close without unlock
    s1 = psycopg.connect(DSN, autocommit=True)
    s1.execute(
        "SELECT pg_advisory_lock(%s, %s)",
        (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY),
    )
    s1.close()  # auto-release on disconnect

    # Session 2: should acquire immediately
    start = time.monotonic()
    with startup_ddl_lock(DSN, timeout="1s"):
        elapsed = time.monotonic() - start
        assert elapsed < 0.5
