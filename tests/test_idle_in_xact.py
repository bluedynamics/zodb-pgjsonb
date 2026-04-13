"""Tests for #118: read tx must be closed at request end so virtualxids
don't accumulate and block CREATE INDEX CONCURRENTLY.
"""

from tests.conftest import clean_db
from tests.conftest import DSN

import contextlib
import psycopg
import pytest


pytestmark = pytest.mark.db


def _backend_state(monitor_conn, pid):
    """Return (state, xact_age_seconds) for a backend pid via a separate conn."""
    with monitor_conn.cursor() as cur:
        cur.execute(
            "SELECT state, "
            "EXTRACT(EPOCH FROM (now() - xact_start)) AS xact_age "
            "FROM pg_stat_activity WHERE pid = %s",
            (pid,),
        )
        row = cur.fetchone()
        if row is None:
            return (None, None)
        return (row["state"], row["xact_age"])


def test_after_completion_closes_read_txn():
    """afterCompletion() must end the REPEATABLE READ snapshot."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        instance = storage.new_instance()
        try:
            # poll_invalidations opens the read txn
            instance.poll_invalidations()
            assert instance._in_read_txn is True

            # afterCompletion must close it
            instance.afterCompletion()
            assert instance._in_read_txn is False
        finally:
            instance.release()
    finally:
        storage.close()


def test_after_completion_idempotent():
    """Calling afterCompletion twice in a row is safe."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        instance = storage.new_instance()
        try:
            instance.poll_invalidations()
            instance.afterCompletion()
            # Second call: no-op, no error
            instance.afterCompletion()
            assert instance._in_read_txn is False
        finally:
            instance.release()
    finally:
        storage.close()


def test_after_completion_no_op_after_tpc_begin():
    """tpc_begin already ends the read tx — afterCompletion is a no-op then."""
    from persistent.mapping import PersistentMapping
    from zodb_pgjsonb.storage import PGJsonbStorage

    import transaction as txn
    import ZODB

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        db = ZODB.DB(storage)
        try:
            conn = db.open()
            root = conn.root()
            root["x"] = PersistentMapping({"k": "v"})
            txn.commit()
            instance = conn._storage
            # After commit, read tx should be closed (commit path ends it)
            instance.afterCompletion()
            assert instance._in_read_txn is False
            conn.close()
        finally:
            db.close()
    finally:
        storage.close()


def test_after_completion_safe_when_conn_killed():
    """If the conn is externally terminated, afterCompletion must not raise."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        instance = storage.new_instance()
        try:
            instance.poll_invalidations()
            assert instance._in_read_txn is True

            # Kill the backend from a separate session
            pid = instance._conn.info.backend_pid
            killer = psycopg.connect(DSN)
            try:
                killer.execute("SELECT pg_terminate_backend(%s)", (pid,))
                killer.commit()
            finally:
                killer.close()

            # afterCompletion must swallow the resulting connection error
            instance.afterCompletion()  # must not raise
        finally:
            # Force release without raising — conn is dead
            with contextlib.suppress(Exception):
                instance.release()
    finally:
        with contextlib.suppress(Exception):
            storage.close()


def test_zodb_connection_close_releases_virtualxid():
    """End-to-end: closing a read-only ZODB connection releases its virtualxid.

    Without afterCompletion, the storage instance's PG connection would
    stay 'idle in transaction' — the connection sits in the ZODB pool
    holding a virtualxid that blocks CREATE INDEX CONCURRENTLY.

    Note: mid-request transaction.abort() / commit() also fires
    afterCompletion, but ZODB.Connection.afterCompletion immediately
    calls newTransaction() → poll_invalidations() which re-opens the
    read tx for the next implicit transaction.  The win is at
    Connection.close() (pool return / request end), where no
    newTransaction follows.
    """
    from psycopg.rows import dict_row
    from zodb_pgjsonb.storage import PGJsonbStorage

    import transaction as txn
    import ZODB

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        db = ZODB.DB(storage)
        try:
            conn = db.open()
            instance = conn._storage
            _ = conn.root()  # triggers poll_invalidations + a load
            assert instance._in_read_txn is True

            pid = instance._conn.info.backend_pid

            # End the implicit transaction first (release synchronizer
            # references), then close — Connection.close() invokes
            # storage.afterCompletion() WITHOUT a follow-up
            # newTransaction, which is the lifecycle moment that
            # matters for the issue.
            txn.abort()
            conn.close()

            # The instance's _in_read_txn flag and PG state are now
            # what new requests against this same pooled connection
            # will see.  Verify both.
            assert instance._in_read_txn is False

            monitor = psycopg.connect(DSN, row_factory=dict_row)
            try:
                state, _age = _backend_state(monitor, pid)
                # 'idle' = transaction closed.  Anything else (esp.
                # 'idle in transaction') means the leak is back.
                assert state == "idle", f"Expected idle, got {state!r}"
            finally:
                monitor.close()
        finally:
            db.close()
    finally:
        storage.close()


def test_idle_timeout_set_on_pool_conn():
    """Every conn from the instance pool has idle_in_transaction_session_timeout set."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        with storage._instance_pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT current_setting('idle_in_transaction_session_timeout') AS v"
            )
            row = cur.fetchone()
        # Default 60_000 ms.  PG returns the numeric as-is when set numerically.
        assert row["v"] in ("60000", "1min")
    finally:
        storage.close()


def test_idle_timeout_env_override():
    """ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS env var overrides default."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    import os

    clean_db()
    old = os.environ.get("ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS")
    os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = "5000"
    try:
        storage = PGJsonbStorage(DSN, cache_warm_pct=0)
        try:
            with storage._instance_pool.connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT current_setting('idle_in_transaction_session_timeout') AS v"
                )
                row = cur.fetchone()
            assert row["v"] == "5000"
        finally:
            storage.close()
    finally:
        if old is None:
            del os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"]
        else:
            os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = old


def test_idle_timeout_disabled_when_zero():
    """Setting the env var to 0 disables the timeout (PG default)."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    import os

    clean_db()
    old = os.environ.get("ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS")
    os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = "0"
    try:
        storage = PGJsonbStorage(DSN, cache_warm_pct=0)
        try:
            with storage._instance_pool.connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT current_setting('idle_in_transaction_session_timeout') AS v"
                )
                row = cur.fetchone()
            assert row["v"] == "0"
        finally:
            storage.close()
    finally:
        if old is None:
            del os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"]
        else:
            os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = old
