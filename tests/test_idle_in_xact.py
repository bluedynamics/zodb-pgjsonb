"""Tests for #118: read tx must be closed at request end so virtualxids
don't accumulate and block CREATE INDEX CONCURRENTLY.
"""

from tests.conftest import clean_db
from tests.conftest import DSN

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
