"""Self-healing admin connection (#103).

The storage's admin connection (``storage._conn``) can die at any time —
a CNPG switchover/failover, an operator kill, an idle timeout.  Runtime
users of the admin connection must transparently reconnect instead of
raising ``psycopg.OperationalError`` forever.

Two kill variants matter:

- client-side close: ``conn.closed`` is True, a liveness check suffices
- server-side kill (``pg_terminate_backend``): the client only notices
  on first use, so the first operation after the kill must retry once
  after reconnecting
"""

from tests.conftest import DSN
from ZODB.Connection import TransactionMetaData
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import os
import psycopg
import pytest
import tempfile
import transaction


# Nothing listens on port 9; connect fails immediately.
UNREACHABLE_DSN = "postgresql://zodb:zodb@127.0.0.1:9/zodb_test"


def _kill_admin_conn_server_side(storage):
    """Terminate the admin connection's backend; client notices on next use."""
    with storage._conn.cursor() as cur:
        cur.execute("SELECT pg_backend_pid() AS pid")
        pid = cur.fetchone()["pid"]
    with psycopg.connect(DSN, autocommit=True) as killer:
        killer.execute("SELECT pg_terminate_backend(%s)", (pid,))


ADMIN_READ_OPS = [
    pytest.param(lambda s: len(s), id="len"),
    pytest.param(lambda s: s.getSize(), id="getSize"),
    pytest.param(lambda s: s.get_blob_stats(), id="get_blob_stats"),
    pytest.param(lambda s: s.get_blob_histogram(), id="get_blob_histogram"),
    pytest.param(lambda s: s.new_oid(), id="new_oid"),
]


@pytest.mark.parametrize("op", ADMIN_READ_OPS)
def test_op_heals_after_client_side_close(storage, op):
    op(storage)  # baseline: op works on a fresh storage
    storage._conn.close()
    op(storage)  # must reconnect instead of raising


@pytest.mark.parametrize("op", ADMIN_READ_OPS)
def test_op_heals_after_server_side_kill(storage, op):
    op(storage)
    _kill_admin_conn_server_side(storage)
    op(storage)  # first use after the kill must heal via retry


def test_len_returns_correct_count_after_heal(db, storage):
    conn = db.open()
    conn.root()["x"] = MinPO(1)
    transaction.commit()
    conn.close()
    expected = len(storage)
    assert expected > 0
    storage._conn.close()
    assert len(storage) == expected


def test_history_heals_after_close(db, storage):
    conn = db.open()
    conn.root()["x"] = MinPO(1)
    transaction.commit()
    conn.close()
    storage._conn.close()
    assert storage.history(z64)


def test_load_before_heals_after_close(db, storage):
    conn = db.open()
    conn.root()["x"] = MinPO(1)
    transaction.commit()
    conn.close()
    after_last = p64(u64(storage.lastTransaction()) + 1)
    storage._conn.close()
    result = storage.loadBefore(z64, after_last)
    assert result is not None


def test_current_max_tid_heals_after_close(storage):
    assert storage.current_max_tid() is not None
    storage._conn.close()
    assert storage.current_max_tid() is not None


def test_current_max_tid_still_degrades_to_none_when_db_unreachable(storage):
    """Contract pin: reconnect failure must not escape current_max_tid.

    This passes before the fix as well; it guards the degrade-to-None
    contract against a healing implementation that lets the reconnect
    error propagate.
    """
    storage._conn.close()
    storage._dsn = UNREACHABLE_DSN
    try:
        assert storage.current_max_tid() is None
    finally:
        storage._dsn = DSN


def test_direct_tpc_write_heals_after_close(storage):
    """The zconsole/direct-use write path: BEGIN must run on a live conn."""
    storage._conn.close()
    t = TransactionMetaData()
    storage.tpc_begin(t)
    oid = storage.new_oid()
    storage.store(oid, z64, zodb_pickle(MinPO(1)), "", t)
    storage.tpc_vote(t)
    storage.tpc_finish(t)
    assert len(storage) == 1


def test_direct_tpc_write_heals_after_server_side_kill(storage):
    """A server-killed conn is only detected at BEGIN; nothing has been
    sent yet at that point, so BEGIN must retry once on a fresh conn."""
    _kill_admin_conn_server_side(storage)
    t = TransactionMetaData()
    storage.tpc_begin(t)
    oid = storage.new_oid()
    storage.store(oid, z64, zodb_pickle(MinPO(1)), "", t)
    storage.tpc_vote(t)
    storage.tpc_finish(t)
    assert len(storage) == 1


def test_load_blob_heals_after_close(storage):
    blob_data = b"blob payload for #103"
    fd, blob_path = tempfile.mkstemp()
    os.write(fd, blob_data)
    os.close(fd)

    t = TransactionMetaData()
    storage.tpc_begin(t)
    oid = storage.new_oid()
    storage.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "", t)
    storage.tpc_vote(t)
    tid = storage.tpc_finish(t)

    storage._conn.close()
    loaded_path = storage.loadBlob(oid, tid)
    with open(loaded_path, "rb") as f:
        assert f.read() == blob_data


def test_closed_storage_does_not_resurrect(storage):
    """After an explicit close() the admin connection must stay dead —
    healing must not leak fresh connections during shutdown."""
    storage.close()
    with pytest.raises(psycopg.OperationalError):
        len(storage)


def test_tpc_abort_tolerates_dead_connection(storage):
    """A conn killed mid-tpc already lost its transaction server-side;
    tpc_abort must discard it quietly, and the storage heals afterwards."""
    t = TransactionMetaData()
    storage.tpc_begin(t)
    oid = storage.new_oid()
    storage.store(oid, z64, zodb_pickle(MinPO(1)), "", t)
    storage._conn.close()
    storage.tpc_abort(t)
    assert len(storage) == 0


def test_no_silent_heal_during_active_tpc(storage):
    """Mid-transaction the admin conn must NOT be silently replaced:
    the BEGIN block would be lost and _vote would write in autocommit
    mode.  Errors must propagate instead."""
    t = TransactionMetaData()
    storage.tpc_begin(t)
    storage._conn.close()
    with pytest.raises(psycopg.OperationalError):
        storage.new_oid()
    storage.tpc_abort(t)


def test_undo_log_heals_after_close(hp_storage):
    """undoLog touches the admin conn only in history-preserving mode."""
    assert hp_storage.undoLog() == []
    hp_storage._conn.close()
    assert hp_storage.undoLog() == []


def test_warmer_flush_heals_after_close(storage):
    storage._conn.close()
    storage._warmer._pending.add(42)
    storage._warmer._flush()
    with psycopg.connect(DSN) as check:
        row = check.execute(
            "SELECT COUNT(*) AS cnt FROM cache_warm_stats WHERE zoid = 42"
        ).fetchone()
    assert row[0] == 1
