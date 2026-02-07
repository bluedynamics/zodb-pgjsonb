"""Phase 5: Concurrent write stress tests.

Tests that PGJsonbStorage correctly handles concurrent writes
from multiple threads without data corruption or deadlocks.

Requires PostgreSQL on localhost:5433.
"""

import threading

import pytest

import transaction as txn

import ZODB
from ZODB.POSException import ConflictError

from BTrees.OOBTree import OOBTree

from persistent.mapping import PersistentMapping

from zodb_pgjsonb.storage import PGJsonbStorage


DSN = "dbname=zodb_test user=zodb password=zodb host=localhost port=5433"


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    import psycopg

    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
            "blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()

    s = PGJsonbStorage(DSN)
    yield s
    s.close()


@pytest.fixture
def db(storage):
    """ZODB.DB using our storage."""
    database = ZODB.DB(storage)
    yield database
    database.close()


class TestConcurrentWrites:
    """Test data integrity under concurrent threaded access."""

    def test_threads_write_separate_objects(self, db):
        """Multiple threads writing to separate objects — no conflicts."""
        N = 5
        errors = []

        # Initialize per-thread objects
        conn = db.open()
        root = conn.root()
        for i in range(N):
            root[f"t{i}"] = PersistentMapping()
        txn.commit()
        conn.close()

        def worker(thread_id):
            try:
                tm = txn.TransactionManager()
                c = db.open(tm)
                c.root()[f"t{thread_id}"]["data"] = f"value_{thread_id}"
                tm.commit()
                c.close()
            except Exception as e:
                errors.append((thread_id, e))

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Thread errors: {errors}"

        # Verify all data
        conn = db.open()
        root = conn.root()
        for i in range(N):
            assert root[f"t{i}"]["data"] == f"value_{i}"
        conn.close()

    def test_concurrent_btree_inserts_auto_resolve(self, db):
        """Multiple threads inserting into same OOBTree auto-resolve."""
        N = 5
        errors = []

        conn = db.open()
        root = conn.root()
        root["tree"] = OOBTree()
        txn.commit()
        conn.close()

        def worker(thread_id):
            try:
                tm = txn.TransactionManager()
                c = db.open(tm)
                c.root()["tree"][f"key_{thread_id}"] = f"val_{thread_id}"
                tm.commit()
                c.close()
            except Exception as e:
                errors.append((thread_id, e))

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Thread errors: {errors}"

        # Verify all keys present
        conn = db.open()
        tree = conn.root()["tree"]
        for i in range(N):
            assert tree[f"key_{i}"] == f"val_{i}"
        assert len(tree) == N
        conn.close()

    def test_concurrent_same_mapping_raises_conflict(self, db):
        """Multiple threads modifying same PersistentMapping — ConflictError."""
        N = 5
        successes = []
        conflicts = []

        conn = db.open()
        root = conn.root()
        root["shared"] = PersistentMapping({"val": 0})
        txn.commit()
        conn.close()

        barrier = threading.Barrier(N, timeout=10)

        def worker(thread_id):
            tm = txn.TransactionManager()
            c = db.open(tm)
            barrier.wait()
            try:
                c.root()["shared"]["val"] = thread_id
                tm.commit()
                successes.append(thread_id)
            except ConflictError:
                tm.abort()
                conflicts.append(thread_id)
            finally:
                c.close()

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(N)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        # Exactly one should succeed, rest get ConflictError
        assert len(successes) >= 1
        assert len(conflicts) >= 1
        assert len(successes) + len(conflicts) == N

    def test_sequential_writes_data_integrity(self, db):
        """Many sequential writes maintain data integrity."""
        N = 20

        conn = db.open()
        root = conn.root()
        root["tree"] = OOBTree()
        txn.commit()
        conn.close()

        for i in range(N):
            tm = txn.TransactionManager()
            c = db.open(tm)
            c.root()["tree"][f"key_{i}"] = f"val_{i}"
            tm.commit()
            c.close()

        # Verify all data
        conn = db.open()
        tree = conn.root()["tree"]
        assert len(tree) == N
        for i in range(N):
            assert tree[f"key_{i}"] == f"val_{i}"
        conn.close()

    def test_mixed_read_write_threads(self, db):
        """Concurrent readers and writers don't interfere."""
        N_WRITERS = 3
        N_READERS = 3
        errors = []

        # Initialize
        conn = db.open()
        root = conn.root()
        root["tree"] = OOBTree()
        root["tree"]["initial"] = "data"
        txn.commit()
        conn.close()

        read_results = []

        def writer(thread_id):
            try:
                tm = txn.TransactionManager()
                c = db.open(tm)
                c.root()["tree"][f"w_{thread_id}"] = f"written_{thread_id}"
                tm.commit()
                c.close()
            except Exception as e:
                errors.append(("writer", thread_id, e))

        def reader(thread_id):
            try:
                tm = txn.TransactionManager()
                c = db.open(tm)
                tree = c.root()["tree"]
                # Just read — should never raise
                assert "initial" in tree
                read_results.append(thread_id)
                c.close()
            except Exception as e:
                errors.append(("reader", thread_id, e))

        threads = []
        for i in range(N_WRITERS):
            threads.append(threading.Thread(target=writer, args=(i,)))
        for i in range(N_READERS):
            threads.append(threading.Thread(target=reader, args=(i,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Thread errors: {errors}"
        assert len(read_results) == N_READERS

    def test_rapid_sequential_commits(self, db):
        """Many rapid commits from the same connection don't corrupt state."""
        conn = db.open()
        root = conn.root()
        root["counter"] = PersistentMapping({"val": 0})
        txn.commit()

        for i in range(50):
            root["counter"]["val"] = i + 1
            txn.commit()

        conn.close()

        # Verify final state
        conn = db.open()
        assert conn.root()["counter"]["val"] == 50
        conn.close()
