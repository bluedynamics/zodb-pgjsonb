"""Tests for incremental parallel import with watermark.

Requires PostgreSQL on localhost:5433.
"""

from persistent.mapping import PersistentMapping
from tests.conftest import clean_db
from tests.conftest import DSN
from ZODB.FileStorage import FileStorage
from zodb_pgjsonb.storage import _write_prepared_transaction
from zodb_pgjsonb.storage import _write_txn_log
from zodb_pgjsonb.storage import PGJsonbStorage

import os
import psycopg
import pytest
import shutil
import tempfile
import transaction as txn_mod
import ZODB


class TestIdempotentWriteTxnLog:
    """_write_txn_log with idempotent=True skips duplicates."""

    @pytest.fixture(autouse=True)
    def setup_db(self):
        clean_db()
        from zodb_pgjsonb.schema import install_schema

        conn = psycopg.connect(DSN, autocommit=False)
        install_schema(conn)
        conn.close()

    def test_duplicate_tid_raises_without_idempotent(self):
        conn = psycopg.connect(DSN, autocommit=True)
        conn.execute("BEGIN")
        with conn.cursor() as cur:
            _write_txn_log(cur, 1, "user", "desc", {})
        conn.execute("COMMIT")
        conn.execute("BEGIN")
        with conn.cursor() as cur, pytest.raises(psycopg.errors.UniqueViolation):
            _write_txn_log(cur, 1, "user", "desc", {})
        conn.execute("ROLLBACK")
        conn.close()

    def test_duplicate_tid_skipped_with_idempotent(self):
        conn = psycopg.connect(DSN, autocommit=True)
        conn.execute("BEGIN")
        with conn.cursor() as cur:
            _write_txn_log(cur, 1, "user", "desc", {})
        conn.execute("COMMIT")
        conn.execute("BEGIN")
        with conn.cursor() as cur:
            _write_txn_log(cur, 1, "user", "desc", {}, idempotent=True)
        conn.execute("COMMIT")
        # Verify only one row
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM transaction_log WHERE tid = 1")
            assert cur.fetchone()[0] == 1
        conn.close()


class TestIdempotentWritePreparedTransaction:
    """_write_prepared_transaction with idempotent=True handles duplicates."""

    @pytest.fixture(autouse=True)
    def setup_db(self):
        clean_db()

    def test_duplicate_transaction_succeeds_with_idempotent(self):
        """Writing the same transaction twice with idempotent=True does not error."""
        dest = PGJsonbStorage(DSN, pool_max_size=2)
        conn = dest._instance_pool.getconn()
        try:
            txn_data = {
                "tid": b"\x00" * 7 + b"\x01",
                "tid_int": 1,
                "user": "test",
                "description": "test txn",
                "extension": {},
                "objects": [
                    {
                        "zoid": 0,
                        "class_mod": "persistent.mapping",
                        "class_name": "PersistentMapping",
                        "state": {"@s": {}},
                        "state_size": 10,
                        "refs": [],
                    }
                ],
                "blobs": [],
            }
            _write_prepared_transaction(conn, txn_data, False, [], [], idempotent=False)
            # Write same transaction again — should not raise
            _write_prepared_transaction(conn, txn_data, False, [], [], idempotent=True)
            # Verify single transaction_log row
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM transaction_log WHERE tid = 1")
                assert cur.fetchone()["count"] == 1
        finally:
            dest._instance_pool.putconn(conn)
            dest.close()


class TestCopyTransactionsFromStartTid:
    """copyTransactionsFrom respects start_tid parameter."""

    @pytest.fixture
    def temp_dir(self):
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d)

    @pytest.fixture
    def source_with_5_txns(self, temp_dir):
        """FileStorage with 5 explicit transactions (+ 1 root = 6 total)."""
        fs_path = os.path.join(temp_dir, "source.fs")
        blob_dir = os.path.join(temp_dir, "blobs")
        os.makedirs(blob_dir)
        source = FileStorage(fs_path, blob_dir=blob_dir)
        db = ZODB.DB(source)
        conn = db.open()
        root = conn.root()
        for i in range(5):
            root[f"key{i}"] = PersistentMapping({"val": i})
            txn_mod.commit()
        conn.close()
        db.close()
        return FileStorage(fs_path, blob_dir=blob_dir, read_only=True)

    def test_sequential_start_tid(self, source_with_5_txns):
        """Sequential copy with start_tid skips earlier transactions."""
        clean_db()

        tids = [txn.tid for txn in source_with_5_txns.iterator()]
        start_tid = tids[3]  # skip first 3

        dest = PGJsonbStorage(DSN, pool_max_size=2)
        dest.copyTransactionsFrom(source_with_5_txns, start_tid=start_tid)

        dest_conn = psycopg.connect(DSN)
        with dest_conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM transaction_log")
            count = cur.fetchone()[0]
        dest_conn.close()
        assert count == len(tids) - 3

        dest.close()
        source_with_5_txns.close()
