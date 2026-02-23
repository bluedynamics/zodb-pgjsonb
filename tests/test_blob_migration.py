"""Test migration of ZODB with blobs to PGJsonbStorage.

Verifies that copyTransactionsFrom correctly handles blob records
by detecting them with is_blob_record() and using restoreBlob().

Requires PostgreSQL on localhost:5433.
"""

from persistent.mapping import PersistentMapping
from tests.conftest import DSN
from ZODB.blob import Blob
from ZODB.FileStorage import FileStorage
from zodb_pgjsonb.storage import PGJsonbStorage

import os
import psycopg
import pytest
import shutil
import tempfile
import transaction as txn
import ZODB


def _clean_db():
    """Drop all tables for a clean test database."""
    conn = psycopg.connect(DSN)
    # Terminate other connections first — REPEATABLE READ blocks DDL.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        cur.execute(
            "DROP TABLE IF EXISTS "
            "blob_state, blob_history, object_state, "
            "object_history, pack_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d)


class TestBlobMigration:
    """Test copyTransactionsFrom with FileStorage+blobs → PGJsonbStorage."""

    def test_blob_migration(self, temp_dir):
        """Blobs are migrated from FileStorage to PGJsonbStorage."""
        _clean_db()

        # 1. Create source FileStorage with blobs
        fs_path = os.path.join(temp_dir, "Data.fs")
        blob_dir = os.path.join(temp_dir, "blobs")
        os.makedirs(blob_dir)

        source = FileStorage(fs_path, blob_dir=blob_dir)
        source_db = ZODB.DB(source)

        conn = source_db.open()
        root = conn.root()
        root["key"] = "value"
        root["myblob"] = Blob(b"Hello from source blob!")
        txn.commit()

        # Second transaction with non-blob data
        root["other"] = PersistentMapping({"nested": "data"})
        txn.commit()
        conn.close()
        source_db.close()

        # 2. Migrate
        source = FileStorage(fs_path, blob_dir=blob_dir, read_only=True)
        dest = PGJsonbStorage(DSN)
        dest.copyTransactionsFrom(source)

        # 3. Verify destination
        dest_db = ZODB.DB(dest)
        conn = dest_db.open()
        root = conn.root()

        assert root["key"] == "value"
        assert root["other"]["nested"] == "data"
        blob = root["myblob"]
        with blob.open("r") as f:
            assert f.read() == b"Hello from source blob!"

        conn.close()
        dest_db.close()
        source.close()

    def test_blob_migration_preserves_source(self, temp_dir):
        """Source blob files are not moved/deleted during migration."""
        _clean_db()

        fs_path = os.path.join(temp_dir, "Data.fs")
        blob_dir = os.path.join(temp_dir, "blobs")
        os.makedirs(blob_dir)

        source = FileStorage(fs_path, blob_dir=blob_dir)
        source_db = ZODB.DB(source)
        conn = source_db.open()
        root = conn.root()
        root["myblob"] = Blob(b"Do not move me!")
        txn.commit()
        conn.close()
        source_db.close()

        # Re-open read-only for migration
        source = FileStorage(fs_path, blob_dir=blob_dir, read_only=True)
        dest = PGJsonbStorage(DSN)
        dest.copyTransactionsFrom(source)

        # Source blob files should still exist
        source_db2 = ZODB.DB(source)
        conn = source_db2.open()
        root = conn.root()
        blob = root["myblob"]
        with blob.open("r") as f:
            assert f.read() == b"Do not move me!"
        conn.close()
        source_db2.close()

        dest.close()

    def test_blob_migration_multiple_blobs(self, temp_dir):
        """Multiple blobs in same and different transactions migrate."""
        _clean_db()

        fs_path = os.path.join(temp_dir, "Data.fs")
        blob_dir = os.path.join(temp_dir, "blobs")
        os.makedirs(blob_dir)

        source = FileStorage(fs_path, blob_dir=blob_dir)
        source_db = ZODB.DB(source)
        conn = source_db.open()
        root = conn.root()
        root["blob1"] = Blob(b"first")
        root["blob2"] = Blob(b"second")
        txn.commit()

        root["blob3"] = Blob(b"third in next txn")
        txn.commit()
        conn.close()
        source_db.close()

        source = FileStorage(fs_path, blob_dir=blob_dir, read_only=True)
        dest = PGJsonbStorage(DSN)
        dest.copyTransactionsFrom(source)

        dest_db = ZODB.DB(dest)
        conn = dest_db.open()
        root = conn.root()
        with root["blob1"].open("r") as f:
            assert f.read() == b"first"
        with root["blob2"].open("r") as f:
            assert f.read() == b"second"
        with root["blob3"].open("r") as f:
            assert f.read() == b"third in next txn"
        conn.close()
        dest_db.close()
        source.close()

    def test_blob_migration_history_preserving(self, temp_dir):
        """Blobs migrate correctly to history-preserving storage."""
        _clean_db()

        fs_path = os.path.join(temp_dir, "DataHP.fs")
        blob_dir = os.path.join(temp_dir, "blobsHP")
        os.makedirs(blob_dir)

        source = FileStorage(fs_path, blob_dir=blob_dir)
        source_db = ZODB.DB(source)
        conn = source_db.open()
        root = conn.root()
        root["myblob"] = Blob(b"Initial blob content")
        txn.commit()

        # Update blob in second transaction
        with root["myblob"].open("w") as f:
            f.write(b"Updated blob content")
        txn.commit()
        conn.close()
        source_db.close()

        source = FileStorage(fs_path, blob_dir=blob_dir, read_only=True)
        dest = PGJsonbStorage(DSN, history_preserving=True)
        dest.copyTransactionsFrom(source)

        dest_db = ZODB.DB(dest)
        conn = dest_db.open()
        root = conn.root()
        blob = root["myblob"]
        with blob.open("r") as f:
            assert f.read() == b"Updated blob content"
        conn.close()
        dest_db.close()
        source.close()

    def test_mixed_blob_and_non_blob_objects(self, temp_dir):
        """Migration handles mix of blob and non-blob objects correctly."""
        _clean_db()

        fs_path = os.path.join(temp_dir, "Data.fs")
        blob_dir = os.path.join(temp_dir, "blobs")
        os.makedirs(blob_dir)

        source = FileStorage(fs_path, blob_dir=blob_dir)
        source_db = ZODB.DB(source)
        conn = source_db.open()
        root = conn.root()

        # Mix of regular objects and blobs in same transaction
        root["name"] = "test"
        root["mapping"] = PersistentMapping({"a": 1, "b": 2})
        root["attachment"] = Blob(b"file contents here")
        root["count"] = 42
        txn.commit()
        conn.close()
        source_db.close()

        source = FileStorage(fs_path, blob_dir=blob_dir, read_only=True)
        dest = PGJsonbStorage(DSN)
        dest.copyTransactionsFrom(source)

        dest_db = ZODB.DB(dest)
        conn = dest_db.open()
        root = conn.root()
        assert root["name"] == "test"
        assert root["mapping"]["a"] == 1
        assert root["mapping"]["b"] == 2
        assert root["count"] == 42
        with root["attachment"].open("r") as f:
            assert f.read() == b"file contents here"
        conn.close()
        dest_db.close()
        source.close()
