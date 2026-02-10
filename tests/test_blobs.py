"""Phase 4+6: Blob storage tests for PGJsonbStorage.

Tests that PGJsonbStorage correctly implements IBlobStorage —
storeBlob(), loadBlob(), openCommittedBlobFile(), temporaryDirectory().
Also tests S3 tiered blob storage (Phase 6).

Requires PostgreSQL on localhost:5433.
"""

from moto import mock_aws
from persistent.mapping import PersistentMapping
from tests.conftest import DSN
from ZODB.blob import Blob
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import POSKeyError
from ZODB.utils import z64
from zodb_pgjsonb.storage import PGJsonbStorage

import boto3
import os
import pytest
import tempfile
import transaction as txn
import ZODB


S3_BUCKET = "test-zodb-blobs"
S3_REGION = "us-east-1"


def _clean_db():
    """Drop all tables for a clean test database."""
    import psycopg

    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "blob_state, blob_history, object_state, "
            "object_history, pack_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    _clean_db()
    s = PGJsonbStorage(DSN)
    yield s
    s.close()


@pytest.fixture
def db(storage):
    """ZODB.DB using our storage."""
    database = ZODB.DB(storage)
    yield database
    database.close()


@pytest.fixture
def s3_storage(tmp_path):
    """PGJsonbStorage with mocked S3 client for tiered blob storage."""
    _clean_db()
    with mock_aws():
        # Create mock S3 bucket
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(Bucket=S3_BUCKET)

        from zodb_s3blobs.cache import S3BlobCache
        from zodb_s3blobs.s3client import S3Client

        s3_client = S3Client(
            bucket_name=S3_BUCKET,
            region_name=S3_REGION,
        )
        cache_dir = str(tmp_path / "blob_cache")
        blob_cache = S3BlobCache(
            cache_dir=cache_dir,
            max_size=10 * 1024 * 1024,  # 10MB cache
        )
        s = PGJsonbStorage(
            DSN,
            s3_client=s3_client,
            blob_cache=blob_cache,
            blob_threshold=1024,  # 1KB — easy to test
        )
        yield s
        s.close()


@pytest.fixture
def s3_db(s3_storage):
    """ZODB.DB using storage with S3 tiering."""
    database = ZODB.DB(s3_storage)
    yield database
    database.close()


class TestBlobStorageInterface:
    """Test that PGJsonbStorage provides IBlobStorage."""

    def test_provides_iblobstorage(self, storage):
        assert IBlobStorage.providedBy(storage)

    def test_temporary_directory_exists(self, storage):
        temp_dir = storage.temporaryDirectory()
        assert os.path.isdir(temp_dir)

    def test_instance_temporary_directory(self, storage):
        inst = storage.new_instance()
        temp_dir = inst.temporaryDirectory()
        assert os.path.isdir(temp_dir)
        # Each instance gets its own temp dir
        assert temp_dir != storage.temporaryDirectory()
        inst.release()

    def test_instance_temp_dir_cleaned_on_release(self, storage):
        inst = storage.new_instance()
        temp_dir = inst.temporaryDirectory()
        assert os.path.isdir(temp_dir)
        inst.release()
        assert not os.path.exists(temp_dir)


class TestBlobStoreAndLoad:
    """Test blob store/load via storage API."""

    def test_store_and_load_blob(self, storage):
        """Store a blob via low-level API and load it back."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        # Create a temp blob file
        blob_data = b"Hello, blob world!"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        # Create a minimal object with blob
        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # Load the blob back
        loaded_path = inst.loadBlob(oid, tid)
        assert os.path.isfile(loaded_path)
        with open(loaded_path, "rb") as f:
            assert f.read() == blob_data

        inst.release()

    def test_store_blob_takes_ownership(self, storage):
        """storeBlob should remove the original blob file during vote."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"ownership test"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)
        assert os.path.exists(blob_path)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        # After vote, the original file should be gone
        assert not os.path.exists(blob_path)
        inst.tpc_finish(t)

        inst.release()

    def test_load_nonexistent_blob_raises(self, storage):
        """loadBlob should raise POSKeyError for missing blobs."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        from ZODB.utils import p64

        with pytest.raises(POSKeyError):
            inst.loadBlob(p64(999), p64(999))

        inst.release()

    def test_open_committed_blob_file(self, storage):
        """openCommittedBlobFile returns a readable file object."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"committed blob data"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # Open without blob object
        f = inst.openCommittedBlobFile(oid, tid)
        assert f.read() == blob_data
        f.close()

        inst.release()

    def test_blob_abort_cleans_up(self, storage):
        """tpc_abort should clean up queued blob temp files."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"abort test"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        # Abort before vote — blob file should be cleaned up
        inst.tpc_abort(t)
        assert not os.path.exists(blob_path)

        inst.release()


class TestBlobsWithZODB:
    """Test blob objects through ZODB.DB."""

    def test_store_and_load_blob_via_zodb(self, db):
        """Store a Blob via ZODB and load it back."""
        conn = db.open()
        root = conn.root()
        root["myblob"] = Blob(b"Hello from ZODB blob!")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        blob = root["myblob"]
        with blob.open("r") as f:
            assert f.read() == b"Hello from ZODB blob!"
        conn.close()

    def test_blob_data_persists_across_connections(self, db):
        """Blob data persists and is readable from new connections."""
        blob_content = b"persistent blob data " * 100
        conn = db.open()
        root = conn.root()
        root["doc"] = PersistentMapping()
        root["doc"]["attachment"] = Blob(blob_content)
        txn.commit()
        conn.close()

        # New connection reads it back
        conn = db.open()
        root = conn.root()
        with root["doc"]["attachment"].open("r") as f:
            assert f.read() == blob_content
        conn.close()

    def test_large_blob(self, db):
        """Test with a blob larger than 1MB."""
        large_data = b"X" * (1024 * 1024 + 1)  # 1MB + 1 byte
        conn = db.open()
        root = conn.root()
        root["large"] = Blob(large_data)
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["large"].open("r") as f:
            loaded = f.read()
        assert loaded == large_data
        conn.close()

    def test_multiple_blobs_same_transaction(self, db):
        """Multiple blobs in the same transaction."""
        conn = db.open()
        root = conn.root()
        root["blob1"] = Blob(b"first blob")
        root["blob2"] = Blob(b"second blob")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["blob1"].open("r") as f:
            assert f.read() == b"first blob"
        with root["blob2"].open("r") as f:
            assert f.read() == b"second blob"
        conn.close()

    def test_blob_update(self, db):
        """Update blob data in a subsequent transaction."""
        conn = db.open()
        root = conn.root()
        root["doc"] = Blob(b"version 1")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["doc"].open("w") as f:
            f.write(b"version 2")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["doc"].open("r") as f:
            assert f.read() == b"version 2"
        conn.close()

    def test_blob_survives_transaction_abort(self, db):
        """Aborting a transaction with blob changes discards them."""
        conn = db.open()
        root = conn.root()
        root["keeper"] = Blob(b"keep me")
        txn.commit()

        root["discard"] = Blob(b"throw me away")
        txn.abort()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["keeper"].open("r") as f:
            assert f.read() == b"keep me"
        assert "discard" not in root
        conn.close()


class TestS3BlobTiering:
    """Test S3 tiered blob storage with mocked S3."""

    def test_small_blob_stays_in_pg(self, s3_storage):
        """Blobs smaller than threshold stay in PG bytea."""
        inst = s3_storage.new_instance()
        inst.poll_invalidations()

        # 100 bytes < 1024 threshold → PG
        blob_data = b"small blob data"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # Verify: blob is in PG (data not NULL, s3_key NULL)
        from ZODB.utils import u64

        import psycopg

        conn = psycopg.connect(DSN)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data IS NOT NULL as has_data, s3_key "
                "FROM blob_state WHERE zoid = %s",
                (u64(oid),),
            )
            row = cur.fetchone()
        conn.close()
        assert row[0] is True  # has_data
        assert row[1] is None  # no s3_key

        # Load it back
        loaded_path = inst.loadBlob(oid, tid)
        with open(loaded_path, "rb") as f:
            assert f.read() == blob_data

        inst.release()

    def test_large_blob_goes_to_s3(self, s3_storage):
        """Blobs >= threshold are uploaded to S3."""
        inst = s3_storage.new_instance()
        inst.poll_invalidations()

        # 2048 bytes >= 1024 threshold → S3
        blob_data = b"L" * 2048
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        _tid = inst.tpc_finish(t)

        # Verify: blob metadata is in PG (data NULL, s3_key set)
        from ZODB.utils import u64

        import psycopg

        conn = psycopg.connect(DSN)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT data IS NULL as no_data, s3_key "
                "FROM blob_state WHERE zoid = %s",
                (u64(oid),),
            )
            row = cur.fetchone()
        conn.close()
        assert row[0] is True  # no data in PG
        assert row[1] is not None  # has s3_key
        assert "blobs/" in row[1]

        inst.release()

    def test_load_blob_from_s3(self, s3_storage):
        """Loading a blob stored in S3 downloads it to local file."""
        inst = s3_storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"S" * 2048  # >= 1024 threshold → S3
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # Load — should download from S3
        loaded_path = inst.loadBlob(oid, tid)
        assert os.path.isfile(loaded_path)
        with open(loaded_path, "rb") as f:
            assert f.read() == blob_data

        inst.release()

    def test_load_blob_uses_cache(self, s3_storage):
        """Second load of an S3 blob should use the cache."""
        inst = s3_storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"C" * 2048
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # First load — downloads from S3 + caches
        path1 = inst.loadBlob(oid, tid)
        assert os.path.isfile(path1)

        # Second load — should return cached path
        # (remove the temp dir file to prove it's not re-downloading to temp)
        if os.path.exists(path1) and "blob_cache" not in path1:
            os.unlink(path1)
        path2 = inst.loadBlob(oid, tid)
        assert os.path.isfile(path2)
        with open(path2, "rb") as f:
            assert f.read() == blob_data

        inst.release()

    def test_no_s3_config_all_pg(self, storage):
        """Without S3 config, all blobs go to PG (backward compatible)."""
        assert storage._s3_client is None
        assert storage._blob_cache is None

    def test_blob_threshold_zero_all_s3(self, tmp_path):
        """With threshold=0, all blobs go to S3."""
        _clean_db()
        with mock_aws():
            client = boto3.client("s3", region_name=S3_REGION)
            client.create_bucket(Bucket=S3_BUCKET)

            from zodb_s3blobs.cache import S3BlobCache
            from zodb_s3blobs.s3client import S3Client

            s3_client = S3Client(
                bucket_name=S3_BUCKET,
                region_name=S3_REGION,
            )
            blob_cache = S3BlobCache(
                cache_dir=str(tmp_path / "cache0"),
                max_size=10 * 1024 * 1024,
            )
            s = PGJsonbStorage(
                DSN,
                s3_client=s3_client,
                blob_cache=blob_cache,
                blob_threshold=0,  # All blobs go to S3
            )
            inst = s.new_instance()
            inst.poll_invalidations()

            # Even a tiny blob should go to S3
            blob_data = b"tiny"
            fd, blob_path = tempfile.mkstemp()
            os.write(fd, blob_data)
            os.close(fd)

            import zodb_json_codec

            record = {
                "@cls": ["persistent.mapping", "PersistentMapping"],
                "@s": {"data": {}},
            }
            data = zodb_json_codec.encode_zodb_record(record)

            t = txn.Transaction()
            inst.tpc_begin(t)
            oid = inst.new_oid()
            inst.storeBlob(oid, z64, data, blob_path, "", t)
            inst.tpc_vote(t)
            tid = inst.tpc_finish(t)

            # Verify it's in S3
            from ZODB.utils import u64

            import psycopg

            conn = psycopg.connect(DSN)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT data IS NULL as no_data, s3_key "
                    "FROM blob_state WHERE zoid = %s",
                    (u64(oid),),
                )
                row = cur.fetchone()
            conn.close()
            assert row[0] is True  # no data in PG
            assert row[1] is not None  # has s3_key

            # Load it back
            loaded_path = inst.loadBlob(oid, tid)
            with open(loaded_path, "rb") as f:
                assert f.read() == blob_data

            inst.release()
            s.close()


class TestS3BlobsWithZODB:
    """Test S3 blobs through ZODB.DB."""

    def test_store_and_load_large_blob(self, s3_db):
        """Store a large blob via ZODB, verify it goes to S3 and loads back."""
        large_data = b"Z" * 2048  # >= 1024 threshold → S3
        conn = s3_db.open()
        root = conn.root()
        root["large_blob"] = Blob(large_data)
        txn.commit()
        conn.close()

        conn = s3_db.open()
        root = conn.root()
        with root["large_blob"].open("r") as f:
            assert f.read() == large_data
        conn.close()

    def test_mixed_small_and_large_blobs(self, s3_db):
        """Mix of small (PG) and large (S3) blobs in same transaction."""
        small_data = b"small"  # < 1024 → PG
        large_data = b"X" * 2048  # >= 1024 → S3
        conn = s3_db.open()
        root = conn.root()
        root["small"] = Blob(small_data)
        root["large"] = Blob(large_data)
        txn.commit()
        conn.close()

        conn = s3_db.open()
        root = conn.root()
        with root["small"].open("r") as f:
            assert f.read() == small_data
        with root["large"].open("r") as f:
            assert f.read() == large_data
        conn.close()

    def test_s3_blob_update(self, s3_db):
        """Update a large blob — new version also goes to S3."""
        conn = s3_db.open()
        root = conn.root()
        root["doc"] = Blob(b"V" * 2048)
        txn.commit()
        conn.close()

        conn = s3_db.open()
        root = conn.root()
        with root["doc"].open("w") as f:
            f.write(b"W" * 3000)
        txn.commit()
        conn.close()

        conn = s3_db.open()
        root = conn.root()
        with root["doc"].open("r") as f:
            assert f.read() == b"W" * 3000
        conn.close()


class TestBlobsHistoryPreserving:
    """Test blob storage in history-preserving mode (covers blob_history writes)."""

    @pytest.fixture
    def hp_storage(self):
        """Fresh HP storage."""
        _clean_db()
        s = PGJsonbStorage(DSN, history_preserving=True)
        yield s
        s.close()

    @pytest.fixture
    def hp_db(self, hp_storage):
        database = ZODB.DB(hp_storage)
        yield database
        database.close()

    def test_blob_writes_to_blob_history(self, hp_db):
        """HP mode writes blobs to both blob_state and blob_history."""
        from psycopg.rows import dict_row

        import psycopg

        conn = hp_db.open()
        root = conn.root()
        root["myblob"] = Blob(b"HP blob data")
        txn.commit()
        conn.close()

        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM blob_state")
            state_count = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) AS cnt FROM blob_history")
            history_count = cur.fetchone()["cnt"]
        pg_conn.close()

        assert state_count >= 1
        assert history_count >= 1


class TestSavepointBlobs:
    """Test that blobs work correctly with ZODB savepoints."""

    def test_savepoint_then_commit(self, db):
        """Basic savepoint + commit with a blob should work."""
        conn = db.open()
        root = conn.root()

        blob = Blob()
        with blob.open("w") as f:
            f.write(b"savepoint blob data")
        root["blob1"] = blob

        txn.savepoint()
        txn.commit()

        conn2 = db.open()
        root2 = conn2.root()
        with root2["blob1"].open("r") as f:
            assert f.read() == b"savepoint blob data"
        conn2.close()
        conn.close()

    def test_multiple_savepoints_with_blobs(self, db):
        """Multiple savepoints, each adding blobs, then commit."""
        conn = db.open()
        root = conn.root()

        blob1 = Blob()
        with blob1.open("w") as f:
            f.write(b"blob one")
        root["blob1"] = blob1
        txn.savepoint()

        blob2 = Blob()
        with blob2.open("w") as f:
            f.write(b"blob two")
        root["blob2"] = blob2
        txn.savepoint()

        blob3 = Blob()
        with blob3.open("w") as f:
            f.write(b"blob three")
        root["blob3"] = blob3

        txn.commit()

        conn2 = db.open()
        root2 = conn2.root()
        with root2["blob1"].open("r") as f:
            assert f.read() == b"blob one"
        with root2["blob2"].open("r") as f:
            assert f.read() == b"blob two"
        with root2["blob3"].open("r") as f:
            assert f.read() == b"blob three"
        conn2.close()
        conn.close()

    def test_savepoint_blob_accessible_after_commit(self, db):
        """Blob from savepoint + dirty object in final commit phase."""
        conn = db.open()
        root = conn.root()

        blob = Blob()
        with blob.open("w") as f:
            f.write(b"important data")
        root["myblob"] = blob
        txn.savepoint()

        root["other"] = PersistentMapping()
        root["other"]["key"] = "value"

        txn.commit()

        conn2 = db.open()
        root2 = conn2.root()
        with root2["myblob"].open("r") as f:
            assert f.read() == b"important data"
        assert root2["other"]["key"] == "value"
        conn2.close()
        conn.close()

    def test_savepoint_rollback_then_new_blob_commit(self, db):
        """Rollback a savepoint, add a new blob, commit."""
        conn = db.open()
        root = conn.root()

        sp = txn.savepoint()

        blob1 = Blob()
        with blob1.open("w") as f:
            f.write(b"will be rolled back")
        root["blob1"] = blob1

        sp.rollback()

        blob2 = Blob()
        with blob2.open("w") as f:
            f.write(b"this one survives")
        root["blob2"] = blob2

        txn.commit()

        conn2 = db.open()
        root2 = conn2.root()
        assert "blob1" not in root2
        with root2["blob2"].open("r") as f:
            assert f.read() == b"this one survives"
        conn2.close()
        conn.close()


class TestBatchSavepointBlobs:
    """Stress test: batch import with periodic savepoints — mimics plone.exportimport.

    plone.exportimport creates objects with blobs in a loop, calling
    transaction.savepoint() every N objects. During savepoint commit,
    TmpStore.close() deletes the savepoint directory (.spb files).
    The real storage must have staged the blob data before that happens.
    """

    def _batch_import(self, database, total=20, savepoint_interval=5):
        conn = database.open()
        root = conn.root()
        root["content"] = PersistentMapping()

        for i in range(total):
            blob = Blob()
            with blob.open("w") as f:
                f.write(f"blob content {i:04d}".encode())
            root["content"][f"item-{i:04d}"] = PersistentMapping()
            root["content"][f"item-{i:04d}"]["file"] = blob

            if (i + 1) % savepoint_interval == 0:
                txn.savepoint()

        txn.commit()

        # Verify all blobs survived
        conn2 = database.open()
        root2 = conn2.root()
        for i in range(total):
            with root2["content"][f"item-{i:04d}"]["file"].open("r") as f:
                assert f.read() == f"blob content {i:04d}".encode()
        conn2.close()
        conn.close()

    def test_batch_import_pg(self, db):
        """20 blobs with savepoints every 5, PG-only storage."""
        self._batch_import(db)

    def test_batch_import_s3(self, s3_db):
        """20 blobs with savepoints every 5, S3-tiered storage."""
        self._batch_import(s3_db)

    def test_mixed_blob_sizes_s3(self, s3_db):
        """Mix of small and large blobs with savepoints (tests S3 threshold)."""
        conn = s3_db.open()
        root = conn.root()
        root["content"] = PersistentMapping()

        for i in range(10):
            blob = Blob()
            # Alternate small (<1KB) and large (>1KB) to exercise both paths
            size = 100 if i % 2 == 0 else 2048
            with blob.open("w") as f:
                f.write(bytes([i % 256]) * size)
            root["content"][f"item-{i}"] = blob

            if (i + 1) % 3 == 0:
                txn.savepoint()

        txn.commit()

        conn2 = s3_db.open()
        root2 = conn2.root()
        for i in range(10):
            size = 100 if i % 2 == 0 else 2048
            with root2["content"][f"item-{i}"].open("r") as f:
                assert f.read() == bytes([i % 256]) * size
        conn2.close()
        conn.close()

    def test_savepoint_then_modify_blob_then_commit(self, db):
        """Savepoint, then overwrite same blob, then commit (last write wins)."""
        conn = db.open()
        root = conn.root()

        blob = Blob()
        with blob.open("w") as f:
            f.write(b"version 1")
        root["doc"] = blob
        txn.savepoint()

        # Overwrite the same blob
        with root["doc"].open("w") as f:
            f.write(b"version 2")
        txn.commit()

        conn2 = db.open()
        root2 = conn2.root()
        with root2["doc"].open("r") as f:
            assert f.read() == b"version 2"
        conn2.close()
        conn.close()
