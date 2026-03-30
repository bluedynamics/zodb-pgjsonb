"""Tests for S3 blob uploads via BlobSink in _batch_write_blobs."""

from moto import mock_aws
from tests.conftest import clean_db
from tests.conftest import DSN
from unittest.mock import MagicMock
from zodb_pgjsonb.blob_sink import InlineBlobSink
from zodb_pgjsonb.storage import _batch_write_blobs
from zodb_pgjsonb.storage import PGJsonbStorage

import boto3
import os
import psycopg
import pytest


S3_BUCKET = "test-parallel-blobs"
S3_REGION = "us-east-1"


@pytest.fixture
def temp_blobs(tmp_path):
    """Create temporary blob files of various sizes."""

    def _make_blobs(count, size=2048):
        paths = []
        for i in range(count):
            path = tmp_path / f"blob_{i}.data"
            path.write_bytes(os.urandom(size))
            paths.append(str(path))
        return paths

    return _make_blobs


@pytest.fixture
def mock_s3_client():
    """Mocked S3 client using moto."""
    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(Bucket=S3_BUCKET)

        from zodb_s3blobs.s3client import S3Client

        s3_client = S3Client(
            bucket_name=S3_BUCKET,
            region_name=S3_REGION,
        )
        yield s3_client


class TestInlineBlobSink:
    """Test InlineBlobSink upload behavior (replaces old _upload_s3_blobs tests)."""

    def test_single_blob_upload(self, temp_blobs, mock_s3_client):
        """Single blob should upload via InlineBlobSink."""
        paths = temp_blobs(1)
        sink = InlineBlobSink(mock_s3_client)
        sink.submit(paths[0], "blobs/0001/0001.blob", 1, os.path.getsize(paths[0]))

        # Verify blob exists in S3
        obj = mock_s3_client._client.get_object(
            Bucket=S3_BUCKET, Key="blobs/0001/0001.blob"
        )
        assert obj["ContentLength"] == os.path.getsize(paths[0])

    def test_multiple_blobs(self, temp_blobs, mock_s3_client):
        """Multiple blobs should upload via InlineBlobSink."""
        paths = temp_blobs(5)
        sink = InlineBlobSink(mock_s3_client)
        for i in range(5):
            sink.submit(
                paths[i],
                f"blobs/{i:016x}/{i:016x}.blob",
                i,
                os.path.getsize(paths[i]),
            )

        # Verify all blobs exist in S3
        for i in range(5):
            key = f"blobs/{i:016x}/{i:016x}.blob"
            obj = mock_s3_client._client.get_object(Bucket=S3_BUCKET, Key=key)
            assert obj["ContentLength"] == os.path.getsize(paths[i])

    def test_upload_failure_propagates(self, temp_blobs):
        """S3 upload errors should propagate."""
        paths = temp_blobs(1)
        failing_client = MagicMock()
        failing_client.upload_file.side_effect = RuntimeError("S3 down")
        sink = InlineBlobSink(failing_client)

        with pytest.raises(RuntimeError, match="S3 down"):
            sink.submit(paths[0], "blobs/0001/0001.blob", 1, os.path.getsize(paths[0]))


class TestBatchWriteBlobsParallel:
    """Test _batch_write_blobs with BlobSink integration."""

    def test_s3_blobs_via_blob_sink(self, temp_blobs, mock_s3_client):
        """Blobs above threshold go to S3 via blob_sink."""
        clean_db()
        storage = PGJsonbStorage(DSN)
        try:
            conn = psycopg.connect(DSN)
            cur = conn.cursor()

            paths = temp_blobs(3, size=2048)
            blobs = [(i + 1, paths[i]) for i in range(3)]
            tid_int = 100

            sink = InlineBlobSink(mock_s3_client)
            _batch_write_blobs(
                cur,
                blobs,
                tid_int,
                blob_sink=sink,
                blob_threshold=1024,  # All blobs above threshold
            )
            conn.commit()

            # Verify S3 keys exist
            for i in range(3):
                key = f"blobs/{i + 1:016x}/{tid_int:016x}.blob"
                obj = mock_s3_client._client.get_object(Bucket=S3_BUCKET, Key=key)
                assert obj["ContentLength"] == 2048

            # Verify metadata rows in PG
            cur.execute("SELECT zoid, tid, s3_key FROM blob_state ORDER BY zoid")
            rows = cur.fetchall()
            assert len(rows) == 3
            for i, row in enumerate(rows):
                assert row[0] == i + 1
                assert row[1] == tid_int
                assert row[2] == f"blobs/{i + 1:016x}/{tid_int:016x}.blob"

            cur.close()
            conn.close()
        finally:
            storage.close()

    def test_small_blobs_go_to_pg(self, temp_blobs):
        """Blobs below threshold go to PG bytea, not S3."""
        clean_db()
        storage = PGJsonbStorage(DSN)
        try:
            conn = psycopg.connect(DSN)
            cur = conn.cursor()

            paths = temp_blobs(2, size=512)
            blobs = [(i + 1, paths[i]) for i in range(2)]
            tid_int = 200

            mock_client = MagicMock()
            sink = InlineBlobSink(mock_client)
            _batch_write_blobs(
                cur,
                blobs,
                tid_int,
                blob_sink=sink,
                blob_threshold=1024,  # Blobs below threshold
            )
            conn.commit()

            # S3 should not have been called
            mock_client.upload_file.assert_not_called()

            # Verify data rows in PG
            cur.execute(
                "SELECT zoid, tid, blob_size, data IS NOT NULL as has_data "
                "FROM blob_state ORDER BY zoid"
            )
            rows = cur.fetchall()
            assert len(rows) == 2
            for row in rows:
                assert row[3] is True  # has_data

            cur.close()
            conn.close()
        finally:
            storage.close()

    def test_temp_files_cleaned_after_upload(self, tmp_path, mock_s3_client):
        """Temp blob files are cleaned up after S3 upload (by the sink)."""
        clean_db()
        storage = PGJsonbStorage(DSN)
        try:
            conn = psycopg.connect(DSN)
            cur = conn.cursor()

            # Create temp blob files marked as is_temp=True
            paths = []
            for i in range(3):
                path = tmp_path / f"temp_blob_{i}.data"
                path.write_bytes(os.urandom(2048))
                paths.append(str(path))

            blobs = [(i + 1, paths[i], True) for i in range(3)]
            tid_int = 300

            sink = InlineBlobSink(mock_s3_client)
            _batch_write_blobs(
                cur,
                blobs,
                tid_int,
                blob_sink=sink,
                blob_threshold=1024,
            )
            conn.commit()

            # Temp files should be deleted (by the sink's cleanup_path handling)
            for path in paths:
                assert not os.path.exists(path)

            cur.close()
            conn.close()
        finally:
            storage.close()
