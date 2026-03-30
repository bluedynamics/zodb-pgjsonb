from unittest.mock import MagicMock
from zodb_pgjsonb.blob_sink import BackgroundBlobSink
from zodb_pgjsonb.blob_sink import DeferredBlobSink
from zodb_pgjsonb.blob_sink import InlineBlobSink

import pytest
import threading
import time


class TestInlineBlobSink:
    def test_submit_uploads_file(self, tmp_path):
        s3 = MagicMock()
        sink = InlineBlobSink(s3)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)

        s3.upload_file.assert_called_once_with(str(blob), "blobs/key.blob")

    def test_submit_retries_on_failure(self, tmp_path):
        s3 = MagicMock()
        s3.upload_file.side_effect = [Exception("S3 down"), None]
        sink = InlineBlobSink(s3, max_retries=2, retry_base_delay=0)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)

        assert s3.upload_file.call_count == 2

    def test_submit_raises_after_retries_exhausted(self, tmp_path):
        s3 = MagicMock()
        s3.upload_file.side_effect = Exception("permanent")
        sink = InlineBlobSink(s3, max_retries=2, retry_base_delay=0)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        with pytest.raises(Exception, match="permanent"):
            sink.submit(str(blob), "blobs/key.blob", 1, 4)

    def test_submit_deletes_temp_file(self, tmp_path):
        s3 = MagicMock()
        sink = InlineBlobSink(s3)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4, cleanup_path=str(blob))

        assert not blob.exists()

    def test_drain_is_noop(self):
        sink = InlineBlobSink(MagicMock())
        sink.drain()  # should not raise

    def test_close_is_noop(self):
        sink = InlineBlobSink(MagicMock())
        sink.close()  # should not raise


class TestBackgroundBlobSink:
    def test_submit_returns_immediately(self, tmp_path):
        """submit() should not block waiting for upload."""
        s3 = MagicMock()
        barrier = threading.Event()

        def slow_upload(*a, **kw):
            barrier.wait(timeout=5)

        s3.upload_file = slow_upload
        sink = BackgroundBlobSink(s3, max_workers=2)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)
        # If submit blocked, we'd never get here
        barrier.set()
        sink.close()

    def test_drain_waits_for_uploads(self, tmp_path):
        s3 = MagicMock()
        completed = []

        def track_upload(path, key):
            time.sleep(0.05)
            completed.append(key)

        s3.upload_file = track_upload
        sink = BackgroundBlobSink(s3, max_workers=2)

        for i in range(5):
            blob = tmp_path / f"blob{i}"
            blob.write_bytes(b"data")
            sink.submit(str(blob), f"blobs/{i}.blob", i, 4)

        sink.drain()
        assert len(completed) == 5

    def test_drain_raises_on_failure(self, tmp_path):
        s3 = MagicMock()
        s3.upload_file.side_effect = Exception("fail")
        sink = BackgroundBlobSink(s3, max_workers=1, max_retries=1, retry_base_delay=0)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)

        with pytest.raises(RuntimeError, match=r"blob upload.*failed"):
            sink.drain()

    def test_submit_cleans_temp_after_upload(self, tmp_path):
        s3 = MagicMock()
        sink = BackgroundBlobSink(s3, max_workers=1)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4, cleanup_path=str(blob))
        sink.drain()

        assert not blob.exists()

    def test_close_drains_and_shuts_down(self, tmp_path):
        s3 = MagicMock()
        sink = BackgroundBlobSink(s3, max_workers=1)
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)
        sink.close()

        s3.upload_file.assert_called_once()


class TestDeferredBlobSink:
    def test_submit_writes_manifest_line(self, tmp_path):
        manifest = tmp_path / "manifest.tsv"
        sink = DeferredBlobSink(str(manifest))
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/1/2.blob", 42, 4)
        sink.close()

        lines = manifest.read_text().strip().split("\n")
        assert len(lines) == 1
        parts = lines[0].split("\t")
        assert parts[0] == str(blob)
        assert parts[1] == "blobs/1/2.blob"
        assert parts[2] == "42"
        assert parts[3] == "4"

    def test_submit_does_not_upload(self, tmp_path):
        """No S3 operations should happen."""
        manifest = tmp_path / "manifest.tsv"
        sink = DeferredBlobSink(str(manifest))
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)
        sink.close()

        assert blob.exists()

    def test_submit_preserves_temp_files(self, tmp_path):
        """DeferredBlobSink must NOT delete temp files (needed for later upload)."""
        manifest = tmp_path / "manifest.tsv"
        sink = DeferredBlobSink(str(manifest))
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4, cleanup_path=str(blob))
        sink.close()

        assert blob.exists()

    def test_multiple_submits(self, tmp_path):
        manifest = tmp_path / "manifest.tsv"
        sink = DeferredBlobSink(str(manifest))

        for i in range(5):
            blob = tmp_path / f"blob{i}"
            blob.write_bytes(b"data")
            sink.submit(str(blob), f"blobs/{i}.blob", i, 4)

        sink.close()

        lines = manifest.read_text().strip().split("\n")
        assert len(lines) == 5

    def test_drain_is_noop(self, tmp_path):
        manifest = tmp_path / "manifest.tsv"
        sink = DeferredBlobSink(str(manifest))
        sink.drain()
        sink.close()

    def test_close_reports_count(self, tmp_path, caplog):
        manifest = tmp_path / "manifest.tsv"
        sink = DeferredBlobSink(str(manifest))
        blob = tmp_path / "test.blob"
        blob.write_bytes(b"data")

        sink.submit(str(blob), "blobs/key.blob", 1, 4)

        import logging

        with caplog.at_level(logging.INFO):
            sink.close()

        assert "1 deferred blob" in caplog.text
