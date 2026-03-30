import pytest
from unittest.mock import MagicMock

from zodb_pgjsonb.blob_sink import InlineBlobSink


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
