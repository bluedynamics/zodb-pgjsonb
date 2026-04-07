"""Tests for extracted migration helpers: WatermarkTracker, ProgressTracker, etc."""

from concurrent.futures import Future
from unittest import mock
from zodb_pgjsonb.migration import _check_worker_errors
from zodb_pgjsonb.migration import _create_blob_sink
from zodb_pgjsonb.migration import _fmt_blob_size
from zodb_pgjsonb.migration import _fmt_elapsed
from zodb_pgjsonb.migration import _stage_blob
from zodb_pgjsonb.migration import ProgressTracker
from zodb_pgjsonb.migration import WatermarkTracker

import os
import pytest
import tempfile


# ── _fmt_blob_size ──────────────────────────────────────────────────────


class TestFmtBlobSize:
    def test_bytes(self):
        assert _fmt_blob_size(500) == "500 B"

    def test_kilobytes(self):
        assert _fmt_blob_size(5000) == "5 KB"

    def test_megabytes(self):
        assert _fmt_blob_size(5_000_000) == "5.0 MB"

    def test_gigabytes(self):
        assert _fmt_blob_size(2_500_000_000) == "2.5 GB"


# ── _fmt_elapsed ────────────────────────────────────────────────────────


class TestFmtElapsed:
    def test_seconds(self):
        assert _fmt_elapsed(45) == " 0:45"

    def test_minutes(self):
        assert _fmt_elapsed(125) == " 2:05"

    def test_hours(self):
        assert _fmt_elapsed(3661) == "1:01:01"


# ── _stage_blob ─────────────────────────────────────────────────────────


class TestStageBlob:
    def test_same_filesystem_hardlinks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "source.blob")
            with open(src, "wb") as f:
                f.write(b"blob data")
            path, is_temp = _stage_blob(src, tmpdir)
            assert is_temp is True
            assert path != src
            assert os.path.exists(path)
            with open(path, "rb") as f:
                assert f.read() == b"blob data"

    def test_cross_filesystem_returns_source(self):
        # Mock os.stat to return different st_dev values
        with tempfile.TemporaryDirectory() as tmpdir:
            src = os.path.join(tmpdir, "source.blob")
            with open(src, "wb") as f:
                f.write(b"data")
            real_stat = os.stat

            def fake_stat(p):
                result = real_stat(p)
                if p == src:
                    # Return a mock with different st_dev
                    m = mock.Mock(wraps=result)
                    m.st_dev = result.st_dev + 999
                    return m
                return result

            with mock.patch("zodb_pgjsonb.migration.os.stat", side_effect=fake_stat):
                path, is_temp = _stage_blob(src, tmpdir)
            assert is_temp is False
            assert path == src


# ── _create_blob_sink ───────────────────────────────────────────────────


class TestCreateBlobSink:
    def test_no_s3_returns_none(self):
        assert _create_blob_sink("inline", None) is None

    def test_inline_with_s3(self):
        from zodb_pgjsonb.blob_sink import InlineBlobSink

        s3 = mock.Mock()
        sink = _create_blob_sink("inline", s3)
        assert isinstance(sink, InlineBlobSink)

    def test_background_with_s3(self):
        from zodb_pgjsonb.blob_sink import BackgroundBlobSink

        s3 = mock.Mock()
        sink = _create_blob_sink("background", s3)
        assert isinstance(sink, BackgroundBlobSink)
        sink.close()

    def test_deferred_mode(self):
        from zodb_pgjsonb.blob_sink import DeferredBlobSink

        with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
            path = f.name
        try:
            sink = _create_blob_sink(f"deferred:{path}", None)
            assert isinstance(sink, DeferredBlobSink)
            sink.close()
        finally:
            os.unlink(path)

    def test_background_without_s3_returns_none(self):
        assert _create_blob_sink("background", None) is None


# ── _check_worker_errors ────────────────────────────────────────────────


class TestCheckWorkerErrors:
    def test_no_errors_passes(self):
        progress = ProgressTracker()
        _check_worker_errors(progress)  # should not raise

    def test_with_errors_raises(self):
        progress = ProgressTracker()
        progress.record_error()
        with pytest.raises(RuntimeError, match="1 worker error"):
            _check_worker_errors(progress)


# ── ProgressTracker ─────────────────────────────────────────────────────


class TestProgressTracker:
    def test_record_write(self):
        pt = ProgressTracker(total_oids=100)
        pt.record_write(10, 2)
        pt.record_write(5, 1)
        txns, objs, blobs, errors = pt.snapshot()
        assert txns == 2
        assert objs == 15
        assert blobs == 3
        assert errors == 0

    def test_record_error(self):
        pt = ProgressTracker()
        pt.record_error()
        pt.record_error()
        assert pt.errors == 2

    def test_snapshot_is_consistent(self):
        pt = ProgressTracker()
        pt.record_write(3, 1)
        pt.record_error()
        txns, objs, blobs, errors = pt.snapshot()
        assert txns == 1
        assert objs == 3
        assert blobs == 1
        assert errors == 1

    def test_drain_workers_returns_immediately_when_done(self):
        pt = ProgressTracker()
        pt.record_write(1, 0)
        pt.drain_workers(txn_count=1)  # should return immediately


# ── WatermarkTracker ────────────────────────────────────────────────────


class TestWatermarkTracker:
    def _make_conn(self):
        """Create a mock PG connection with cursor context manager."""
        conn = mock.MagicMock()
        cur = mock.MagicMock()
        cur.fetchone.return_value = None  # no existing watermark
        conn.cursor.return_value.__enter__ = mock.Mock(return_value=cur)
        conn.cursor.return_value.__exit__ = mock.Mock(return_value=False)
        return conn

    def test_no_start_tid(self):
        conn = self._make_conn()
        wm = WatermarkTracker(conn, start_tid=None)
        assert wm.effective_start is None
        assert wm.idempotent is False

    def test_fresh_start_tid(self):
        from ZODB.utils import p64

        conn = self._make_conn()
        start = p64(100)
        wm = WatermarkTracker(conn, start_tid=start)
        assert wm.effective_start == start
        assert wm.idempotent is True

    def test_resume_from_watermark(self):
        from ZODB.utils import p64
        from ZODB.utils import u64

        conn = self._make_conn()
        # Simulate existing watermark at TID 50
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = {"tid": 50}
        start = p64(100)
        wm = WatermarkTracker(conn, start_tid=start)
        # effective_start should be min(100, 50+1) = 51
        assert u64(wm.effective_start) == 51

    def test_append_and_advance(self):
        conn = self._make_conn()
        wm = WatermarkTracker(conn, start_tid=None)

        done_future = Future()
        done_future.set_result(None)
        pending_future = Future()

        wm.append(100, done_future)
        wm.append(200, pending_future)
        wm.advance()
        # Should have advanced past the done future but not the pending one
        assert wm._idx == 1

    def test_flush_on_error_is_safe(self):
        conn = self._make_conn()
        wm = WatermarkTracker(conn, start_tid=None)

        done = Future()
        done.set_result(None)
        failed = Future()
        failed.set_exception(ValueError("boom"))

        wm.append(100, done)
        wm.append(200, failed)
        # Should not raise
        wm.flush_on_error()

    def test_drop_table(self):
        conn = self._make_conn()
        wm = WatermarkTracker(conn, start_tid=None)
        wm.drop_table()
        conn.execute.assert_any_call("DROP TABLE IF EXISTS migration_watermark")
