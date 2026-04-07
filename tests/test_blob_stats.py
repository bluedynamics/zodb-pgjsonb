"""Tests for blob storage statistics methods on PGJsonbStorage."""

from unittest import mock
from zodb_pgjsonb.stats import _format_size


# ---------------------------------------------------------------------------
# _format_size helper
# ---------------------------------------------------------------------------


class TestFormatSize:
    """Tests for the _format_size() helper."""

    def test_zero(self):
        assert _format_size(0) == "0 B"

    def test_bytes(self):
        assert _format_size(500) == "500 B"

    def test_kilobytes(self):
        assert _format_size(1536) == "1.5 KB"

    def test_megabytes(self):
        assert _format_size(10485760) == "10.0 MB"

    def test_gigabytes(self):
        assert _format_size(1073741824) == "1.0 GB"

    def test_terabytes(self):
        assert _format_size(1099511627776) == "1.0 TB"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_storage(s3_client=None, blob_threshold=102400):
    """Create a mock PGJsonbStorage with a mock _conn."""
    storage = mock.MagicMock()
    storage.__class__ = _get_storage_class()
    storage._s3_client = s3_client
    storage._blob_threshold = blob_threshold
    return storage


def _get_storage_class():
    from zodb_pgjsonb.storage import PGJsonbStorage

    return PGJsonbStorage


def _setup_mock_conn(storage, fetchone_returns):
    """Configure storage._conn mock with sequential fetchone results."""
    mock_conn = mock.MagicMock()
    cursor_ctx = mock_conn.cursor().__enter__()
    cursor_ctx.fetchone.side_effect = fetchone_returns
    storage._conn = mock_conn
    return mock_conn


# ---------------------------------------------------------------------------
# get_blob_stats
# ---------------------------------------------------------------------------


class TestGetBlobStats:
    """Tests for PGJsonbStorage.get_blob_stats()."""

    def _call(self, storage):
        from zodb_pgjsonb.storage import PGJsonbStorage

        return PGJsonbStorage.get_blob_stats(storage)

    def test_table_not_exists(self):
        storage = _make_storage()
        _setup_mock_conn(storage, [{"exists": False}])
        result = self._call(storage)
        assert result == {"available": False}

    def test_empty_table(self):
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {
                    "total_blobs": 0,
                    "unique_objects": 0,
                    "total_size": 0,
                    "pg_count": 0,
                    "pg_size": 0,
                    "s3_count": 0,
                    "s3_size": 0,
                    "largest_blob": 0,
                    "avg_blob_size": 0,
                },
            ],
        )
        result = self._call(storage)
        assert result["available"] is True
        assert result["total_blobs"] == 0
        assert result["unique_objects"] == 0
        assert result["total_size_display"] == "0 B"
        assert result["avg_versions"] == 0

    def test_pg_only_blobs(self):
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {
                    "total_blobs": 100,
                    "unique_objects": 50,
                    "total_size": 10485760,
                    "pg_count": 100,
                    "pg_size": 10485760,
                    "s3_count": 0,
                    "s3_size": 0,
                    "largest_blob": 1048576,
                    "avg_blob_size": 104857,
                },
            ],
        )
        result = self._call(storage)
        assert result["available"] is True
        assert result["total_blobs"] == 100
        assert result["unique_objects"] == 50
        assert result["pg_count"] == 100
        assert result["s3_count"] == 0
        assert result["avg_versions"] == 2.0
        assert result["total_size_display"] == "10.0 MB"
        assert result["largest_blob_display"] == "1.0 MB"

    def test_mixed_tiers(self):
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {
                    "total_blobs": 200,
                    "unique_objects": 150,
                    "total_size": 1073741824,
                    "pg_count": 80,
                    "pg_size": 73741824,
                    "s3_count": 120,
                    "s3_size": 1000000000,
                    "largest_blob": 52428800,
                    "avg_blob_size": 5368709,
                },
            ],
        )
        result = self._call(storage)
        assert result["pg_count"] == 80
        assert result["s3_count"] == 120
        assert result["total_size_display"] == "1.0 GB"

    def test_avg_versions_single_version(self):
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {
                    "total_blobs": 10,
                    "unique_objects": 10,
                    "total_size": 1024,
                    "pg_count": 10,
                    "pg_size": 1024,
                    "s3_count": 0,
                    "s3_size": 0,
                    "largest_blob": 512,
                    "avg_blob_size": 102,
                },
            ],
        )
        result = self._call(storage)
        assert result["avg_versions"] == 1.0


# ---------------------------------------------------------------------------
# get_blob_histogram
# ---------------------------------------------------------------------------


class TestGetBlobHistogram:
    """Tests for PGJsonbStorage.get_blob_histogram()."""

    def _call(self, storage):
        from zodb_pgjsonb.storage import PGJsonbStorage

        return PGJsonbStorage.get_blob_histogram(storage)

    def test_table_not_exists(self):
        storage = _make_storage()
        _setup_mock_conn(storage, [{"exists": False}])
        result = self._call(storage)
        assert result == []

    def test_empty_table(self):
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": None, "max_size": None, "cnt": 0},
            ],
        )
        result = self._call(storage)
        assert result == []

    def test_small_blobs_single_bucket(self):
        """All blobs < 10 KB -> single bucket 0-10 KB."""
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": 10, "max_size": 500, "cnt": 42},
                {"bucket_0": 42},
            ],
        )
        result = self._call(storage)
        assert len(result) == 1
        assert result[0]["count"] == 42
        assert result[0]["pct"] == 100
        assert "10.0 KB" in result[0]["label"]

    def test_spans_multiple_decades(self):
        """Blobs from 500 B to 5 MB -> 4 buckets."""
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": 500, "max_size": 5_000_000, "cnt": 100},
                {"bucket_0": 10, "bucket_1": 30, "bucket_2": 25, "bucket_3": 35},
            ],
        )
        result = self._call(storage)
        assert len(result) == 4
        # Largest bucket (35) gets pct=100
        assert result[3]["count"] == 35
        assert result[3]["pct"] == 100
        assert "10.0 MB" in result[3]["label"]

    def test_pct_relative_to_max(self):
        """pct should be relative to the largest bucket."""
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": 100, "max_size": 2000, "cnt": 30},
                {"bucket_0": 30},
            ],
        )
        result = self._call(storage)
        assert len(result) == 1
        assert result[0]["pct"] == 100

    def test_labels_use_format_size(self):
        """Labels should use human-readable sizes."""
        storage = _make_storage()
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": 500, "max_size": 50_000, "cnt": 10},
                {"bucket_0": 5, "bucket_1": 5},
            ],
        )
        result = self._call(storage)
        assert result[0]["label"] == "0 B \u2013 10.0 KB"
        assert "100.0 KB" in result[1]["label"]

    def test_tier_without_s3(self):
        """Without S3, tier should be empty string."""
        storage = _make_storage(s3_client=None)
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": 100, "max_size": 2000, "cnt": 10},
                {"bucket_0": 10},
            ],
        )
        result = self._call(storage)
        assert result[0]["tier"] == ""

    def test_tier_with_s3_threshold(self):
        """With S3, buckets should be classified as pg/s3/mixed."""
        s3_mock = mock.MagicMock()
        storage = _make_storage(s3_client=s3_mock, blob_threshold=1048576)
        _setup_mock_conn(
            storage,
            [
                {"exists": True},
                {"min_size": 500, "max_size": 5_000_000, "cnt": 100},
                {"bucket_0": 10, "bucket_1": 20, "bucket_2": 30, "bucket_3": 40},
            ],
        )
        result = self._call(storage)
        assert len(result) == 4
        # 0-10KB: entirely below 1MB -> pg
        assert result[0]["tier"] == "pg"
        # 10KB-100KB: entirely below 1MB -> pg
        assert result[1]["tier"] == "pg"
        # 100KB-1MB: hi == threshold -> pg (hi <= threshold)
        assert result[2]["tier"] == "pg"
        # 1MB-10MB: lo >= threshold -> s3
        assert result[3]["tier"] == "s3"
