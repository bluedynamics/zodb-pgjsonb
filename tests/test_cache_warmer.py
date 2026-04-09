"""Tests for CacheWarmer — unit + integration tests."""

from unittest import mock

import pytest


class TestCacheWarmerRecord:
    """Test recording phase."""

    def test_records_unique_zoids(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=100, flush_interval=50)
        w.record(1)
        w.record(2)
        w.record(1)  # duplicate
        assert len(w._recorded) == 2

    def test_stops_recording_at_target(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=3, flush_interval=100)
        w._flush = mock.Mock()  # stub out DB writes
        w.record(1)
        w.record(2)
        assert w.recording is True
        w.record(3)
        assert w.recording is False

    def test_flushes_at_interval(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=100, flush_interval=3)
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        assert w._flush.call_count == 0
        w.record(3)
        assert w._flush.call_count == 1

    def test_first_flush_applies_decay(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=100, flush_interval=2)
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        w._flush.assert_called_once_with(decay=True)
        w.record(3)
        w.record(4)
        assert w._flush.call_args == mock.call(decay=False)

    def test_final_flush_on_target_reached(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=3, flush_interval=100)
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        w.record(3)
        # Should flush remaining pending even though interval not reached
        assert w._flush.call_count == 1

    def test_no_recording_when_disabled(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=0)
        assert w.recording is False

    def test_record_noop_after_recording_stops(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=2, flush_interval=100)
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        assert w.recording is False
        # This must hit the early return on line 65
        w.record(3)
        assert len(w._recorded) == 2
        assert 3 not in w._recorded


class TestCacheWarmerL2:
    """Test L2 warm cache get/invalidate."""

    def test_get_returns_none_before_warming_done(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._warm_cache = {1: (b"data", b"tid")}
        w._warming_done.clear()
        assert w.get(1) is None

    def test_get_returns_data_after_warming_done(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._warm_cache = {1: (b"data", b"tid")}
        w._warming_done.set()
        assert w.get(1) == (b"data", b"tid")

    def test_get_returns_none_for_missing(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._warming_done.set()
        assert w.get(999) is None

    def test_invalidate_removes_entry(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._warm_cache = {1: (b"data", b"tid"), 2: (b"d2", b"t2")}
        w._warming_done.set()
        w.invalidate(1)
        assert w.get(1) is None
        assert w.get(2) == (b"d2", b"t2")

    def test_invalidate_nonexistent_is_noop(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._warm_cache = {}
        w._warming_done.set()
        w.invalidate(999)  # should not raise


class TestCacheWarmerWarm:
    """Test warming phase."""

    def test_warm_populates_cache(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._read_top_oids = mock.Mock(return_value=[1, 2, 3])

        def fake_load(oids):
            from ZODB.utils import p64

            return {p64(z): (f"data{z}".encode(), p64(100)) for z in [1, 2, 3]}

        w.warm(fake_load)
        assert w._warming_done.is_set()
        assert len(w._warm_cache) == 3

    def test_warm_empty_stats(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=mock.Mock(), target_count=10)
        w._read_top_oids = mock.Mock(return_value=[])
        w.warm(mock.Mock())
        assert w._warming_done.is_set()
        assert len(w._warm_cache) == 0


@pytest.mark.db
class TestCacheWarmerDB:
    """Integration tests for DB-dependent methods (require PostgreSQL)."""

    @pytest.fixture(autouse=True)
    def _setup_db(self):
        """Create cache_warm_stats table in test DB."""
        from psycopg.rows import dict_row
        from tests.conftest import DSN

        import psycopg

        try:
            self.conn = psycopg.connect(DSN, row_factory=dict_row)
        except Exception:
            pytest.skip("PostgreSQL not available")
        self.conn.autocommit = True
        self.conn.execute("DROP TABLE IF EXISTS cache_warm_stats")
        self.conn.execute(
            "CREATE TABLE cache_warm_stats ("
            "  zoid BIGINT PRIMARY KEY,"
            "  score FLOAT NOT NULL DEFAULT 1.0"
            ")"
        )
        yield
        self.conn.execute("DROP TABLE IF EXISTS cache_warm_stats")
        self.conn.close()

    def test_flush_writes_scores(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=self.conn, target_count=100, flush_interval=50)
        w._pending = {10, 20, 30}
        w._flush(decay=False)

        with self.conn.cursor() as cur:
            cur.execute("SELECT zoid, score FROM cache_warm_stats ORDER BY zoid")
            rows = cur.fetchall()
        assert len(rows) == 3
        assert rows[0]["zoid"] == 10
        assert rows[0]["score"] == 1.0

    def test_flush_with_decay(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # Pre-populate
        self.conn.execute("INSERT INTO cache_warm_stats (zoid, score) VALUES (10, 5.0)")

        w = CacheWarmer(conn=self.conn, target_count=100, decay=0.5)
        w._pending = {20}
        w._flush(decay=True)

        with self.conn.cursor() as cur:
            cur.execute("SELECT score FROM cache_warm_stats WHERE zoid = 10")
            row = cur.fetchone()
        # 5.0 * 0.5 = 2.5
        assert row["score"] == 2.5

    def test_flush_prunes_low_scores(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        self.conn.execute(
            "INSERT INTO cache_warm_stats (zoid, score) VALUES (10, 0.001)"
        )

        w = CacheWarmer(conn=self.conn, target_count=100, decay=0.5)
        w._pending = {20}
        w._flush(decay=True)

        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM cache_warm_stats WHERE zoid = 10")
            assert cur.fetchone()["cnt"] == 0

    def test_read_top_oids(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        self.conn.execute(
            "INSERT INTO cache_warm_stats (zoid, score) VALUES "
            "(1, 10.0), (2, 5.0), (3, 20.0), (4, 1.0)"
        )

        w = CacheWarmer(conn=self.conn, target_count=2)
        oids = w._read_top_oids()
        # Top 2 by score: 3 (20.0), 1 (10.0)
        assert oids == [3, 1]

    def test_full_record_flush_read_cycle(self):
        """End-to-end: record OIDs → flush → read back top."""
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(conn=self.conn, target_count=5, flush_interval=100, decay=0.8)
        for zoid in [100, 200, 300, 400, 500]:
            w.record(zoid)

        assert w.recording is False

        # Read back
        w2 = CacheWarmer(conn=self.conn, target_count=3)
        oids = w2._read_top_oids()
        assert len(oids) == 3
        assert set(oids).issubset({100, 200, 300, 400, 500})
