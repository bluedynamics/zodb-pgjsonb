"""Tests for CacheWarmer — unit + integration tests."""

from unittest import mock
from zodb_pgjsonb.cache_warmer import CacheWarmer

import pytest


def _mk_shared_cache():
    from zodb_pgjsonb.storage import SharedLoadCache

    return SharedLoadCache(max_mb=4)


class _FakeCursor:
    def __init__(self, top_oids):
        self._top_oids = top_oids

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a, **kw):
        return None

    def fetchall(self):
        return [{"zoid": z} for z in self._top_oids]


class _FakeConn:
    """Minimal stand-in for a psycopg connection used by CacheWarmer tests.

    Supports the subset used by ``_read_top_oids``: ``cursor()`` context
    manager returning rows with a ``"zoid"`` key.
    """

    def __init__(self, top_oids):
        self._top_oids = list(top_oids)

    def cursor(self):
        return _FakeCursor(self._top_oids)

    def execute(self, *a, **kw):
        return None


class TestCacheWarmerRecord:
    """Test recording phase."""

    def test_records_unique_zoids(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=50,
        )
        w.record(1)
        w.record(2)
        w.record(1)  # duplicate
        assert len(w._recorded) == 2

    def test_stops_recording_at_target(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=3,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=100,
        )
        w._flush = mock.Mock()  # stub out DB writes
        w.record(1)
        w.record(2)
        assert w.recording is True
        w.record(3)
        assert w.recording is False

    def test_flushes_at_interval(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=3,
        )
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        assert w._flush.call_count == 0
        w.record(3)
        assert w._flush.call_count == 1

    def test_first_flush_applies_decay(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=2,
        )
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        w._flush.assert_called_once_with(decay=True)
        w.record(3)
        w.record(4)
        assert w._flush.call_args == mock.call(decay=False)

    def test_final_flush_on_target_reached(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=3,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=100,
        )
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        w.record(3)
        # Should flush remaining pending even though interval not reached
        assert w._flush.call_count == 1

    def test_no_recording_when_disabled(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=0,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        assert w.recording is False

    def test_record_noop_after_recording_stops(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=2,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=100,
        )
        w._flush = mock.Mock()
        w.record(1)
        w.record(2)
        assert w.recording is False
        # This must hit the early return on line 65
        w.record(3)
        assert len(w._recorded) == 2
        assert 3 not in w._recorded


class TestCacheWarmerWarm:
    """Test warming phase."""

    def test_warm_populates_cache(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        w.warm(loader)
        assert shared.get(zoid=1, polled_tid=100) == (b"data-" + p64(1), p64(50))
        assert shared.get(zoid=2, polled_tid=100) == (b"data-" + p64(2), p64(50))
        assert shared.get(zoid=3, polled_tid=100) == (b"data-" + p64(3), p64(50))

    def test_warm_empty_stats(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )
        w.warm(lambda oids: {})
        # No stats → nothing written
        assert shared._consensus_tid is None
        assert len(shared._cache) == 0

    def test_warm_handles_load_multiple_exception(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def raising_loader(oids):
            raise RuntimeError("simulated failure")

        # warm must swallow the exception and leave the cache empty
        w.warm(raising_loader)
        assert len(shared._cache) == 0


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

        w = CacheWarmer(
            conn=self.conn,
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=50,
        )
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

        w = CacheWarmer(
            conn=self.conn,
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            decay=0.5,
        )
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

        w = CacheWarmer(
            conn=self.conn,
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            decay=0.5,
        )
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

        w = CacheWarmer(
            conn=self.conn,
            target_count=2,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        oids = w._read_top_oids()
        # Top 2 by score: 3 (20.0), 1 (10.0)
        assert oids == [3, 1]

    def test_full_record_flush_read_cycle(self):
        """End-to-end: record OIDs → flush → read back top."""
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=self.conn,
            target_count=5,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            flush_interval=100,
            decay=0.8,
        )
        for zoid in [100, 200, 300, 400, 500]:
            w.record(zoid)

        assert w.recording is False

        # Read back
        w2 = CacheWarmer(
            conn=self.conn,
            target_count=3,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        oids = w2._read_top_oids()
        assert len(oids) == 3
        assert set(oids).issubset({100, 200, 300, 400, 500})


class TestCacheWarmerFlushEdge:
    """Edge-case tests for _flush() — no DB required."""

    def test_flush_empty_pending_is_noop(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        conn = mock.Mock()
        w = CacheWarmer(
            conn=conn,
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        w._pending = set()  # already empty
        w._flush(decay=False)
        conn.execute.assert_not_called()

    def test_flush_exception_triggers_rollback(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        conn = mock.Mock()
        conn.execute.side_effect = RuntimeError("connection lost")
        w = CacheWarmer(
            conn=conn,
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        w._pending = {10, 20}
        # Must not propagate the exception
        w._flush(decay=False)
        # ROLLBACK attempted (second call after BEGIN failed)
        calls = [c.args[0] for c in conn.execute.call_args_list]
        assert "BEGIN" in calls
        assert "ROLLBACK" in calls

    def test_flush_sorts_zoids_for_deterministic_locking(self):
        """Prevent PK-index deadlock between concurrent workers.

        The INSERT must receive zoids in sorted order so that two
        workers with overlapping pending sets acquire row locks in the
        same order and never deadlock.
        """
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        conn = mock.MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value

        w = CacheWarmer(
            conn=conn,
            target_count=100,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        w._pending = {42, 7, 99, 3, 58}
        w._flush(decay=False)

        insert_calls = [
            call
            for call in cursor.execute.call_args_list
            if "INSERT INTO cache_warm_stats" in call.args[0]
        ]
        assert len(insert_calls) == 1
        zoids_param = insert_calls[0].args[1]["z"]
        assert zoids_param == sorted(zoids_param), (
            f"zoids must be sorted for deterministic lock order, got {zoids_param}"
        )
        assert zoids_param == [3, 7, 42, 58, 99]


class TestWarmLoadMultiple:
    """Integration test for PGJsonbStorage._warm_load_multiple()."""

    def test_warm_load_multiple_end_to_end(self, db):
        """Exercise _warm_load_multiple() with real objects in PG."""
        from persistent.mapping import PersistentMapping

        import transaction as txn
        import zodb_json_codec

        conn = db.open()
        root = conn.root()
        root["x"] = PersistentMapping({"key": "val1"})
        root["y"] = PersistentMapping({"key": "val2"})
        root["z"] = PersistentMapping({"key": "val3"})
        txn.commit()

        oids = [root["x"]._p_oid, root["y"]._p_oid, root["z"]._p_oid]
        conn.close()

        result = db.storage._warm_load_multiple(oids)

        assert len(result) == 3
        for oid in oids:
            data, tid = result[oid]
            assert isinstance(data, bytes)
            assert isinstance(tid, bytes)
            assert len(tid) == 8
            # Verify the data round-trips through the codec
            record = zodb_json_codec.decode_zodb_record(data)
            assert "@cls" in record
            assert "@s" in record


@pytest.mark.db
class TestWarmerPopulatesSharedCache:
    """After #63: warmer writes go into the shared cache, not a private dict."""

    def test_warm_populates_shared_cache(self, storage):
        """CacheWarmer.warm loads into PGJsonbStorage._shared_cache."""
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # Seed the stats table
        with storage._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cache_warm_stats (zoid, score) "
                "VALUES (1, 10.0), (2, 9.0), (3, 8.0)"
            )
        storage._conn.commit()

        # Manually trigger a warm cycle
        warmer = CacheWarmer(
            conn=storage._conn,
            target_count=10,
            shared_cache=storage._shared_cache,
            load_current_tid_fn=lambda: 100,
        )

        def load_multiple_fn(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        # Prime consensus so set() is accepted
        storage._shared_cache.poll_advance(new_tid=100, changed_zoids=[])
        warmer.warm(load_multiple_fn)

        shared = storage._shared_cache
        assert shared.get(zoid=1, polled_tid=100) == (b"data-" + p64(1), p64(50))
        assert shared.get(zoid=2, polled_tid=100) == (b"data-" + p64(2), p64(50))


class TestWarmerRaceRecovery:
    """#65 I3: warmer tolerates a consensus race and logs silent failures."""

    def test_warm_uses_effective_consensus_when_race_advances_it(self):
        """If consensus is already ahead, warmer's writes still land."""
        from ZODB.utils import p64
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        # Simulate another instance's poll_advance racing ahead
        shared.poll_advance(new_tid=1000, changed_zoids=[])

        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 500,  # stale — before the race
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(500)) for oid in oids}

        w.warm(loader)

        # Entries must be present: the warmer re-read consensus and
        # used 1000 (the actual value) as polled_tid instead of 500.
        assert shared.get(zoid=1, polled_tid=1000) == (b"data-" + p64(1), p64(500))
        assert shared.get(zoid=2, polled_tid=1000) == (b"data-" + p64(2), p64(500))

    def test_warm_logs_warning_when_all_writes_rejected(self, caplog):
        """If consensus advances past the warmer's effective polled_tid
        mid-loop, every set() is rejected and the warmer flags it."""
        from ZODB.utils import p64
        from zodb_pgjsonb.storage import SharedLoadCache

        import logging

        # A cache subclass whose set() always returns False (simulating
        # a mid-loop consensus advance that we can't actually time).
        class _RejectingCache(SharedLoadCache):
            def set(self, *a, **kw):
                super().set(*a, **kw)
                return False

        shared = _RejectingCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        with caplog.at_level(logging.WARNING, logger="zodb_pgjsonb.cache_warmer"):
            w.warm(loader)

        warnings = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING and "rejected" in r.getMessage().lower()
        ]
        assert warnings, (
            f"expected a WARNING about rejected writes, got: "
            f"{[r.getMessage() for r in caplog.records]}"
        )

    def test_warm_info_log_on_normal_writes(self, caplog):
        """Happy path logs at INFO, not WARNING."""
        from ZODB.utils import p64
        from zodb_pgjsonb.storage import SharedLoadCache

        import logging

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        with caplog.at_level(logging.INFO, logger="zodb_pgjsonb.cache_warmer"):
            w.warm(loader)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert not warnings, (
            f"expected no warnings, got: {[r.getMessage() for r in warnings]}"
        )
        infos = [
            r
            for r in caplog.records
            if r.levelno == logging.INFO and "loaded 2 of 2 objects" in r.getMessage()
        ]
        assert infos, (
            f"expected INFO with loaded count, got: "
            f"{[r.getMessage() for r in caplog.records]}"
        )
