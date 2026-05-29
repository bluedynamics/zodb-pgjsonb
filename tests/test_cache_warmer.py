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


class TestCacheWarmerNewKwargs:
    """Confirms the six herd-mitigation kwargs are accepted and stored
    on the instance with their defaults (off)."""

    def test_defaults_are_off(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        assert w._delay == 0
        assert w._jitter == 0
        assert w._concurrency == 0
        assert w._wait_max == 300
        assert w._batch_size == 500
        assert w._batch_pause == 0.0
        assert w._dsn is None

    def test_kwargs_override(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=foo",
            delay=15,
            jitter=30,
            concurrency=2,
            wait_max=120,
            batch_size=250,
            batch_pause=0.25,
        )
        assert w._delay == 15
        assert w._jitter == 30
        assert w._concurrency == 2
        assert w._wait_max == 120
        assert w._batch_size == 250
        assert w._batch_pause == 0.25
        assert w._dsn == "dbname=foo"


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

    def test_warm_sleeps_delay_plus_jitter(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            delay=15,
            jitter=30,
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        with (
            mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep,
            mock.patch(
                "zodb_pgjsonb.cache_warmer.random.uniform",
                return_value=7.0,
            ),
        ):
            w.warm(loader)

        # First sleep call should be delay + uniform(0, jitter) = 15 + 7 = 22
        assert mock_sleep.call_args_list[0] == mock.call(22.0)

    def test_warm_batches_with_pause(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        # 1100 OIDs at batch_size=500 → 3 batches (500+500+100),
        # 2 inter-batch sleeps (after batches 1 and 2, not after 3).
        top = list(range(1, 1101))
        shared = SharedLoadCache(max_mb=64)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=top),
            target_count=1100,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            batch_size=500,
            batch_pause=0.25,
        )

        call_chunks = []

        def loader(oids):
            call_chunks.append(len(oids))
            return {oid: (b"x", p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep:
            w.warm(loader)

        # Three loader calls, sizes 500, 500, 100.
        assert call_chunks == [500, 500, 100]
        # Exactly two batch_pause sleeps of 0.25 (no trailing).
        pause_calls = [c for c in mock_sleep.call_args_list if c.args == (0.25,)]
        assert len(pause_calls) == 2

    def test_warm_single_batch_no_pause(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            batch_size=500,
            batch_pause=0.5,
        )

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep:
            w.warm(loader)

        # Single batch → zero batch_pause sleeps.
        pause_calls = [c for c in mock_sleep.call_args_list if c.args == (0.5,)]
        assert pause_calls == []

    def test_warm_no_sleep_when_disabled(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            delay=0,
            jitter=0,
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep:
            w.warm(loader)

        # When both delay and jitter are zero, no initial sleep call.
        for c in mock_sleep.call_args_list:
            assert c.args[0] == 0 or c.args == ()

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

    def test_warm_acquires_and_releases_slot(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
        )
        # Pretend slot acquisition succeeds without touching PG.
        acquire_calls = []
        release_calls = []

        def fake_acquire():
            sentinel = object()
            acquire_calls.append(sentinel)
            return (sentinel, 1)

        def fake_release(lock_conn, slot):
            release_calls.append((lock_conn, slot))

        w._acquire_slot = fake_acquire
        w._release_slot = fake_release

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        w.warm(loader)
        assert len(acquire_calls) == 1
        assert len(release_calls) == 1
        assert release_calls[0][1] == 1  # slot number
        assert release_calls[0][0] is acquire_calls[0]  # same conn

    def test_warm_skips_on_acquire_failure(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
        )
        w._acquire_slot = lambda: None

        load_called = []

        def loader(oids):
            load_called.append(oids)
            return {}

        w.warm(loader)
        # No load when acquire fails.
        assert load_called == []
        assert len(shared._cache) == 0

    def test_warm_no_slot_when_concurrency_zero(self):
        """When concurrency=0, the slot path is bypassed entirely
        (preserves backward-compat for direct test instantiation)."""
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            concurrency=0,
        )
        acquired = []
        w._acquire_slot = lambda: acquired.append(True) or None

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        w.warm(loader)
        # _acquire_slot must not be called when concurrency=0.
        assert acquired == []
        # Warming still happens.
        assert len(shared._cache) == 2

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


class TestCacheWarmerSlot:
    """Unit tests for the B2b advisory-lock slot helpers."""

    def _make_lock_conn(self, slot_results):
        """Return a mock connection whose execute('SELECT pg_try_advisory_lock(...)')
        returns ``slot_results[i]`` on the i-th call (one row, one column).
        """
        call_count = [0]

        class _LockCursor:
            def __init__(self):
                self._row = None

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=None):
                if "pg_try_advisory_lock" in sql:
                    i = call_count[0]
                    call_count[0] += 1
                    self._row = (
                        (slot_results[i],) if i < len(slot_results) else (False,)
                    )
                else:
                    self._row = None
                return self

            def fetchone(self):
                return self._row

        class _LockConn:
            def cursor(self):
                return _LockCursor()

            def execute(self, *a, **kw):
                return None

            def close(self):
                pass

        return _LockConn()

    def test_try_acquire_slot_returns_slot_when_first_free(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=3,
        )
        lock_conn = self._make_lock_conn(slot_results=[True])
        slot = w._try_acquire_slot_once(lock_conn)
        assert slot == 1

    def test_try_acquire_slot_returns_higher_slot_when_first_taken(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=3,
        )
        lock_conn = self._make_lock_conn(slot_results=[False, False, True])
        slot = w._try_acquire_slot_once(lock_conn)
        assert slot == 3

    def test_try_acquire_slot_returns_none_when_all_taken(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=3,
        )
        lock_conn = self._make_lock_conn(slot_results=[False, False, False])
        slot = w._try_acquire_slot_once(lock_conn)
        assert slot is None

    def test_acquire_slot_succeeds_after_retries(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # First two attempts: all slots taken.  Third attempt: slot 1 free.
        # With concurrency=2 that's 2 + 2 + 1 = 5 try-lock calls before success.
        slot_results = [False, False, False, False, True]
        lock_conn = self._make_lock_conn(slot_results=slot_results)

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
            wait_max=30,
        )

        # Patch psycopg.connect to return our pre-built lock_conn, and
        # patch sleep so the test doesn't actually wait.
        with (
            mock.patch(
                "zodb_pgjsonb.cache_warmer.psycopg.connect",
                return_value=lock_conn,
            ),
            mock.patch("zodb_pgjsonb.cache_warmer.time.sleep"),
            mock.patch(
                "zodb_pgjsonb.cache_warmer.random.uniform",
                return_value=3.0,
            ),
            mock.patch(
                "zodb_pgjsonb.cache_warmer.time.monotonic",
                side_effect=[0, 3, 6, 9],
            ),
        ):
            conn, slot = w._acquire_slot()

        assert conn is lock_conn
        assert slot == 1

    def test_acquire_slot_times_out(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # All slots permanently taken.
        lock_conn = self._make_lock_conn(slot_results=[False] * 100)
        # Track that the lock connection is closed when timeout fires.
        close_calls = []
        lock_conn.close = lambda: close_calls.append(True)

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
            wait_max=10,
        )

        # monotonic side_effect: simulate 0s, 3s, 6s, 9s, 12s — wait_max hit.
        with (
            mock.patch(
                "zodb_pgjsonb.cache_warmer.psycopg.connect",
                return_value=lock_conn,
            ),
            mock.patch("zodb_pgjsonb.cache_warmer.time.sleep"),
            mock.patch(
                "zodb_pgjsonb.cache_warmer.random.uniform",
                return_value=3.0,
            ),
            mock.patch(
                "zodb_pgjsonb.cache_warmer.time.monotonic",
                side_effect=[0, 3, 6, 9, 12],
            ),
        ):
            result = w._acquire_slot()

        assert result is None
        # Connection must be closed on the timeout path (no leak).
        assert close_calls == [True]

    def test_acquire_slot_handles_connect_failure(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
            wait_max=30,
        )

        with mock.patch(
            "zodb_pgjsonb.cache_warmer.psycopg.connect",
            side_effect=RuntimeError("connect refused"),
        ):
            result = w._acquire_slot()

        assert result is None

    def test_release_slot_unlocks_and_closes(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.cache_warmer import WARMER_LOCK_NS
        from zodb_pgjsonb.cache_warmer import WARMER_SLOT_BASE

        executed = []
        closed = []

        class _ReleaseConn:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def close(self):
                closed.append(True)

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=2,
        )
        lock_conn = _ReleaseConn()
        w._release_slot(lock_conn, slot=2)

        assert len(executed) == 1
        sql, params = executed[0]
        assert "pg_advisory_unlock" in sql
        assert params == (WARMER_LOCK_NS, WARMER_SLOT_BASE + 2)
        assert closed == [True]

    def test_release_slot_swallows_errors(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        class _BadConn:
            def execute(self, sql, params=None):
                raise RuntimeError("connection gone")

            def close(self):
                raise RuntimeError("close gone too")

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=2,
        )
        # Must not raise.
        w._release_slot(_BadConn(), slot=1)


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

    def test_concurrency_1_serializes_two_warmers(self):
        """Two warmers with concurrency=1 must not warm in parallel."""
        from tests.conftest import DSN
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        import threading
        import time

        # Seed warm stats so the warmers have something to do.
        self.conn.execute(
            "INSERT INTO cache_warm_stats (zoid, score) "
            "VALUES (1, 5.0), (2, 4.0), (3, 3.0)"
        )

        shared = SharedLoadCache(max_mb=4)
        load_times = []
        load_times_lock = threading.Lock()

        def slow_loader(oids):
            with load_times_lock:
                load_times.append(("enter", time.monotonic()))
            time.sleep(0.5)  # simulate slow load
            with load_times_lock:
                load_times.append(("exit", time.monotonic()))
            return {oid: (b"x", p64(50)) for oid in oids}

        def run_warmer():
            # Fresh connection per thread; psycopg connections are not
            # thread-safe, and self.conn would otherwise be shared.
            from psycopg.rows import dict_row

            import psycopg

            own_conn = psycopg.connect(DSN, autocommit=True, row_factory=dict_row)
            try:
                w = CacheWarmer(
                    conn=own_conn,
                    target_count=10,
                    shared_cache=shared,
                    load_current_tid_fn=lambda: 100,
                    dsn=DSN,
                    concurrency=1,
                    wait_max=30,
                    batch_size=500,
                    batch_pause=0,
                )
                w.warm(slow_loader)
            finally:
                own_conn.close()

        t1 = threading.Thread(target=run_warmer)
        t2 = threading.Thread(target=run_warmer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Two enter/exit pairs, non-overlapping.
        enters = sorted(t for k, t in load_times if k == "enter")
        exits = sorted(t for k, t in load_times if k == "exit")
        assert len(enters) == 2 and len(exits) == 2
        # Strict serialization by the advisory lock: first exit before second enter.
        assert exits[0] <= enters[1] + 0.05  # 50ms slack for thread sched

    def test_concurrency_2_allows_two_parallel_one_waits(self):
        """Three warmers, concurrency=2 — two warm in parallel, third waits."""
        from tests.conftest import DSN
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        import threading
        import time

        self.conn.execute(
            "INSERT INTO cache_warm_stats (zoid, score) "
            "VALUES (1, 5.0), (2, 4.0), (3, 3.0)"
        )

        shared = SharedLoadCache(max_mb=4)
        load_events = []
        load_events_lock = threading.Lock()

        def slow_loader(oids):
            with load_events_lock:
                load_events.append(("enter", time.monotonic()))
            time.sleep(0.5)
            with load_events_lock:
                load_events.append(("exit", time.monotonic()))
            return {oid: (b"x", p64(50)) for oid in oids}

        def run_warmer():
            # Fresh connection per thread (psycopg connections are not
            # thread-safe).
            from psycopg.rows import dict_row

            import psycopg

            own_conn = psycopg.connect(DSN, autocommit=True, row_factory=dict_row)
            try:
                w = CacheWarmer(
                    conn=own_conn,
                    target_count=10,
                    shared_cache=shared,
                    load_current_tid_fn=lambda: 100,
                    dsn=DSN,
                    concurrency=2,
                    wait_max=30,
                    batch_size=500,
                    batch_pause=0,
                )
                w.warm(slow_loader)
            finally:
                own_conn.close()

        threads = [threading.Thread(target=run_warmer) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        enters = sorted(t for k, t in load_events if k == "enter")
        exits = sorted(t for k, t in load_events if k == "exit")
        assert len(enters) == 3 and len(exits) == 3
        # First two enters within a small window (parallel), third
        # enter must be after at least one exit.
        assert enters[1] - enters[0] < 0.3
        assert enters[2] >= exits[0] - 0.05  # 50ms slack

    def test_lock_released_on_connection_close(self):
        """If the warmer's lock connection closes (e.g. pod crash),
        PG auto-releases the session-level lock so another warmer can
        proceed."""
        from tests.conftest import DSN
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.cache_warmer import WARMER_LOCK_NS
        from zodb_pgjsonb.cache_warmer import WARMER_SLOT_BASE

        import psycopg

        # Pod 1: open a connection, acquire slot 1 manually.
        conn1 = psycopg.connect(DSN, autocommit=True)
        with conn1.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(%s, %s)",
                (WARMER_LOCK_NS, WARMER_SLOT_BASE + 1),
            )
            assert cur.fetchone()[0] is True

        class _StubShared:
            consensus_tid = 1

            def poll_advance(self, *a, **k):
                return None

            def set(self, *a, **k):
                return True

        # Pod 2: a fresh warmer with concurrency=1 must time out quickly
        # because slot 1 is held.
        w = CacheWarmer(
            conn=self.conn,
            target_count=10,
            shared_cache=_StubShared(),
            load_current_tid_fn=lambda: 100,
            dsn=DSN,
            concurrency=1,
            wait_max=3,
        )
        assert w._acquire_slot() is None

        # Now close pod 1's connection — auto-releases the lock.
        conn1.close()

        # Pod 3: a new warmer should now acquire slot 1.
        w2 = CacheWarmer(
            conn=self.conn,
            target_count=10,
            shared_cache=_StubShared(),
            load_current_tid_fn=lambda: 100,
            dsn=DSN,
            concurrency=1,
            wait_max=10,
        )
        result = w2._acquire_slot()
        assert result is not None
        lock_conn, slot = result
        assert slot == 1
        # Clean up.
        w2._release_slot(lock_conn, slot)


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
