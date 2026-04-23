"""Unit tests for SharedLoadCache (#63).

Pure-Python tests — no PostgreSQL required. Focus on correctness
invariants: consensus-TID gating, LRU eviction, byte accounting.
"""

from ZODB.utils import p64
from zodb_pgjsonb.storage import SharedLoadCache


class TestConstruction:
    def test_empty_cache(self):
        cache = SharedLoadCache(max_mb=4)
        assert cache._consensus_tid is None
        assert len(cache._cache) == 0
        assert cache._current_bytes == 0

    def test_max_bytes_from_mb(self):
        cache = SharedLoadCache(max_mb=8)
        assert cache._max_bytes == 8 * 1_000_000


class TestGetSetBasic:
    def test_set_rejected_when_consensus_uninitialized(self):
        cache = SharedLoadCache(max_mb=4)
        cache.set(zoid=1, data=b"x", tid_bytes=p64(100), polled_tid=100)
        assert len(cache._cache) == 0

    def test_get_returns_none_when_consensus_uninitialized(self):
        cache = SharedLoadCache(max_mb=4)
        assert cache.get(zoid=1, polled_tid=100) is None

    def test_set_and_get_after_consensus_initialized(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"xyz", tid_bytes=p64(100), polled_tid=100)
        assert cache.get(zoid=1, polled_tid=100) == (b"xyz", p64(100))


class TestConsensusTIDGating:
    def test_stale_reader_bypasses_cache(self):
        """A reader with polled_tid < consensus_tid gets None."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"at100", tid_bytes=p64(100), polled_tid=100)

        # Consensus moves forward via another instance's poll
        cache.poll_advance(new_tid=200, changed_zoids=[2])

        # Reader with old snapshot is gated out
        assert cache.get(zoid=1, polled_tid=150) is None

    def test_current_reader_hits(self):
        """A reader with polled_tid >= consensus_tid hits normally."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"at100", tid_bytes=p64(100), polled_tid=100)

        cache.poll_advance(new_tid=200, changed_zoids=[])  # no invalidations

        assert cache.get(zoid=1, polled_tid=200) == (b"at100", p64(100))

    def test_stale_writer_rejected(self):
        """A writer with polled_tid < consensus_tid cannot set."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.poll_advance(new_tid=200, changed_zoids=[])

        # Stale writer tries to set
        cache.set(zoid=1, data=b"stale", tid_bytes=p64(100), polled_tid=100)
        assert cache.get(zoid=1, polled_tid=200) is None

    def test_older_tid_never_replaces_newer(self):
        """set() with a tid_bytes older than the existing entry is ignored."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.set(zoid=1, data=b"newer", tid_bytes=p64(200), polled_tid=200)

        # Another writer with an older tid_bytes (but same polled_tid)
        cache.set(zoid=1, data=b"older", tid_bytes=p64(150), polled_tid=200)
        assert cache.get(zoid=1, polled_tid=200) == (b"newer", p64(200))

    def test_poll_advance_invalidates_changed_zoids(self):
        """poll_advance removes entries for zoids in changed list."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"a", tid_bytes=p64(100), polled_tid=100)
        cache.set(zoid=2, data=b"b", tid_bytes=p64(100), polled_tid=100)

        cache.poll_advance(new_tid=150, changed_zoids=[1])

        assert cache.get(zoid=1, polled_tid=150) is None
        assert cache.get(zoid=2, polled_tid=150) == (b"b", p64(100))

    def test_poll_advance_with_older_tid_is_noop(self):
        """poll_advance with new_tid < current consensus does nothing."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.set(zoid=1, data=b"x", tid_bytes=p64(200), polled_tid=200)

        # An instance pollling in from behind shouldn't do anything
        cache.poll_advance(new_tid=100, changed_zoids=[1])

        assert cache._consensus_tid == 200
        assert cache.get(zoid=1, polled_tid=200) == (b"x", p64(200))
