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
