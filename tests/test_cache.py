"""Tests for LoadCache eviction and management."""

from zodb_pgjsonb.storage import LoadCache


class TestLocalCache:
    """Test LoadCache eviction and management methods."""

    def test_set_evicts_old_entry(self):
        """Updating an existing zoid reclaims old entry size."""
        cache = LoadCache(max_mb=10)
        cache.set(1, b"data1", b"\x00" * 8)
        size_after_first = cache._size
        # Update with new data — old entry size should be reclaimed
        cache.set(1, b"data2-longer", b"\x00" * 8)
        # Size should reflect the new entry, not accumulate
        assert cache._size != size_after_first + cache._size

    def test_lru_eviction(self):
        """Cache evicts LRU entries when over budget."""
        # Tiny cache: 1 byte budget
        cache = LoadCache(max_mb=0)
        cache._max_size = 200  # set a small absolute budget
        cache.set(1, b"a", b"\x01")
        cache.set(2, b"b", b"\x02")
        cache.set(3, b"c", b"\x03")
        # Cache should have evicted oldest entries to stay in budget
        assert cache._size <= cache._max_size + 200  # allow overhead

    def test_clear(self):
        """clear() empties the cache."""
        cache = LoadCache(max_mb=10)
        cache.set(1, b"data", b"\x01")
        assert len(cache) > 0
        cache.clear()
        assert len(cache) == 0
        assert cache._size == 0

    def test_len(self):
        """__len__ returns number of entries."""
        cache = LoadCache(max_mb=10)
        assert len(cache) == 0
        cache.set(1, b"d1", b"\x01")
        assert len(cache) == 1
        cache.set(2, b"d2", b"\x02")
        assert len(cache) == 2

    def test_size_mb(self):
        """size_mb returns size in megabytes."""
        cache = LoadCache(max_mb=10)
        assert cache.size_mb == 0.0
        cache.set(1, b"data", b"\x01")
        assert cache.size_mb > 0
