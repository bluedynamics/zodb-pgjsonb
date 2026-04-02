"""Tests for PGJsonbStorageInstance.load_multiple() and refs prefetch."""

from persistent.mapping import PersistentMapping
from tests.conftest import DSN
from ZODB.utils import p64
from ZODB.utils import u64

import pytest
import transaction as txn


def _pg_available():
    """Check if test PostgreSQL is reachable."""
    try:
        import psycopg

        conn = psycopg.connect(DSN, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _pg_available(),
    reason="PostgreSQL not available",
)


class TestLoadMultiple:
    """Tests for the load_multiple() batch loading method."""

    def test_load_multiple_returns_dict(self, db):
        """load_multiple returns a dict mapping oid -> (data, tid)."""
        conn = db.open()
        root = conn.root()
        root["a"] = PersistentMapping()
        root["b"] = PersistentMapping()
        txn.commit()

        oid_a = root["a"]._p_oid
        oid_b = root["b"]._p_oid
        conn.close()

        # Get a fresh storage instance to avoid connection-level caching
        inst = db.storage.new_instance()
        try:
            result = inst.load_multiple([oid_a, oid_b])

            assert isinstance(result, dict)
            assert oid_a in result
            assert oid_b in result

            # Each value is a (data_bytes, tid_bytes) tuple
            data_a, tid_a = result[oid_a]
            assert isinstance(data_a, bytes)
            assert isinstance(tid_a, bytes)
            assert len(tid_a) == 8

            data_b, tid_b = result[oid_b]
            assert isinstance(data_b, bytes)
            assert isinstance(tid_b, bytes)
            assert len(tid_b) == 8
        finally:
            inst.close()

    def test_load_multiple_caches_results(self, db):
        """Second load_multiple for same oids hits cache."""
        conn = db.open()
        root = conn.root()
        root["cached_test"] = PersistentMapping()
        txn.commit()

        oid = root["cached_test"]._p_oid
        conn.close()

        inst = db.storage.new_instance()
        try:
            # First load — should query PG
            inst.load_multiple([oid])
            hits_before = inst._load_cache.hits

            # Second load — should hit cache
            inst.load_multiple([oid])
            assert inst._load_cache.hits > hits_before
        finally:
            inst.close()

    def test_load_multiple_skips_missing_oids(self, db):
        """Missing oids are silently omitted from the result."""
        inst = db.storage.new_instance()
        try:
            fake_oid = p64(999999999)
            result = inst.load_multiple([fake_oid])
            assert fake_oid not in result
            assert result == {}
        finally:
            inst.close()

    def test_load_multiple_empty_list(self, db):
        """Empty oid list returns empty dict."""
        inst = db.storage.new_instance()
        try:
            result = inst.load_multiple([])
            assert result == {}
        finally:
            inst.close()

    def test_load_multiple_mixed_cached_and_fresh(self, db):
        """When some oids are cached and others are not, both are returned."""
        conn = db.open()
        root = conn.root()
        root["cached"] = PersistentMapping()
        root["fresh"] = PersistentMapping()
        txn.commit()

        oid_cached = root["cached"]._p_oid
        oid_fresh = root["fresh"]._p_oid
        conn.close()

        inst = db.storage.new_instance()
        try:
            # Pre-load one oid into cache via single load()
            data_cached, tid_cached = inst.load(oid_cached)
            cache_hits_before = inst._load_cache.hits

            # Now batch-load both: one cached, one fresh
            result = inst.load_multiple([oid_cached, oid_fresh])

            assert oid_cached in result
            assert oid_fresh in result

            # The cached oid should have been a cache hit
            assert inst._load_cache.hits > cache_hits_before

            # Data for the pre-cached oid should match the single load
            assert result[oid_cached] == (data_cached, tid_cached)

            # The fresh oid should also be in the cache now
            fresh_cached = inst._load_cache.get(u64(oid_fresh))
            assert fresh_cached is not None
        finally:
            inst.close()


class TestPrefetchRefs:
    """Tests for automatic refs prefetching on load()."""

    def test_load_prefetches_refs(self, db):
        """load() prefetches directly referenced objects into cache."""
        conn = db.open()
        root = conn.root()
        parent = PersistentMapping()
        child = PersistentMapping({"key": "value"})
        parent["child"] = child
        root["parent"] = parent
        txn.commit()

        oid_parent = parent._p_oid
        oid_child = child._p_oid

        # Use the ZODB connection's own storage instance
        # (guaranteed to see committed data in its snapshot)
        storage = conn._storage

        # Clear load cache to force a fresh load with refs prefetch
        storage._load_cache._data.clear()
        storage._load_cache._size = 0

        # Load parent — should also prefetch child via refs
        storage.load(oid_parent)

        # Child should now be in cache (prefetched via refs)
        child_zoid = u64(oid_child)
        cached = storage._load_cache.get(child_zoid)
        assert cached is not None, (
            f"Referenced child (zoid={child_zoid}) should be prefetched into cache"
        )
        conn.close()

    def test_load_prefetch_skips_cached_refs(self, db):
        """load() does not re-fetch refs that are already cached."""
        conn = db.open()
        root = conn.root()
        parent = PersistentMapping()
        child = PersistentMapping()
        parent["child"] = child
        root["parent2"] = parent
        txn.commit()

        oid_parent = parent._p_oid
        oid_child = child._p_oid

        storage = conn._storage
        storage._load_cache._data.clear()
        storage._load_cache._size = 0

        # Pre-load child
        storage.load(oid_child)

        # Load parent — child is already cached
        storage.load(oid_parent)

        # Verify child is still in cache
        assert storage._load_cache.get(u64(oid_child)) is not None
        conn.close()
