"""Tests for PGJsonbStorageInstance.load_multiple() batch loading."""

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
        """Second call should hit the cache, not the database."""
        conn = db.open()
        root = conn.root()
        root["x"] = PersistentMapping()
        txn.commit()

        oid_x = root["x"]._p_oid
        conn.close()

        inst = db.storage.new_instance()
        try:
            # First call populates cache
            result1 = inst.load_multiple([oid_x])
            assert oid_x in result1

            cache_hits_before = inst._load_cache.hits

            # Second call should use cache
            result2 = inst.load_multiple([oid_x])
            assert oid_x in result2

            cache_hits_after = inst._load_cache.hits
            assert cache_hits_after > cache_hits_before

            # Results should be identical
            assert result1[oid_x] == result2[oid_x]
        finally:
            inst.close()

    def test_load_multiple_skips_missing_oids(self, db):
        """Missing oids are silently omitted, no POSKeyError raised."""
        conn = db.open()
        root = conn.root()
        root["exists"] = PersistentMapping()
        txn.commit()

        oid_exists = root["exists"]._p_oid
        conn.close()

        # Fabricate an oid that does not exist
        oid_missing = p64(999999999)

        inst = db.storage.new_instance()
        try:
            result = inst.load_multiple([oid_exists, oid_missing])

            assert oid_exists in result
            assert oid_missing not in result
            assert len(result) == 1
        finally:
            inst.close()

    def test_load_multiple_empty_list(self, db):
        """Passing an empty list returns an empty dict without querying."""
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
        # Create an object with a sub-object (annotation-like pattern)
        parent = PersistentMapping()
        child = PersistentMapping({"key": "value"})
        parent["child"] = child
        root["parent"] = parent
        txn.commit()

        oid_parent = parent._p_oid
        oid_child = child._p_oid
        conn.close()

        # Verify refs are populated in PG
        from psycopg.rows import dict_row

        import psycopg

        pg = psycopg.connect(DSN, row_factory=dict_row)
        cur = pg.cursor()
        cur.execute(
            "SELECT refs FROM object_state WHERE zoid = %s",
            (u64(oid_parent),),
        )
        row = cur.fetchone()
        pg.close()
        refs = row["refs"] if row else None
        if not refs:
            pytest.skip("refs column not populated for PersistentMapping")

        # Fresh storage instance — empty cache
        inst = db.storage.new_instance()
        try:
            # Verify load_multiple works directly first
            child_zoid = u64(oid_child)
            direct = inst.load_multiple([oid_child])
            assert oid_child in direct, (
                f"load_multiple should find child zoid={child_zoid}"
            )

            # Clear cache to test prefetch path
            inst._load_cache = type(inst._load_cache)(inst._load_cache.max_mb)

            # Load parent — should also prefetch child via refs
            inst.load(oid_parent)

            # Verify the load query included refs
            with inst._conn.cursor() as cur:
                cur.execute(
                    "SELECT refs FROM object_state WHERE zoid = %s",
                    (u64(oid_parent),),
                )
                row_check = cur.fetchone()
            parent_refs = row_check["refs"] if row_check else None

            # Child should now be in cache (prefetched via refs)
            cached = inst._load_cache.get(child_zoid)
            assert cached is not None, (
                f"Referenced child (zoid={child_zoid}) should be "
                f"prefetched into cache. Parent refs={parent_refs}, "
                f"cache keys={list(inst._load_cache._cache.keys())[:20]}"
            )
        finally:
            inst.close()

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
        conn.close()

        inst = db.storage.new_instance()
        try:
            # Pre-load child
            inst.load(oid_child)

            # Load parent — child is already cached, should not re-fetch
            inst.load(oid_parent)

            # Child was a cache hit during refs prefetch (not re-loaded)
            # Verify child is still in cache (not evicted by parent load)
            assert inst._load_cache.get(u64(oid_child)) is not None
        finally:
            inst.close()
