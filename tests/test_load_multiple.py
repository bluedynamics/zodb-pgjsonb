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
