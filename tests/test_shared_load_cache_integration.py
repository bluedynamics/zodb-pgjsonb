"""Integration tests: shared cache and instance.load() (#63).

Requires PostgreSQL. Verifies the L1 → shared → PG cascade and the
TID-gated population of the shared cache.
"""

from persistent.mapping import PersistentMapping

import pytest
import transaction as txn


pytestmark = pytest.mark.db


def _create_tree(db, n):
    """Create n children under root, commit."""
    conn = db.open()
    try:
        root = conn.root()
        for i in range(n):
            root[f"c{i}"] = PersistentMapping({"i": i})
        txn.commit()
    finally:
        conn.close()


class TestSharedCachePopulatedByLoad:
    def test_load_populates_shared_cache(self, db):
        """After a load(), the shared cache holds the same entry."""
        _create_tree(db, 5)

        # Ghost every object in the Connection cache and clear L1
        # so reads actually hit storage.load() and populate shared.
        conn = db.open()
        try:
            instance = conn._storage
            root = conn.root()
            zoid = root["c0"]._p_oid
            conn.cacheMinimize()
            instance._load_cache.clear()
            _ = root["c0"]["i"]  # triggers load() through PG
            from ZODB.utils import u64

            shared = instance._main._shared_cache
            entry = shared.get(u64(zoid), instance._polled_tid)
            assert entry is not None
        finally:
            conn.close()

    def test_shared_cache_hit_promoted_to_l1(self, db):
        """A hit on the shared cache populates the L1 for next time."""
        _create_tree(db, 5)

        conn1 = db.open()
        try:
            root1 = conn1.root()
            _ = root1["c0"]["i"]  # populates both L1 and shared
        finally:
            conn1.close()

        conn2 = db.open()
        try:
            instance = conn2._storage
            root2 = conn2.root()
            zoid = root2["c0"]._p_oid
            # Force L1 miss AND ghost the Python object so activation
            # must go through storage.load()
            conn2.cacheMinimize()
            instance._load_cache.clear()
            _ = root2["c0"]["i"]  # should hit shared, not PG
            from ZODB.utils import u64

            # Now L1 should hold the entry (promoted from shared)
            assert instance._load_cache.get(u64(zoid)) is not None
        finally:
            conn2.close()


class TestLoadMultipleUsesSharedCache:
    def test_load_multiple_populates_shared(self, db):
        _create_tree(db, 10)

        conn = db.open()
        try:
            instance = conn._storage
            instance._load_cache.clear()
            conn.cacheMinimize()
            # Trigger load_multiple by loading several OIDs at once
            root = conn.root()
            oids = [root[f"c{i}"]._p_oid for i in range(5)]
            result = instance.load_multiple(oids)
            assert len(result) == 5

            from ZODB.utils import u64

            shared = instance._main._shared_cache
            for oid in oids:
                assert shared.get(u64(oid), instance._polled_tid) is not None
        finally:
            conn.close()

    def test_load_multiple_hits_shared_before_pg(self, db):
        _create_tree(db, 10)

        # Warm shared cache via another connection
        conn1 = db.open()
        try:
            instance1 = conn1._storage
            root = conn1.root()
            oids = [root[f"c{i}"]._p_oid for i in range(5)]
            instance1.load_multiple(oids)
        finally:
            conn1.close()

        # Second connection: clear L1, load_multiple should hit shared
        conn2 = db.open()
        try:
            instance2 = conn2._storage
            instance2._load_cache.clear()
            conn2.cacheMinimize()
            # Get oids again from root in this connection
            root = conn2.root()
            oids = [root[f"c{i}"]._p_oid for i in range(5)]
            result = instance2.load_multiple(oids)
            assert len(result) == 5
            # All 5 should now be in L1 (promoted from shared)
            from ZODB.utils import u64

            for oid in oids:
                assert instance2._load_cache.get(u64(oid)) is not None
        finally:
            conn2.close()


class TestPollInvalidatesShared:
    def test_poll_invalidations_evicts_from_shared(self, db):
        """A commit from another connection invalidates shared entries."""
        _create_tree(db, 5)

        # Reader populates shared
        conn_reader = db.open()
        try:
            root = conn_reader.root()
            _ = root["c0"]["i"]
            zoid = root["c0"]._p_oid
        finally:
            conn_reader.close()

        # Writer modifies c0
        conn_writer = db.open()
        try:
            root = conn_writer.root()
            root["c0"]["i"] = 999
            txn.commit()
        finally:
            conn_writer.close()

        # New reader — poll_invalidations must evict c0 from shared
        conn2 = db.open()
        try:
            instance2 = conn2._storage
            from ZODB.utils import u64

            shared = instance2._main._shared_cache
            # poll_invalidations fired on conn2.open() — c0 should be gone
            assert shared.get(u64(zoid), instance2._polled_tid) is None
        finally:
            conn2.close()

    def test_poll_invalidations_advances_consensus(self, db):
        """poll_invalidations updates _consensus_tid on the shared cache."""
        _create_tree(db, 2)

        conn = db.open()
        try:
            instance = conn._storage
            shared = instance._main._shared_cache
            # After opening (which polls), consensus must be set
            assert shared._consensus_tid is not None
            assert shared._consensus_tid == instance._polled_tid
        finally:
            conn.close()
