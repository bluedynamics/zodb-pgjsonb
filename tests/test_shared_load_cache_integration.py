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
