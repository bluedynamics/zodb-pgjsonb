"""Tests for the ZODB prefetch hook — batch object loading.

``Connection.prefetch`` calls ``storage.prefetch(oids)``; without the hook ZODB
installs a no-op fallback, so a result set is loaded one object at a time (N+1).
The hook delegates to ``load_multiple`` to collapse that into one query.
"""

from persistent.mapping import PersistentMapping
from ZODB.utils import u64

import pytest
import transaction


pytestmark = pytest.mark.db


def _make_objects(db, n):
    conn = db.open()
    try:
        root = conn.root()
        for i in range(n):
            root[f"o{i}"] = PersistentMapping({"i": i})
        transaction.commit()
        return [root[f"o{i}"]._p_oid for i in range(n)]
    finally:
        conn.close()


def test_prefetch_fetches_cold_objects_from_pg_in_one_batch(db, storage):
    oids = _make_objects(db, 25)
    inst = storage.new_instance()
    try:
        inst.poll_invalidations()
        assert inst._pg_load_count == 0
        inst.prefetch(oids)
        # All 25 fetched from PostgreSQL (via one batched query).
        assert inst._pg_load_count == 25
    finally:
        inst.release()


def test_prefetch_warms_cache_so_subsequent_loads_avoid_pg(db, storage):
    oids = _make_objects(db, 25)
    inst = storage.new_instance()
    try:
        inst.poll_invalidations()
        inst.prefetch(oids)
        for oid in oids:
            assert inst._load_cache.get(u64(oid)) is not None

        # Individual loads now hit the cache — no further PostgreSQL loads.
        pg_after_prefetch = inst._pg_load_count
        for oid in oids:
            inst.load(oid)
        assert inst._pg_load_count == pg_after_prefetch
    finally:
        inst.release()


def test_connection_prefetch_is_not_a_noop(db):
    """ZODB's Connection.prefetch reaches the hook and warms the cache."""
    oids = _make_objects(db, 10)
    conn = db.open()
    try:
        inst = conn._storage
        inst._load_cache.clear()
        conn.prefetch(oids)  # ZODB flattens + routes to inst.prefetch
        for oid in oids:
            assert inst._load_cache.get(u64(oid)) is not None
    finally:
        conn.close()
