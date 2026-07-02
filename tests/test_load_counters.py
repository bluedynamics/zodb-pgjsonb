"""Per-connection load counters for observability (dependency-neutral).

Each instance exposes plain-int counters of how its object loads were served
(shared L2 cache vs PostgreSQL), so a soft-coupled observer (plone.observability)
can attribute per-span cache effectiveness without zodb-pgjsonb depending on it.
"""

from persistent.mapping import PersistentMapping

import pytest
import transaction


pytestmark = pytest.mark.db


def _store_object(db):
    """Commit one PersistentMapping and return its oid."""
    conn = db.open()
    try:
        obj = PersistentMapping({"x": 1})
        conn.root()["obj"] = obj
        transaction.commit()
        return obj._p_oid
    finally:
        conn.close()


def test_load_counts_pg_then_l2(db, storage):
    oid = _store_object(db)

    # First fresh instance: cold L1 + cold L2 → served from PostgreSQL.
    inst1 = storage.new_instance()
    try:
        inst1.poll_invalidations()
        inst1.load(oid)
        assert inst1._pg_load_count == 1
        assert inst1._l2_load_hits == 0
    finally:
        inst1.release()

    # Second fresh instance: cold L1, but L2 was populated by inst1's PG load
    # → served from the shared cache, no PG query.
    inst2 = storage.new_instance()
    try:
        inst2.poll_invalidations()
        inst2.load(oid)
        assert inst2._l2_load_hits == 1
        assert inst2._pg_load_count == 0
    finally:
        inst2.release()


def test_load_multiple_counts_pg_then_l2(db, storage):
    oid = _store_object(db)

    inst1 = storage.new_instance()
    try:
        inst1.poll_invalidations()
        inst1.load_multiple([oid])
        assert inst1._pg_load_count == 1
        assert inst1._l2_load_hits == 0
    finally:
        inst1.release()

    inst2 = storage.new_instance()
    try:
        inst2.poll_invalidations()
        inst2.load_multiple([oid])
        assert inst2._l2_load_hits == 1
        assert inst2._pg_load_count == 0
    finally:
        inst2.release()
