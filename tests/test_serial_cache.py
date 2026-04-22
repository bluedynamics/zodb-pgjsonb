"""Tests for _serial_cache lifecycle (issue #62).

The _serial_cache supports tryToResolveConflict by keeping pickle bytes
for previously loaded (oid, tid) pairs. Without bounded lifetime this
dict grows monotonically for the life of the storage instance.

Two invariants under test:

1. History-preserving mode never populates _serial_cache because
   _do_loadSerial can retrieve old versions from object_history
   directly.

2. History-free mode populates _serial_cache during a transaction
   (conflict resolution needs it) but clears it on afterCompletion —
   the base versions required for tryToResolveConflict have already
   been consumed by the time the transaction completes.
"""

from persistent.mapping import PersistentMapping

import pytest
import transaction as txn


pytestmark = pytest.mark.db


def _load_many(db, n):
    """Open a connection, create n child mappings, commit."""
    conn = db.open()
    try:
        root = conn.root()
        for i in range(n):
            root[f"child_{i}"] = PersistentMapping({"i": i})
        txn.commit()
    finally:
        conn.close()


def _touch_load_path(db, n):
    """Open a connection, read n mappings through load(), abort."""
    conn = db.open()
    try:
        root = conn.root()
        for i in range(n):
            child = root[f"child_{i}"]
            # Access state to trigger load()
            _ = child["i"]
        txn.abort()
    finally:
        conn.close()


class TestHistoryPreservingSkipsSerialCache:
    """Issue #62 Option C: history-preserving mode never populates cache."""

    def test_serial_cache_empty_after_load(self, hp_db):
        """In history-preserving mode, load() must not add entries."""
        _load_many(hp_db, 20)

        conn = hp_db.open()
        try:
            instance = conn._storage
            root = conn.root()
            for i in range(20):
                _ = root[f"child_{i}"]["i"]
            # _serial_cache must not grow — history_preserving can serve
            # loadSerial from object_history directly.
            assert len(instance._serial_cache) == 0
        finally:
            conn.close()

    def test_serial_cache_empty_after_many_loads(self, hp_db):
        """Repeated load()/close() cycles keep the cache at size 0."""
        _load_many(hp_db, 10)

        for _ in range(5):
            _touch_load_path(hp_db, 10)

        conn = hp_db.open()
        try:
            instance = conn._storage
            assert len(instance._serial_cache) == 0
        finally:
            conn.close()


class TestHistoryFreeClearsOnAfterCompletion:
    """Issue #62 Option A: history-free mode clears cache on tx end."""

    def test_serial_cache_cleared_after_transaction(self, db):
        """After a completed transaction, _serial_cache is empty."""
        _load_many(db, 20)

        conn = db.open()
        try:
            instance = conn._storage
            root = conn.root()
            for i in range(20):
                _ = root[f"child_{i}"]["i"]
            # During the transaction, the cache is populated.
            assert len(instance._serial_cache) > 0

            txn.abort()
            # afterCompletion must have cleared the cache.
            assert len(instance._serial_cache) == 0
        finally:
            conn.close()

    def test_serial_cache_does_not_accumulate_across_transactions(self, db):
        """Across many tx cycles the cache size stays bounded (~0)."""
        _load_many(db, 10)

        for _ in range(5):
            _touch_load_path(db, 10)

        conn = db.open()
        try:
            instance = conn._storage
            # No work in this transaction → cache must be empty.
            assert len(instance._serial_cache) == 0
        finally:
            conn.close()
