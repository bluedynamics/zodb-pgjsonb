"""Tests for PG sequence-based OID allocation (#31)."""

from ZODB.utils import p64
from ZODB.utils import u64
from zodb_pgjsonb.storage import PGJsonbStorage

import os
import psycopg
import pytest


DSN = os.environ.get(
    "ZODB_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)

pytestmark = pytest.mark.skipif(not DSN, reason="No PostgreSQL test database available")


ALL_TABLES = (
    "pack_state",
    "blob_history",
    "object_history",
    "blob_state",
    "object_state",
    "transaction_log",
)


def _clean():
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        cur.execute("DROP TABLE IF EXISTS " + ", ".join(ALL_TABLES) + " CASCADE")
        cur.execute("DROP SEQUENCE IF EXISTS zoid_seq")
    conn.commit()
    conn.close()


def _make_storage(**kw):
    return PGJsonbStorage(DSN, pool_size=1, pool_max_size=4, **kw)


class TestOidSequence:
    def setup_method(self):
        _clean()

    def test_new_oid_returns_unique_oids(self):
        """Basic: new_oid returns incrementing unique OIDs."""
        storage = _make_storage()
        try:
            oids = [storage.new_oid() for _ in range(100)]
            assert len(set(oids)) == 100
        finally:
            storage.close()

    def test_new_oid_unique_across_instances(self):
        """Two storage instances must never return the same OID."""
        s1 = _make_storage()
        s2 = _make_storage()
        try:
            oids1 = {s1.new_oid() for _ in range(50)}
            oids2 = {s2.new_oid() for _ in range(50)}
            assert not oids1 & oids2, f"OID collision: {oids1 & oids2}"
            assert len(oids1 | oids2) == 100
        finally:
            s1.close()
            s2.close()

    def test_sequence_survives_restart(self):
        """After close + reopen, OIDs continue from where they left off."""
        s1 = _make_storage()
        try:
            last_oid = None
            for _ in range(10):
                last_oid = s1.new_oid()
            last_val = u64(last_oid)
        finally:
            s1.close()

        s2 = _make_storage()
        try:
            next_oid = s2.new_oid()
            assert u64(next_oid) > last_val
        finally:
            s2.close()

    def test_sequence_starts_after_existing_data(self):
        """If objects already exist, sequence starts above MAX(zoid)."""
        import transaction

        storage = _make_storage()
        try:
            txn = transaction.get()
            storage.tpc_begin(txn)
            storage.store(p64(50000), p64(0), b"data", "", txn)
            storage.tpc_vote(txn)
            storage.tpc_finish(txn)
        finally:
            storage.close()

        storage2 = _make_storage()
        try:
            oid = storage2.new_oid()
            assert u64(oid) > 50000
        finally:
            storage2.close()

    def test_zoid_seq_created_in_schema(self):
        """The zoid_seq sequence should exist after storage init."""
        storage = _make_storage()
        try:
            with storage._conn.cursor() as cur:
                cur.execute(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM pg_sequences"
                    "  WHERE sequencename = 'zoid_seq'"
                    ") AS exists"
                )
                assert cur.fetchone()["exists"]
        finally:
            storage.close()
