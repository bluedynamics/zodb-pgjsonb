"""Phase 5: IStorageIteration + IStorageRestoreable tests.

Tests that PGJsonbStorage correctly implements storage iteration
and restore for zodbconvert compatibility.

Requires PostgreSQL on localhost:5433.
"""

import pytest

import transaction as txn

import ZODB
from ZODB.interfaces import IStorageIteration
from ZODB.interfaces import IStorageRestoreable
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import zodb_json_codec

from persistent.mapping import PersistentMapping

from zodb_pgjsonb.storage import PGJsonbStorage


DSN = "dbname=zodb_test user=zodb password=zodb host=localhost port=5433"


def _clean_db():
    """Drop all tables for a fresh start."""
    import psycopg

    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
            "blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    _clean_db()
    s = PGJsonbStorage(DSN)
    yield s
    s.close()


@pytest.fixture
def hp_storage():
    """Fresh PGJsonbStorage in history-preserving mode."""
    _clean_db()
    s = PGJsonbStorage(DSN, history_preserving=True)
    yield s
    s.close()


@pytest.fixture
def db(storage):
    """ZODB.DB using our storage."""
    database = ZODB.DB(storage)
    yield database
    database.close()


@pytest.fixture
def hp_db(hp_storage):
    """ZODB.DB using history-preserving storage."""
    database = ZODB.DB(hp_storage)
    yield database
    database.close()


class TestInterfaces:
    """Verify interface declarations."""

    def test_provides_istorageiteration(self, storage):
        assert IStorageIteration.providedBy(storage)

    def test_provides_istoragerestoreable(self, storage):
        assert IStorageRestoreable.providedBy(storage)


class TestIterator:
    """Test iterator() yields correct TransactionRecord objects."""

    def test_iterator_yields_transactions(self, db, storage):
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        txn.commit()
        conn.close()

        txns = list(storage.iterator())
        assert len(txns) >= 1
        for t in txns:
            assert t.tid is not None
            assert len(t.tid) == 8

    def test_iterator_records_have_data(self, db, storage):
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        txn.commit()
        conn.close()

        txns = list(storage.iterator())
        last_txn = txns[-1]
        records = list(last_txn)
        assert len(records) >= 1
        for r in records:
            assert r.oid is not None
            assert r.tid is not None
            assert r.data is not None
            assert r.version == ''

    def test_iterator_data_is_valid_pickle(self, db, storage):
        """Iterator records contain valid pickle bytes (decodable)."""
        conn = db.open()
        root = conn.root()
        root["test"] = "data"
        txn.commit()
        conn.close()

        for t in storage.iterator():
            for r in t:
                record = zodb_json_codec.decode_zodb_record(r.data)
                assert "@cls" in record
                assert "@s" in record

    def test_iterator_with_start(self, db, storage):
        conn = db.open()
        root = conn.root()
        root["v1"] = 1
        txn.commit()
        tid1 = storage.lastTransaction()

        root["v2"] = 2
        txn.commit()
        tid2 = storage.lastTransaction()
        conn.close()

        txns = list(storage.iterator(start=tid2))
        assert len(txns) >= 1
        for t in txns:
            assert t.tid >= tid2

    def test_iterator_with_stop(self, db, storage):
        conn = db.open()
        root = conn.root()
        root["v1"] = 1
        txn.commit()
        tid1 = storage.lastTransaction()

        root["v2"] = 2
        txn.commit()
        conn.close()

        txns = list(storage.iterator(stop=tid1))
        for t in txns:
            assert t.tid <= tid1

    def test_iterator_with_start_and_stop(self, db, storage):
        conn = db.open()
        root = conn.root()
        root["v1"] = 1
        txn.commit()
        tid1 = storage.lastTransaction()

        root["v2"] = 2
        txn.commit()
        tid2 = storage.lastTransaction()

        root["v3"] = 3
        txn.commit()
        conn.close()

        txns = list(storage.iterator(start=tid1, stop=tid2))
        for t in txns:
            assert t.tid >= tid1
            assert t.tid <= tid2

    def test_iterator_transaction_metadata(self, db, storage):
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        txn.get().setUser("testuser")
        txn.get().note("test description")
        txn.commit()
        conn.close()

        txns = list(storage.iterator())
        last_txn = txns[-1]
        assert b"testuser" in last_txn.user
        assert last_txn.description == b"test description"

    def test_iterator_hp_multiple_revisions(self, hp_db, hp_storage):
        """HP mode iterator includes all revisions."""
        conn = hp_db.open()
        root = conn.root()
        root["v"] = 1
        txn.commit()
        root["v"] = 2
        txn.commit()
        root["v"] = 3
        txn.commit()
        conn.close()

        txns = list(hp_storage.iterator())
        # HP mode should have all transactions including root creation
        assert len(txns) >= 3


class TestRestore:
    """Test restore() writes pre-committed data without conflict checking."""

    def test_restore_basic(self, storage):
        """Restore a pre-committed object via 2PC."""
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {"key": "value"}},
        }
        data = zodb_json_codec.encode_zodb_record(record)
        oid = p64(1)
        tid = p64(1)

        from ZODB.BaseStorage import TransactionRecord

        txn_meta = TransactionRecord(tid, ' ', '', '', b'')
        storage.tpc_begin(txn_meta, tid)
        storage.restore(oid, tid, data, '', None, txn_meta)
        storage.tpc_vote(txn_meta)
        storage.tpc_finish(txn_meta)

        loaded_data, loaded_tid = storage.load(oid)
        assert loaded_tid == tid
        loaded_record = zodb_json_codec.decode_zodb_record(loaded_data)
        assert loaded_record["@s"]["data"]["key"] == "value"

    def test_restore_none_data_skipped(self, storage):
        """Restore with data=None (undo of creation) is a no-op."""
        oid = p64(99)
        tid = p64(1)

        from ZODB.BaseStorage import TransactionRecord

        txn_meta = TransactionRecord(tid, ' ', '', '', b'')
        storage.tpc_begin(txn_meta, tid)
        storage.restore(oid, tid, None, '', None, txn_meta)
        storage.tpc_vote(txn_meta)
        storage.tpc_finish(txn_meta)

        from ZODB.POSException import POSKeyError
        with pytest.raises(POSKeyError):
            storage.load(oid)

    def test_restore_no_conflict_check(self, storage):
        """Restore should not check for conflicts."""
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {"initial": True}},
        }
        data = zodb_json_codec.encode_zodb_record(record)
        oid = p64(1)
        tid1 = p64(1)

        from ZODB.BaseStorage import TransactionRecord

        # First restore
        txn1 = TransactionRecord(tid1, ' ', '', '', b'')
        storage.tpc_begin(txn1, tid1)
        storage.restore(oid, tid1, data, '', None, txn1)
        storage.tpc_vote(txn1)
        storage.tpc_finish(txn1)

        # Second restore with stale serial — should NOT raise ConflictError
        record2 = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {"updated": True}},
        }
        data2 = zodb_json_codec.encode_zodb_record(record2)
        tid2 = p64(2)
        txn2 = TransactionRecord(tid2, ' ', '', '', b'')
        storage.tpc_begin(txn2, tid2)
        storage.restore(oid, tid2, data2, '', None, txn2)
        storage.tpc_vote(txn2)
        storage.tpc_finish(txn2)

        loaded_data, loaded_tid = storage.load(oid)
        assert loaded_tid == tid2
        loaded_record = zodb_json_codec.decode_zodb_record(loaded_data)
        assert loaded_record["@s"]["data"]["updated"] is True

    def test_instance_restore(self, storage):
        """Restore works on storage instances too."""
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {"inst": "data"}},
        }
        data = zodb_json_codec.encode_zodb_record(record)
        oid = p64(1)
        tid = p64(1)

        inst = storage.new_instance()
        try:
            from ZODB.BaseStorage import TransactionRecord

            txn_meta = TransactionRecord(tid, ' ', '', '', b'')
            inst.tpc_begin(txn_meta, tid)
            inst.restore(oid, tid, data, '', None, txn_meta)
            inst.tpc_vote(txn_meta)
            inst.tpc_finish(txn_meta)

            loaded_data, loaded_tid = inst.load(oid)
            assert loaded_tid == tid
        finally:
            inst.release()


class TestCopyViaIteratorAndRestore:
    """End-to-end test: iterate + restore roundtrip."""

    def test_roundtrip(self):
        """Store data, iterate, restore into fresh DB — verify data."""
        # Phase 1: create source data
        _clean_db()
        src = PGJsonbStorage(DSN)
        src_db = ZODB.DB(src)
        c = src_db.open()
        root = c.root()
        root["title"] = "Hello"
        root["count"] = 42
        root["child"] = PersistentMapping({"nested": "value"})
        txn.commit()
        c.close()
        src_db.close()

        # Phase 2: iterate and collect all records
        src = PGJsonbStorage(DSN)
        collected = []
        for t in src.iterator():
            recs = [(r.oid, r.tid, r.data, r.data_txn) for r in t]
            collected.append({
                "tid": t.tid,
                "status": t.status,
                "user": t.user,
                "description": t.description,
                "extension_bytes": t.extension_bytes,
                "records": recs,
            })
        src.close()

        assert len(collected) >= 1

        # Phase 3: recreate DB and restore
        _clean_db()
        dst = PGJsonbStorage(DSN)
        from ZODB.BaseStorage import TransactionRecord

        for t in collected:
            txn_meta = TransactionRecord(
                t["tid"], t["status"], t["user"],
                t["description"], t["extension_bytes"],
            )
            dst.tpc_begin(txn_meta, t["tid"], t["status"])
            for oid, tid, data, data_txn in t["records"]:
                dst.restore(oid, tid, data, '', data_txn, txn_meta)
            dst.tpc_vote(txn_meta)
            dst.tpc_finish(txn_meta)

        # Phase 4: verify via ZODB
        dst_db = ZODB.DB(dst)
        c = dst_db.open()
        root = c.root()
        assert root["title"] == "Hello"
        assert root["count"] == 42
        assert root["child"]["nested"] == "value"
        c.close()
        dst_db.close()
