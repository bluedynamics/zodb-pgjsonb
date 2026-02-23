"""Direct unit tests for packer.pack() — covers edge paths not hit by ZODB-level tests.

Tests call packer.pack() directly with raw SQL setup to target specific
coverage gaps: HP-without-pack_tid paths and S3 key collection.

Requires PostgreSQL on localhost:5433.
"""

from tests.conftest import DSN
from zodb_pgjsonb.packer import pack
from zodb_pgjsonb.schema import install_schema

import psycopg
import pytest


@pytest.fixture
def hp_conn():
    """Fresh HP schema + raw psycopg connection."""
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
            "blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    install_schema(conn, history_preserving=True)

    yield conn
    conn.close()


@pytest.fixture
def hf_conn():
    """Fresh HF schema + raw psycopg connection."""
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
            "blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    install_schema(conn, history_preserving=False)

    yield conn
    conn.close()


def _seed_root_and_orphan(cur):
    """Insert a root object (zoid=0) and an unreachable orphan (zoid=99).

    Returns the tid used.
    """
    tid = 1
    cur.execute(
        "INSERT INTO transaction_log (tid, username, description) VALUES (%s, '', '')",
        (tid,),
    )
    # Root object — reachable
    cur.execute(
        "INSERT INTO object_state (zoid, tid, class_mod, class_name, state, state_size, refs) "
        "VALUES (0, %s, 'persistent.mapping', 'PersistentMapping', '{}', 2, '{}')",
        (tid,),
    )
    # Orphan — not reachable from root
    cur.execute(
        "INSERT INTO object_state (zoid, tid, class_mod, class_name, state, state_size, refs) "
        "VALUES (99, %s, 'some.module', 'Orphan', '{}', 2, '{}')",
        (tid,),
    )
    return tid


class TestPackerHPWithoutPackTid:
    """HP mode with pack_time=None — covers unreachable object/blob cleanup."""

    def test_hp_no_pack_tid_cleans_unreachable_history(self, hp_conn):
        """object_history + blob_state of unreachable objects are deleted."""
        with hp_conn.cursor() as cur:
            tid = _seed_root_and_orphan(cur)

            # History rows for the orphan
            cur.execute(
                "INSERT INTO object_history "
                "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                "VALUES (99, %s, 'some.module', 'Orphan', '{}', 2, '{}')",
                (tid,),
            )
            # Blob for the orphan in blob_state (with S3 key)
            cur.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, s3_key) "
                "VALUES (99, %s, 100, 'orphan/blob.dat')",
                (tid,),
            )
        hp_conn.commit()

        deleted_objects, deleted_blobs, s3_keys = pack(
            hp_conn, pack_time=None, history_preserving=True
        )

        assert deleted_objects == 1  # orphan removed from object_state
        assert deleted_blobs == 1  # orphan blob removed from blob_state
        assert s3_keys == ["orphan/blob.dat"]  # s3_key collected

        # Verify history tables are clean
        with hp_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM object_history WHERE zoid = 99")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT COUNT(*) FROM blob_state WHERE zoid = 99")
            assert cur.fetchone()[0] == 0


class TestPackerS3KeyCollection:
    """S3 key collection from blob_state and blob_history."""

    def test_hf_blob_s3_keys_collected(self, hf_conn):
        """blob_state S3 keys returned when unreachable blobs are packed (HF).

        Covers lines 86-87.
        """
        with hf_conn.cursor() as cur:
            tid = _seed_root_and_orphan(cur)

            # Blob for the orphan with an S3 key
            cur.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, s3_key) "
                "VALUES (99, %s, 5000, 'blobs/orphan-99.dat')",
                (tid,),
            )
        hf_conn.commit()

        _deleted_objects, deleted_blobs, s3_keys = pack(
            hf_conn, pack_time=None, history_preserving=False
        )

        assert deleted_blobs == 1
        assert s3_keys == ["blobs/orphan-99.dat"]

    def test_hp_old_blob_state_revisions_cleaned(self, hp_conn):
        """Old blob_state revisions for reachable objects are cleaned by pack.

        blob_state PK is (zoid, tid), so old versions accumulate. Pack
        removes superseded revisions and collects their S3 keys.
        """
        from ZODB.utils import p64

        tid1 = 10
        tid2 = 20
        with hp_conn.cursor() as cur:
            # Two transactions
            cur.execute(
                "INSERT INTO transaction_log (tid) VALUES (%s), (%s)",
                (tid1, tid2),
            )
            # Root object — reachable, with two revisions
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                "VALUES (0, %s, 'persistent.mapping', 'PersistentMapping', "
                "'{\"v\": 2}', 10, '{}')",
                (tid2,),
            )
            cur.execute(
                "INSERT INTO object_history "
                "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                "VALUES (0, %s, 'persistent.mapping', 'PersistentMapping', "
                "'{\"v\": 1}', 10, '{}')",
                (tid1,),
            )
            # Two blob_state revisions for root, both with S3 keys
            cur.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, s3_key) "
                "VALUES (0, %s, 100, 'blobs/root-v1.dat')",
                (tid1,),
            )
            cur.execute(
                "INSERT INTO blob_state (zoid, tid, blob_size, s3_key) "
                "VALUES (0, %s, 200, 'blobs/root-v2.dat')",
                (tid2,),
            )
        hp_conn.commit()

        # Pack at tid2 — old revision (tid1) should be cleaned
        pack_time = p64(tid2)
        _deleted_objects, _deleted_blobs, s3_keys = pack(
            hp_conn, pack_time=pack_time, history_preserving=True
        )

        # The old blob_state revision (tid1) should be deleted, its s3_key collected
        assert "blobs/root-v1.dat" in s3_keys
        # The current revision (tid2) should survive
        with hp_conn.cursor() as cur:
            cur.execute("SELECT s3_key FROM blob_state WHERE zoid = 0")
            remaining = [r[0] for r in cur.fetchall()]
        assert "blobs/root-v2.dat" in remaining
