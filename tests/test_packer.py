"""Direct unit tests for packer.pack() — covers edge paths not hit by ZODB-level tests.

Tests call packer.pack() directly with raw SQL setup to target specific
coverage gaps: HP-without-pack_tid paths and S3 key collection.

Requires PostgreSQL on localhost:5433.
"""

from psycopg.rows import dict_row
from psycopg.rows import tuple_row
from tests.conftest import DSN
from zodb_pgjsonb.packer import pack
from zodb_pgjsonb.schema import install_schema

import psycopg
import pytest


pytestmark = pytest.mark.db

# pack() receives the storage's admin connection, which is opened with
# row_factory=dict_row (storage.py).  Run every packer test against both
# factories so the code cannot silently depend on one of them (#108).
_ROW_FACTORIES = [tuple_row, dict_row]
_ROW_FACTORY_IDS = ["tuple_row", "dict_row"]


@pytest.fixture(params=_ROW_FACTORIES, ids=_ROW_FACTORY_IDS)
def hp_conn(request):
    """Fresh HP schema + raw psycopg connection."""
    conn = psycopg.connect(DSN, row_factory=request.param)
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


@pytest.fixture(params=_ROW_FACTORIES, ids=_ROW_FACTORY_IDS)
def hf_conn(request):
    """Fresh HF schema + raw psycopg connection."""
    conn = psycopg.connect(DSN, row_factory=request.param)
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

        # Verify history tables are clean (explicit tuple_row: the fixture
        # connection's row factory is parametrized)
        with hp_conn.cursor(row_factory=tuple_row) as cur:
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
        with hp_conn.cursor(row_factory=tuple_row) as cur:
            cur.execute("SELECT s3_key FROM blob_state WHERE zoid = 0")
            remaining = [r[0] for r in cur.fetchall()]
        assert "blobs/root-v2.dat" in remaining


_LEGACY_BLOB_HISTORY_DDL = (
    "CREATE TABLE blob_history ("
    "  zoid BIGINT NOT NULL,"
    "  tid BIGINT NOT NULL,"
    "  blob_size BIGINT NOT NULL,"
    "  data BYTEA,"
    "  s3_key TEXT,"
    "  PRIMARY KEY (zoid, tid)"
    ")"
)


class TestPackerLegacyBlobHistory:
    """S3 key collection from the deprecated blob_history table.

    New schemas no longer create blob_history, but pack() still cleans it
    up on old databases.  Both collection sites must work regardless of
    the connection's row factory (#108).
    """

    def test_hp_unreachable_blob_history_s3_keys_collected(self, hp_conn):
        """blob_history rows of unreachable objects: keys are collected."""
        with hp_conn.cursor() as cur:
            tid = _seed_root_and_orphan(cur)
            cur.execute(_LEGACY_BLOB_HISTORY_DDL)
            cur.execute(
                "INSERT INTO blob_history (zoid, tid, blob_size, s3_key) "
                "VALUES (99, %s, 100, 'legacy/orphan-99.dat')",
                (tid,),
            )
        hp_conn.commit()

        _deleted_objects, _deleted_blobs, s3_keys = pack(
            hp_conn, pack_time=None, history_preserving=True
        )

        assert "legacy/orphan-99.dat" in s3_keys

    def test_hp_old_blob_history_revisions_s3_keys_collected(self, hp_conn):
        """Superseded blob_history revisions of reachable objects: keys collected."""
        from ZODB.utils import p64

        tid1 = 10
        tid2 = 20
        with hp_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transaction_log (tid) VALUES (%s), (%s)",
                (tid1, tid2),
            )
            # Root object — reachable
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                "VALUES (0, %s, 'persistent.mapping', 'PersistentMapping', "
                "'{}', 2, '{}')",
                (tid2,),
            )
            cur.execute(_LEGACY_BLOB_HISTORY_DDL)
            # Two legacy revisions for root; the older one is superseded
            cur.execute(
                "INSERT INTO blob_history (zoid, tid, blob_size, s3_key) "
                "VALUES (0, %s, 100, 'legacy/root-v1.dat'), "
                "(0, %s, 200, 'legacy/root-v2.dat')",
                (tid1, tid2),
            )
        hp_conn.commit()

        _deleted_objects, _deleted_blobs, s3_keys = pack(
            hp_conn, pack_time=p64(tid2), history_preserving=True
        )

        assert "legacy/root-v1.dat" in s3_keys
        # The newest revision at pack time survives
        with hp_conn.cursor(row_factory=tuple_row) as cur:
            cur.execute("SELECT s3_key FROM blob_history WHERE zoid = 0")
            remaining = [r[0] for r in cur.fetchall()]
        assert "legacy/root-v2.dat" in remaining
