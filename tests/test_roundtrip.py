"""Phase 0: Proof-of-concept roundtrip test.

Proves: ZODB → pickle → zodb-json-codec → JSONB → PostgreSQL → JSONB → zodb-json-codec → pickle → ZODB

Requires a running PostgreSQL on localhost:5433 (see docker command in ARCHITECTURE.md).
"""

import pytest

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

import zodb_json_codec

from zodb_pgjsonb.schema import install_schema


DSN = "dbname=zodb_test user=zodb password=zodb host=localhost port=5433"


@pytest.fixture
def conn():
    """Fresh database connection with clean schema."""
    c = psycopg.connect(DSN, row_factory=dict_row)
    # Drop and recreate tables for a clean slate
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE")
    c.commit()
    install_schema(c)
    yield c
    c.close()


class TestCodecRoundtrip:
    """Test that pickle → JSON → pickle roundtrips correctly."""

    def test_simple_object(self):
        """A simple dict state roundtrips through the codec."""
        import pickle

        class_pickle = pickle.dumps(("myapp.models", "Document"), protocol=3)
        state_pickle = pickle.dumps({"title": "Hello", "count": 42}, protocol=3)
        original = class_pickle + state_pickle

        decoded = zodb_json_codec.decode_zodb_record(original)
        assert decoded["@cls"] == ["myapp.models", "Document"]
        assert decoded["@s"]["title"] == "Hello"
        assert decoded["@s"]["count"] == 42

        re_encoded = zodb_json_codec.encode_zodb_record(decoded)
        re_decoded = zodb_json_codec.decode_zodb_record(re_encoded)
        assert decoded == re_decoded

    def test_object_with_various_types(self):
        """Various Python types survive roundtrip."""
        import pickle

        class_pickle = pickle.dumps(("myapp.models", "Folder"), protocol=3)
        state = {"name": "root", "count": 42, "active": True, "data": None}
        state_pickle = pickle.dumps(state, protocol=3)
        original = class_pickle + state_pickle

        decoded = zodb_json_codec.decode_zodb_record(original)
        re_encoded = zodb_json_codec.encode_zodb_record(decoded)
        re_decoded = zodb_json_codec.decode_zodb_record(re_encoded)
        assert decoded == re_decoded


class TestPostgresRoundtrip:
    """Test store → PostgreSQL JSONB → load roundtrip."""

    def test_store_and_load(self, conn):
        """Store an object as JSONB, load it back, verify roundtrip."""
        import pickle

        # Build a ZODB record
        class_pickle = pickle.dumps(("myapp.models", "Document"), protocol=3)
        state_pickle = pickle.dumps(
            {"title": "Test Document", "version": 1, "tags": ["a", "b"]},
            protocol=3,
        )
        original_record = class_pickle + state_pickle

        # Decode to JSON-ready dict with ref extraction
        class_mod, class_name, state, refs = (
            zodb_json_codec.decode_zodb_record_for_pg(original_record)
        )

        # Store in PostgreSQL
        zoid = 1
        tid = 1
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transaction_log (tid) VALUES (%s)", (tid,)
            )
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (zoid, tid, class_mod, class_name, Json(state),
                 len(original_record), refs),
            )
        conn.commit()

        # Load from PostgreSQL
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()

        assert row is not None
        assert row["class_mod"] == "myapp.models"
        assert row["class_name"] == "Document"
        assert row["state"]["title"] == "Test Document"
        assert row["state"]["tags"] == ["a", "b"]

        # Reconstruct pickle from JSONB
        loaded_record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": row["state"],
        }
        loaded_pickle = zodb_json_codec.encode_zodb_record(loaded_record)

        # Verify the roundtrip at the logical level
        original_decoded = zodb_json_codec.decode_zodb_record(original_record)
        loaded_decoded = zodb_json_codec.decode_zodb_record(loaded_pickle)
        assert original_decoded == loaded_decoded

    def test_jsonb_queryability(self, conn):
        """Verify we can query JSONB attributes directly in SQL."""
        # Store two objects of different classes
        with conn.cursor() as cur:
            cur.execute("INSERT INTO transaction_log (tid) VALUES (1)")

            for zoid, cls, state in [
                (1, ("myapp", "Document"), {"title": "Report", "year": 2025}),
                (2, ("myapp", "Document"), {"title": "Invoice", "year": 2024}),
                (3, ("myapp", "Folder"), {"name": "root"}),
            ]:
                cur.execute(
                    "INSERT INTO object_state "
                    "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                    "VALUES (%s, 1, %s, %s, %s, 0, '{}')",
                    (zoid, cls[0], cls[1], Json(state)),
                )
        conn.commit()

        # Query by class
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE class_mod = 'myapp' AND class_name = 'Document' "
                "ORDER BY zoid"
            )
            rows = cur.fetchall()
        assert [r["zoid"] for r in rows] == [1, 2]

        # Query by JSONB attribute (cast to jsonb for @> operator)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state "
                "WHERE state @> %s::jsonb",
                (Json({"title": "Report"}),),
            )
            rows = cur.fetchall()
        assert [r["zoid"] for r in rows] == [1]

        # Query with JSONB path operator
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, state->>'title' as title "
                "FROM object_state "
                "WHERE class_name = 'Document' AND (state->>'year')::int > 2024"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["title"] == "Report"

    def test_refs_column_for_pack(self, conn):
        """Verify refs column is populated and queryable."""
        state_with_refs = {
            "title": "Has refs",
            "author": {"@ref": "0000000000000003"},
            "folder": {"@ref": "000000000000000a"},
        }
        refs = [3, 10]  # OIDs from the @ref markers above

        with conn.cursor() as cur:
            cur.execute("INSERT INTO transaction_log (tid) VALUES (1)")
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs) "
                "VALUES (%s, 1, 'myapp', 'Doc', %s, 0, %s)",
                (1, Json(state_with_refs), refs),
            )
        conn.commit()

        # Verify refs are stored and queryable
        with conn.cursor() as cur:
            cur.execute("SELECT refs FROM object_state WHERE zoid = 1")
            row = cur.fetchone()
        assert sorted(row["refs"]) == [3, 10]

        # GIN index query: find objects referencing zoid=3
        with conn.cursor() as cur:
            cur.execute(
                "SELECT zoid FROM object_state WHERE refs @> ARRAY[%s]::bigint[]",
                (3,),
            )
            rows = cur.fetchall()
        assert [r["zoid"] for r in rows] == [1]

    def test_notify_trigger(self, conn):
        """Verify the LISTEN/NOTIFY trigger fires on transaction_log insert."""
        # Open a separate connection for LISTEN
        listen_conn = psycopg.connect(DSN, autocommit=True)
        listen_conn.execute("LISTEN zodb_invalidations")

        # Insert a transaction (triggers notify)
        with conn.cursor() as cur:
            cur.execute("INSERT INTO transaction_log (tid) VALUES (42)")
        conn.commit()

        # Check for notification
        notifications = list(listen_conn.notifies(timeout=2))
        listen_conn.close()

        assert len(notifications) == 1
        assert notifications[0].payload == "42"
