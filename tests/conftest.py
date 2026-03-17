"""Shared test configuration and fixtures for zodb-pgjsonb tests."""

from zodb_pgjsonb.storage import PGJsonbStorage

import os
import psycopg
import pytest
import ZODB


# Allow DSN override via environment variable for CI.
# Default: local Docker on port 5433 (development setup).
DSN = os.environ.get(
    "ZODB_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)

# All tables that may exist across history-free and history-preserving modes.
ALL_TABLES = (
    "pack_state",
    "blob_history",
    "object_history",
    "blob_state",
    "object_state",
    "transaction_log",
)


def clean_db():
    """Drop all tables for a fresh start.

    Terminates other connections to the test database first to avoid
    blocking on open REPEATABLE READ transactions left by prior tests.
    """
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        cur.execute("DROP TABLE IF EXISTS " + ", ".join(ALL_TABLES) + " CASCADE")
    conn.commit()
    conn.close()


# ── Shared pytest fixtures ────────────────────────────────────────────


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage (history-free) with clean database."""
    clean_db()
    s = PGJsonbStorage(DSN)
    yield s
    s.close()


@pytest.fixture
def db(storage):
    """ZODB.DB using our storage."""
    database = ZODB.DB(storage)
    yield database
    database.close()


@pytest.fixture
def hp_storage():
    """Fresh PGJsonbStorage in history-preserving mode with clean database."""
    clean_db()
    s = PGJsonbStorage(DSN, history_preserving=True)
    yield s
    s.close()


@pytest.fixture
def hp_db(hp_storage):
    """ZODB.DB using history-preserving storage."""
    database = ZODB.DB(hp_storage)
    yield database
    database.close()
