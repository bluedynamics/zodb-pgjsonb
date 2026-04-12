"""Session-level PostgreSQL advisory locks for startup DDL coordination.

Serializes ``_apply_pending_ddl`` across replicas so only one process at a
time runs schema DDL and deferred startup actions.  Non-winners wait on
``pg_advisory_lock`` and then find the work already done (idempotent DDL).

See docs/superpowers/specs/2026-04-10-startup-ddl-advisory-lock-design.md.
"""

from contextlib import contextmanager

import logging
import os
import psycopg


log = logging.getLogger(__name__)

# Advisory lock coordinates — two-arg form keeps us in a separate keyspace
# from ``pg_advisory_xact_lock(0)`` used for TID serialization.
STARTUP_DDL_LOCK_NS = 0x5A4442  # "ZDB"
STARTUP_DDL_LOCK_KEY = 1  # startup DDL serializer

DEFAULT_TIMEOUT = "15min"
ENV_TIMEOUT = "ZODB_PGJSONB_DDL_LOCK_TIMEOUT"


@contextmanager
def startup_ddl_lock(dsn, timeout=None):
    """Acquire a session-level PG advisory lock for startup DDL.

    Opens a dedicated autocommit connection, sets ``lock_timeout``, then
    calls ``pg_advisory_lock(ns, key)``.  The lock is held for the full
    body of the ``with`` block and released via ``pg_advisory_unlock``.
    If the process crashes, PostgreSQL auto-releases on session close.

    Args:
        dsn: PostgreSQL connection string.
        timeout: ``SET lock_timeout`` value (e.g. "15min").  Defaults to
            ``$ZODB_PGJSONB_DDL_LOCK_TIMEOUT`` or "15min".

    Raises:
        psycopg.errors.LockNotAvailable: if the lock cannot be acquired
            within ``timeout``.
    """
    effective_timeout = timeout or os.environ.get(ENV_TIMEOUT) or DEFAULT_TIMEOUT
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        conn.execute(f"SET lock_timeout = '{effective_timeout}'")
        conn.execute(
            "SELECT pg_advisory_lock(%s, %s)",
            (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY),
        )
        log.debug(
            "Acquired startup DDL advisory lock (%s, %s)",
            STARTUP_DDL_LOCK_NS,
            STARTUP_DDL_LOCK_KEY,
        )
        try:
            yield conn
        finally:
            try:
                conn.execute(
                    "SELECT pg_advisory_unlock(%s, %s)",
                    (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY),
                )
            except Exception:
                log.warning(
                    "Failed to release startup DDL advisory lock", exc_info=True
                )
    finally:
        conn.close()
