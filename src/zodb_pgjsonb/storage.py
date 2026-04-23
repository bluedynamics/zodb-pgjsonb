"""PGJsonbStorage — ZODB storage using PostgreSQL JSONB.

Implements IMVCCStorage using psycopg3 (sync) and zodb-json-codec for
transparent pickle ↔ JSONB transcoding.

PGJsonbStorage is the main storage (factory) that manages schema, OIDs,
and shared state.  Each ZODB Connection gets its own
PGJsonbStorageInstance via new_instance(), providing per-connection
snapshot isolation through separate PostgreSQL connections.
"""

from .batch import _batch_delete_objects
from .batch import _batch_write_blobs
from .batch import _batch_write_objects
from .batch import _write_txn_log
from .conflict import _batch_check_read_conflicts
from .conflict import _batch_resolve_conflicts
from .interfaces import IPGJsonbStorage
from .migration import CopyTransactionsMixin
from .schema import _table_exists as schema_table_exists
from .schema import drop_history_tables
from .schema import HISTORY_PRESERVING_ADDITIONS
from .schema import install_schema
from .serialization import _deserialize_extension
from .serialization import _unsanitize_from_pg
from .startup_locks import startup_ddl_lock
from .stats import get_blob_histogram as _get_blob_histogram
from .stats import get_blob_stats as _get_blob_stats
from .undo import _compute_undo
from collections import OrderedDict
from persistent.TimeStamp import TimeStamp
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from ZODB.BaseStorage import BaseStorage
from ZODB.BaseStorage import DataRecord
from ZODB.BaseStorage import TransactionRecord
from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.interfaces import IBlobStorage
from ZODB.interfaces import IMVCCStorage
from ZODB.interfaces import IStorageIteration
from ZODB.interfaces import IStorageRestoreable
from ZODB.interfaces import IStorageUndoable
from ZODB.POSException import POSKeyError
from ZODB.POSException import ReadOnlyError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import UndoError
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import contextlib
import dataclasses
import logging
import os
import psycopg
import re
import shutil
import sys
import tempfile
import threading
import time
import zodb_json_codec
import zope.interface


logger = logging.getLogger(__name__)


_VALID_SQL_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


# ── Idle-in-transaction safety net (#118) ─────────────────────────────
# Bound the worst-case duration of any leaked virtualxid so it doesn't
# block CREATE INDEX CONCURRENTLY indefinitely.  Tunable via env var.
DEFAULT_IDLE_IN_XACT_TIMEOUT_MS = 60_000
ENV_IDLE_IN_XACT_TIMEOUT = "ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"


def _configure_pool_conn(conn):
    """psycopg_pool ``configure`` hook applied on every new pool conn.

    Sets autocommit (required by the instance state machine) and the
    idle_in_transaction_session_timeout safety net.  ``0`` disables
    the timeout (matches PG default).
    """
    conn.autocommit = True
    timeout_ms = int(
        os.environ.get(ENV_IDLE_IN_XACT_TIMEOUT, DEFAULT_IDLE_IN_XACT_TIMEOUT_MS)
    )
    conn.execute(f"SET idle_in_transaction_session_timeout = {timeout_ms}")


@dataclasses.dataclass
class ExtraColumn:
    """Declares an extra column for object_state written by a state processor.

    Attributes:
        name: PostgreSQL column name (must be a valid SQL identifier:
            letters, digits, underscores; must start with letter or underscore).
        value_expr: SQL value expression for INSERT, e.g. ``"%(name)s"`` or
            ``"to_tsvector('simple'::regconfig, %(name)s)"``.
        update_expr: Optional ON CONFLICT update expression.  Defaults to
            ``"EXCLUDED.{name}"`` when *None*.

    Security note:
        **WARNING: State processors run with full SQL injection capability.**
        ``value_expr`` and ``update_expr`` are interpolated directly into
        INSERT statements without escaping.  Only register processors from
        trusted, audited code.  A compromised processor can read, modify, or
        delete any data in the database.  Column names are validated against
        a strict identifier pattern; expressions are not, because they may
        legitimately contain SQL function calls.
    """

    name: str
    value_expr: str
    update_expr: str | None = None

    def __post_init__(self):
        if not _VALID_SQL_IDENTIFIER.match(self.name):
            raise ValueError(
                f"ExtraColumn name must be a valid SQL identifier "
                f"(letters, digits, underscores), got: {self.name!r}"
            )


# Default cache size: 16 MB per instance (tunable via cache_local_mb parameter)
DEFAULT_CACHE_LOCAL_MB = 16


class _NoopSerialCache:
    """Drop-in dict substitute that never stores anything (#62).

    Used in history-preserving mode where ``_do_loadSerial`` retrieves
    old revisions from ``object_history`` and the serial cache is
    redundant.  Avoids the unbounded-growth leak of a real dict.
    """

    __slots__ = ()

    def get(self, key, default=None):
        return default

    def __setitem__(self, key, value):
        pass

    def __getitem__(self, key):
        raise KeyError(key)

    def __contains__(self, key):
        return False

    def __len__(self):
        return 0

    def clear(self):
        pass


class LoadCache:
    """Bounded LRU cache for load() results.

    Stores (pickle_bytes, tid_bytes) keyed by zoid (int).
    Evicts least-recently-used entries when byte size exceeds the limit.
    Thread-safety is not needed: each PGJsonbStorageInstance has its own.
    """

    __slots__ = ("_data", "_max_size", "_size", "hits", "misses")

    def __init__(self, max_mb=DEFAULT_CACHE_LOCAL_MB):
        self._data = OrderedDict()  # zoid → (data, tid, entry_size)
        self._size = 0
        self._max_size = int(max_mb * 1_000_000)
        self.hits = 0
        self.misses = 0

    def get(self, zoid):
        """Look up by zoid. Returns (data, tid) or None. Promotes on hit."""
        entry = self._data.get(zoid)
        if entry is not None:
            self.hits += 1
            self._data.move_to_end(zoid)
            return entry[0], entry[1]
        self.misses += 1
        return None

    def set(self, zoid, data, tid):
        """Store (data, tid) for zoid. Evicts LRU if over budget."""
        entry_size = sys.getsizeof(data) + sys.getsizeof(tid) + 64  # overhead
        # Remove old entry if exists
        old = self._data.pop(zoid, None)
        if old is not None:
            self._size -= old[2]
        # Evict LRU until we fit
        while self._size + entry_size > self._max_size and self._data:
            _, evicted = self._data.popitem(last=False)
            self._size -= evicted[2]
        self._data[zoid] = (data, tid, entry_size)
        self._size += entry_size

    def invalidate(self, zoid):
        """Remove a single zoid from the cache."""
        old = self._data.pop(zoid, None)
        if old is not None:
            self._size -= old[2]

    def clear(self):
        """Remove all entries."""
        self._data.clear()
        self._size = 0

    def __len__(self):
        return len(self._data)

    @property
    def size_mb(self):
        return self._size / 1_000_000


class SharedLoadCache:
    """Process-wide LoadCache shared across all PGJsonbStorageInstance.

    Stores ``(pickle_bytes, tid_bytes)`` keyed by ``zoid`` (int).  A
    process-wide ``_consensus_tid`` gates reads and writes so that an
    instance holding a stale snapshot cannot pollute the cache after
    another instance's ``poll_advance`` has already invalidated a zoid
    at a newer TID.  See #63 for the race analysis.

    Thread-safe: a single ``RLock`` protects the OrderedDict, byte
    accounting, and ``_consensus_tid`` atomically.
    """

    __slots__ = (
        "_cache",
        "_consensus_tid",
        "_current_bytes",
        "_lock",
        "_max_bytes",
        "hits",
        "misses",
    )

    def __init__(self, max_mb):
        self._cache = OrderedDict()  # zoid → (data_bytes, tid_bytes)
        self._consensus_tid = None  # int; advanced by poll_advance()
        self._max_bytes = int(max_mb * 1_000_000)
        self._current_bytes = 0
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, zoid, polled_tid):
        """Return (data, tid) for zoid or None.

        Returns None when the cache has never been initialized, when
        the caller's snapshot is older than the current consensus, or
        when the zoid is not cached.
        """
        with self._lock:
            if self._consensus_tid is None or polled_tid is None:
                self.misses += 1
                return None
            if polled_tid < self._consensus_tid:
                self.misses += 1
                return None
            entry = self._cache.get(zoid)
            if entry is None:
                self.misses += 1
                return None
            self._cache.move_to_end(zoid)
            self.hits += 1
            return entry

    def set(self, zoid, data, tid_bytes, polled_tid):
        """Store (data, tid_bytes) for zoid if the caller is up to date.

        Rejects writes from callers whose snapshot is older than the
        current consensus (which could be carrying stale pre-
        invalidation bytes), and never replaces a newer entry with an
        older one.
        """
        with self._lock:
            if self._consensus_tid is None or polled_tid is None:
                return
            if polled_tid < self._consensus_tid:
                return
            tid_int = u64(tid_bytes)
            existing = self._cache.get(zoid)
            if existing is not None and u64(existing[1]) >= tid_int:
                return
            if existing is not None:
                self._current_bytes -= len(existing[0])
            self._cache[zoid] = (data, tid_bytes)
            self._cache.move_to_end(zoid)
            self._current_bytes += len(data)
            while self._current_bytes > self._max_bytes and self._cache:
                _, (evicted_data, _) = self._cache.popitem(last=False)
                self._current_bytes -= len(evicted_data)

    def poll_advance(self, new_tid, changed_zoids):
        """Advance consensus_tid and invalidate changed zoids atomically."""
        with self._lock:
            if self._consensus_tid is None or new_tid >= self._consensus_tid:
                for z in changed_zoids:
                    entry = self._cache.pop(z, None)
                    if entry is not None:
                        self._current_bytes -= len(entry[0])
                self._consensus_tid = new_tid


class PGTransactionRecord(TransactionRecord):
    """Transaction record yielded by PGJsonbStorage.iterator()."""

    def __init__(self, tid, status, user, description, extension, records):
        super().__init__(tid, status, user, description, extension)
        self._records = records

    def __iter__(self):
        return iter(self._records)


@zope.interface.implementer(
    IPGJsonbStorage,
    IMVCCStorage,
    IBlobStorage,
    IStorageUndoable,
    IStorageIteration,
    IStorageRestoreable,
)
class PGJsonbStorage(CopyTransactionsMixin, ConflictResolvingStorage, BaseStorage):
    """ZODB storage that stores object state as JSONB in PostgreSQL.

    Implements IMVCCStorage: ZODB.DB uses new_instance() to create
    per-connection storage instances with independent snapshots.

    Cache topology (after #63):

    - Each ``PGJsonbStorageInstance`` owns a small L1 ``LoadCache``
      sized by ``cache_per_connection_mb`` (default 16 MB).  Lock-
      free, per-connection, fast-path reads.
    - The main ``PGJsonbStorage`` owns a single process-wide
      ``SharedLoadCache`` sized by ``cache_shared_mb`` (default
      256 MB), visible to every instance.  Consensus-TID gated for
      MVCC correctness.
    - Read path: L1 → shared → PG.  A PG hit populates both.  A
      shared hit promotes to L1.

    Extends BaseStorage which handles:
    - Lock management (_lock, _commit_lock)
    - TID generation (monotonic timestamps)
    - OID allocation (new_oid)
    - 2PC protocol orchestration (tpc_begin/vote/finish/abort)

    The main storage keeps its own PG connection for schema init,
    admin queries (__len__, getSize, pack), and backward-compatible
    direct use (without ZODB.DB).
    """

    def __init__(
        self,
        dsn,
        name="pgjsonb",
        history_preserving=False,
        blob_temp_dir=None,
        cache_local_mb=None,  # deprecated alias for cache_shared_mb
        cache_shared_mb=256,
        cache_per_connection_mb=16,
        pool_size=1,
        pool_max_size=10,
        pool_timeout=30.0,
        s3_client=None,
        blob_cache=None,
        blob_threshold=102_400,
        cache_warm_pct=10,
        cache_warm_decay=0.8,
    ):
        BaseStorage.__init__(self, name)
        self._dsn = dsn
        self._history_preserving = history_preserving

        # Deprecation: cache_local_mb used to be per-instance; now it is
        # an alias for cache_shared_mb (see #63).  Warn once per storage.
        if cache_local_mb is not None:
            import warnings

            warnings.warn(
                "cache_local_mb is deprecated since 1.12.0; use "
                "cache_shared_mb for the process-wide cache and "
                "cache_per_connection_mb for the per-instance L1. "
                "cache_local_mb is being mapped to cache_shared_mb.",
                DeprecationWarning,
                stacklevel=2,
            )
            cache_shared_mb = cache_local_mb

        self._cache_shared_mb = cache_shared_mb
        self._cache_per_connection_mb = cache_per_connection_mb
        self._ltid = z64
        self._pack_tid = None  # Integer TID of last pack time

        # S3 tiered blob storage (optional)
        self._s3_client = s3_client  # None = PG-only mode
        self._blob_cache = blob_cache  # S3BlobCache for local caching
        self._blob_threshold = blob_threshold  # bytes; blobs >= this go to S3

        # State processors (plugins that extract extra column data during writes)
        self._state_processors = []
        self._pending_ddl = []  # Deferred DDL: [(sql, name), ...]

        # Refs prefetch: SQL expression for the refs column in load().
        # Set via register_prefetch_refs_expr(). When None, load() does
        # not include refs and no prefetching occurs.
        self._prefetch_refs_expr = None
        self._load_sql = (
            "SELECT tid, class_mod, class_name, state FROM object_state WHERE zoid = %s"
        )

        # Pending stores for current transaction (direct use only)
        self._tmp = []
        self._blob_tmp = {}  # pending blob stores: {oid_int: blob_path}

        # Blob temp directory
        self._blob_temp_dir = blob_temp_dir or tempfile.mkdtemp(
            prefix="zodb-pgjsonb-blobs-"
        )
        os.makedirs(self._blob_temp_dir, exist_ok=True)

        # Per-instance L1 load cache (small, fast path, no lock).
        # Size from cache_per_connection_mb.  See instance.py.
        self._load_cache = LoadCache(max_mb=cache_per_connection_mb)

        # Process-wide shared load cache (L2).  One per PGJsonbStorage,
        # visible to all PGJsonbStorageInstance objects on the same
        # process.  Gated by consensus_tid for MVCC correctness (#63).
        self._shared_cache = SharedLoadCache(max_mb=cache_shared_mb)

        # Cache for conflict resolution: (oid_bytes, tid_bytes) → pickle_bytes
        # In history-free mode, loadSerial can't find old versions after they're
        # overwritten. Caching data from load() makes it available for
        # tryToResolveConflict's loadSerial(oid, oldSerial) calls.
        # In history-preserving mode object_history suffices, so skip the
        # cache to avoid unbounded growth (#62).
        self._serial_cache = _NoopSerialCache() if history_preserving else {}

        # Database connection (schema init + admin queries)
        logger.debug("Connecting to PostgreSQL: %s", _mask_dsn(dsn))
        self._conn = psycopg.connect(dsn, row_factory=dict_row)
        logger.debug("Connected to PostgreSQL")

        # Connection pool for MVCC instances (autocommit=True, dict_row)
        logger.debug(
            "Creating connection pool (min=%d, max=%d)", pool_size, pool_max_size
        )
        self._instance_pool = ConnectionPool(
            dsn,
            min_size=pool_size,
            max_size=pool_max_size,
            timeout=pool_timeout,
            kwargs={"row_factory": dict_row},
            configure=_configure_pool_conn,
            open=True,
        )
        logger.debug("Connection pool ready")

        # Initialize schema
        logger.debug("Installing schema (history_preserving=%s)", history_preserving)
        install_schema(self._conn, history_preserving=history_preserving)
        logger.debug("Schema installed")

        # Warn if HP tables exist but running in HF mode
        if not history_preserving and schema_table_exists(self._conn, "object_history"):
            logger.warning(
                "History-preserving tables (object_history, pack_state) exist "
                "but storage is running in history-free mode. "
                "Call storage.convert_to_history_free() to remove them and "
                "reclaim disk space."
            )

        # Load max OID and last TID from database
        self._restore_state()
        self._conn.commit()

        # Switch to autocommit so SELECTs (new_oid, load, history, stats)
        # don't leave the connection "idle in transaction" indefinitely (#45).
        # Write paths that need transactions use explicit BEGIN/COMMIT.
        self._conn.autocommit = True

        # Learning cache warmer (#48): pre-load frequently needed objects
        self._warmer = None
        if cache_warm_pct > 0:
            from zodb_pgjsonb.cache_warmer import CacheWarmer
            from zodb_pgjsonb.cache_warmer import WARM_STATS_DDL

            with contextlib.suppress(Exception):
                self._conn.execute(WARM_STATS_DDL)
            # Estimate target count from actual avg object size (#51)
            avg_size = 2000  # fallback
            with contextlib.suppress(Exception), self._conn.cursor() as cur:
                cur.execute("SELECT AVG(state_size) AS avg FROM object_state")
                row = cur.fetchone()
                if row and row["avg"]:
                    avg_size = max(100, int(row["avg"]))
            estimated_objects = int(cache_per_connection_mb * 1_000_000 / avg_size)
            target = max(1, int(estimated_objects * cache_warm_pct / 100))

            def _current_max_tid():
                try:
                    with self._conn.cursor() as cur:
                        cur.execute(
                            "SELECT COALESCE(MAX(tid), 0) AS t FROM transaction_log"
                        )
                        row = cur.fetchone()
                        return row["t"] if row else 0
                except Exception:
                    return 0

            self._warmer = CacheWarmer(
                self._conn,
                target_count=target,
                shared_cache=self._shared_cache,
                load_current_tid_fn=_current_max_tid,
                decay=cache_warm_decay,
            )
            import threading

            threading.Thread(
                target=self._warmer.warm,
                args=(self._warm_load_multiple,),
                daemon=True,
                name="zodb-cache-warmer",
            ).start()
            logger.info(
                "Cache warmer started (target=%d, decay=%.1f)",
                target,
                cache_warm_decay,
            )

        logger.debug("Storage initialized (max_oid=%s, ltid=%s)", self._oid, self._ltid)

    def _warm_load_multiple(self, oids):
        """Load multiple objects for cache warming using a pool connection.

        Uses a pool connection (not self._conn) because the warmer runs
        in a background thread and self._conn may be in use.
        """
        conn = self._instance_pool.getconn()
        try:
            from ZODB.utils import p64
            from ZODB.utils import u64

            zoid_list = [u64(oid) for oid in oids]
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT zoid, tid, class_mod, class_name, state "
                    "FROM object_state WHERE zoid = ANY(%s)",
                    (zoid_list,),
                )
                rows = cur.fetchall()
            result = {}
            for row in rows:
                record = {
                    "@cls": [row["class_mod"], row["class_name"]],
                    "@s": _unsanitize_from_pg(row["state"]),
                }
                data = zodb_json_codec.encode_zodb_record(record)
                tid = p64(row["tid"])
                oid = p64(row["zoid"])
                result[oid] = (data, tid)
            return result
        finally:
            self._instance_pool.putconn(conn)

    def _restore_state(self):
        """Load max OID and last TID from existing data."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(zoid), 0) AS max_oid FROM object_state")
            row = cur.fetchone()
            max_oid = row["max_oid"]
            if max_oid > 0:
                self._oid = p64(max_oid)

            cur.execute("SELECT COALESCE(MAX(tid), 0) AS max_tid FROM transaction_log")
            row = cur.fetchone()
            max_tid = row["max_tid"]
            if max_tid > 0:
                self._ltid = p64(max_tid)
                # Ensure _ts is at least as recent as the last committed TID
                # so _new_tid() generates monotonically increasing TIDs.
                self._ts = TimeStamp(self._ltid)

    def new_oid(self):
        """Allocate a new OID via PostgreSQL sequence (cross-process safe).

        Overrides BaseStorage.new_oid() which uses an in-memory counter
        that causes OID collisions when multiple processes share the same
        database (#31).
        """
        if self._is_read_only:
            raise ReadOnlyError()
        with self._conn.cursor() as cur:
            cur.execute("SELECT nextval('zoid_seq') AS oid")
            row = cur.fetchone()
            oid_int = row["oid"]
        return p64(oid_int)

    # ── State Processors ───────────────────────────────────────────

    def register_state_processor(self, processor):
        """Register a processor that extracts extra column data from state.

        The *processor* must implement:

        - ``get_extra_columns() -> list[ExtraColumn]``
        - ``process(zoid, class_mod, class_name, state) -> dict | None``

        Optionally:

        - ``get_schema_sql() -> str | None``
          Return DDL to apply (e.g. ALTER TABLE, CREATE INDEX).  Applied
          via a separate autocommit connection.  If blocked by startup
          read transactions, deferred to the first tpc_begin().

        ``process`` may modify *state* in-place (e.g. pop annotation keys).
        It returns a dict of ``{column_name: value}`` to be written as extra
        columns alongside the object, or *None* when no extra data applies.

        **Security:** Processors have full SQL access via ``ExtraColumn.value_expr``.
        Only register processors from trusted, audited sources.
        """
        self._state_processors.append(processor)
        # Apply processor schema DDL if available
        if hasattr(processor, "get_schema_sql"):
            sql = processor.get_schema_sql()
            if sql:
                self._apply_processor_ddl(sql, type(processor).__name__)

    def register_prefetch_refs_expr(self, sql_expr):
        """Register a SQL expression for refs prefetching in load().

        When set, load() includes the expression as an extra column
        aliased ``refs``.  If it evaluates to a non-NULL array, the
        referenced objects are prefetched into the load cache.

        Example (plone-pgcatalog uses this to prefetch only for
        cataloged content objects)::

            storage.register_prefetch_refs_expr(
                "CASE WHEN idx IS NOT NULL THEN refs END"
            )

        Call with ``None`` to disable prefetching.

        **Security:** ``sql_expr`` is interpolated directly into SELECT
        statements without escaping.  Only pass expressions from trusted,
        audited code.  A malicious expression can read or modify any data
        in the database.  See :class:`ExtraColumn` for the same trust model.
        """
        self._prefetch_refs_expr = sql_expr
        if sql_expr:
            self._load_sql = (
                "SELECT tid, class_mod, class_name, state, "
                f"{sql_expr} AS refs "
                "FROM object_state WHERE zoid = %s"
            )
        else:
            self._load_sql = (
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s"
            )
        logger.info("Prefetch refs expr: %s", sql_expr or "(disabled)")

    def _apply_processor_ddl(self, sql, processor_name):
        """Defer DDL from a state processor to the first write transaction.

        At registration time (IDatabaseOpenedWithRoot), a ZODB Connection
        always holds an open REPEATABLE READ snapshot with ACCESS SHARE on
        object_state.  DDL (ALTER TABLE, CREATE INDEX) needs ACCESS
        EXCLUSIVE, which would deadlock against that snapshot (#100).

        Always deferring avoids the race entirely.  The DDL runs in
        ``_apply_pending_ddl()`` called from ``tpc_begin()`` / ``_begin()``
        when the read transaction has been committed.
        """
        logger.info(
            "DDL from %s deferred to first write transaction.",
            processor_name,
        )
        self._pending_ddl.append((sql, processor_name))

    def defer_startup_action(self, action, name):
        """Defer a callable to be executed on the first write transaction.

        Like ``_apply_processor_ddl`` but accepts an arbitrary callable
        instead of a SQL string.  The callable receives the DSN as its
        only argument and should open its own autocommit connection.

        Used by plugins (e.g. plone-pgcatalog) to defer index creation
        that would deadlock against open REPEATABLE READ snapshots at
        startup.
        """
        logger.info("Startup action '%s' deferred to first write transaction.", name)
        self._pending_ddl.append((action, name))

    def _apply_pending_ddl(self):
        """Apply deferred DDL and startup actions.

        Called from ``tpc_begin()`` / ``_begin()`` after the read
        transaction is committed (ACCESS SHARE released).

        Each entry is either ``(sql_string, name)`` or
        ``(callable, name)``.  Callables receive ``self._dsn``.

        Wraps the whole batch in a session-level PG advisory lock so
        concurrent replicas serialize their DDL.  On lock timeout, the
        pending entries are requeued for the next call.
        """
        if not self._pending_ddl:
            return
        pending = self._pending_ddl[:]
        self._pending_ddl.clear()
        try:
            with startup_ddl_lock(self._dsn):
                for action, name in pending:
                    try:
                        if callable(action):
                            action(self._dsn)
                        else:
                            with psycopg.connect(
                                self._dsn, autocommit=True
                            ) as ddl_conn:
                                ddl_conn.execute("SET lock_timeout = '30s'")
                                ddl_conn.execute(action)
                        logger.info("Applied deferred DDL/action from %s", name)
                    except Exception:
                        logger.warning(
                            "Failed to apply deferred DDL from %s "
                            "(lock timeout or other error — will retry on next startup)",
                            name,
                            exc_info=True,
                        )
        except psycopg.errors.LockNotAvailable:
            # Could not acquire the startup lock — requeue pending work
            # so the next tpc_begin / poll_invalidations retries.
            self._pending_ddl = pending + self._pending_ddl
            logger.warning(
                "Could not acquire startup DDL advisory lock within timeout — "
                "will retry on next tpc_begin / poll_invalidations",
            )

    def _process_state(self, zoid, class_mod, class_name, state):
        """Run all registered state processors, return merged extra data."""
        extra = {}
        for proc in self._state_processors:
            result = proc.process(zoid, class_mod, class_name, state)
            if result:
                extra.update(result)
        return extra or None

    def _get_extra_columns(self):
        """Collect extra column definitions from all state processors."""
        columns = []
        for proc in self._state_processors:
            columns.extend(proc.get_extra_columns())
        return columns or None

    # ── IMVCCStorage ─────────────────────────────────────────────────

    def new_instance(self):
        """Create a per-connection storage instance.

        Each ZODB Connection gets its own instance with an independent
        PG connection for snapshot isolation.
        """
        from .instance import PGJsonbStorageInstance

        return PGJsonbStorageInstance(self)

    def release(self):
        """Release resources (no-op for the main storage)."""

    def poll_invalidations(self):
        """Poll for invalidations (no-op for main storage)."""
        return []

    def sync(self, force=True):
        """Sync snapshot (no-op for main storage)."""

    # ── TID generation (thread-safe, shared across instances) ────────

    def _new_tid(self):
        """Generate a new transaction ID.

        Called by instances while holding the PG advisory lock.
        Uses BaseStorage's _lock for Python-level thread safety.

        Note: All write transactions are serialized through a single
        advisory lock (pg_advisory_xact_lock(0)).  This is a deliberate
        simplicity trade-off: it guarantees correct TID ordering but
        limits write throughput to one transaction at a time.  For
        higher write throughput, consider OID-range-based advisory locks.
        """
        with self._lock:
            now = time.time()
            t = TimeStamp(*(*time.gmtime(now)[:5], now % 60))
            self._ts = t = t.laterThan(self._ts)
            return t.raw()

    # ── IStorage: load ───────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state, returning (pickle_bytes, tid_bytes).

        If ``_prefetch_refs_expr`` is set on the main storage, the load
        query includes it as an extra column aliased ``refs``.  When the
        expression evaluates to a non-NULL array, the referenced objects
        are prefetched into ``_load_cache`` via ``load_multiple()``.
        """
        zoid = u64(oid)

        # Check load cache first
        cached = self._load_cache.get(zoid)
        if cached is not None:
            return cached

        with self._conn.cursor() as cur:
            cur.execute(
                self._load_sql,
                (zoid,),
                prepare=True,
            )
            row = cur.fetchone()

        if row is None:
            raise POSKeyError(oid)

        record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": _unsanitize_from_pg(row["state"]),
        }
        data = zodb_json_codec.encode_zodb_record(record)
        tid = p64(row["tid"])
        self._serial_cache[(oid, tid)] = data
        self._load_cache.set(zoid, data, tid)

        # Prefetch refs if the expression yielded a non-NULL array
        refs = row.get("refs") if isinstance(row, dict) else None
        if refs:
            ref_oids = [
                p64(ref_zoid)
                for ref_zoid in refs
                if self._load_cache.get(ref_zoid) is None
            ]
            if ref_oids:
                self.load_multiple(ref_oids)

        return data, tid

    def loadBefore(self, oid, tid):
        """Load object data before a given TID."""
        zoid = u64(oid)
        tid_int = u64(tid)

        with self._conn.cursor() as cur:
            if self._history_preserving:
                return _loadBefore_hp(cur, oid, zoid, tid_int)
            return _loadBefore_hf(cur, oid, zoid, tid_int)

    def loadSerial(self, oid, serial):
        """Load a specific revision of an object."""
        return _do_loadSerial(
            self._conn, self._serial_cache, self._history_preserving, oid, serial
        )

    # ── IStorage: store ──────────────────────────────────────────────

    def store(self, oid, serial, data, version, transaction):
        """Queue an object for storage during the current transaction.

        Conflict detection is deferred to _vote() where all conflicts are
        checked in a single batch query, eliminating per-object round trips.
        """
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)

        if version:
            raise TypeError("versions are not supported")

        zoid = u64(oid)

        class_mod, class_name, state, refs = (
            zodb_json_codec.decode_zodb_record_for_pg_json(data)
        )

        entry = {
            "zoid": zoid,
            "class_mod": class_mod,
            "class_name": class_name,
            "state": state,
            "state_size": len(data),
            "refs": refs,
        }
        # Save conflict detection data for batch check in _vote()
        if serial != z64:
            entry["_oid"] = oid
            entry["_serial"] = serial
            entry["_data"] = data
        extra = self._process_state(zoid, class_mod, class_name, state)
        if extra:
            entry["_extra"] = extra
        self._tmp.append(entry)

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        """Queue a read-conflict check for batch verification in _vote()."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        self._read_conflicts.append((oid, serial))

    # ── BaseStorage hooks (2PC) ──────────────────────────────────────

    def _begin(self, tid, u, d, e):
        """Called by BaseStorage.tpc_begin after acquiring commit lock."""
        self._apply_pending_ddl()
        self._ude = (u, d, e)
        self._voted = False
        self._read_conflicts = []
        self._conn.execute("BEGIN")
        self._conn.execute("SELECT pg_advisory_xact_lock(0)")

    def _vote(self):
        """Flush pending stores + blobs to PostgreSQL.

        Called by BaseStorage.tpc_vote, and also from tpc_finish to handle
        cases where tpc_vote is skipped (e.g. undo → tpc_finish).
        Idempotent: only flushes once per transaction.

        Performs batch conflict detection for all queued stores in a single
        round trip, then writes all objects in batched executemany calls.
        """
        if self._voted:
            return None
        self._voted = True

        tid_int = u64(self._tid)
        hp = self._history_preserving

        with self._conn.cursor() as cur:
            # ── Batch conflict detection ──────────────────────────
            _batch_resolve_conflicts(cur, self._tmp, self._resolved, self)
            _batch_check_read_conflicts(cur, self._read_conflicts)

            u, d, e = self._ude
            user = u.decode("utf-8") if isinstance(u, bytes) else u
            desc = d.decode("utf-8") if isinstance(d, bytes) else d
            _write_txn_log(cur, tid_int, user, desc, e)

            # Separate objects by action for batch writes
            writes = []
            deletes = []
            for obj in self._tmp:
                if obj.get("action") == "delete":
                    deletes.append(obj["zoid"])
                else:
                    writes.append(obj)

            extra_columns = self._get_extra_columns()
            _batch_write_objects(cur, writes, tid_int, hp, extra_columns=extra_columns)
            _batch_delete_objects(cur, deletes, tid_int, hp)
            if self._s3_client is not None:
                from zodb_pgjsonb.blob_sink import InlineBlobSink

                _vote_blob_sink = InlineBlobSink(self._s3_client)
            else:
                _vote_blob_sink = None
            _batch_write_blobs(
                cur,
                self._blob_tmp.items(),
                tid_int,
                hp,
                blob_sink=_vote_blob_sink,
                blob_threshold=self._blob_threshold,
            )

            # ── State processor finalize hook ────────────────────
            for proc in self._state_processors:
                if hasattr(proc, "finalize"):
                    proc.finalize(cur)

        return self._resolved or None

    def tpc_finish(self, transaction, f=None):
        """Commit PG transaction, then run callback.

        Overrides BaseStorage.tpc_finish to ensure the PG transaction is
        committed and _ltid is updated BEFORE the callback fires.
        BaseStorage calls f(tid) before _finish(), but our _finish() does
        the PG COMMIT — so other threads would see stale data during the
        callback.

        Also ensures _vote() is called if tpc_vote was skipped (e.g.
        undo → tpc_finish without explicit tpc_vote).

        The callback runs AFTER the lock is released so that concurrent
        readers (lastTransaction, getTid) can proceed during the callback.
        This is safe because commit + _ltid are already done.
        """
        with self._lock:
            if transaction is not self._transaction:
                raise StorageTransactionError(
                    "tpc_finish called with wrong transaction"
                )
            try:
                self._vote()  # idempotent — flushes if not already done
                self._finish(self._tid, None, None, None)
            finally:
                self._clear_temp()
                self._ude = None
                self._transaction = None
                self._commit_lock.release()
        tid = self._tid
        if f is not None:
            f(tid)
        return tid

    def _finish(self, tid, u, d, e):
        """Commit PG transaction and update _ltid."""
        self._conn.commit()
        self._ltid = tid
        # Invalidate the shared cache for any zoid we wrote so that
        # other instances don't serve stale state.  Required because
        # some write paths (main-storage direct undo/restore) don't go
        # through an instance whose poll_invalidations would do this.
        changed_zoids = [obj["zoid"] for obj in self._tmp]
        self._shared_cache.poll_advance(u64(tid), changed_zoids)

    def _abort(self):
        """Called by BaseStorage.tpc_abort — rollback PG transaction."""
        try:
            self._conn.rollback()
        except Exception:  # pragma: no cover
            logger.exception("Error during rollback")
        # Clean up queued blob temp files
        for blob_path in self._blob_tmp.values():
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        self._blob_tmp.clear()

    def _clear_temp(self):
        """Clear pending stores between transactions."""
        self._tmp.clear()
        self._blob_tmp.clear()

    # ── IStorage: metadata ───────────────────────────────────────────

    def lastTransaction(self):
        """Return TID of the last committed transaction."""
        return self._ltid

    def __len__(self):
        """Return approximate number of objects."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM object_state")
            row = cur.fetchone()
        return row["cnt"]

    def getSize(self):
        """Return approximate database size in bytes."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(state_size), 0) AS total FROM object_state"
            )
            row = cur.fetchone()
        return row["total"]

    def get_blob_stats(self):
        """Return blob storage statistics."""
        return _get_blob_stats(self._conn, self._s3_client, self._blob_threshold)

    def get_blob_histogram(self):
        """Return blob size distribution as logarithmic buckets."""
        return _get_blob_histogram(self._conn, self._s3_client, self._blob_threshold)

    def history(self, oid, size=1):
        """Return revision history for an object."""
        zoid = u64(oid)
        with self._conn.cursor() as cur:
            if self._history_preserving:
                cur.execute(
                    "SELECT sub.tid, sub.state_size, "
                    "t.username, t.description "
                    "FROM ("
                    "  SELECT tid, state_size FROM object_history WHERE zoid = %s"
                    "  UNION"
                    "  SELECT tid, state_size FROM object_state WHERE zoid = %s"
                    ") sub "
                    "LEFT JOIN transaction_log t ON sub.tid = t.tid "
                    "ORDER BY sub.tid DESC LIMIT %s",
                    (zoid, zoid, size),
                )
            else:
                cur.execute(
                    "SELECT o.tid, o.state_size, "
                    "t.username, t.description "
                    "FROM object_state o "
                    "LEFT JOIN transaction_log t ON o.tid = t.tid "
                    "WHERE o.zoid = %s "
                    "ORDER BY o.tid DESC LIMIT %s",
                    (zoid, size),
                )
            rows = cur.fetchall()

        if not rows:
            raise POSKeyError(oid)

        result = []
        for row in rows:
            ts = TimeStamp(p64(row["tid"]))
            result.append(
                {
                    "time": ts.timeTime(),
                    "tid": p64(row["tid"]),
                    "serial": p64(row["tid"]),
                    "user_name": row["username"] or "",
                    "description": row["description"] or "",
                    "size": row["state_size"],
                }
            )
        return result

    def pack(self, t, referencesf):
        """Pack the storage — remove unreachable objects and S3 blobs."""
        from .packer import pack as do_pack

        pack_time = None
        if self._history_preserving and t is not None:
            # Convert float timestamp to TID bytes
            pack_time = TimeStamp(*(*time.gmtime(t)[:5], t % 60)).raw()
            self._pack_tid = u64(pack_time)
        _deleted_objects, _deleted_blobs, s3_keys = do_pack(
            self._conn,
            pack_time=pack_time,
            history_preserving=self._history_preserving,
        )
        # Clean up S3 blobs that were removed during pack
        if s3_keys and self._s3_client:
            for key in s3_keys:
                try:
                    self._s3_client.delete_object(key)
                except Exception:  # pragma: no cover
                    logger.warning("Failed to delete S3 blob: %s", key)

    # ── IStorageUndoable ─────────────────────────────────────────────

    def supportsUndo(self):
        """Undo is only supported in history-preserving mode."""
        return self._history_preserving

    def undoLog(self, first=0, last=-20, filter=None):  # noqa: A002
        """Return a list of transaction descriptions for undo.

        Returns list of dicts: {id, time, user_name, description}.
        """
        if not self._history_preserving:
            return []

        limit = -last if last < 0 else last - first

        with self._conn.cursor() as cur:
            if self._pack_tid is not None:
                cur.execute(
                    "SELECT tid, username, description, extension "
                    "FROM transaction_log "
                    "WHERE tid > %s "
                    "ORDER BY tid DESC "
                    "LIMIT %s OFFSET %s",
                    (self._pack_tid, limit, first),
                )
            else:
                cur.execute(
                    "SELECT tid, username, description, extension "
                    "FROM transaction_log "
                    "ORDER BY tid DESC "
                    "LIMIT %s OFFSET %s",
                    (limit, first),
                )
            rows = cur.fetchall()

        result = []
        for row in rows:
            username = row["username"] or ""
            description = row["description"] or ""
            # ZODB expects bytes for user_name/description
            if isinstance(username, str):
                username = username.encode("utf-8")
            if isinstance(description, str):
                description = description.encode("utf-8")
            d = {
                "id": p64(row["tid"]),
                "time": TimeStamp(p64(row["tid"])).timeTime(),
                "user_name": username,
                "description": description,
            }
            # Merge extension metadata into the result dict
            ext_dict = _deserialize_extension(row.get("extension"))
            if ext_dict:
                d.update(ext_dict)
            if filter is None or filter(d):
                result.append(d)
        return result

    def undo(self, transaction_id, transaction=None):
        """Undo a transaction by restoring previous object states.

        For each object modified in the undone transaction, find its
        previous revision in object_history and re-store that state.
        If the object was subsequently modified, attempt conflict
        resolution; raise UndoError if resolution fails.

        Returns (tid, [oid_bytes, ...]) for the new undo transaction.
        """
        if not self._history_preserving:
            raise UndoError("Undo is not supported in history-free mode")

        tid_int = u64(transaction_id)

        with self._conn.cursor() as cur:
            undo_data = _compute_undo(cur, tid_int, self, self._tmp)

        # Queue the undo data — will be written during _vote.
        # Replace any prior pending entry for the same zoid
        # (happens when multiple undos target the same objects).
        oid_list = []
        for item in undo_data:
            oid_bytes = p64(item["zoid"])
            oid_list.append(oid_bytes)
            # Remove any existing entry for this zoid
            self._tmp = [e for e in self._tmp if e.get("zoid") != item["zoid"]]
            if item["action"] == "delete":
                self._tmp.append(
                    {
                        "zoid": item["zoid"],
                        "action": "delete",
                    }
                )
            else:
                entry = {
                    "zoid": item["zoid"],
                    "class_mod": item["class_mod"],
                    "class_name": item["class_name"],
                    "state": item["state"],
                    "state_size": item["state_size"],
                    "refs": item["refs"],
                }
                # Run state processors so catalog columns are recomputed
                # from the restored state, not left as NULL.
                extra = self._process_state(
                    item["zoid"],
                    item["class_mod"],
                    item["class_name"],
                    item["state"],
                )
                if extra:
                    entry["_extra"] = extra
                self._tmp.append(entry)

        return self._tid, oid_list

    # ── IStorageIteration ─────────────────────────────────────────────

    def iterator(self, start=None, stop=None):
        """Iterate over transactions yielding TransactionRecord objects.

        Borrows a connection from the pool so iteration doesn't interfere
        with other storage operations.

        In history-free mode, each object appears once at its current TID.
        In history-preserving mode, all revisions are included.
        """
        conn = self._instance_pool.getconn()
        try:
            conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            table = "object_history" if self._history_preserving else "object_state"
            yield from _iter_transactions(conn, table, start, stop)
        finally:
            self._instance_pool.putconn(conn)

    # ── IStorageRestoreable ──────────────────────────────────────────

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        """Write pre-committed data without conflict checking.

        Used by copyTransactionsFrom / zodbconvert to import data
        from another storage.
        """
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if data is None:
            return  # undo of object creation
        zoid = u64(oid)
        class_mod, class_name, state, refs = (
            zodb_json_codec.decode_zodb_record_for_pg_json(data)
        )
        entry = {
            "zoid": zoid,
            "class_mod": class_mod,
            "class_name": class_name,
            "state": state,
            "state_size": len(data),
            "refs": refs,
        }
        extra = self._process_state(zoid, class_mod, class_name, state)
        if extra:
            entry["_extra"] = extra
        self._tmp.append(entry)

    def restoreBlob(self, oid, serial, data, blobfilename, prev_txn, transaction):
        """Restore object data + blob without conflict checking."""
        self.restore(oid, serial, data, "", prev_txn, transaction)
        if blobfilename is not None:
            zoid = u64(oid)
            staged = os.path.join(self._blob_temp_dir, f"{zoid:016x}.pending.blob")
            shutil.move(blobfilename, staged)
            self._blob_tmp[zoid] = staged

    # ── IBlobStorage ─────────────────────────────────────────────────

    def storeBlob(self, oid, oldserial, data, blobfilename, version, transaction):
        """Store object data + blob file (direct-use path)."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if version:
            raise TypeError("versions are not supported")
        self.store(oid, oldserial, data, "", transaction)
        # Stage the blob to a stable location — the caller (e.g. TmpStore
        # during savepoint commit) may delete the source after we return.
        zoid = u64(oid)
        staged = os.path.join(self._blob_temp_dir, f"{zoid:016x}.pending.blob")
        shutil.move(blobfilename, staged)
        self._blob_tmp[zoid] = staged

    def loadBlob(self, oid, serial):
        """Return path to a file containing the blob data.

        Uses deterministic filenames so repeated calls for the same
        (oid, serial) return the same path — required by ZODB.blob.Blob.
        """
        zoid = u64(oid)
        tid_int = u64(serial)
        # Check pending blobs first (staged in current txn, not yet in DB)
        pending = self._blob_tmp.get(zoid)
        if pending is not None and os.path.exists(pending):
            return pending
        path = os.path.join(self._blob_temp_dir, f"{zoid:016x}-{tid_int:016x}.blob")
        if os.path.exists(path):
            return path
        # Check S3 blob cache before hitting the database
        if self._blob_cache is not None:
            cached = self._blob_cache.get(oid, serial)
            if cached:
                return cached
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT data, s3_key FROM blob_state WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
            )
            row = cur.fetchone()
        if row is None:
            raise POSKeyError(oid)
        if row["s3_key"]:
            return _load_blob_from_s3(
                self._s3_client,
                self._blob_cache,
                row["s3_key"],
                oid,
                serial,
                path,
            )
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, row["data"])
        finally:
            os.close(fd)
        return path

    def openCommittedBlobFile(self, oid, serial, blob=None):
        """Open committed blob file for reading."""
        blob_path = self.loadBlob(oid, serial)
        if blob is None:
            return open(blob_path, "rb")
        from ZODB.blob import BlobFile

        return BlobFile(blob_path, "r", blob)

    def temporaryDirectory(self):
        """Return directory for uncommitted blob data."""
        return self._blob_temp_dir

    # ── IStorage: close ──────────────────────────────────────────────

    def close(self):
        """Close all database connections, pool, and clean up temp dir."""
        if self._conn and not self._conn.closed:
            self._conn.close()
        if hasattr(self, "_instance_pool"):
            self._instance_pool.close()
        if os.path.exists(self._blob_temp_dir):
            shutil.rmtree(self._blob_temp_dir, ignore_errors=True)

    def cleanup(self):
        """Remove all data (used by tests)."""
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM blob_state")
            cur.execute("DELETE FROM object_state")
            if self._history_preserving:
                if _table_exists(cur, "blob_history"):
                    cur.execute("DELETE FROM blob_history")
                cur.execute("DELETE FROM object_history")
                cur.execute("DELETE FROM pack_state")
            cur.execute("DELETE FROM transaction_log")
        self._conn.commit()
        self._ltid = z64
        self._oid = z64

    def compact_history(self):
        """Remove duplicate entries created by the old dual-write mode.

        In the old storage model, every store() wrote the same data to both
        ``object_state`` and ``object_history`` (and ``blob_state`` /
        ``blob_history``).  The optimized model only keeps *previous* versions
        in ``object_history`` and eliminates ``blob_history`` entirely.

        This method removes the overlap so that ``object_history`` contains
        only previous versions, and truncates ``blob_history``.  The UNION
        queries used by the optimized read paths handle the overlap correctly,
        so running this is optional — but it reclaims significant disk space.

        Returns:
            Tuple of (deleted_object_history_rows, deleted_blob_history_rows).
        """
        if not self._history_preserving:
            return 0, 0

        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM object_history oh "
                "WHERE EXISTS ("
                "  SELECT 1 FROM object_state os "
                "  WHERE os.zoid = oh.zoid AND os.tid = oh.tid"
                ")"
            )
            deleted_objects = cur.rowcount

            deleted_blobs = 0
            if _table_exists(cur, "blob_history"):
                cur.execute("DELETE FROM blob_history")
                deleted_blobs = cur.rowcount

        self._conn.commit()

        logger.info(
            "compact_history: removed %d object_history rows, %d blob_history rows",
            deleted_objects,
            deleted_blobs,
        )
        return deleted_objects, deleted_blobs

    def convert_to_history_free(self):
        """Convert a history-preserving database to history-free mode.

        Drops ``object_history``, ``pack_state``, and the deprecated
        ``blob_history`` table.  Also removes old blob versions from
        ``blob_state`` (keeps only the latest tid per zoid) and orphaned
        ``transaction_log`` entries.

        This is an offline maintenance operation — no ZODB connections
        should be active while it runs.  After calling this method,
        reconfigure the storage with ``history_preserving=False`` and
        restart.

        Returns:
            Dict with removal counts (see :func:`drop_history_tables`).

        Raises:
            RuntimeError: If no history tables exist to clean up.
        """
        if not schema_table_exists(self._conn, "object_history"):
            raise RuntimeError(
                "No history tables found — database is already history-free."
            )
        counts = drop_history_tables(self._conn)
        logger.info(
            "convert_to_history_free: removed %d history rows, "
            "%d pack rows, %d blob_history rows, "
            "%d old blob versions, %d orphan transactions",
            counts["history_rows"],
            counts["pack_rows"],
            counts["blob_history_rows"],
            counts["old_blob_versions"],
            counts["orphan_transactions"],
        )
        return counts

    def convert_to_history_preserving(self):
        """Convert a history-free database to history-preserving mode.

        Creates the ``object_history`` and ``pack_state`` tables.
        Existing objects will only gain history tracking on their next
        modification.

        After calling this method, reconfigure the storage with
        ``history_preserving=True`` and restart.

        Raises:
            RuntimeError: If history tables already exist.
        """
        if schema_table_exists(self._conn, "object_history"):
            raise RuntimeError(
                "History tables already exist — database is already history-preserving."
            )
        self._conn.execute(HISTORY_PRESERVING_ADDITIONS)
        self._conn.commit()
        logger.info(
            "convert_to_history_preserving: created object_history "
            "and pack_state tables"
        )


_DSN_PASSWORD_RE = re.compile(r"(password\s*=\s*)(\S+|'[^']*')", re.IGNORECASE)


def _table_exists(cur, table_name):
    """Check if a table exists in the current schema."""
    cur.execute(
        "SELECT to_regclass(%s) IS NOT NULL AS exists",
        (table_name,),
    )
    row = cur.fetchone()
    # Support both tuple rows and dict rows
    return row["exists"] if isinstance(row, dict) else row[0]


def _mask_dsn(dsn):
    """Mask password in a PostgreSQL DSN string for safe logging."""
    return _DSN_PASSWORD_RE.sub(r"\1***", dsn)


def _load_blob_from_s3(s3_client, blob_cache, s3_key, oid, serial, path):
    """Download a blob from S3 and optionally cache it locally.

    Args:
        s3_client: S3Client instance
        blob_cache: S3BlobCache instance or None
        s3_key: S3 key string
        oid: OID bytes
        serial: TID bytes
        path: local file path to write to

    Returns:
        Path to the local blob file.
    """
    s3_client.download_file(s3_key, path)
    if blob_cache is not None:
        blob_cache.put(oid, serial, path)
    return path


def _do_loadSerial(conn, serial_cache, history_preserving, oid, serial):
    """Load a specific revision of an object (shared implementation).

    Used by both PGJsonbStorage and PGJsonbStorageInstance.
    """
    cached = serial_cache.get((oid, serial))
    if cached is not None:
        return cached

    zoid = u64(oid)
    tid_int = u64(serial)
    with conn.cursor() as cur:
        if history_preserving:
            cur.execute(
                "SELECT class_mod, class_name, state FROM ("
                "  SELECT class_mod, class_name, state"
                "  FROM object_history WHERE zoid = %s AND tid = %s"
                "  UNION"
                "  SELECT class_mod, class_name, state"
                "  FROM object_state WHERE zoid = %s AND tid = %s"
                ") sub LIMIT 1",
                (zoid, tid_int, zoid, tid_int),
                prepare=True,
            )
        else:
            cur.execute(
                "SELECT class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
                prepare=True,
            )
        row = cur.fetchone()

    if row is None:
        raise POSKeyError(oid)

    record = {
        "@cls": [row["class_mod"], row["class_name"]],
        "@s": _unsanitize_from_pg(row["state"]),
    }
    return zodb_json_codec.encode_zodb_record(record)


def _loadBefore_hf(cur, oid, zoid, tid_int):
    """Load object data before tid — history-free mode.

    In history-free mode there's only one revision per object (in
    object_state).  Return it if its tid < requested tid, else None.
    """
    cur.execute(
        "SELECT tid, class_mod, class_name, state FROM object_state WHERE zoid = %s",
        (zoid,),
        prepare=True,
    )
    row = cur.fetchone()
    if row is None:
        raise POSKeyError(oid)
    if row["tid"] >= tid_int:
        return None
    record = {
        "@cls": [row["class_mod"], row["class_name"]],
        "@s": _unsanitize_from_pg(row["state"]),
    }
    data = zodb_json_codec.encode_zodb_record(record)
    return data, p64(row["tid"]), None


def _loadBefore_hp(cur, oid, zoid, tid_int):
    """Load object data before tid — history-preserving mode.

    Queries object_history + object_state for the most recent revision with
    tid < requested tid.  Also looks up end_tid (the next revision's tid)
    so ZODB can cache the validity window.
    """
    cur.execute(
        "SELECT tid, class_mod, class_name, state FROM ("
        "  SELECT tid, class_mod, class_name, state"
        "  FROM object_history WHERE zoid = %s AND tid < %s"
        "  UNION"
        "  SELECT tid, class_mod, class_name, state"
        "  FROM object_state WHERE zoid = %s AND tid < %s"
        ") sub ORDER BY tid DESC LIMIT 1",
        (zoid, tid_int, zoid, tid_int),
        prepare=True,
    )
    row = cur.fetchone()
    if row is None:
        # No revision before this tid — either doesn't exist or created later
        return None
    if row["state"] is None:
        # Tombstone/zombie record (undo of creation) — object doesn't exist
        raise POSKeyError(oid)
    record = {
        "@cls": [row["class_mod"], row["class_name"]],
        "@s": _unsanitize_from_pg(row["state"]),
    }
    data = zodb_json_codec.encode_zodb_record(record)
    start_tid = p64(row["tid"])
    # Find end_tid: next revision's tid
    cur.execute(
        "SELECT MIN(tid) AS next_tid FROM ("
        "  SELECT tid FROM object_history WHERE zoid = %s AND tid > %s"
        "  UNION ALL"
        "  SELECT tid FROM object_state WHERE zoid = %s AND tid > %s"
        ") sub",
        (zoid, row["tid"], zoid, row["tid"]),
        prepare=True,
    )
    next_row = cur.fetchone()
    end_tid = p64(next_row["next_tid"]) if next_row["next_tid"] else None
    return data, start_tid, end_tid


def _iter_transactions(conn, table, start, stop):
    """Yield PGTransactionRecord objects for each transaction.

    Iterates from transaction_log (authoritative list of all transactions)
    and joins to the object table for records.  In history-preserving mode,
    queries both object_history and object_state via UNION.  In history-free
    mode, old transactions whose objects were all updated later will yield
    with an empty record list — this is correct and preserves transaction
    metadata for zodbconvert.

    Args:
        conn: psycopg connection (dedicated for iteration)
        table: 'object_state' (HF) or 'object_history' (HP)
        start: start TID bytes or None
        stop: stop TID bytes or None
    """
    conditions = []
    params = []
    if start is not None:
        conditions.append("tid >= %s")
        params.append(u64(start))
    if stop is not None:
        conditions.append("tid <= %s")
        params.append(u64(stop))

    where = " AND ".join(conditions) if conditions else "TRUE"
    hp = table == "object_history"

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT tid, username, description, extension "
            f"FROM transaction_log WHERE {where} ORDER BY tid",
            params,
        )
        txn_rows = cur.fetchall()

        for txn_row in txn_rows:
            tid_int = txn_row["tid"]
            tid_bytes = p64(tid_int)

            if hp:
                cur.execute(
                    "SELECT zoid, class_mod, class_name, state "
                    "FROM object_history WHERE tid = %s "
                    "UNION "
                    "SELECT zoid, class_mod, class_name, state "
                    "FROM object_state WHERE tid = %s",
                    (tid_int, tid_int),
                )
            else:
                cur.execute(
                    "SELECT zoid, class_mod, class_name, state "
                    "FROM object_state WHERE tid = %s",
                    (tid_int,),
                )
            obj_rows = cur.fetchall()

            records = []
            for obj_row in obj_rows:
                if obj_row["state"] is None:
                    # Zombie/deleted object (undo of creation)
                    data = None
                else:
                    record = {
                        "@cls": [obj_row["class_mod"], obj_row["class_name"]],
                        "@s": _unsanitize_from_pg(obj_row["state"]),
                    }
                    data = zodb_json_codec.encode_zodb_record(record)
                records.append(
                    DataRecord(
                        p64(obj_row["zoid"]),
                        tid_bytes,
                        data,
                        None,
                    )
                )

            # Deserialize extension bytes (stored as JSON) back to dict.
            # TransactionMetaData expects a dict for .extension property.
            ext_dict = _deserialize_extension(txn_row["extension"])

            yield PGTransactionRecord(
                tid_bytes,
                " ",
                txn_row["username"] or "",
                txn_row["description"] or "",
                ext_dict,
                records,
            )
