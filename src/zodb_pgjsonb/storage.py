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
from .batch import _write_prepared_transaction
from .batch import _write_txn_log
from .conflict import _batch_check_read_conflicts
from .conflict import _batch_resolve_conflicts
from .interfaces import IPGJsonbStorage
from .migration import _fmt_elapsed
from .migration import _stage_blob
from .schema import _table_exists as schema_table_exists
from .schema import drop_history_tables
from .schema import HISTORY_PRESERVING_ADDITIONS
from .schema import install_schema
from .serialization import _deserialize_extension
from .serialization import _unsanitize_from_pg
from .undo import _compute_undo
from collections import OrderedDict
from persistent.TimeStamp import TimeStamp
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from ZODB.BaseStorage import BaseStorage
from ZODB.BaseStorage import DataRecord
from ZODB.BaseStorage import TransactionRecord
from ZODB.blob import is_blob_record
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
import traceback
import zodb_json_codec
import zope.interface


logger = logging.getLogger(__name__)


_VALID_SQL_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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

# Logarithmic bucket boundaries for blob size histograms.
_HISTOGRAM_BOUNDARIES = [
    10240,  # 10 KB
    102400,  # 100 KB
    1048576,  # 1 MB
    10485760,  # 10 MB
    104857600,  # 100 MB
    1073741824,  # 1 GB
]


def _format_size(size_bytes):
    """Format a byte count as a human-readable string."""
    if size_bytes == 0:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            if unit == "B":
                return f"{size_bytes} B"
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


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
class PGJsonbStorage(ConflictResolvingStorage, BaseStorage):
    """ZODB storage that stores object state as JSONB in PostgreSQL.

    Implements IMVCCStorage: ZODB.DB uses new_instance() to create
    per-connection storage instances with independent snapshots.

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
        cache_local_mb=DEFAULT_CACHE_LOCAL_MB,
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
        self._cache_local_mb = cache_local_mb
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

        # Load cache: zoid → (pickle_bytes, tid_bytes), bounded LRU
        self._load_cache = LoadCache(max_mb=cache_local_mb)

        # Cache for conflict resolution: (oid_bytes, tid_bytes) → pickle_bytes
        # In history-free mode, loadSerial can't find old versions after they're
        # overwritten. Caching data from load() makes it available for
        # tryToResolveConflict's loadSerial(oid, oldSerial) calls.
        self._serial_cache = {}

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
            configure=lambda conn: setattr(conn, "autocommit", True),
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
            estimated_objects = int(cache_local_mb * 1_000_000 / avg_size)
            target = max(1, int(estimated_objects * cache_warm_pct / 100))
            self._warmer = CacheWarmer(
                self._conn, target_count=target, decay=cache_warm_decay
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
        """Apply DDL from a state processor, handling lock conflicts.

        ALTER TABLE needs ACCESS EXCLUSIVE which conflicts with
        ACCESS SHARE held by REPEATABLE READ pool connections.

        During Zope startup, a ZODB Connection loads objects via
        REPEATABLE READ, holding ACCESS SHARE on object_state.
        The IDatabaseOpenedWithRoot subscriber fires while that
        read transaction is still open — so DDL would deadlock.

        Strategy: try with a short lock_timeout.  If blocked,
        defer the DDL.  It will be applied in tpc_begin() after
        the read transaction is committed.
        """
        try:
            with psycopg.connect(self._dsn, autocommit=True) as ddl_conn:
                ddl_conn.execute("SET lock_timeout = '2s'")
                ddl_conn.execute(sql)
            logger.info("Applied schema DDL from %s", processor_name)
        except psycopg.errors.LockNotAvailable:
            logger.info(
                "DDL from %s deferred (lock conflict at startup). "
                "Will apply on first write transaction.",
                processor_name,
            )
            self._pending_ddl.append((sql, processor_name))

    def _apply_pending_ddl(self):
        """Apply any deferred DDL.  Called from tpc_begin() after
        the read transaction is committed (ACCESS SHARE released).
        """
        if not self._pending_ddl:
            return
        pending = self._pending_ddl[:]
        self._pending_ddl.clear()
        for sql, name in pending:
            try:
                with psycopg.connect(self._dsn, autocommit=True) as ddl_conn:
                    ddl_conn.execute("SET lock_timeout = '30s'")
                    ddl_conn.execute(sql)
                logger.info("Applied deferred schema DDL from %s", name)
            except Exception:
                logger.warning(
                    "Failed to apply deferred DDL from %s "
                    "(lock timeout or other error — will retry on next startup)",
                    name,
                    exc_info=True,
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
        """Return blob storage statistics.

        Returns a dict with keys: available, total_blobs, unique_objects,
        avg_versions, total_size(_display), pg_count, pg_size(_display),
        s3_count, s3_size(_display), largest_blob(_display),
        avg_blob_size(_display).
        """
        with self._conn.cursor() as cur:
            cur.execute("SELECT to_regclass('blob_state') IS NOT NULL AS exists")
            if not cur.fetchone()["exists"]:
                return {"available": False}

            cur.execute(
                "SELECT"
                "  COUNT(*) AS total_blobs,"
                "  COUNT(DISTINCT zoid) AS unique_objects,"
                "  COALESCE(SUM(blob_size), 0) AS total_size,"
                "  COUNT(*) FILTER (WHERE data IS NOT NULL) AS pg_count,"
                "  COALESCE(SUM(blob_size) FILTER (WHERE data IS NOT NULL), 0)"
                "    AS pg_size,"
                "  COUNT(*) FILTER (WHERE s3_key IS NOT NULL AND data IS NULL)"
                "    AS s3_count,"
                "  COALESCE(SUM(blob_size)"
                "    FILTER (WHERE s3_key IS NOT NULL AND data IS NULL), 0)"
                "    AS s3_size,"
                "  COALESCE(MAX(blob_size), 0) AS largest_blob,"
                "  COALESCE(AVG(blob_size)::bigint, 0) AS avg_blob_size"
                " FROM blob_state"
            )
            row = cur.fetchone()

        total_blobs = row["total_blobs"]
        unique_objects = row["unique_objects"]
        avg_versions = (
            round(total_blobs / unique_objects, 1) if unique_objects > 0 else 0
        )

        return {
            "available": True,
            "total_blobs": total_blobs,
            "unique_objects": unique_objects,
            "total_size": row["total_size"],
            "total_size_display": _format_size(row["total_size"]),
            "pg_count": row["pg_count"],
            "pg_size": row["pg_size"],
            "pg_size_display": _format_size(row["pg_size"]),
            "s3_count": row["s3_count"],
            "s3_size": row["s3_size"],
            "s3_size_display": _format_size(row["s3_size"]),
            "largest_blob": row["largest_blob"],
            "largest_blob_display": _format_size(row["largest_blob"]),
            "avg_blob_size": row["avg_blob_size"],
            "avg_blob_size_display": _format_size(row["avg_blob_size"]),
            "avg_versions": avg_versions,
            "s3_configured": self._s3_client is not None,
            "blob_threshold": self._blob_threshold,
            "blob_threshold_display": _format_size(self._blob_threshold),
        }

    def get_blob_histogram(self):
        """Return blob size distribution as logarithmic buckets.

        Returns a list of dicts with keys: label, count, pct, tier.
        tier is 'pg', 's3', or 'mixed' based on the S3 blob threshold.
        Empty list if no blobs exist.
        """
        with self._conn.cursor() as cur:
            cur.execute("SELECT to_regclass('blob_state') IS NOT NULL AS exists")
            if not cur.fetchone()["exists"]:
                return []

            cur.execute(
                "SELECT MIN(blob_size) AS min_size,"
                "  MAX(blob_size) AS max_size,"
                "  COUNT(*) AS cnt"
                " FROM blob_state"
            )
            info = cur.fetchone()
            if info["cnt"] == 0:
                return []

            # Build boundaries that cover the actual data range
            boundaries = [0]
            for b in _HISTOGRAM_BOUNDARIES:
                boundaries.append(b)
                if b > info["max_size"]:
                    break
            else:
                # max_size exceeds all predefined boundaries
                boundaries.append(info["max_size"] + 1)

            # Single SQL: count per bucket using CASE/WHEN
            cases = []
            params = {}
            for i in range(len(boundaries) - 1):
                lo_key = f"lo_{i}"
                hi_key = f"hi_{i}"
                cases.append(
                    f"COUNT(*) FILTER ("
                    f"WHERE blob_size >= %({lo_key})s"
                    f"  AND blob_size < %({hi_key})s"
                    f") AS bucket_{i}"
                )
                params[lo_key] = boundaries[i]
                params[hi_key] = boundaries[i + 1]

            cur.execute(
                f"SELECT {', '.join(cases)} FROM blob_state",
                params,
            )
            row = cur.fetchone()

        max_count = max(row[f"bucket_{i}"] for i in range(len(boundaries) - 1))
        max_count = max_count or 1
        threshold = (
            self._blob_threshold
            if getattr(self, "_s3_client", None) is not None
            else None
        )
        buckets = []
        for i in range(len(boundaries) - 1):
            lo = boundaries[i]
            hi = boundaries[i + 1]
            count = row[f"bucket_{i}"]
            pct = round(count / max_count * 100)
            # Classify tier based on S3 threshold
            if threshold is None:
                tier = ""
            elif hi <= threshold:
                tier = "pg"
            elif lo >= threshold:
                tier = "s3"
            else:
                tier = "mixed"
            buckets.append(
                {
                    "label": f"{_format_size(lo)} – {_format_size(hi)}",
                    "count": count,
                    "pct": pct,
                    "tier": tier,
                }
            )
        return buckets

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

    def copyTransactionsFrom(
        self, other, workers=1, start_tid=None, blob_mode="inline"
    ):
        """Copy all transactions from another storage, including blobs.

        Overrides BaseStorage.copyTransactionsFrom to correctly handle
        blob records.  The default implementation only calls restore()
        and silently drops blob data.

        Blob files are copied (not moved) from the source storage so
        the source remains intact after migration.

        When *workers* > 1, uses parallel PG connections to write
        transactions concurrently.  The main thread handles source
        iteration and pickle decoding; worker threads handle PG writes.
        OID ordering is preserved by the main-thread orchestrator.

        When *start_tid* is given (bytes), only transactions with
        ``tid >= start_tid`` are copied.  This allows resuming an
        interrupted import from the last successfully written TID.

        *blob_mode* controls S3 blob uploads:
        - ``"inline"`` (default): upload synchronously in worker threads
        - ``"background"``: upload via a background thread pool
        - ``"deferred:/path/to/manifest.tsv"``: write manifest for later upload
        """
        if workers > 1:
            return self._copyTransactionsFrom_parallel(
                other,
                workers,
                start_tid=start_tid,
                blob_mode=blob_mode,
            )
        return self._copyTransactionsFrom_sequential(other, start_tid=start_tid)

    def _prepare_transaction(self, txn_info, source_storage):
        """Decode all records in a transaction for batch writing.

        Iterates records, decodes pickle → JSON via the Rust codec,
        runs state processors, and copies blob files to temp paths.

        Returns a dict ready for ``_write_prepared_transaction()``.
        """
        tid = txn_info.tid
        tid_int = u64(tid)
        source_has_blobs = IBlobStorage.providedBy(source_storage)

        objects = []
        blobs = []  # list of (zoid, temp_blob_path, is_temp)
        byte_size = 0
        missing_blobs = 0

        for record in txn_info:
            if record.data is None:
                continue

            zoid = u64(record.oid)
            class_mod, class_name, state, refs = (
                zodb_json_codec.decode_zodb_record_for_pg_json(record.data)
            )
            entry = {
                "zoid": zoid,
                "class_mod": class_mod,
                "class_name": class_name,
                "state": state,
                "state_size": len(record.data),
                "refs": refs,
            }
            extra = self._process_state(zoid, class_mod, class_name, state)
            if extra:
                entry["_extra"] = extra
            objects.append(entry)

            # Blob handling
            if source_has_blobs and is_blob_record(record.data):
                try:
                    blob_filename = source_storage.loadBlob(record.oid, record.tid)
                except (POSKeyError, KeyError, OSError) as exc:
                    blob_filename = None
                    missing_blobs += 1
                    logger.warning(
                        "Missing blob for oid=0x%016x tid=0x%016x: %s",
                        zoid,
                        tid_int,
                        exc,
                    )
                if blob_filename is not None:
                    path, is_temp = _stage_blob(blob_filename, self._blob_temp_dir)
                    blobs.append((zoid, path, is_temp))

            if record.data:
                byte_size += len(record.data)

        user = txn_info.user
        desc = txn_info.description
        if isinstance(user, bytes):
            user = user.decode("utf-8")
        if isinstance(desc, bytes):
            desc = desc.decode("utf-8")

        return {
            "tid": tid,
            "tid_int": tid_int,
            "user": user,
            "description": desc,
            "extension": txn_info.extension,
            "objects": objects,
            "blobs": blobs,
            "byte_size": byte_size,
            "missing_blobs": missing_blobs,
        }

    def _copyTransactionsFrom_sequential(self, other, start_tid=None):
        """Sequential copy — one transaction at a time via TPC protocol."""
        begin_time = time.time()
        txnum = 0
        total_size = 0
        total_missing_blobs = 0
        logger.info("Copying transactions (sequential) ...")
        for txn_info in other.iterator(start=start_tid):
            txnum += 1
            self.tpc_begin(txn_info, txn_info.tid, txn_info.status)
            num_txn_records = 0
            for record in txn_info:
                if record.data is None:
                    continue
                blobfilename = None
                if is_blob_record(record.data):
                    try:
                        blobfilename = other.loadBlob(record.oid, record.tid)
                    except (POSKeyError, KeyError, OSError) as exc:
                        total_missing_blobs += 1
                        logger.warning(
                            "Missing blob for oid=0x%016x tid=0x%016x: %s",
                            u64(record.oid),
                            u64(record.tid),
                            exc,
                        )
                if blobfilename is not None:
                    # restoreBlob moves the file, so we must stage it
                    # (hard link or copy) to preserve the source blob.
                    path, is_temp = _stage_blob(blobfilename, self._blob_temp_dir)
                    if not is_temp:
                        # Cross-device: must copy since restoreBlob moves
                        fd, tmp = tempfile.mkstemp(
                            suffix=".blob", dir=self._blob_temp_dir
                        )
                        os.close(fd)
                        shutil.copy2(blobfilename, tmp)
                        path = tmp
                    self.restoreBlob(
                        record.oid,
                        record.tid,
                        record.data,
                        path,
                        record.data_txn,
                        txn_info,
                    )
                else:
                    self.restore(
                        record.oid,
                        record.tid,
                        record.data,
                        "",
                        record.data_txn,
                        txn_info,
                    )
                num_txn_records += 1
                if record.data:
                    total_size += len(record.data)
            self.tpc_vote(txn_info)
            self.tpc_finish(txn_info)
            elapsed = time.time() - begin_time
            rate = total_size / 1_000_000 / elapsed if elapsed else 0
            logger.info(
                "Copied tid %d,%5d records | %6.3f MB/s (%6d txns)",
                u64(txn_info.tid),
                num_txn_records,
                rate,
                txnum,
            )
        elapsed = time.time() - begin_time
        logger.info(
            "All %d transactions copied successfully in %4.1f minutes.",
            txnum,
            elapsed / 60.0,
        )
        if total_missing_blobs:
            logger.error(
                "%d blob(s) missing in source storage and skipped during import!",
                total_missing_blobs,
            )

    def _copyTransactionsFrom_parallel(
        self, other, num_workers, start_tid=None, blob_mode="inline"
    ):
        """Parallel copy — N worker threads write to PG concurrently.

        The main thread reads from the source storage, decodes pickles,
        and dispatches pre-decoded transaction dicts to worker threads.
        OID ordering is guaranteed: if a transaction touches an OID that
        is still being written by a worker, the main thread waits for
        that worker to finish before dispatching.
        """
        from concurrent.futures import ThreadPoolExecutor
        from concurrent.futures import wait as futures_wait

        begin_time = time.time()

        pool_max = self._instance_pool.max_size
        # Reserve 1 connection for the watermark tracker.
        max_workers = max(pool_max - 1, 1)
        if num_workers > max_workers:
            logger.warning(
                "Requested %d workers but pool_max_size is %d "
                "(reserving 1 for watermark). Using %d workers.",
                num_workers,
                pool_max,
                max_workers,
            )
            num_workers = max_workers

        # Total OIDs for progress % (O(1) for FileStorage).
        total_oids = 0
        with contextlib.suppress(TypeError):
            total_oids = len(other)
        logger.info(
            "Copying transactions with %d parallel workers (total OIDs: %s) ...",
            num_workers,
            f"{total_oids:,}" if total_oids else "unknown",
        )

        extra_columns = self._get_extra_columns()
        hp = self._history_preserving
        processors = list(self._state_processors)

        # ── Watermark setup ──────────────────────────────────────────
        idempotent = start_tid is not None
        wm_conn = self._instance_pool.getconn()
        try:
            wm_conn.execute(
                "CREATE TABLE IF NOT EXISTS migration_watermark ("
                "  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),"
                "  tid BIGINT NOT NULL"
                ")"
            )
            # Read existing watermark (from interrupted previous run)
            with wm_conn.cursor() as cur:
                cur.execute("SELECT tid FROM migration_watermark WHERE id = TRUE")
                row = cur.fetchone()
            if row is not None and start_tid is not None:
                watermark_tid = row["tid"]
                effective_start_int = min(u64(start_tid), watermark_tid + 1)
                effective_start = p64(effective_start_int)
                logger.info(
                    "Resuming from watermark TID 0x%016x "
                    "(start_tid=0x%016x, effective=0x%016x)",
                    watermark_tid,
                    u64(start_tid),
                    effective_start_int,
                )
            elif start_tid is not None:
                effective_start = start_tid
                logger.info(
                    "Starting incremental copy from TID 0x%016x",
                    u64(start_tid),
                )
            else:
                effective_start = None
        except BaseException:
            self._instance_pool.putconn(wm_conn)
            raise

        # ── BlobSink setup ─────────────────────────────────────────
        from zodb_pgjsonb.blob_sink import BackgroundBlobSink
        from zodb_pgjsonb.blob_sink import DeferredBlobSink
        from zodb_pgjsonb.blob_sink import InlineBlobSink

        if blob_mode == "background" and self._s3_client is not None:
            blob_sink = BackgroundBlobSink(self._s3_client)
        elif isinstance(blob_mode, str) and blob_mode.startswith("deferred:"):
            manifest_path = blob_mode.split(":", 1)[1]
            blob_sink = DeferredBlobSink(manifest_path)
        elif self._s3_client is not None:
            blob_sink = InlineBlobSink(self._s3_client)
        else:
            blob_sink = None

        # Thread-local PG connections — one per worker thread.
        _local = threading.local()
        worker_conns = []
        _conn_lock = threading.Lock()

        def _get_worker_conn():
            if not hasattr(_local, "conn"):
                conn = self._instance_pool.getconn()
                _local.conn = conn
                with _conn_lock:
                    worker_conns.append(conn)
            return _local.conn

        # Backpressure: limit in-flight prepared transactions so blob
        # temp files don't accumulate unbounded (important for large blobs).
        _dispatch_sem = threading.BoundedSemaphore(num_workers * 2)

        # Track completed work for accurate ETA (written to PG/S3, not just
        # dispatched).  Workers increment after commit; main thread reads for
        # progress.  Using a simple int + Lock — the lock is held for
        # microseconds per transaction, contention negligible with 6 workers.
        _written_txns = 0
        _written_objs = 0
        _written_blobs = 0
        _written_errors = 0
        _written_lock = threading.Lock()

        def _write_worker(txn_data):
            nonlocal _written_txns, _written_objs, _written_blobs, _written_errors
            try:
                conn = _get_worker_conn()
                _write_prepared_transaction(
                    conn,
                    txn_data,
                    hp,
                    extra_columns,
                    processors,
                    blob_sink=blob_sink,
                    blob_threshold=self._blob_threshold,
                    idempotent=idempotent,
                )
                n_objs = len(txn_data["objects"])
                n_blobs = len(txn_data["blobs"])
                with _written_lock:
                    _written_txns += 1
                    _written_objs += n_objs
                    _written_blobs += n_blobs
            except Exception:
                logger.error(
                    "Worker error on tid=0x%016x: %s",
                    txn_data["tid_int"],
                    traceback.format_exc(),
                )
                with _written_lock:
                    _written_errors += 1
                raise
            finally:
                _dispatch_sem.release()

        # OID dependency tracking: zoid → Future currently writing it.
        in_flight = {}
        txn_count = 0
        total_missing_blobs = 0
        last_tid = None
        last_log_time = begin_time
        log_interval = 10.0  # seconds
        # Column width for right-aligned counters (based on total_oids).
        _cw = len(f"{total_oids:,}") if total_oids else 0
        # ETA: exponential moving average of OID throughput rate.
        # Smoothing factor alpha=0.3 balances responsiveness vs stability.
        _prev_done_objs = 0
        _prev_done_time = begin_time
        _ema_rate = 0.0  # OIDs/sec, smoothed
        _EMA_ALPHA = 0.3

        # Watermark tracking: ordered list of dispatched (tid_int, future).
        dispatched = []  # [(tid_int, future), ...]
        watermark_idx = 0
        _last_wm_flush = begin_time
        _wm_flush_interval = 1.0  # seconds

        executor = ThreadPoolExecutor(max_workers=num_workers)
        try:
            for txn_info in other.iterator(start=effective_start):
                txn_data = self._prepare_transaction(txn_info, other)
                txn_oids = {obj["zoid"] for obj in txn_data["objects"]}
                # Include blob OIDs (they reference the same zoid)
                txn_oids.update(entry[0] for entry in txn_data["blobs"])

                # Wait for any in-flight transactions touching same OIDs.
                blocking = set()
                for oid in txn_oids:
                    fut = in_flight.get(oid)
                    if fut is not None and not fut.done():
                        blocking.add(fut)
                if blocking:
                    futures_wait(blocking)
                    # Re-raise worker exceptions early.
                    with _written_lock:
                        errors = _written_errors
                    if errors:
                        raise RuntimeError(
                            f"Aborting: {errors} worker error(s). "
                            "Check log for details. "
                            "Is the target database empty?"
                        )

                # Backpressure: block if too many txns queued (limits
                # temp blob disk usage to num_workers * 2 transactions).
                _dispatch_sem.acquire()
                future = executor.submit(_write_worker, txn_data)
                for oid in txn_oids:
                    in_flight[oid] = future

                dispatched.append((txn_data["tid_int"], future))

                # Advance watermark: highest contiguous committed TID.
                while watermark_idx < len(dispatched):
                    _wm_tid, _wm_fut = dispatched[watermark_idx]
                    if not _wm_fut.done():
                        break
                    _wm_fut.result()  # re-raise worker errors
                    watermark_idx += 1

                # Flush watermark to PG periodically.
                now_wm = time.time()
                if watermark_idx > 0 and now_wm - _last_wm_flush >= _wm_flush_interval:
                    _wm_tid_val = dispatched[watermark_idx - 1][0]
                    wm_conn.execute(
                        "INSERT INTO migration_watermark (id, tid) "
                        "VALUES (TRUE, %s) "
                        "ON CONFLICT (id) DO UPDATE SET tid = EXCLUDED.tid",
                        (_wm_tid_val,),
                    )
                    _last_wm_flush = now_wm

                # Prune dispatched list to avoid unbounded memory.
                if watermark_idx > 1000:
                    dispatched = dispatched[watermark_idx:]
                    watermark_idx = 0

                txn_count += 1
                total_missing_blobs += txn_data["missing_blobs"]
                last_tid = txn_data["tid"]

                # Abort early on worker errors (e.g. duplicate keys
                # from a previous non-cleaned import).
                with _written_lock:
                    errors = _written_errors
                if errors:
                    raise RuntimeError(
                        f"Aborting: {errors} worker error(s). "
                        "Check log for details. "
                        "Is the target database empty?"
                    )

                now = time.time()
                if now - last_log_time >= log_interval:  # pragma: no cover
                    elapsed = now - begin_time
                    hms = _fmt_elapsed(elapsed)
                    with _written_lock:
                        done_txns = _written_txns
                        done_objs = _written_objs
                        done_blobs = _written_blobs
                        errors = _written_errors
                    if _cw:
                        label = (
                            f"[{hms}] Read {txn_count:>{_cw},} txns |"
                            f" Written {done_txns:>{_cw},} txns,"
                            f" {done_objs:>{_cw},} OIDs,"
                            f" {done_blobs:>{_cw},} blobs"
                        )
                    else:
                        label = (
                            f"[{hms}] Read {txn_count:,} txns |"
                            f" Written {done_txns:,} txns,"
                            f" {done_objs:,} OIDs,"
                            f" {done_blobs:,} blobs"
                        )
                    if errors:
                        label += f" ({errors} errors!)"
                    parts = [label]
                    if total_oids and done_objs > 0:
                        pct = min(done_objs * 100.0 / total_oids, 99.9)
                        parts[0] += f" ({pct:5.1f}%)"
                        # ETA from exponential moving average of throughput.
                        # Smooths out spikes from S3 retries or variable
                        # transaction sizes while still adapting over time.
                        dt = now - _prev_done_time
                        d_objs = done_objs - _prev_done_objs
                        if dt > 0 and d_objs > 0:
                            window_rate = d_objs / dt
                            if _ema_rate <= 0:
                                _ema_rate = window_rate  # seed
                            else:
                                _ema_rate = (
                                    _EMA_ALPHA * window_rate
                                    + (1 - _EMA_ALPHA) * _ema_rate
                                )
                            remaining = total_oids - done_objs
                            eta_s = remaining / _ema_rate
                            if eta_s < 60:
                                eta = f"{eta_s:.0f}s"
                            elif eta_s < 3600:
                                eta = f"{eta_s / 60:.0f}m"
                            else:
                                eta = f"{eta_s / 3600:.1f}h"
                            parts.append(f"ETA: {eta}")
                    _prev_done_objs = done_objs
                    _prev_done_time = now
                    logger.info(" | ".join(parts))
                    last_log_time = now

            # Wait for all remaining workers to finish.
            with _written_lock:
                pending = txn_count - _written_txns
            if pending:
                logger.info(
                    "[%s] Reader done (%d txns). Waiting for %d in-flight ...",
                    _fmt_elapsed(time.time() - begin_time),
                    txn_count,
                    pending,
                )

            # Drain with periodic progress instead of blocking forever.
            executor.shutdown(wait=False)
            shutdown_start = time.time()
            while True:
                # Check if all work is done.
                with _written_lock:
                    done = _written_txns + _written_errors
                if done >= txn_count:
                    break
                # pragma: no cover — drain loop only runs when workers
                # take > 1s (slow S3 uploads); tests finish instantly.
                time.sleep(1)  # pragma: no cover
                waited = time.time() - shutdown_start  # pragma: no cover
                if int(waited) % 10 == 0:  # pragma: no cover
                    with _written_lock:
                        w_txns = _written_txns
                        w_errs = _written_errors
                    still = txn_count - w_txns - w_errs
                    logger.info(
                        "[%s] Still waiting for %d worker(s) ... (%d done, %d errors)",
                        _fmt_elapsed(time.time() - begin_time),
                        still,
                        w_txns,
                        w_errs,
                    )

            # Check for errors that occurred after the last loop check.
            with _written_lock:
                errors = _written_errors
            if errors:  # pragma: no cover — tested via in-loop abort
                raise RuntimeError(
                    f"Aborting: {errors} worker error(s). "
                    "Check log for details. "
                    "Is the target database empty?"
                )

            # Final watermark advancement after drain.
            while watermark_idx < len(dispatched):
                _wm_tid, _wm_fut = dispatched[watermark_idx]
                if _wm_fut.done():
                    _wm_fut.result()
                    watermark_idx += 1
                else:
                    break

            if watermark_idx > 0:
                _wm_tid_val = dispatched[watermark_idx - 1][0]
                wm_conn.execute(
                    "INSERT INTO migration_watermark (id, tid) "
                    "VALUES (TRUE, %s) "
                    "ON CONFLICT (id) DO UPDATE SET tid = EXCLUDED.tid",
                    (_wm_tid_val,),
                )

            # Drain and close the blob sink (waits for background uploads).
            if blob_sink is not None:
                blob_sink.close()

            # Clean completion — drop watermark table.
            wm_conn.execute("DROP TABLE IF EXISTS migration_watermark")

            # Update last TID on main storage.
            if last_tid is not None:
                self._ltid = last_tid

        except BaseException:
            executor.shutdown(wait=False, cancel_futures=True)
            if blob_sink is not None:
                with contextlib.suppress(Exception):
                    blob_sink.close()
            # Flush watermark on error so interrupted imports can resume.
            try:
                while watermark_idx < len(dispatched):
                    _wm_tid, _wm_fut = dispatched[watermark_idx]
                    if _wm_fut.done():
                        try:
                            _wm_fut.result()
                        except Exception:
                            break
                        watermark_idx += 1
                    else:
                        break
                if watermark_idx > 0:
                    _wm_tid_val = dispatched[watermark_idx - 1][0]
                    wm_conn.execute(
                        "INSERT INTO migration_watermark (id, tid) "
                        "VALUES (TRUE, %s) "
                        "ON CONFLICT (id) DO UPDATE SET tid = EXCLUDED.tid",
                        (_wm_tid_val,),
                    )
            except Exception:
                logger.debug("Failed to flush watermark on error", exc_info=True)
            raise

        finally:
            for conn in worker_conns:
                with contextlib.suppress(Exception):
                    self._instance_pool.putconn(conn)
            with contextlib.suppress(Exception):
                self._instance_pool.putconn(wm_conn)

        elapsed = time.time() - begin_time
        logger.info(
            "[%s] All %d transactions copied with %d workers.",
            _fmt_elapsed(elapsed),
            txn_count,
            num_workers,
        )
        if total_missing_blobs:  # pragma: no cover — source storage issue
            logger.error(
                "%d blob(s) missing in source storage and skipped during import!",
                total_missing_blobs,
            )

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


@zope.interface.implementer(IBlobStorage)
class PGJsonbStorageInstance(ConflictResolvingStorage):
    """Per-connection MVCC storage instance.

    Created by PGJsonbStorage.new_instance() — each ZODB Connection
    gets one.  Has its own PG connection (autocommit=True) so reads
    always see the latest committed data, and writes use explicit
    BEGIN/COMMIT transactions with advisory locking.
    """

    def __init__(self, main_storage):
        self._main = main_storage
        self._history_preserving = main_storage._history_preserving
        self._instance_pool = main_storage._instance_pool
        self._conn = self._instance_pool.getconn()
        self._polled_tid = None  # None = never polled, int = last seen TID
        self._in_read_txn = False  # True when inside REPEATABLE READ snapshot
        self._tmp = []
        self._blob_tmp = {}  # pending blob stores: {oid_int: blob_path}
        self._tid = None
        self._transaction = None
        self._resolved = []
        self._blob_temp_dir = tempfile.mkdtemp(prefix="zodb-pgjsonb-blobs-")
        # S3 tiered blob storage (inherited from main)
        self._s3_client = main_storage._s3_client
        self._blob_cache = main_storage._blob_cache
        self._blob_threshold = main_storage._blob_threshold
        # Load cache: zoid → (pickle_bytes, tid_bytes), bounded LRU
        self._load_cache = LoadCache(max_mb=main_storage._cache_local_mb)
        # Cache for conflict resolution: (oid_bytes, tid_bytes) → pickle_bytes
        self._serial_cache = {}
        # Propagate conflict resolution transform hooks from main storage
        self._crs_transform_record_data = main_storage._crs_transform_record_data
        self._crs_untransform_record_data = main_storage._crs_untransform_record_data
        # Build load SQL with optional refs prefetch expression
        expr = main_storage._prefetch_refs_expr
        if expr:
            self._load_sql = (
                "SELECT tid, class_mod, class_name, state, "
                f"{expr} AS refs "
                "FROM object_state WHERE zoid = %s"
            )
        else:
            self._load_sql = (
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s"
            )

    @property
    def pg_connection(self):
        """The underlying psycopg connection for read queries.

        Shares the same REPEATABLE READ snapshot used for ZODB loads,
        so catalog queries see a consistent point-in-time view.
        """
        return self._conn

    # ── IMVCCStorage ─────────────────────────────────────────────────

    def new_instance(self):
        """Delegate to main storage."""
        return self._main.new_instance()

    def release(self):
        """Return connection to pool and clean up temp dir."""
        if self._conn and not self._conn.closed:
            self._end_read_txn()
            self._instance_pool.putconn(self._conn)
            self._conn = None
        if os.path.exists(self._blob_temp_dir):
            shutil.rmtree(self._blob_temp_dir, ignore_errors=True)

    def _end_read_txn(self):
        """End the current REPEATABLE READ snapshot transaction, if any."""
        if self._in_read_txn:
            self._conn.execute("COMMIT")
            self._in_read_txn = False

    def _begin_read_txn(self):
        """Start a REPEATABLE READ snapshot transaction for consistent reads.

        All subsequent load()/loadBefore() queries will see a consistent
        point-in-time snapshot until _end_read_txn() is called.
        """
        self._conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        self._in_read_txn = True

    def poll_invalidations(self):
        """Return OIDs changed since last poll.

        Returns [] if nothing changed, list of OID bytes otherwise.

        Starts a REPEATABLE READ snapshot FIRST, then queries for changes
        within that snapshot.  This ensures that all subsequent load()
        calls see the exact same database state as the invalidation
        queries — preventing races where a concurrent commit lands
        between the poll and the first load.
        """
        # End any previous read snapshot
        self._end_read_txn()

        # Start a new REPEATABLE READ snapshot immediately.
        # The first query anchors the snapshot — all subsequent queries
        # (invalidation lookups AND load() calls) see this same state.
        self._begin_read_txn()

        with self._conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(tid), 0) AS max_tid FROM transaction_log")
            row = cur.fetchone()
            new_tid = row["max_tid"]

        result = []
        if self._polled_tid is not None and new_tid != self._polled_tid:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT zoid FROM object_state "
                    "WHERE tid > %s AND tid <= %s",
                    (self._polled_tid, new_tid),
                )
                rows = cur.fetchall()
            warmer = self._main._warmer
            for r in rows:
                zoid = r["zoid"]
                result.append(p64(zoid))
                self._load_cache.invalidate(zoid)
                if warmer:
                    warmer.invalidate(zoid)

        self._polled_tid = new_tid
        return result

    def sync(self, force=True):
        """Sync snapshot.

        With autocommit=True, each statement already sees the latest
        committed data, so this is a no-op.
        """

    # ── Read path ────────────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state."""
        zoid = u64(oid)

        # L1: instance load cache
        cached = self._load_cache.get(zoid)
        if cached is not None:
            return cached

        # L2: shared warm cache (populated at startup, #48)
        warmer = self._main._warmer
        if warmer:
            warm_hit = warmer.get(zoid)
            if warm_hit is not None:
                self._load_cache.set(zoid, *warm_hit)
                return warm_hit

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

        # Record for cache warmer (#48)
        if warmer and warmer.recording:
            warmer.record(zoid)

        return data, tid

    def load_multiple(self, oids):
        """Load multiple objects in a single query.

        Args:
            oids: iterable of oid bytes

        Returns:
            dict mapping oid_bytes -> (pickle_bytes, tid_bytes).
            Only includes oids that exist; missing oids are silently omitted.
        """
        result = {}
        miss_oids = []  # list of (oid_bytes, zoid_int) for cache misses

        for oid in oids:
            zoid = u64(oid)
            cached = self._load_cache.get(zoid)
            if cached is not None:
                result[oid] = cached
            else:
                miss_oids.append((oid, zoid))

        if not miss_oids:
            return result

        zoid_list = [zoid for _, zoid in miss_oids]
        zoid_to_oid = {zoid: oid for oid, zoid in miss_oids}

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = ANY(%s)",
                (zoid_list,),
                prepare=True,
            )
            rows = cur.fetchall()

        for row in rows:
            record = {
                "@cls": [row["class_mod"], row["class_name"]],
                "@s": _unsanitize_from_pg(row["state"]),
            }
            data = zodb_json_codec.encode_zodb_record(record)
            tid = p64(row["tid"])
            oid = zoid_to_oid[row["zoid"]]
            self._serial_cache[(oid, tid)] = data
            self._load_cache.set(row["zoid"], data, tid)
            result[oid] = (data, tid)

        return result

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

    # ── Write path ───────────────────────────────────────────────────

    def new_oid(self):
        """Delegate OID allocation to main storage (thread-safe)."""
        return self._main.new_oid()

    def store(self, oid, serial, data, version, transaction):
        """Queue an object for storage during the current transaction.

        Conflict detection is deferred to tpc_vote() where all conflicts
        are checked in a single batch query, eliminating per-object
        round trips while holding the advisory lock.
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
        # Save conflict detection data for batch check in tpc_vote()
        if serial != z64:
            entry["_oid"] = oid
            entry["_serial"] = serial
            entry["_data"] = data
        extra = self._main._process_state(zoid, class_mod, class_name, state)
        if extra:
            entry["_extra"] = extra
        self._tmp.append(entry)

    # ── 2PC ──────────────────────────────────────────────────────────

    def tpc_begin(self, transaction, tid=None, status=" "):
        """Begin a two-phase commit.

        Ends any active read snapshot, starts an explicit PG transaction,
        acquires the advisory lock, and generates a TID.
        """
        self._end_read_txn()
        # Apply any DDL deferred from startup (read txn now committed,
        # ACCESS SHARE released, so ALTER TABLE can proceed).
        self._main._apply_pending_ddl()
        self._transaction = transaction
        self._resolved = []
        self._tmp = []
        self._blob_tmp = {}
        self._read_conflicts = []
        self._conn.execute("BEGIN")
        self._conn.execute("SELECT pg_advisory_xact_lock(0)")
        if tid is None:
            self._tid = self._main._new_tid()
        else:
            self._tid = tid

    def tpc_vote(self, transaction):
        """Flush pending stores + blobs to PostgreSQL.

        Performs batch conflict detection for all queued stores in a single
        round trip, then writes all objects in batched executemany calls.
        """
        if transaction is not self._transaction:
            if transaction is not None:
                raise StorageTransactionError(self, transaction)
            return

        tid_int = u64(self._tid)
        hp = self._history_preserving

        with self._conn.cursor() as cur:
            # ── Batch conflict detection ──────────────────────────
            _batch_resolve_conflicts(cur, self._tmp, self._resolved, self)
            _batch_check_read_conflicts(cur, self._read_conflicts)

            user = transaction.user
            desc = transaction.description
            ext = transaction.extension
            if isinstance(user, bytes):
                user = user.decode("utf-8")
            if isinstance(desc, bytes):
                desc = desc.decode("utf-8")
            _write_txn_log(cur, tid_int, user, desc, ext)

            # Separate objects by action for batch writes
            writes = []
            deletes = []
            for obj in self._tmp:
                if obj.get("action") == "delete":
                    deletes.append(obj["zoid"])
                else:
                    writes.append(obj)

            extra_columns = self._main._get_extra_columns()
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
            for proc in self._main._state_processors:
                if hasattr(proc, "finalize"):
                    proc.finalize(cur)

        return self._resolved or None

    def tpc_finish(self, transaction, f=None):
        """Commit the PG transaction and update shared state."""
        self._conn.execute("COMMIT")
        tid = self._tid
        self._main._ltid = tid
        if f is not None:
            f(tid)
        self._tmp.clear()
        self._blob_tmp.clear()
        self._transaction = None
        return tid

    def tpc_abort(self, transaction):
        """Rollback the PG transaction."""
        try:
            self._conn.execute("ROLLBACK")
        except Exception:  # pragma: no cover
            logger.exception("Error during rollback")
        self._tmp.clear()
        # Clean up queued blob temp files
        for blob_path in self._blob_tmp.values():
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        self._blob_tmp.clear()
        self._transaction = None

    def checkCurrentSerialInTransaction(self, oid, serial, transaction):
        """Queue a read-conflict check for batch verification in tpc_vote()."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        self._read_conflicts.append((oid, serial))

    # ── IBlobStorage ─────────────────────────────────────────────────

    def storeBlob(self, oid, oldserial, data, blobfilename, version, transaction):
        """Store object data + blob file."""
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

    # ── Metadata (delegates to main) ─────────────────────────────────

    def sortKey(self):
        return self._main.sortKey()

    def getName(self):
        return self._main.getName()

    @property
    def __name__(self):
        return self._main.__name__

    def isReadOnly(self):
        return False

    def lastTransaction(self):
        return self._main._ltid

    def __len__(self):
        return len(self._main)

    def getSize(self):
        return self._main.getSize()

    def history(self, oid, size=1):
        return self._main.history(oid, size)

    def pack(self, t, referencesf):
        return self._main.pack(t, referencesf)

    def supportsUndo(self):
        return self._main.supportsUndo()

    def undoLog(self, first=0, last=-20, filter=None):  # noqa: A002
        return self._main.undoLog(first, last, filter)

    def undoInfo(self, first=0, last=-20, specification=None):
        return self._main.undoInfo(first, last, specification)

    def undo(self, transaction_id, transaction=None):
        """Undo a transaction — uses this instance's connection."""
        if not self._history_preserving:
            raise UndoError("Undo is not supported in history-free mode")

        tid_int = u64(transaction_id)

        with self._conn.cursor() as cur:
            undo_data = _compute_undo(cur, tid_int, self, self._tmp)

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
                # Run state processors so catalog columns (path, idx,
                # searchable_text, etc.) are recomputed from the restored
                # state, not left as NULL (#30 in plone-pgcatalog).
                extra = self._main._process_state(
                    item["zoid"],
                    item["class_mod"],
                    item["class_name"],
                    item["state"],
                )
                if extra:
                    entry["_extra"] = extra
                self._tmp.append(entry)

        return self._tid, oid_list

    # ── IStorageRestoreable ───────────────────────────────────────

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        """Write pre-committed data without conflict checking."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if data is None:
            return
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
        extra = self._main._process_state(zoid, class_mod, class_name, state)
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

    def registerDB(self, db):
        pass

    def close(self):
        self.release()


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
