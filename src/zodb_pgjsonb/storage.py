"""PGJsonbStorage — ZODB storage using PostgreSQL JSONB.

Implements IMVCCStorage using psycopg3 (sync) and zodb-json-codec for
transparent pickle ↔ JSONB transcoding.

PGJsonbStorage is the main storage (factory) that manages schema, OIDs,
and shared state.  Each ZODB Connection gets its own
PGJsonbStorageInstance via new_instance(), providing per-connection
snapshot isolation through separate PostgreSQL connections.
"""

from .interfaces import IPGJsonbStorage
from .schema import install_schema
from collections import OrderedDict
from persistent.TimeStamp import TimeStamp
from psycopg.rows import dict_row
from psycopg.types.json import Json
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
from ZODB.POSException import ConflictError
from ZODB.POSException import POSKeyError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import UndoError
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import base64
import dataclasses
import logging
import os
import psycopg
import re
import shutil
import sys
import tempfile
import time
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
        State processors are trusted code — ``value_expr`` and ``update_expr``
        are interpolated into SQL.  Only register processors from sources you
        trust.  Column names are validated; expressions are not, because they
        may legitimately contain SQL function calls.
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
        s3_client=None,
        blob_cache=None,
        blob_threshold=1_048_576,
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

        # Pending stores for current transaction (direct use only)
        self._tmp = []
        self._blob_tmp = {}  # pending blob stores: {oid_int: blob_path}

        # Blob temp directory
        self._blob_temp_dir = blob_temp_dir or tempfile.mkdtemp(
            prefix="zodb-pgjsonb-blobs-"
        )

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
            kwargs={"row_factory": dict_row},
            configure=lambda conn: setattr(conn, "autocommit", True),
            open=True,
        )
        logger.debug("Connection pool ready")

        # Initialize schema
        logger.debug("Installing schema (history_preserving=%s)", history_preserving)
        install_schema(self._conn, history_preserving=history_preserving)
        logger.debug("Schema installed")

        # Load max OID and last TID from database
        self._restore_state()
        # Commit implicit transaction so self._conn is clean for later DDL
        # (register_state_processor needs ACCESS EXCLUSIVE for ALTER TABLE)
        self._conn.commit()
        logger.debug("Storage initialized (max_oid=%s, ltid=%s)", self._oid, self._ltid)

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
        """
        self._state_processors.append(processor)
        # Apply processor schema DDL if available
        if hasattr(processor, "get_schema_sql"):
            sql = processor.get_schema_sql()
            if sql:
                self._apply_processor_ddl(sql, type(processor).__name__)

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
                    ddl_conn.execute(sql)
                logger.info("Applied deferred schema DDL from %s", name)
            except Exception:
                logger.warning(
                    "Failed to apply deferred DDL from %s",
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
        """
        with self._lock:
            now = time.time()
            t = TimeStamp(*(*time.gmtime(now)[:5], now % 60))
            self._ts = t = t.laterThan(self._ts)
            return t.raw()

    # ── IStorage: load ───────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state, returning (pickle_bytes, tid_bytes)."""
        zoid = u64(oid)

        # Check load cache first
        cached = self._load_cache.get(zoid)
        if cached is not None:
            return cached

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
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
        # Check serial cache first (needed for conflict resolution in
        # history-free mode where old versions are overwritten)
        cached = self._serial_cache.get((oid, serial))
        if cached is not None:
            return cached

        zoid = u64(oid)
        tid_int = u64(serial)
        table = "object_history" if self._history_preserving else "object_state"
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT class_mod, class_name, state "
                f"FROM {table} WHERE zoid = %s AND tid = %s",
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

        class_mod, class_name, state, refs = zodb_json_codec.decode_zodb_record_for_pg(
            data
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
            _batch_write_blobs(
                cur,
                self._blob_tmp.items(),
                tid_int,
                hp,
                s3_client=self._s3_client,
                blob_threshold=self._blob_threshold,
            )

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

    def history(self, oid, size=1):
        """Return revision history for an object."""
        zoid = u64(oid)
        table = "object_history" if self._history_preserving else "object_state"
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT o.tid, o.state_size, "
                f"t.username, t.description "
                f"FROM {table} o "
                f"LEFT JOIN transaction_log t ON o.tid = t.tid "
                f"WHERE o.zoid = %s "
                f"ORDER BY o.tid DESC LIMIT %s",
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
                self._tmp.append(
                    {
                        "zoid": item["zoid"],
                        "class_mod": item["class_mod"],
                        "class_name": item["class_name"],
                        "state": item["state"],
                        "state_size": item["state_size"],
                        "refs": item["refs"],
                    }
                )

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
        class_mod, class_name, state, refs = zodb_json_codec.decode_zodb_record_for_pg(
            data
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
                cur.execute("DELETE FROM blob_history")
                cur.execute("DELETE FROM object_history")
                cur.execute("DELETE FROM pack_state")
            cur.execute("DELETE FROM transaction_log")
        self._conn.commit()
        self._ltid = z64
        self._oid = z64


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
            for r in rows:
                zoid = r["zoid"]
                result.append(p64(zoid))
                self._load_cache.invalidate(zoid)

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

        # Check load cache first
        cached = self._load_cache.get(zoid)
        if cached is not None:
            return cached

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
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
        # Check serial cache first (needed for conflict resolution in
        # history-free mode where old versions are overwritten)
        cached = self._serial_cache.get((oid, serial))
        if cached is not None:
            return cached

        zoid = u64(oid)
        tid_int = u64(serial)
        table = "object_history" if self._history_preserving else "object_state"
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT class_mod, class_name, state "
                f"FROM {table} WHERE zoid = %s AND tid = %s",
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

        class_mod, class_name, state, refs = zodb_json_codec.decode_zodb_record_for_pg(
            data
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
            _batch_write_blobs(
                cur,
                self._blob_tmp.items(),
                tid_int,
                hp,
                s3_client=self._s3_client,
                blob_threshold=self._blob_threshold,
            )

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
                self._tmp.append(
                    {
                        "zoid": item["zoid"],
                        "class_mod": item["class_mod"],
                        "class_name": item["class_name"],
                        "state": item["state"],
                        "state_size": item["state_size"],
                        "refs": item["refs"],
                    }
                )

        return self._tid, oid_list

    # ── IStorageRestoreable ───────────────────────────────────────

    def restore(self, oid, serial, data, version, prev_txn, transaction):
        """Write pre-committed data without conflict checking."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if data is None:
            return
        zoid = u64(oid)
        class_mod, class_name, state, refs = zodb_json_codec.decode_zodb_record_for_pg(
            data
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


_DSN_PASSWORD_RE = re.compile(r"(password\s*=\s*)\S+", re.IGNORECASE)


def _mask_dsn(dsn):
    """Mask password in a PostgreSQL DSN string for safe logging."""
    return _DSN_PASSWORD_RE.sub(r"\1***", dsn)


def _serialize_extension(ext):
    """Serialize transaction extension for BYTEA storage.

    ZODB extensions are dicts; we store them as JSON bytes in PG.
    BaseStorage passes extension_bytes (pickle) — we always convert to JSON.
    """
    import json

    if isinstance(ext, bytes):
        if not ext:
            return b""
        # BaseStorage passes zodbpickle-serialized bytes — decode to dict
        try:
            from ZODB._compat import loads as zodb_loads

            ext_dict = zodb_loads(ext)
            if isinstance(ext_dict, dict) and ext_dict:
                return json.dumps(ext_dict).encode("utf-8")
            return b""
        except (
            ImportError,
            TypeError,
            KeyError,
            AttributeError,
            EOFError,
            ValueError,
            OverflowError,
        ):
            return ext  # fallback: store as-is
    if isinstance(ext, dict):
        if not ext:
            return b""
        return json.dumps(ext).encode("utf-8")
    return b""


def _deserialize_extension(ext_data):
    """Deserialize extension bytes from PG BYTEA back to a dict.

    Stored as JSON by _serialize_extension.  Falls back to pickle
    for legacy data written before JSON serialization was in place.
    """
    import json

    if not ext_data:
        return {}
    if isinstance(ext_data, memoryview):
        ext_data = bytes(ext_data)
    try:
        result = json.loads(ext_data)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        pass
    # Fallback: try pickle (legacy data)
    try:
        from ZODB._compat import loads as zodb_loads

        result = zodb_loads(ext_data)
        if isinstance(result, dict):
            return result
    except (
        ImportError,
        TypeError,
        KeyError,
        AttributeError,
        EOFError,
        ValueError,
        OverflowError,
    ):
        pass
    return {}


def _write_txn_log(cur, tid_int, user, desc, ext):
    """Write a transaction log entry."""
    cur.execute(
        "INSERT INTO transaction_log "
        "(tid, username, description, extension) "
        "VALUES (%s, %s, %s, %s)",
        (tid_int, user, desc, _serialize_extension(ext)),
    )


def _compute_undo(cur, tid_int, storage, pending=None):
    """Compute undo data for a transaction.

    For each object modified in the undone transaction:
    - If the object's current version matches the undone tid, restore
      the previous revision (or delete if the object was created).
    - If the object was modified after the undone transaction, attempt
      conflict resolution. Raises UndoError if resolution fails.

    Args:
        cur: database cursor
        tid_int: integer tid of the transaction to undo
        storage: storage instance (for conflict resolution)
        pending: list of pending _tmp entries (for multi-undo in same txn)

    Returns:
        List of dicts with 'action' ('restore' or 'delete') and data.
    """
    # Build index of pending writes by zoid for multi-undo
    pending_by_zoid = {}
    if pending:
        for entry in pending:
            pending_by_zoid[entry["zoid"]] = entry
    # Verify transaction exists
    cur.execute(
        "SELECT tid FROM transaction_log WHERE tid = %s",
        (tid_int,),
    )
    if cur.fetchone() is None:
        raise UndoError("Transaction not found")

    # Find all objects modified in that transaction
    cur.execute(
        "SELECT zoid, class_mod, class_name, state, state_size, refs "
        "FROM object_history WHERE tid = %s",
        (tid_int,),
    )
    undone_objects = cur.fetchall()

    if not undone_objects:
        raise UndoError("Transaction has no object changes")

    undo_data = []
    for obj in undone_objects:
        zoid = obj["zoid"]

        # Check current version of the object
        cur.execute(
            "SELECT tid FROM object_state WHERE zoid = %s",
            (zoid,),
        )
        current = cur.fetchone()
        current_tid = current["tid"] if current else None

        # Find previous revision (state before the undone transaction)
        cur.execute(
            "SELECT tid, class_mod, class_name, state, "
            "state_size, refs "
            "FROM object_history "
            "WHERE zoid = %s AND tid < %s "
            "ORDER BY tid DESC LIMIT 1",
            (zoid, tid_int),
        )
        prev = cur.fetchone()

        # Check if there's a pending write for this zoid (multi-undo)
        pending_entry = pending_by_zoid.get(zoid)

        if current_tid is not None and current_tid != tid_int:
            # Object was modified after the undone transaction —
            # check if states actually differ before triggering
            # conflict resolution (cascading undos may change TID
            # but preserve the same state).

            undone_state = obj["state"]
            cur_row = None

            if pending_entry is not None:
                # A prior undo in this same transaction already
                # queued a write for this zoid. Use that pending
                # state for comparison instead of the DB state.
                current_state = pending_entry.get("state")
                cur_row = pending_entry
            else:
                # Get current state from object_state
                cur.execute(
                    "SELECT class_mod, class_name, state "
                    "FROM object_state WHERE zoid = %s",
                    (zoid,),
                )
                cur_row = cur.fetchone()
                current_state = cur_row["state"] if cur_row else None

            if current_state == undone_state and current is not None:
                # States match — this is a cascading undo scenario.
                # The TID changed (from a prior undo) but data is the same.
                # Treat as simple undo: restore previous revision.
                if prev is None:
                    undo_data.append(
                        {
                            "zoid": zoid,
                            "action": "delete",
                        }
                    )
                else:
                    undo_data.append(
                        {
                            "zoid": zoid,
                            "action": "restore",
                            "class_mod": prev["class_mod"],
                            "class_name": prev["class_name"],
                            "state": prev["state"],
                            "state_size": prev["state_size"],
                            "refs": prev["refs"],
                        }
                    )
            else:
                # States genuinely differ — requires conflict resolution
                oid_bytes = p64(zoid)

                # Get the state we want to restore (pre-undo)
                if prev is None:
                    # Object was created in undone txn, but modified later
                    raise UndoError(
                        f"Can't undo creation of object {zoid:#x}: "
                        f"modified in later transaction"
                    )

                # Encode pre-undo state as pickle for conflict resolution
                pre_undo_record = {
                    "@cls": [prev["class_mod"], prev["class_name"]],
                    "@s": _unsanitize_from_pg(prev["state"]),
                }
                pre_undo_data = zodb_json_codec.encode_zodb_record(pre_undo_record)

                # Get current state as pickle
                current_record = {
                    "@cls": [cur_row["class_mod"], cur_row["class_name"]],
                    "@s": _unsanitize_from_pg(cur_row["state"]),
                }
                current_data = zodb_json_codec.encode_zodb_record(current_record)

                # Try conflict resolution
                try:
                    resolved = storage.tryToResolveConflict(
                        oid_bytes,
                        p64(current_tid),
                        current_data,
                        pre_undo_data,
                    )
                except ConflictError as err:
                    raise UndoError(
                        f"Can't undo: conflict on object {zoid:#x}"
                    ) from err

                if resolved is None:
                    raise UndoError(f"Can't undo: conflict on object {zoid:#x}")

                # Decode resolved pickle back to JSONB
                r_mod, r_name, r_state, r_refs = (
                    zodb_json_codec.decode_zodb_record_for_pg(resolved)
                )
                undo_data.append(
                    {
                        "zoid": zoid,
                        "action": "restore",
                        "class_mod": r_mod,
                        "class_name": r_name,
                        "state": r_state,
                        "state_size": len(resolved),
                        "refs": r_refs,
                    }
                )
        elif prev is None:
            # Object was created in this txn — delete it
            undo_data.append(
                {
                    "zoid": zoid,
                    "action": "delete",
                }
            )
        else:
            # Simple undo: restore previous revision
            undo_data.append(
                {
                    "zoid": zoid,
                    "action": "restore",
                    "class_mod": prev["class_mod"],
                    "class_name": prev["class_name"],
                    "state": prev["state"],
                    "state_size": prev["state_size"],
                    "refs": prev["refs"],
                }
            )

    return undo_data


# ── Batch conflict detection ─────────────────────────────────────────


def _batch_resolve_conflicts(cur, tmp, resolved, storage):
    """Check all queued stores for write conflicts in a single round trip.

    Entries with ``_serial`` are checked: if the committed TID differs from
    the expected serial, conflict resolution is attempted.  Resolved entries
    are updated in-place with re-decoded state.
    """
    conflict_entries = [e for e in tmp if "_serial" in e]
    if not conflict_entries:
        return

    check_zoids = [e["zoid"] for e in conflict_entries]
    cur.execute(
        "SELECT zoid, tid FROM object_state WHERE zoid = ANY(%s)",
        (check_zoids,),
        prepare=True,
    )
    committed = {row["zoid"]: row["tid"] for row in cur.fetchall()}

    for entry in conflict_entries:
        zoid = entry["zoid"]
        if zoid not in committed:
            continue  # New object, no conflict possible
        committed_tid = p64(committed[zoid])
        if committed_tid == entry["_serial"]:
            continue  # Serial matches, no conflict

        # Conflict detected — attempt resolution
        oid = entry["_oid"]
        committed_data = storage.loadSerial(oid, committed_tid)
        result = storage.tryToResolveConflict(
            oid,
            committed_tid,
            entry["_serial"],
            entry["_data"],
            committedData=committed_data,
        )
        if result:
            # Re-decode resolved pickle and update entry in-place
            class_mod, class_name, state, refs = (
                zodb_json_codec.decode_zodb_record_for_pg(result)
            )
            entry["class_mod"] = class_mod
            entry["class_name"] = class_name
            entry["state"] = state
            entry["state_size"] = len(result)
            entry["refs"] = refs
            # Re-process state for catalog/extra columns
            if hasattr(storage, "_main"):
                extra = storage._main._process_state(zoid, class_mod, class_name, state)
            else:
                extra = storage._process_state(zoid, class_mod, class_name, state)
            if extra:
                entry["_extra"] = extra
            resolved.append(oid)
        else:
            raise ConflictError(oid=oid, serials=(committed_tid, entry["_serial"]))

    # Clean up conflict detection data from entries
    for entry in conflict_entries:
        entry.pop("_oid", None)
        entry.pop("_serial", None)
        entry.pop("_data", None)


def _batch_check_read_conflicts(cur, read_conflicts):
    """Check all queued read-conflict checks in a single round trip."""
    if not read_conflicts:
        return

    zoids = [u64(oid) for oid, _ in read_conflicts]
    serial_map = {u64(oid): serial for oid, serial in read_conflicts}
    cur.execute(
        "SELECT zoid, tid FROM object_state WHERE zoid = ANY(%s)",
        (zoids,),
        prepare=True,
    )
    for row in cur.fetchall():
        zoid = row["zoid"]
        current_serial = p64(row["tid"])
        expected = serial_map[zoid]
        if current_serial != expected:
            raise ReadConflictError(
                oid=p64(zoid),
                serials=(current_serial, expected),
            )


def _batch_write_objects(
    cur, objects, tid_int, history_preserving=False, extra_columns=None
):
    """Write multiple objects in batch using executemany (pipelined).

    psycopg3's executemany() automatically uses pipeline mode, sending
    all statements in a single network round-trip instead of waiting for
    each individual result.

    When *extra_columns* is provided (a list of :class:`ExtraColumn`),
    additional columns are included in the ``object_state`` INSERT.
    History tables always use the base columns only.
    """
    if not objects:
        return

    # ── Base columns (always present) ────────────────────────────
    base_cols = [
        "zoid",
        "tid",
        "class_mod",
        "class_name",
        "state",
        "state_size",
        "refs",
    ]
    base_vals = [
        "%(zoid)s",
        "%(tid)s",
        "%(class_mod)s",
        "%(class_name)s",
        "%(state)s",
        "%(state_size)s",
        "%(refs)s",
    ]

    # ── Build params list ────────────────────────────────────────
    params_list = []
    for obj in objects:
        params = {
            "zoid": obj["zoid"],
            "tid": tid_int,
            "class_mod": obj["class_mod"],
            "class_name": obj["class_name"],
            "state": Json(obj["state"]),
            "state_size": obj["state_size"],
            "refs": obj["refs"],
        }
        if extra_columns:
            obj_extra = obj.get("_extra") or {}
            for ec in extra_columns:
                params[ec.name] = obj_extra.get(ec.name)
        params_list.append(params)

    # ── History table (base columns only) ────────────────────────
    if history_preserving:
        cur.executemany(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "VALUES (%(zoid)s, %(tid)s, %(class_mod)s, %(class_name)s, "
            "%(state)s, %(state_size)s, %(refs)s)",
            params_list,
        )

    # ── object_state INSERT with extra columns ───────────────────
    if extra_columns:
        cols = base_cols + [ec.name for ec in extra_columns]
        vals = base_vals + [ec.value_expr for ec in extra_columns]
        update_parts = []
        for c in cols[1:]:  # skip zoid (PK)
            update_parts.append(f"{c} = EXCLUDED.{c}")
    else:
        cols = base_cols
        vals = base_vals
        update_parts = [f"{c} = EXCLUDED.{c}" for c in cols[1:]]

    cols_str = ", ".join(cols)
    vals_str = ", ".join(vals)
    update_str = ", ".join(update_parts)

    cur.executemany(
        f"INSERT INTO object_state ({cols_str}) "
        f"VALUES ({vals_str}) "
        f"ON CONFLICT (zoid) DO UPDATE SET {update_str}",
        params_list,
    )


def _batch_delete_objects(cur, zoids, tid_int, history_preserving=False):
    """Delete multiple objects in batch.

    In history-preserving mode, records tombstones with state=NULL.
    Removes objects from object_state (current state table).
    """
    if not zoids:
        return
    if history_preserving:
        cur.executemany(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "VALUES (%s, %s, '', '', NULL, 0, '{}')",
            [(zoid, tid_int) for zoid in zoids],
        )
    cur.executemany(
        "DELETE FROM object_state WHERE zoid = %s",
        [(zoid,) for zoid in zoids],
    )


def _batch_write_blobs(
    cur,
    blobs,
    tid_int,
    history_preserving=False,
    s3_client=None,
    blob_threshold=1_048_576,
):
    """Write multiple blobs in batch with optional S3 tiering.

    When s3_client is set, blobs >= blob_threshold are uploaded to S3
    and only metadata (s3_key, no data) is stored in PG. Smaller blobs
    go directly to PG bytea.
    """
    if not blobs:
        return

    pg_params = []  # (zoid, tid, size, data) — PG bytea blobs
    s3_params = []  # (zoid, tid, size, s3_key) — S3 metadata-only rows

    for zoid, blob_path in blobs:
        size = os.path.getsize(blob_path)
        if s3_client is not None and size >= blob_threshold:
            # Large blob → S3
            s3_key = f"blobs/{zoid:016x}/{tid_int:016x}.blob"
            s3_client.upload_file(blob_path, s3_key)
            s3_params.append((zoid, tid_int, size, s3_key))
            os.unlink(blob_path)
        else:
            # Small blob or no S3 → PG bytea
            with open(blob_path, "rb") as f:
                blob_data = f.read()
            pg_params.append((zoid, tid_int, size, blob_data))
            os.unlink(blob_path)

    # Batch insert PG bytea blobs
    if pg_params:
        if history_preserving:
            cur.executemany(
                "INSERT INTO blob_history (zoid, tid, blob_size, data) "
                "VALUES (%s, %s, %s, %s)",
                pg_params,
            )
        cur.executemany(
            "INSERT INTO blob_state (zoid, tid, blob_size, data) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (zoid, tid) DO UPDATE SET "
            "blob_size = EXCLUDED.blob_size, data = EXCLUDED.data",
            pg_params,
        )

    # Batch insert S3 metadata (data=NULL, s3_key=key)
    if s3_params:
        if history_preserving:
            cur.executemany(
                "INSERT INTO blob_history "
                "(zoid, tid, blob_size, s3_key) "
                "VALUES (%s, %s, %s, %s)",
                s3_params,
            )
        cur.executemany(
            "INSERT INTO blob_state "
            "(zoid, tid, blob_size, s3_key) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (zoid, tid) DO UPDATE SET "
            "blob_size = EXCLUDED.blob_size, "
            "s3_key = EXCLUDED.s3_key, data = NULL",
            s3_params,
        )


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

    Queries object_history for the most recent revision with tid < requested
    tid.  Also looks up end_tid (the next revision's tid) so ZODB can cache
    the validity window.
    """
    cur.execute(
        "SELECT tid, class_mod, class_name, state "
        "FROM object_history WHERE zoid = %s AND tid < %s "
        "ORDER BY tid DESC LIMIT 1",
        (zoid, tid_int),
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
        "SELECT MIN(tid) AS next_tid FROM object_history WHERE zoid = %s AND tid > %s",
        (zoid, row["tid"]),
        prepare=True,
    )
    next_row = cur.fetchone()
    end_tid = p64(next_row["next_tid"]) if next_row["next_tid"] else None
    return data, start_tid, end_tid


def _unsanitize_from_pg(obj):
    """Reverse ``@ns`` markers back to strings with null bytes.

    Returns original objects unchanged when no @ns markers are found
    (zero allocations in the common case).
    """
    if isinstance(obj, dict):
        if "@ns" in obj and len(obj) == 1:
            return base64.b64decode(obj["@ns"]).decode(
                "utf-8",
                errors="surrogatepass",
            )
        new = {}
        changed = False
        for k, v in obj.items():
            new_v = _unsanitize_from_pg(v)
            if new_v is not v:
                changed = True
            new[k] = new_v
        return new if changed else obj
    if isinstance(obj, list):
        new = []
        changed = False
        for item in obj:
            new_item = _unsanitize_from_pg(item)
            if new_item is not item:
                changed = True
            new.append(new_item)
        return new if changed else obj
    return obj


def _iter_transactions(conn, table, start, stop):
    """Yield PGTransactionRecord objects for each transaction.

    Iterates from transaction_log (authoritative list of all transactions)
    and joins to the object table for records.  In history-free mode, old
    transactions whose objects were all updated later will yield with an
    empty record list — this is correct and preserves transaction metadata
    for zodbconvert.

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

            cur.execute(
                f"SELECT zoid, class_mod, class_name, state "
                f"FROM {table} WHERE tid = %s",
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
