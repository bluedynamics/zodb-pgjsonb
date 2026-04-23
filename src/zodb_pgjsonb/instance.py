"""PGJsonbStorageInstance — per-connection MVCC storage instance.

Each ZODB Connection gets its own instance via
PGJsonbStorage.new_instance(), providing per-connection snapshot
isolation through a separate PostgreSQL connection.
"""

from .batch import _batch_delete_objects
from .batch import _batch_write_blobs
from .batch import _batch_write_objects
from .batch import _write_txn_log
from .conflict import _batch_check_read_conflicts
from .conflict import _batch_resolve_conflicts
from .serialization import _unsanitize_from_pg
from .storage import _do_loadSerial
from .storage import _load_blob_from_s3
from .storage import _loadBefore_hf
from .storage import _loadBefore_hp
from .storage import _NoopSerialCache
from .storage import LoadCache
from .undo import _compute_undo
from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import POSKeyError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import UndoError
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import logging
import os
import shutil
import tempfile
import zodb_json_codec
import zope.interface


logger = logging.getLogger(__name__)


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
        self._load_cache = LoadCache(max_mb=main_storage._cache_per_connection_mb)
        # Cache for conflict resolution: (oid_bytes, tid_bytes) → pickle_bytes.
        # History-preserving mode can retrieve old revisions from
        # object_history, so the cache is never needed there (#62).
        self._serial_cache = _NoopSerialCache() if self._history_preserving else {}
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

    def afterCompletion(self):
        """ZODB hook: end any open REPEATABLE READ snapshot.

        Called by ZODB after every transaction commit/abort and on
        ``Connection.close()`` (see ``ZODB.Connection.close``).
        Closing the read transaction at request end avoids holding a
        ``virtualxid`` that blocks ``CREATE INDEX CONCURRENTLY`` and
        similar maintenance operations (#118).

        Idempotent: ``_end_read_txn`` short-circuits when no read tx
        is open (e.g. right after a write transaction's ``tpc_begin``
        already ended it).  Never raises — the connection may have
        been killed by an external operator or by
        ``idle_in_transaction_session_timeout``; in that case we just
        log and let the next operation rebuild via the pool.
        """
        try:
            self._end_read_txn()
        except Exception:
            logger.warning("afterCompletion: _end_read_txn failed", exc_info=True)
        # Drop conflict-resolution cache entries; the base versions
        # tryToResolveConflict needed are consumed by tpc_vote, and
        # keeping them past the transaction leaks memory (#62).
        self._serial_cache.clear()

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

        # Apply any DDL deferred from startup (e.g. ALTER TABLE ADD COLUMN)
        # before starting the read snapshot.  At this point the startup
        # REPEATABLE READ has been committed, so ACCESS SHARE is released
        # and the DDL can acquire ACCESS EXCLUSIVE.  Without this, a
        # read-only request that hits a column added by a state processor
        # (e.g. 'meta') would crash with UndefinedColumn (#105).
        self._main._apply_pending_ddl()

        # Start a new REPEATABLE READ snapshot immediately.
        # The first query anchors the snapshot — all subsequent queries
        # (invalidation lookups AND load() calls) see this same state.
        self._begin_read_txn()

        with self._conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(tid), 0) AS max_tid FROM transaction_log")
            row = cur.fetchone()
            new_tid = row["max_tid"]

        result = []
        changed_zoids = []
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
                changed_zoids.append(zoid)
                result.append(p64(zoid))
                self._load_cache.invalidate(zoid)
                if warmer:
                    warmer.invalidate(zoid)

        # Advance shared cache consensus TID atomically with its own
        # invalidation.  This is what gates shared.set() writes, so
        # load() must be able to populate shared after poll. (#63)
        self._main._shared_cache.poll_advance(new_tid, changed_zoids)
        self._polled_tid = new_tid
        return result

    def sync(self, force=True):
        """Sync snapshot.

        With autocommit=True, each statement already sees the latest
        committed data, so this is a no-op.
        """

    # ── Read path ────────────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state.

        Three-tier cascade: per-instance L1 (fast, no lock) → process-
        wide shared cache (L2, consensus-TID gated) → PostgreSQL.
        """
        zoid = u64(oid)

        # L1: instance load cache (fast path, no lock)
        cached = self._load_cache.get(zoid)
        if cached is not None:
            return cached

        # L2: process-wide shared cache
        shared = self._main._shared_cache
        shared_hit = shared.get(zoid, self._polled_tid)
        if shared_hit is not None:
            data, tid = shared_hit
            self._load_cache.set(zoid, data, tid)
            # Also populate per-instance serial cache so that
            # conflict resolution can retrieve this revision without
            # re-hitting PG (history-free mode only — HP uses the
            # NoopSerialCache and pulls from object_history).
            self._serial_cache[(oid, tid)] = data
            return shared_hit

        # Miss — go to PG
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
        shared.set(zoid, data, tid, self._polled_tid)

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
        warmer = self._main._warmer
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
        # Invalidate shared cache for any zoid we just wrote so that
        # other instances see the new state on next read.
        changed_zoids = [obj["zoid"] for obj in self._tmp]
        self._main._shared_cache.poll_advance(u64(tid), changed_zoids)
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
