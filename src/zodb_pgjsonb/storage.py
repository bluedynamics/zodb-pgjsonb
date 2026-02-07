"""PGJsonbStorage — ZODB storage using PostgreSQL JSONB.

Implements IMVCCStorage using psycopg3 (sync) and zodb-json-codec for
transparent pickle ↔ JSONB transcoding.

PGJsonbStorage is the main storage (factory) that manages schema, OIDs,
and shared state.  Each ZODB Connection gets its own
PGJsonbStorageInstance via new_instance(), providing per-connection
snapshot isolation through separate PostgreSQL connections.
"""

import base64
import logging
import os
import shutil
import tempfile
import time

import psycopg
import zope.interface
from psycopg.rows import dict_row
from psycopg.types.json import Json

from persistent.TimeStamp import TimeStamp
from ZODB.BaseStorage import BaseStorage
from ZODB.ConflictResolution import ConflictResolvingStorage
from ZODB.interfaces import IBlobStorage
from ZODB.interfaces import IMVCCStorage
from ZODB.interfaces import IStorageUndoable
from ZODB.POSException import ConflictError
from ZODB.POSException import POSKeyError
from ZODB.POSException import ReadConflictError
from ZODB.POSException import StorageTransactionError
from ZODB.POSException import UndoError
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64

import zodb_json_codec

from .interfaces import IPGJsonbStorage
from .schema import install_schema


logger = logging.getLogger(__name__)


@zope.interface.implementer(
    IPGJsonbStorage, IMVCCStorage, IBlobStorage, IStorageUndoable,
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

    def __init__(self, dsn, name="pgjsonb", history_preserving=False,
                 blob_temp_dir=None):
        BaseStorage.__init__(self, name)
        self._dsn = dsn
        self._history_preserving = history_preserving
        self._ltid = z64

        # Pending stores for current transaction (direct use only)
        self._tmp = []
        self._blob_tmp = []  # pending blob stores: [(oid_int, blob_path), ...]

        # Blob temp directory
        self._blob_temp_dir = blob_temp_dir or tempfile.mkdtemp(
            prefix="zodb-pgjsonb-blobs-"
        )

        # Cache for conflict resolution: (oid_bytes, tid_bytes) → pickle_bytes
        # In history-free mode, loadSerial can't find old versions after they're
        # overwritten. Caching data from load() makes it available for
        # tryToResolveConflict's loadSerial(oid, oldSerial) calls.
        self._serial_cache = {}

        # Database connection (schema init + admin queries)
        self._conn = psycopg.connect(dsn, row_factory=dict_row)

        # Initialize schema
        install_schema(self._conn, history_preserving=history_preserving)

        # Load max OID and last TID from database
        self._restore_state()

    def _restore_state(self):
        """Load max OID and last TID from existing data."""
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(zoid), 0) AS max_oid FROM object_state"
            )
            row = cur.fetchone()
            max_oid = row["max_oid"]
            if max_oid > 0:
                self._oid = p64(max_oid)

            cur.execute(
                "SELECT COALESCE(MAX(tid), 0) AS max_tid FROM transaction_log"
            )
            row = cur.fetchone()
            max_tid = row["max_tid"]
            if max_tid > 0:
                self._ltid = p64(max_tid)
                # Ensure _ts is at least as recent as the last committed TID
                # so _new_tid() generates monotonically increasing TIDs.
                self._ts = TimeStamp(self._ltid)

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
            t = TimeStamp(*(time.gmtime(now)[:5] + (now % 60,)))
            self._ts = t = t.laterThan(self._ts)
            return t.raw()

    # ── IStorage: load ───────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state, returning (pickle_bytes, tid_bytes)."""
        zoid = u64(oid)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
                (zoid,),
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
        table = ("object_history" if self._history_preserving
                 else "object_state")
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT class_mod, class_name, state "
                f"FROM {table} WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
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
        """Queue an object for storage during the current transaction."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)

        if version:
            raise TypeError("versions are not supported")

        zoid = u64(oid)

        if serial != z64:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT tid FROM object_state WHERE zoid = %s",
                    (zoid,),
                )
                row = cur.fetchone()
            if row is not None:
                committed_tid = p64(row["tid"])
                if committed_tid != serial:
                    committed_data = self.loadSerial(oid, committed_tid)
                    resolved = self.tryToResolveConflict(
                        oid, committed_tid, serial, data,
                        committedData=committed_data,
                    )
                    if resolved:
                        data = resolved
                        self._resolved.append(oid)
                    else:
                        raise ConflictError(
                            oid=oid, serials=(committed_tid, serial)
                        )

        record = zodb_json_codec.decode_zodb_record(data)
        class_mod, class_name = record["@cls"]
        state = record["@s"]
        refs = _extract_refs(state)

        self._tmp.append({
            "zoid": zoid,
            "class_mod": class_mod,
            "class_name": class_name,
            "state": state,
            "state_size": len(data),
            "refs": refs,
        })

    # ── BaseStorage hooks (2PC) ──────────────────────────────────────

    def _begin(self, tid, u, d, e):
        """Called by BaseStorage.tpc_begin after acquiring commit lock."""
        self._conn.execute("BEGIN")
        self._conn.execute("SELECT pg_advisory_xact_lock(0)")

    def _vote(self):
        """Called by BaseStorage.tpc_vote — flush pending stores + blobs."""
        tid_int = u64(self._tid)
        hp = self._history_preserving

        with self._conn.cursor() as cur:
            u, d, e = self._ude
            user = u.decode("utf-8") if isinstance(u, bytes) else u
            desc = d.decode("utf-8") if isinstance(d, bytes) else d
            _write_txn_log(cur, tid_int, user, desc, e)

            for obj in self._tmp:
                _write_object(cur, obj, tid_int, hp)

            for zoid, blob_path in self._blob_tmp:
                _write_blob(cur, zoid, tid_int, blob_path, hp)

        return self._resolved or None

    def _finish(self, tid, u, d, e):
        """Called by BaseStorage.tpc_finish — commit PG transaction."""
        self._conn.commit()
        self._ltid = tid

    def _abort(self):
        """Called by BaseStorage.tpc_abort — rollback PG transaction."""
        try:
            self._conn.rollback()
        except Exception:
            logger.exception("Error during rollback")
        # Clean up queued blob temp files
        for _, blob_path in self._blob_tmp:
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
                "SELECT COALESCE(SUM(state_size), 0) AS total "
                "FROM object_state"
            )
            row = cur.fetchone()
        return row["total"]

    def history(self, oid, size=1):
        """Return revision history for an object."""
        zoid = u64(oid)
        table = ("object_history" if self._history_preserving
                 else "object_state")
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT o.tid, o.state_size, "
                f"t.username, t.description "
                f"FROM {table} o "
                f"JOIN transaction_log t ON o.tid = t.tid "
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
            result.append({
                "time": ts.timeTime(),
                "tid": p64(row["tid"]),
                "serial": p64(row["tid"]),
                "user_name": row["username"] or "",
                "description": row["description"] or "",
                "size": row["state_size"],
            })
        return result

    def pack(self, t, referencesf):
        """Pack the storage — remove unreachable objects."""
        from .packer import pack as do_pack
        pack_time = None
        if self._history_preserving and t is not None:
            # Convert float timestamp to TID bytes
            pack_time = TimeStamp(
                *time.gmtime(t)[:5] + (t % 60,)
            ).raw()
        do_pack(
            self._conn,
            pack_time=pack_time,
            history_preserving=self._history_preserving,
        )

    # ── IStorageUndoable ─────────────────────────────────────────────

    def supportsUndo(self):
        """Undo is only supported in history-preserving mode."""
        return self._history_preserving

    def undoLog(self, first=0, last=-20, filter=None):
        """Return a list of transaction descriptions for undo.

        Returns list of dicts: {id, time, user_name, description}.
        """
        if not self._history_preserving:
            return []

        if last < 0:
            last = first - last + 1

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, username, description "
                "FROM transaction_log "
                "ORDER BY tid DESC "
                "LIMIT %s OFFSET %s",
                (last, first),
            )
            rows = cur.fetchall()

        result = []
        for row in rows:
            d = {
                "id": p64(row["tid"]),
                "time": TimeStamp(p64(row["tid"])).timeTime(),
                "user_name": row["username"] or "",
                "description": row["description"] or "",
            }
            if filter is None or filter(d):
                result.append(d)
        return result

    def undo(self, transaction_id, transaction=None):
        """Undo a transaction by restoring previous object states.

        For each object modified in the undone transaction, find its
        previous revision in object_history and re-store that state.

        Returns (tid, [oid_bytes, ...]) for the new undo transaction.
        """
        if not self._history_preserving:
            raise UndoError("Undo is not supported in history-free mode")

        tid_int = u64(transaction_id)

        with self._conn.cursor() as cur:
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

            # For each object, find its previous revision
            undo_data = []
            for obj in undone_objects:
                zoid = obj["zoid"]
                cur.execute(
                    "SELECT tid, class_mod, class_name, state, "
                    "state_size, refs "
                    "FROM object_history "
                    "WHERE zoid = %s AND tid < %s "
                    "ORDER BY tid DESC LIMIT 1",
                    (zoid, tid_int),
                )
                prev = cur.fetchone()
                if prev is None:
                    # Object was created in this txn — delete it
                    undo_data.append({
                        "zoid": zoid,
                        "action": "delete",
                    })
                else:
                    undo_data.append({
                        "zoid": zoid,
                        "action": "restore",
                        "class_mod": prev["class_mod"],
                        "class_name": prev["class_name"],
                        "state": prev["state"],
                        "state_size": prev["state_size"],
                        "refs": prev["refs"],
                    })

        # Queue the undo data — will be written during _vote
        oid_list = []
        for item in undo_data:
            oid_bytes = p64(item["zoid"])
            oid_list.append(oid_bytes)
            if item["action"] == "restore":
                self._tmp.append({
                    "zoid": item["zoid"],
                    "class_mod": item["class_mod"],
                    "class_name": item["class_name"],
                    "state": item["state"],
                    "state_size": item["state_size"],
                    "refs": item["refs"],
                })

        return oid_list

    # ── IBlobStorage ─────────────────────────────────────────────────

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        """Store object data + blob file (direct-use path)."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if version:
            raise TypeError("versions are not supported")
        self.store(oid, oldserial, data, '', transaction)
        self._blob_tmp.append((u64(oid), blobfilename))

    def loadBlob(self, oid, serial):
        """Return path to a file containing the blob data.

        Uses deterministic filenames so repeated calls for the same
        (oid, serial) return the same path — required by ZODB.blob.Blob.
        """
        zoid = u64(oid)
        tid_int = u64(serial)
        path = os.path.join(
            self._blob_temp_dir, f"{zoid:016x}-{tid_int:016x}.blob"
        )
        if os.path.exists(path):
            return path
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT data, s3_key FROM blob_state "
                "WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
            )
            row = cur.fetchone()
        if row is None:
            raise POSKeyError(oid)
        if row["s3_key"]:
            raise NotImplementedError("S3 blob loading not yet implemented")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            os.write(fd, row["data"])
        finally:
            os.close(fd)
        return path

    def openCommittedBlobFile(self, oid, serial, blob=None):
        """Open committed blob file for reading."""
        blob_path = self.loadBlob(oid, serial)
        if blob is None:
            return open(blob_path, 'rb')
        from ZODB.blob import BlobFile
        return BlobFile(blob_path, 'r', blob)

    def temporaryDirectory(self):
        """Return directory for uncommitted blob data."""
        return self._blob_temp_dir

    # ── IStorage: close ──────────────────────────────────────────────

    def close(self):
        """Close all database connections and clean up temp dir."""
        if self._conn and not self._conn.closed:
            self._conn.close()
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
        self._conn = psycopg.connect(
            main_storage._dsn, row_factory=dict_row, autocommit=True,
        )
        self._polled_tid = None  # None = never polled, int = last seen TID
        self._tmp = []
        self._blob_tmp = []  # pending blob stores: [(oid_int, blob_path), ...]
        self._tid = None
        self._transaction = None
        self._resolved = []
        self._blob_temp_dir = tempfile.mkdtemp(prefix="zodb-pgjsonb-blobs-")
        # Cache for conflict resolution: (oid_bytes, tid_bytes) → pickle_bytes
        self._serial_cache = {}
        # Propagate conflict resolution transform hooks from main storage
        self._crs_transform_record_data = main_storage._crs_transform_record_data
        self._crs_untransform_record_data = main_storage._crs_untransform_record_data

    # ── IMVCCStorage ─────────────────────────────────────────────────

    def new_instance(self):
        """Delegate to main storage."""
        return self._main.new_instance()

    def release(self):
        """Close this instance's PG connection and clean up temp dir."""
        if self._conn and not self._conn.closed:
            self._conn.close()
        if os.path.exists(self._blob_temp_dir):
            shutil.rmtree(self._blob_temp_dir, ignore_errors=True)

    def poll_invalidations(self):
        """Return OIDs changed since last poll.

        Returns [] if nothing changed, list of OID bytes otherwise.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(tid), 0) AS max_tid "
                "FROM transaction_log"
            )
            row = cur.fetchone()
            new_tid = row["max_tid"]

        if self._polled_tid is not None and new_tid == self._polled_tid:
            return []

        if self._polled_tid is None:
            # First poll — establish baseline, nothing to invalidate
            self._polled_tid = new_tid
            return []

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT zoid FROM object_state "
                "WHERE tid > %s AND tid <= %s",
                (self._polled_tid, new_tid),
            )
            rows = cur.fetchall()

        self._polled_tid = new_tid
        return [p64(r["zoid"]) for r in rows]

    def sync(self, force=True):
        """Sync snapshot.

        With autocommit=True, each statement already sees the latest
        committed data, so this is a no-op.
        """

    # ── Read path ────────────────────────────────────────────────────

    def load(self, oid, version=""):
        """Load current object state."""
        zoid = u64(oid)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = %s",
                (zoid,),
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
        table = ("object_history" if self._history_preserving
                 else "object_state")
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT class_mod, class_name, state "
                f"FROM {table} WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
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
        """Queue an object for storage during the current transaction."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)

        if version:
            raise TypeError("versions are not supported")

        zoid = u64(oid)

        # Conflict detection: serial must match committed TID
        if serial != z64:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT tid FROM object_state WHERE zoid = %s",
                    (zoid,),
                )
                row = cur.fetchone()
            if row is not None:
                committed_tid = p64(row["tid"])
                if committed_tid != serial:
                    committed_data = self.loadSerial(oid, committed_tid)
                    resolved = self.tryToResolveConflict(
                        oid, committed_tid, serial, data,
                        committedData=committed_data,
                    )
                    if resolved:
                        data = resolved
                        self._resolved.append(oid)
                    else:
                        raise ConflictError(
                            oid=oid, serials=(committed_tid, serial)
                        )

        record = zodb_json_codec.decode_zodb_record(data)
        class_mod, class_name = record["@cls"]
        state = record["@s"]
        refs = _extract_refs(state)

        self._tmp.append({
            "zoid": zoid,
            "class_mod": class_mod,
            "class_name": class_name,
            "state": state,
            "state_size": len(data),
            "refs": refs,
        })

    # ── 2PC ──────────────────────────────────────────────────────────

    def tpc_begin(self, transaction, tid=None, status=' '):
        """Begin a two-phase commit.

        Starts an explicit PG transaction, acquires the advisory lock,
        and generates a TID via the main storage.
        """
        self._transaction = transaction
        self._resolved = []
        self._tmp = []
        self._blob_tmp = []
        self._conn.execute("BEGIN")
        self._conn.execute("SELECT pg_advisory_xact_lock(0)")
        if tid is None:
            self._tid = self._main._new_tid()
        else:
            self._tid = tid

    def tpc_vote(self, transaction):
        """Flush pending stores + blobs to PostgreSQL."""
        if transaction is not self._transaction:
            if transaction is not None:
                raise StorageTransactionError(self, transaction)
            return

        tid_int = u64(self._tid)
        hp = self._history_preserving

        with self._conn.cursor() as cur:
            user = transaction.user
            desc = transaction.description
            ext = transaction.extension
            if isinstance(user, bytes):
                user = user.decode("utf-8")
            if isinstance(desc, bytes):
                desc = desc.decode("utf-8")
            _write_txn_log(cur, tid_int, user, desc, ext)

            for obj in self._tmp:
                _write_object(cur, obj, tid_int, hp)

            for zoid, blob_path in self._blob_tmp:
                _write_blob(cur, zoid, tid_int, blob_path, hp)

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
        except Exception:
            logger.exception("Error during rollback")
        self._tmp.clear()
        # Clean up queued blob temp files
        for _, blob_path in self._blob_tmp:
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        self._blob_tmp.clear()
        self._transaction = None

    def checkCurrentSerialInTransaction(self, transaction, oid, serial):
        """Verify that the object's serial hasn't changed."""
        zoid = u64(oid)
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid FROM object_state WHERE zoid = %s", (zoid,)
            )
            row = cur.fetchone()
        if row is not None:
            current_serial = p64(row["tid"])
            if current_serial != serial:
                raise ReadConflictError(oid=oid)

    # ── IBlobStorage ─────────────────────────────────────────────────

    def storeBlob(self, oid, oldserial, data, blobfilename, version,
                  transaction):
        """Store object data + blob file."""
        if transaction is not self._transaction:
            raise StorageTransactionError(self, transaction)
        if version:
            raise TypeError("versions are not supported")
        self.store(oid, oldserial, data, '', transaction)
        self._blob_tmp.append((u64(oid), blobfilename))

    def loadBlob(self, oid, serial):
        """Return path to a file containing the blob data.

        Uses deterministic filenames so repeated calls for the same
        (oid, serial) return the same path — required by ZODB.blob.Blob.
        """
        zoid = u64(oid)
        tid_int = u64(serial)
        path = os.path.join(
            self._blob_temp_dir, f"{zoid:016x}-{tid_int:016x}.blob"
        )
        if os.path.exists(path):
            return path
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT data, s3_key FROM blob_state "
                "WHERE zoid = %s AND tid = %s",
                (zoid, tid_int),
            )
            row = cur.fetchone()
        if row is None:
            raise POSKeyError(oid)
        if row["s3_key"]:
            raise NotImplementedError("S3 blob loading not yet implemented")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            os.write(fd, row["data"])
        finally:
            os.close(fd)
        return path

    def openCommittedBlobFile(self, oid, serial, blob=None):
        """Open committed blob file for reading."""
        blob_path = self.loadBlob(oid, serial)
        if blob is None:
            return open(blob_path, 'rb')
        from ZODB.blob import BlobFile
        return BlobFile(blob_path, 'r', blob)

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

    def undoLog(self, first=0, last=-20, filter=None):
        return self._main.undoLog(first, last, filter)

    def undoInfo(self, first=0, last=-20, specification=None):
        return self._main.undoInfo(first, last, specification)

    def undo(self, transaction_id, transaction=None):
        """Undo a transaction — uses this instance's connection."""
        if not self._history_preserving:
            raise UndoError("Undo is not supported in history-free mode")

        tid_int = u64(transaction_id)

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT tid FROM transaction_log WHERE tid = %s",
                (tid_int,),
            )
            if cur.fetchone() is None:
                raise UndoError("Transaction not found")

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
                cur.execute(
                    "SELECT tid, class_mod, class_name, state, "
                    "state_size, refs "
                    "FROM object_history "
                    "WHERE zoid = %s AND tid < %s "
                    "ORDER BY tid DESC LIMIT 1",
                    (zoid, tid_int),
                )
                prev = cur.fetchone()
                if prev is None:
                    undo_data.append({
                        "zoid": zoid,
                        "action": "delete",
                    })
                else:
                    undo_data.append({
                        "zoid": zoid,
                        "action": "restore",
                        "class_mod": prev["class_mod"],
                        "class_name": prev["class_name"],
                        "state": prev["state"],
                        "state_size": prev["state_size"],
                        "refs": prev["refs"],
                    })

        oid_list = []
        for item in undo_data:
            oid_bytes = p64(item["zoid"])
            oid_list.append(oid_bytes)
            if item["action"] == "restore":
                self._tmp.append({
                    "zoid": item["zoid"],
                    "class_mod": item["class_mod"],
                    "class_name": item["class_name"],
                    "state": item["state"],
                    "state_size": item["state_size"],
                    "refs": item["refs"],
                })

        return oid_list

    def registerDB(self, db):
        pass

    def close(self):
        self.release()


def _serialize_extension(ext):
    """Serialize transaction extension for BYTEA storage.

    ZODB extensions are dicts; we store them as JSON bytes in PG.
    """
    if isinstance(ext, bytes):
        return ext
    if isinstance(ext, dict):
        if not ext:
            return b''
        import json
        return json.dumps(ext).encode('utf-8')
    return b''


def _write_txn_log(cur, tid_int, user, desc, ext):
    """Write a transaction log entry."""
    cur.execute(
        "INSERT INTO transaction_log "
        "(tid, username, description, extension) "
        "VALUES (%s, %s, %s, %s)",
        (tid_int, user, desc, _serialize_extension(ext)),
    )


def _write_object(cur, obj, tid_int, history_preserving=False):
    """Write a single object state to PostgreSQL."""
    params = {
        "zoid": obj["zoid"],
        "tid": tid_int,
        "class_mod": obj["class_mod"],
        "class_name": obj["class_name"],
        "state": Json(_sanitize_for_pg(obj["state"])),
        "state_size": obj["state_size"],
        "refs": obj["refs"],
    }
    if history_preserving:
        cur.execute(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "VALUES (%(zoid)s, %(tid)s, %(class_mod)s, %(class_name)s, "
            "%(state)s, %(state_size)s, %(refs)s)",
            params,
        )
    cur.execute(
        "INSERT INTO object_state "
        "(zoid, tid, class_mod, class_name, state, state_size, refs) "
        "VALUES (%(zoid)s, %(tid)s, %(class_mod)s, %(class_name)s, "
        "%(state)s, %(state_size)s, %(refs)s) "
        "ON CONFLICT (zoid) DO UPDATE SET "
        "tid = EXCLUDED.tid, class_mod = EXCLUDED.class_mod, "
        "class_name = EXCLUDED.class_name, state = EXCLUDED.state, "
        "state_size = EXCLUDED.state_size, refs = EXCLUDED.refs",
        params,
    )


def _write_blob(cur, zoid, tid_int, blob_path, history_preserving=False):
    """Write a blob to blob_state and remove the temp file."""
    size = os.path.getsize(blob_path)
    with open(blob_path, 'rb') as f:
        blob_data = f.read()
    if history_preserving:
        cur.execute(
            "INSERT INTO blob_history (zoid, tid, blob_size, data) "
            "VALUES (%s, %s, %s, %s)",
            (zoid, tid_int, size, blob_data),
        )
    cur.execute(
        "INSERT INTO blob_state (zoid, tid, blob_size, data) "
        "VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (zoid, tid) DO UPDATE SET "
        "blob_size = EXCLUDED.blob_size, data = EXCLUDED.data",
        (zoid, tid_int, size, blob_data),
    )
    os.unlink(blob_path)


def _loadBefore_hf(cur, oid, zoid, tid_int):
    """Load object data before tid — history-free mode.

    In history-free mode there's only one revision per object (in
    object_state).  Return it if its tid < requested tid, else None.
    """
    cur.execute(
        "SELECT tid, class_mod, class_name, state "
        "FROM object_state WHERE zoid = %s",
        (zoid,),
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
    )
    row = cur.fetchone()
    if row is None:
        # No revision before this tid — either doesn't exist or created later
        return None
    record = {
        "@cls": [row["class_mod"], row["class_name"]],
        "@s": _unsanitize_from_pg(row["state"]),
    }
    data = zodb_json_codec.encode_zodb_record(record)
    start_tid = p64(row["tid"])
    # Find end_tid: next revision's tid
    cur.execute(
        "SELECT MIN(tid) AS next_tid FROM object_history "
        "WHERE zoid = %s AND tid > %s",
        (zoid, row["tid"]),
    )
    next_row = cur.fetchone()
    end_tid = p64(next_row["next_tid"]) if next_row["next_tid"] else None
    return data, start_tid, end_tid


def _extract_refs(state):
    """Recursively walk decoded state and collect all @ref OIDs as integers."""
    refs = []
    if isinstance(state, dict):
        if "@ref" in state:
            ref_val = state["@ref"]
            # @ref is hex OID string, or [hex_oid, class_path]
            oid_hex = ref_val if isinstance(ref_val, str) else ref_val[0]
            refs.append(int(oid_hex, 16))
        else:
            for v in state.values():
                refs.extend(_extract_refs(v))
    elif isinstance(state, list):
        for item in state:
            refs.extend(_extract_refs(item))
    return refs


def _sanitize_for_pg(obj):
    """Replace strings containing null bytes with @ns markers.

    PostgreSQL JSONB cannot store \\u0000 in strings.  We base64-encode
    such strings with a ``@ns`` marker for lossless round-tripping.

    Returns the original objects unchanged when no null bytes are found
    (zero allocations in the common case).
    """
    if isinstance(obj, str):
        if '\x00' in obj:
            return {
                "@ns": base64.b64encode(
                    obj.encode('utf-8', errors='surrogatepass')
                ).decode('ascii'),
            }
        return obj
    if isinstance(obj, dict):
        new = {}
        changed = False
        for k, v in obj.items():
            new_v = _sanitize_for_pg(v)
            if new_v is not v:
                changed = True
            new[k] = new_v
        return new if changed else obj
    if isinstance(obj, list):
        new = []
        changed = False
        for item in obj:
            new_item = _sanitize_for_pg(item)
            if new_item is not item:
                changed = True
            new.append(new_item)
        return new if changed else obj
    return obj


def _unsanitize_from_pg(obj):
    """Reverse ``@ns`` markers back to strings with null bytes.

    Returns original objects unchanged when no @ns markers are found
    (zero allocations in the common case).
    """
    if isinstance(obj, dict):
        if "@ns" in obj and len(obj) == 1:
            return base64.b64decode(obj["@ns"]).decode(
                'utf-8', errors='surrogatepass',
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
