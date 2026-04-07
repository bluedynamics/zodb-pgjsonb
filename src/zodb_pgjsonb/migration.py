"""Migration helper functions for zodbconvert / copy-transactions."""

from .batch import _write_prepared_transaction
from ZODB.blob import is_blob_record
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import POSKeyError
from ZODB.utils import p64
from ZODB.utils import u64

import contextlib
import logging
import os
import shutil
import tempfile
import threading
import time
import traceback
import zodb_json_codec


logger = logging.getLogger(__name__)


def _fmt_blob_size(size):
    """Format byte size as human-readable string."""
    if size >= 1_000_000_000:
        return f"{size / 1_000_000_000:.1f} GB"
    if size >= 1_000_000:
        return f"{size / 1_000_000:.1f} MB"
    if size >= 1_000:
        return f"{size / 1_000:.0f} KB"
    return f"{size} B"


def _fmt_elapsed(seconds):
    """Format elapsed seconds as H:MM:SS or MM:SS."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:2d}:{s:02d}"


def _stage_blob(src, temp_dir):
    """Stage a blob for import: hard-link to temp (same FS) or use source directly.

    Returns (path, is_temp) — caller must only unlink if is_temp is True.
    """
    if os.stat(src).st_dev != os.stat(temp_dir).st_dev:
        # Cross-filesystem — hard-link impossible, use source directly.
        return src, False
    fd, tmp = tempfile.mkstemp(suffix=".blob", dir=temp_dir)
    os.close(fd)
    os.unlink(tmp)  # remove empty placeholder
    try:
        os.link(src, tmp)
        return tmp, True
    except OSError:
        # Hard-link failed (e.g. fs.protected_hardlinks blocks linking
        # to files owned by another user). Use source directly.
        return src, False


def _create_blob_sink(blob_mode, s3_client):
    """Create a BlobSink based on the blob_mode string and S3 client.

    Returns None when no S3 client is configured and mode is not deferred.
    """
    from zodb_pgjsonb.blob_sink import BackgroundBlobSink
    from zodb_pgjsonb.blob_sink import DeferredBlobSink
    from zodb_pgjsonb.blob_sink import InlineBlobSink

    if blob_mode == "background" and s3_client is not None:
        return BackgroundBlobSink(s3_client)
    if isinstance(blob_mode, str) and blob_mode.startswith("deferred:"):
        manifest_path = blob_mode.split(":", 1)[1]
        return DeferredBlobSink(manifest_path)
    if s3_client is not None:
        return InlineBlobSink(s3_client)
    return None


# ── Watermark tracking ──────────────────────────────────────────────────


class WatermarkTracker:
    """Tracks the highest contiguously committed TID during parallel migration.

    Manages the ``migration_watermark`` table lifecycle and periodic flushing.
    On clean completion, the table is dropped.  On error, the watermark is
    flushed so interrupted imports can resume.
    """

    _UPSERT_SQL = (
        "INSERT INTO migration_watermark (id, tid) "
        "VALUES (TRUE, %s) "
        "ON CONFLICT (id) DO UPDATE SET tid = EXCLUDED.tid"
    )

    def __init__(self, conn, start_tid=None, flush_interval=1.0):
        self._conn = conn
        self._flush_interval = flush_interval
        self._dispatched = []  # [(tid_int, future), ...]
        self._idx = 0
        self._last_flush = time.time()
        self.effective_start = self._setup(start_tid)
        self.idempotent = start_tid is not None

    def _setup(self, start_tid):
        """Create watermark table and compute effective start TID."""
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS migration_watermark ("
            "  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),"
            "  tid BIGINT NOT NULL"
            ")"
        )
        with self._conn.cursor() as cur:
            cur.execute("SELECT tid FROM migration_watermark WHERE id = TRUE")
            row = cur.fetchone()
        if row is not None and start_tid is not None:
            watermark_tid = row["tid"]
            effective_start_int = min(u64(start_tid), watermark_tid + 1)
            logger.info(
                "Resuming from watermark TID 0x%016x "
                "(start_tid=0x%016x, effective=0x%016x)",
                watermark_tid,
                u64(start_tid),
                effective_start_int,
            )
            return p64(effective_start_int)
        if start_tid is not None:
            logger.info(
                "Starting incremental copy from TID 0x%016x",
                u64(start_tid),
            )
            return start_tid
        return None

    def append(self, tid_int, future):
        """Record a dispatched transaction."""
        self._dispatched.append((tid_int, future))

    def advance(self):
        """Advance past contiguously completed futures. Re-raises worker errors."""
        while self._idx < len(self._dispatched):
            _tid, fut = self._dispatched[self._idx]
            if not fut.done():
                break
            fut.result()  # re-raise worker errors
            self._idx += 1

    def maybe_flush(self):
        """Flush watermark to PG if enough time has elapsed."""
        now = time.time()
        if self._idx > 0 and now - self._last_flush >= self._flush_interval:
            tid_val = self._dispatched[self._idx - 1][0]
            self._conn.execute(self._UPSERT_SQL, (tid_val,))
            self._last_flush = now
        # Prune dispatched list to avoid unbounded memory.
        if self._idx > 1000:
            self._dispatched = self._dispatched[self._idx :]
            self._idx = 0

    def flush_final(self):
        """Advance and flush after all workers are done."""
        self.advance()
        if self._idx > 0:
            tid_val = self._dispatched[self._idx - 1][0]
            self._conn.execute(self._UPSERT_SQL, (tid_val,))

    def flush_on_error(self):
        """Best-effort flush for interrupted imports (swallows exceptions)."""
        try:
            while self._idx < len(self._dispatched):
                _tid, fut = self._dispatched[self._idx]
                if not fut.done():
                    break
                try:
                    fut.result()
                except Exception:
                    break
                self._idx += 1
            if self._idx > 0:
                tid_val = self._dispatched[self._idx - 1][0]
                self._conn.execute(self._UPSERT_SQL, (tid_val,))
        except Exception:
            logger.debug("Failed to flush watermark on error", exc_info=True)

    def drop_table(self):
        """Drop the watermark table on clean completion."""
        self._conn.execute("DROP TABLE IF EXISTS migration_watermark")


# ── Progress tracking ────────────────────────────────────────────────────


class ProgressTracker:
    """Thread-safe progress counters and ETA estimation for parallel migration.

    Workers call :meth:`record_write` after each committed transaction.
    The main thread calls :meth:`log_progress` periodically.
    """

    _EMA_ALPHA = 0.3

    def __init__(self, total_oids=0, log_interval=10.0):
        self.total_oids = total_oids
        self._log_interval = log_interval
        self._lock = threading.Lock()
        self.written_txns = 0
        self.written_objs = 0
        self.written_blobs = 0
        self.written_errors = 0
        self._begin_time = time.time()
        self._last_log_time = self._begin_time
        self._prev_done_objs = 0
        self._prev_done_time = self._begin_time
        self._ema_rate = 0.0
        # Column width for right-aligned counters.
        self._cw = len(f"{total_oids:,}") if total_oids else 0

    def record_write(self, n_objs, n_blobs):
        """Called by worker threads after a successful commit."""
        with self._lock:
            self.written_txns += 1
            self.written_objs += n_objs
            self.written_blobs += n_blobs

    def record_error(self):
        """Called by worker threads on failure."""
        with self._lock:
            self.written_errors += 1

    @property
    def errors(self):
        with self._lock:
            return self.written_errors

    def snapshot(self):
        """Return a consistent snapshot of (txns, objs, blobs, errors)."""
        with self._lock:
            return (
                self.written_txns,
                self.written_objs,
                self.written_blobs,
                self.written_errors,
            )

    def log_progress(self, txn_count):  # pragma: no cover
        """Log progress if enough time has elapsed. Returns immediately otherwise."""
        now = time.time()
        if now - self._last_log_time < self._log_interval:
            return
        elapsed = now - self._begin_time
        hms = _fmt_elapsed(elapsed)
        done_txns, done_objs, done_blobs, errors = self.snapshot()
        cw = self._cw
        if cw:
            label = (
                f"[{hms}] Read {txn_count:>{cw},} txns |"
                f" Written {done_txns:>{cw},} txns,"
                f" {done_objs:>{cw},} OIDs,"
                f" {done_blobs:>{cw},} blobs"
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
        if self.total_oids and done_objs > 0:
            pct = min(done_objs * 100.0 / self.total_oids, 99.9)
            parts[0] += f" ({pct:5.1f}%)"
            dt = now - self._prev_done_time
            d_objs = done_objs - self._prev_done_objs
            if dt > 0 and d_objs > 0:
                window_rate = d_objs / dt
                if self._ema_rate <= 0:
                    self._ema_rate = window_rate
                else:
                    self._ema_rate = (
                        self._EMA_ALPHA * window_rate
                        + (1 - self._EMA_ALPHA) * self._ema_rate
                    )
                remaining = self.total_oids - done_objs
                eta_s = remaining / self._ema_rate
                if eta_s < 60:
                    eta = f"{eta_s:.0f}s"
                elif eta_s < 3600:
                    eta = f"{eta_s / 60:.0f}m"
                else:
                    eta = f"{eta_s / 3600:.1f}h"
                parts.append(f"ETA: {eta}")
        self._prev_done_objs = done_objs
        self._prev_done_time = now
        logger.info(" | ".join(parts))
        self._last_log_time = now

    def log_drain_wait(self, txn_count):
        """Log that the reader is done and we're waiting for workers."""
        _, _, _, _ = self.snapshot()
        pending = txn_count - self.written_txns
        if pending:
            logger.info(
                "[%s] Reader done (%d txns). Waiting for %d in-flight ...",
                _fmt_elapsed(time.time() - self._begin_time),
                txn_count,
                pending,
            )

    def drain_workers(self, txn_count):
        """Wait for all workers to finish, logging periodic progress."""
        shutdown_start = time.time()
        while True:
            done_txns, _, _, done_errors = self.snapshot()
            if done_txns + done_errors >= txn_count:
                break
            # pragma: no cover — drain loop only runs when workers
            # take > 1s (slow S3 uploads); tests finish instantly.
            time.sleep(1)  # pragma: no cover
            waited = time.time() - shutdown_start  # pragma: no cover
            if int(waited) % 10 == 0:  # pragma: no cover
                still = txn_count - done_txns - done_errors
                logger.info(
                    "[%s] Still waiting for %d worker(s) ... (%d done, %d errors)",
                    _fmt_elapsed(time.time() - self._begin_time),
                    still,
                    done_txns,
                    done_errors,
                )

    def log_complete(self, txn_count, num_workers, total_missing_blobs):
        """Log final completion message."""
        elapsed = time.time() - self._begin_time
        logger.info(
            "[%s] All %d transactions copied with %d workers.",
            _fmt_elapsed(elapsed),
            txn_count,
            num_workers,
        )
        if total_missing_blobs:  # pragma: no cover
            logger.error(
                "%d blob(s) missing in source storage and skipped during import!",
                total_missing_blobs,
            )


def _check_worker_errors(progress):
    """Raise RuntimeError if any worker errors have been recorded."""
    if progress.errors:
        raise RuntimeError(
            f"Aborting: {progress.errors} worker error(s). "
            "Check log for details. "
            "Is the target database empty?"
        )


# ── Mixin ────────────────────────────────────────────────────────────────


class CopyTransactionsMixin:
    """Mixin providing copyTransactionsFrom and helpers for PGJsonbStorage."""

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

        pool_max = self._instance_pool.max_size
        max_workers = max(pool_max - 1, 1)  # reserve 1 for watermark
        if num_workers > max_workers:
            logger.warning(
                "Requested %d workers but pool_max_size is %d "
                "(reserving 1 for watermark). Using %d workers.",
                num_workers,
                pool_max,
                max_workers,
            )
            num_workers = max_workers

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

        # ── Setup ──────────────────────────────────────────────────
        wm_conn = self._instance_pool.getconn()
        try:
            watermark = WatermarkTracker(wm_conn, start_tid)
        except BaseException:
            self._instance_pool.putconn(wm_conn)
            raise

        blob_sink = _create_blob_sink(blob_mode, self._s3_client)
        progress = ProgressTracker(total_oids)

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

        _dispatch_sem = threading.BoundedSemaphore(num_workers * 2)

        def _write_worker(txn_data):
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
                    idempotent=watermark.idempotent,
                )
                progress.record_write(len(txn_data["objects"]), len(txn_data["blobs"]))
            except Exception:
                logger.error(
                    "Worker error on tid=0x%016x: %s",
                    txn_data["tid_int"],
                    traceback.format_exc(),
                )
                progress.record_error()
                raise
            finally:
                _dispatch_sem.release()

        # ── Dispatch loop ──────────────────────────────────────────
        in_flight = {}  # zoid → Future
        txn_count = 0
        total_missing_blobs = 0
        last_tid = None

        executor = ThreadPoolExecutor(max_workers=num_workers)
        try:
            for txn_info in other.iterator(start=watermark.effective_start):
                txn_data = self._prepare_transaction(txn_info, other)
                txn_oids = {obj["zoid"] for obj in txn_data["objects"]}
                txn_oids.update(entry[0] for entry in txn_data["blobs"])

                # Wait for any in-flight transactions touching same OIDs.
                blocking = {
                    in_flight[oid]
                    for oid in txn_oids
                    if oid in in_flight and not in_flight[oid].done()
                }
                if blocking:
                    futures_wait(blocking)
                    _check_worker_errors(progress)

                _dispatch_sem.acquire()
                future = executor.submit(_write_worker, txn_data)
                for oid in txn_oids:
                    in_flight[oid] = future

                watermark.append(txn_data["tid_int"], future)
                watermark.advance()
                watermark.maybe_flush()

                txn_count += 1
                total_missing_blobs += txn_data["missing_blobs"]
                last_tid = txn_data["tid"]

                _check_worker_errors(progress)
                progress.log_progress(txn_count)

            # ── Drain ──────────────────────────────────────────────
            progress.log_drain_wait(txn_count)
            executor.shutdown(wait=False)
            progress.drain_workers(txn_count)
            _check_worker_errors(progress)

            watermark.flush_final()
            if blob_sink is not None:
                blob_sink.close()
            watermark.drop_table()

            if last_tid is not None:
                self._ltid = last_tid

        except BaseException:
            executor.shutdown(wait=False, cancel_futures=True)
            if blob_sink is not None:
                with contextlib.suppress(Exception):
                    blob_sink.close()
            watermark.flush_on_error()
            raise

        finally:
            for conn in worker_conns:
                with contextlib.suppress(Exception):
                    self._instance_pool.putconn(conn)
            with contextlib.suppress(Exception):
                self._instance_pool.putconn(wm_conn)

        progress.log_complete(txn_count, num_workers, total_missing_blobs)
