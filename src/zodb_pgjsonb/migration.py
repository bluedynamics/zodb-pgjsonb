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
