"""Pluggable blob upload sinks for parallel migration.

BlobSink decouples S3 blob uploads from PG writes during
copyTransactionsFrom. Three modes:

- InlineBlobSink: upload in the calling thread (blocking, current behavior)
- BackgroundBlobSink: upload via background thread pool (PG continues)
- DeferredBlobSink: write manifest file, upload later via CLI tool
"""

from __future__ import annotations

from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

import contextlib
import logging
import os
import threading
import time


logger = logging.getLogger(__name__)

_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_BASE_DELAY = 2.0
_DEFAULT_BACKGROUND_WORKERS = 16


def _upload_with_retry(
    s3_client,
    blob_path,
    s3_key,
    zoid,
    size,
    max_retries=_DEFAULT_MAX_RETRIES,
    retry_base_delay=_DEFAULT_RETRY_BASE_DELAY,
):
    """Upload a single blob to S3 with exponential-backoff retry."""
    if size >= 10_000_000:
        logger.info(
            "Uploading blob oid=0x%016x (%s) to S3 ...",
            zoid,
            _fmt_size(size),
        )
    t0 = time.time()
    last_exc = None
    for attempt in range(max_retries):
        try:
            s3_client.upload_file(blob_path, s3_key)
            break
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                delay = retry_base_delay * (2**attempt)
                logger.warning(
                    "S3 upload oid=0x%016x attempt %d/%d failed (%s), "
                    "retrying in %.0fs ...",
                    zoid,
                    attempt + 1,
                    max_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
    else:
        raise last_exc  # type: ignore[misc]
    elapsed = time.time() - t0
    if elapsed >= 5.0:
        logger.info(
            "S3 upload oid=0x%016x (%s) took %.0fs",
            zoid,
            _fmt_size(size),
            elapsed,
        )


def _fmt_size(size):
    if size >= 1_000_000_000:
        return f"{size / 1_000_000_000:.1f} GB"
    if size >= 1_000_000:
        return f"{size / 1_000_000:.1f} MB"
    if size >= 1_000:
        return f"{size / 1_000:.0f} KB"
    return f"{size} B"


class BlobSink(Protocol):
    """Protocol for blob upload destinations."""

    def submit(
        self,
        blob_path: str,
        s3_key: str,
        zoid: int,
        size: int,
        cleanup_path: str | None = None,
    ) -> None:
        """Submit a blob for upload. May block (inline) or enqueue (background)."""
        ...

    def drain(self) -> None:
        """Wait for all pending uploads to complete. Raises on failures."""
        ...

    def close(self) -> None:
        """Drain and release resources."""
        ...


class InlineBlobSink:
    """Upload blobs synchronously in the calling thread (current behavior)."""

    def __init__(
        self,
        s3_client,
        max_retries=_DEFAULT_MAX_RETRIES,
        retry_base_delay=_DEFAULT_RETRY_BASE_DELAY,
    ):
        self._s3 = s3_client
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay

    def submit(self, blob_path, s3_key, zoid, size, cleanup_path=None):
        _upload_with_retry(
            self._s3,
            blob_path,
            s3_key,
            zoid,
            size,
            self._max_retries,
            self._retry_base_delay,
        )
        if cleanup_path:
            with contextlib.suppress(OSError):
                os.unlink(cleanup_path)

    def drain(self):
        pass

    def close(self):
        pass


class BackgroundBlobSink:
    """Upload blobs via a background thread pool, decoupled from PG workers."""

    def __init__(
        self,
        s3_client,
        max_workers=_DEFAULT_BACKGROUND_WORKERS,
        max_retries=_DEFAULT_MAX_RETRIES,
        retry_base_delay=_DEFAULT_RETRY_BASE_DELAY,
    ):
        self._s3 = s3_client
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: list[Future] = []
        self._closed = False

    def _do_upload(self, blob_path, s3_key, zoid, size, cleanup_path):
        try:
            _upload_with_retry(
                self._s3,
                blob_path,
                s3_key,
                zoid,
                size,
                self._max_retries,
                self._retry_base_delay,
            )
        finally:
            if cleanup_path:
                with contextlib.suppress(OSError):
                    os.unlink(cleanup_path)

    def submit(self, blob_path, s3_key, zoid, size, cleanup_path=None):
        if self._closed:
            raise RuntimeError("BlobSink is closed")
        # Prune completed futures every 256 submits to bound memory.
        if len(self._futures) >= 256:
            self._futures = [f for f in self._futures if not f.done()]
        fut = self._pool.submit(
            self._do_upload,
            blob_path,
            s3_key,
            zoid,
            size,
            cleanup_path,
        )
        self._futures.append(fut)

    def drain(self):
        errors = []
        for fut in self._futures:
            try:
                fut.result()
            except Exception as exc:
                errors.append(exc)
        self._futures.clear()
        if errors:
            raise RuntimeError(
                f"{len(errors)} blob upload(s) failed. First error: {errors[0]}"
            )

    def close(self):
        if not self._closed:
            self.drain()
            self._pool.shutdown(wait=True)
            self._closed = True


class DeferredBlobSink:
    """Write blob upload tasks to a manifest file for later processing.

    No S3 uploads happen during the migration. The manifest is a TSV
    file with columns: blob_path, s3_key, zoid, size.

    Temp files are intentionally preserved (not deleted) because they
    are needed by the upload tool that processes the manifest later.
    """

    def __init__(self, manifest_path):
        self._path = manifest_path
        self._file = open(manifest_path, "a")  # noqa: SIM115
        self._lock = threading.Lock()
        self._count = 0

    def submit(self, blob_path, s3_key, zoid, size, cleanup_path=None):
        # Intentionally ignore cleanup_path — temp files must survive.
        with self._lock:
            self._file.write(f"{blob_path}\t{s3_key}\t{zoid}\t{size}\n")
            self._count += 1

    def drain(self):
        pass

    def close(self):
        self._file.close()
        if self._count:
            logger.info(
                "%d deferred blob upload(s) written to %s",
                self._count,
                self._path,
            )
