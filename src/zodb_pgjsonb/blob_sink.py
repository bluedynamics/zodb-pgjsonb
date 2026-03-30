"""Pluggable blob upload sinks for parallel migration.

BlobSink decouples S3 blob uploads from PG writes during
copyTransactionsFrom. Three modes:

- InlineBlobSink: upload in the calling thread (blocking, current behavior)
- BackgroundBlobSink: upload via background thread pool (PG continues)
- DeferredBlobSink: write manifest file, upload later via CLI tool
"""
from __future__ import annotations

import contextlib
import logging
import os
import time
from typing import Protocol


logger = logging.getLogger(__name__)

_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_BASE_DELAY = 2.0


def _upload_with_retry(s3_client, blob_path, s3_key, zoid, size,
                       max_retries=_DEFAULT_MAX_RETRIES,
                       retry_base_delay=_DEFAULT_RETRY_BASE_DELAY):
    """Upload a single blob to S3 with exponential-backoff retry."""
    if size >= 10_000_000:
        logger.info(
            "Uploading blob oid=0x%016x (%s) to S3 ...",
            zoid, _fmt_size(size),
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
                delay = retry_base_delay * (2 ** attempt)
                logger.warning(
                    "S3 upload oid=0x%016x attempt %d/%d failed (%s), "
                    "retrying in %.0fs ...",
                    zoid, attempt + 1, max_retries, exc, delay,
                )
                time.sleep(delay)
    else:
        raise last_exc  # type: ignore[misc]
    elapsed = time.time() - t0
    if elapsed >= 5.0:
        logger.info(
            "S3 upload oid=0x%016x (%s) took %.0fs",
            zoid, _fmt_size(size), elapsed,
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

    def submit(self, blob_path: str, s3_key: str, zoid: int, size: int,
               cleanup_path: str | None = None) -> None:
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

    def __init__(self, s3_client, max_retries=_DEFAULT_MAX_RETRIES,
                 retry_base_delay=_DEFAULT_RETRY_BASE_DELAY):
        self._s3 = s3_client
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay

    def submit(self, blob_path, s3_key, zoid, size, cleanup_path=None):
        _upload_with_retry(
            self._s3, blob_path, s3_key, zoid, size,
            self._max_retries, self._retry_base_delay,
        )
        if cleanup_path:
            with contextlib.suppress(OSError):
                os.unlink(cleanup_path)

    def drain(self):
        pass

    def close(self):
        pass
