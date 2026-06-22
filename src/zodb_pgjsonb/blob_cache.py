"""Bounded local filesystem cache for materialized blob files.

``loadBlob`` must return a local file path for every blob it is asked
for.  Without a bound, those materialized files accumulate for the whole
lifetime of a worker process and fill local/ephemeral disk (#71).

``LocalBlobCache`` is a size-bounded LRU (by access time) cache used as
the single materialization target for *both* PG-bytea blobs and blobs
downloaded from S3.  It mirrors the get/put interface of
``zodb_s3blobs.cache.S3BlobCache`` (which is used directly when S3 is
configured) so ``loadBlob`` can treat either interchangeably.

Eviction-while-open is safe: ZODB opens the returned path immediately and
on POSIX an open file descriptor keeps the inode alive after the path is
unlinked, so an in-flight read survives a concurrent eviction.  The same
guarantee already backs the S3 cache.
"""

from ZODB.utils import oid_repr

import contextlib
import logging
import os
import shutil
import threading


logger = logging.getLogger(__name__)


def _hex(data):
    """Convert oid/tid bytes to a compact hex string (no 0x prefix)."""
    return oid_repr(data).removeprefix("0x").lstrip("0") or "0"


class LocalBlobCache:
    """Local filesystem LRU cache for materialized blobs.

    Files are stored as ``{cache_dir}/{oid_hex}/{tid_hex}.blob``.  A
    background thread removes the oldest files (by atime) once the total
    size exceeds ``max_size``.
    """

    def __init__(self, cache_dir, max_size=1024 * 1024 * 1024):
        self.cache_dir = cache_dir
        self.max_size = max_size
        self._target_size = int(max_size * 0.9)
        self._check_threshold = max(int(max_size * 0.1), 1)
        self._bytes_loaded = 0
        self._lock = threading.Lock()
        self._checker_thread = None
        os.makedirs(cache_dir, exist_ok=True, mode=0o700)

    def _blob_path(self, oid, tid):
        return os.path.join(self.cache_dir, _hex(oid), f"{_hex(tid)}.blob")

    def get(self, oid, tid):
        """Return the cached path for (oid, tid), or None on miss.

        Touches atime so the LRU keeps recently used blobs.
        """
        path = self._blob_path(oid, tid)
        try:
            os.utime(path)  # touch atime; also verifies the file exists
            return path
        except FileNotFoundError:
            return None

    def put(self, oid, tid, source_path):
        """Copy ``source_path`` into the cache and return its cached path."""
        path = self._blob_path(oid, tid)
        os.makedirs(os.path.dirname(path), exist_ok=True, mode=0o700)
        shutil.copy2(source_path, path)
        self.notify_loaded(os.path.getsize(path))
        return path

    def notify_loaded(self, byte_count):
        with self._lock:
            self._bytes_loaded += byte_count
            if self._bytes_loaded >= self._check_threshold:
                self._bytes_loaded = 0
                self._start_cleanup()

    def _start_cleanup(self):
        """Start the background cleanup thread (call with ``_lock`` held)."""
        if self._checker_thread is not None and self._checker_thread.is_alive():
            return
        t = threading.Thread(target=self._cleanup, daemon=True)
        self._checker_thread = t
        t.start()

    def _cleanup(self):
        """Remove oldest files (by atime) until under the target size."""
        try:
            files = []
            for dirpath, _dirnames, filenames in os.walk(self.cache_dir):
                for fn in filenames:
                    if fn.endswith(".blob"):
                        fp = os.path.join(dirpath, fn)
                        try:
                            st = os.stat(fp)
                            files.append((st.st_atime, st.st_size, fp))
                        except OSError:
                            pass

            total_size = sum(size for _, size, _ in files)
            if total_size <= self.max_size:
                return

            files.sort(key=lambda x: x[0])  # oldest atime first
            for _atime, size, fp in files:
                if total_size <= self._target_size:
                    break
                try:
                    os.remove(fp)
                    total_size -= size
                    parent = os.path.dirname(fp)
                    if parent != self.cache_dir:
                        with contextlib.suppress(OSError):
                            os.rmdir(parent)
                except OSError:
                    pass
        except Exception:
            logger.exception("Error during blob cache cleanup")

    def close(self):
        """Wait for any running cleanup thread to finish."""
        with self._lock:
            t = self._checker_thread
        if t is not None:
            t.join(timeout=5)

    def wait_for_cleanup(self):
        """Wait for any running cleanup thread to finish (for testing)."""
        with self._lock:
            t = self._checker_thread
        if t is not None:
            t.join(timeout=10)

    def current_size(self):
        """Total size of cached files (for testing)."""
        total = 0
        for dirpath, _dirnames, filenames in os.walk(self.cache_dir):
            for fn in filenames:
                if fn.endswith(".blob"):
                    with contextlib.suppress(OSError):
                        total += os.path.getsize(os.path.join(dirpath, fn))
        return total
