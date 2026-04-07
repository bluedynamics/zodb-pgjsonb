"""Migration helper functions for zodbconvert / copy-transactions."""

import os
import tempfile


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
