"""Blob storage statistics and histogram queries."""

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


def get_blob_stats(conn, s3_client, blob_threshold):
    """Return blob storage statistics.

    Returns a dict with keys: available, total_blobs, unique_objects,
    avg_versions, total_size(_display), pg_count, pg_size(_display),
    s3_count, s3_size(_display), largest_blob(_display),
    avg_blob_size(_display).
    """
    with conn.cursor() as cur:
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
    avg_versions = round(total_blobs / unique_objects, 1) if unique_objects > 0 else 0

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
        "s3_configured": s3_client is not None,
        "blob_threshold": blob_threshold,
        "blob_threshold_display": _format_size(blob_threshold),
    }


def get_blob_histogram(conn, s3_client, blob_threshold):
    """Return blob size distribution as logarithmic buckets.

    Returns a list of dicts with keys: label, count, pct, tier.
    tier is 'pg', 's3', or 'mixed' based on the S3 blob threshold.
    Empty list if no blobs exist.
    """
    with conn.cursor() as cur:
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
    threshold = blob_threshold if s3_client is not None else None
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
