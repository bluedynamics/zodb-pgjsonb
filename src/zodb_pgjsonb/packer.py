"""Packer — Pure SQL graph traversal for object garbage collection.

Unlike RelStorage/FileStorage which must load and unpickle every object
to extract references, we use the pre-extracted `refs` column for a
server-side recursive CTE query. No data leaves PostgreSQL during pack.
"""

from .storage import _table_exists
from ZODB.utils import u64

import logging


logger = logging.getLogger(__name__)

# Reachability query: find all objects reachable from root (zoid=0).
# PostgreSQL recursive CTEs terminate naturally when the recursive term
# produces no new rows — there is no explicit depth limit.  For very
# large databases with deep reference chains, ensure adequate work_mem
# and consider setting statement_timeout to prevent runaway queries.
REACHABLE_QUERY = """\
WITH RECURSIVE reachable AS (
    SELECT zoid FROM object_state WHERE zoid = 0
    UNION
    SELECT unnest(o.refs)
    FROM object_state o
    JOIN reachable r ON o.zoid = r.zoid
)
SELECT zoid FROM reachable
"""

# Anti-join pattern: NOT EXISTS is faster than NOT IN for large tables.
# NOT IN builds a hash of the entire subquery and checks every row.
# NOT EXISTS uses an anti-join that short-circuits on the first match.
_NOT_REACHABLE = (
    "NOT EXISTS (SELECT 1 FROM reachable_oids r WHERE r.zoid = {alias}.zoid)"
)


def pack(conn, pack_time=None, history_preserving=False):
    """Remove unreachable objects and their blobs.

    Args:
        conn: psycopg connection
        pack_time: pack_time bytes (TID) — used in history-preserving mode
            to remove old revisions before this point
        history_preserving: if True, also clean up history tables

    In history-preserving mode with pack_time, objects created or modified
    after pack_time are preserved even if currently unreachable — they may
    be needed for undo operations.

    Returns:
        Tuple of (deleted_objects, deleted_blobs, s3_keys_to_delete)
    """
    s3_keys = []
    pack_tid = u64(pack_time) if pack_time is not None else None

    with conn.cursor() as cur:
        # Phase 1: Find reachable objects
        cur.execute(f"SELECT zoid INTO TEMP reachable_oids FROM ({REACHABLE_QUERY}) q")
        cur.execute("CREATE INDEX ON reachable_oids (zoid)")
        logger.info("Pack: reachability trace complete")

        # Phase 2: Delete unreachable objects
        not_reachable_os = _NOT_REACHABLE.format(alias="o")
        if history_preserving and pack_tid is not None:
            cur.execute(
                f"DELETE FROM object_state o WHERE {not_reachable_os} AND o.tid <= %s",
                (pack_tid,),
            )
        else:
            cur.execute(f"DELETE FROM object_state o WHERE {not_reachable_os}")
        deleted_objects = cur.rowcount
        logger.info("Pack: deleted %d unreachable objects", deleted_objects)

        # Phase 3: Delete unreachable blobs, collecting S3 keys
        not_reachable_bs = _NOT_REACHABLE.format(alias="b")
        if history_preserving and pack_tid is not None:
            cur.execute(
                f"DELETE FROM blob_state b WHERE {not_reachable_bs} "
                "AND b.tid <= %s "
                "RETURNING s3_key",
                (pack_tid,),
            )
        else:
            cur.execute(
                f"DELETE FROM blob_state b WHERE {not_reachable_bs} RETURNING s3_key"
            )
        deleted_blobs = cur.rowcount
        for row in cur.fetchall():
            if row[0]:
                s3_keys.append(row[0])
        logger.info("Pack: deleted %d unreachable blobs", deleted_blobs)

        # Phase 4: History cleanup (history-preserving mode only)
        deleted_history = 0
        deleted_blob_revisions = 0
        if history_preserving:
            not_reachable_oh = _NOT_REACHABLE.format(alias="oh")
            if pack_tid is not None:
                cur.execute(
                    f"DELETE FROM object_history oh "
                    f"WHERE {not_reachable_oh} AND oh.tid <= %s",
                    (pack_tid,),
                )
            else:
                cur.execute(f"DELETE FROM object_history oh WHERE {not_reachable_oh}")
            deleted_history += cur.rowcount

            # Clean up blob_history for unreachable objects (backward compat)
            if _table_exists(cur, "blob_history"):
                not_reachable_bh = _NOT_REACHABLE.format(alias="bh")
                if pack_tid is not None:
                    cur.execute(
                        f"DELETE FROM blob_history bh "
                        f"WHERE {not_reachable_bh} AND bh.tid <= %s "
                        "RETURNING s3_key",
                        (pack_tid,),
                    )
                else:
                    cur.execute(
                        f"DELETE FROM blob_history bh "
                        f"WHERE {not_reachable_bh} "
                        "RETURNING s3_key"
                    )
                for row in cur.fetchall():
                    if row[0]:
                        s3_keys.append(row[0])

            # Remove old revisions for reachable objects before pack_time
            if pack_tid is not None:
                cur.execute(
                    "DELETE FROM object_history oh "
                    "WHERE oh.tid < %s "
                    "AND EXISTS ("
                    "  SELECT 1 FROM reachable_oids r WHERE r.zoid = oh.zoid"
                    ") "
                    "AND ("
                    "  EXISTS ("
                    "    SELECT 1 FROM object_history oh2 "
                    "    WHERE oh2.zoid = oh.zoid "
                    "    AND oh2.tid > oh.tid AND oh2.tid <= %s"
                    "  )"
                    "  OR EXISTS ("
                    "    SELECT 1 FROM object_state os "
                    "    WHERE os.zoid = oh.zoid "
                    "    AND os.tid > oh.tid AND os.tid <= %s"
                    "  )"
                    ")",
                    (pack_tid, pack_tid, pack_tid),
                )
                deleted_history += cur.rowcount

                # Clean old blob_state revisions for reachable objects
                cur.execute(
                    "DELETE FROM blob_state bs "
                    "WHERE bs.tid < %s "
                    "AND EXISTS ("
                    "  SELECT 1 FROM reachable_oids r WHERE r.zoid = bs.zoid"
                    ") "
                    "AND EXISTS ("
                    "  SELECT 1 FROM blob_state bs2 "
                    "  WHERE bs2.zoid = bs.zoid "
                    "  AND bs2.tid > bs.tid AND bs2.tid <= %s"
                    ") "
                    "RETURNING s3_key",
                    (pack_tid, pack_tid),
                )
                deleted_blob_revisions += cur.rowcount
                for row in cur.fetchall():
                    if row[0]:
                        s3_keys.append(row[0])

                # Clean old blob_history revisions (backward compat)
                if _table_exists(cur, "blob_history"):
                    cur.execute(
                        "DELETE FROM blob_history bh "
                        "WHERE bh.tid < %s "
                        "AND EXISTS ("
                        "  SELECT 1 FROM reachable_oids r "
                        "  WHERE r.zoid = bh.zoid"
                        ") "
                        "AND EXISTS ("
                        "  SELECT 1 FROM blob_history bh2 "
                        "  WHERE bh2.zoid = bh.zoid "
                        "  AND bh2.tid > bh.tid AND bh2.tid <= %s"
                        ") "
                        "RETURNING s3_key",
                        (pack_tid, pack_tid),
                    )
                    for row in cur.fetchall():
                        if row[0]:
                            s3_keys.append(row[0])

        # Phase 5: Clean up transaction_log entries at or before pack_time
        # that are no longer referenced by object_state (FK constraint).
        _deleted_txns = 0
        if history_preserving and pack_tid is not None:
            cur.execute(
                "DELETE FROM transaction_log t "
                "WHERE t.tid <= %s "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM object_state os "
                "  WHERE os.tid = t.tid"
                ")",
                (pack_tid,),
            )
            _deleted_txns = cur.rowcount

        # Cleanup temp table
        cur.execute("DROP TABLE reachable_oids")

    conn.commit()

    logger.info(
        "Pack complete: removed %d objects, %d blobs, "
        "%d history rows, %d blob revisions, "
        "%d S3 keys to clean",
        deleted_objects,
        deleted_blobs,
        deleted_history,
        deleted_blob_revisions,
        len(s3_keys),
    )

    return deleted_objects, deleted_blobs, s3_keys
