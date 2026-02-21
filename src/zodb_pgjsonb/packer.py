"""Packer — Pure SQL graph traversal for object garbage collection.

Unlike RelStorage/FileStorage which must load and unpickle every object
to extract references, we use the pre-extracted `refs` column for a
server-side recursive CTE query. No data leaves PostgreSQL during pack.
"""

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

        # Phase 2: Delete unreachable objects
        if history_preserving and pack_tid is not None:
            # HP mode: only delete unreachable objects whose current
            # tid <= pack_time (preserve objects created/modified after)
            cur.execute(
                "DELETE FROM object_state "
                "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
                "AND tid <= %s",
                (pack_tid,),
            )
        else:
            cur.execute(
                "DELETE FROM object_state "
                "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids)"
            )
        deleted_objects = cur.rowcount

        # Phase 3: Delete unreachable blobs, collecting S3 keys
        if history_preserving and pack_tid is not None:
            cur.execute(
                "DELETE FROM blob_state "
                "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
                "AND tid <= %s "
                "RETURNING s3_key",
                (pack_tid,),
            )
        else:
            cur.execute(
                "DELETE FROM blob_state "
                "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
                "RETURNING s3_key"
            )
        deleted_blobs = cur.rowcount
        for row in cur.fetchall():
            if row[0]:
                s3_keys.append(row[0])

        # Phase 4: History cleanup (history-preserving mode only)
        deleted_history = 0
        deleted_blob_history = 0
        if history_preserving:
            if pack_tid is not None:
                # Delete history for unreachable objects only before pack_time
                cur.execute(
                    "DELETE FROM object_history "
                    "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
                    "AND tid <= %s",
                    (pack_tid,),
                )
            else:
                # No pack_time: delete all history for unreachable objects
                cur.execute(
                    "DELETE FROM object_history "
                    "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids)"
                )
            deleted_history += cur.rowcount

            if pack_tid is not None:
                cur.execute(
                    "DELETE FROM blob_history "
                    "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
                    "AND tid <= %s "
                    "RETURNING s3_key",
                    (pack_tid,),
                )
            else:
                cur.execute(
                    "DELETE FROM blob_history "
                    "WHERE zoid NOT IN (SELECT zoid FROM reachable_oids) "
                    "RETURNING s3_key"
                )
            deleted_blob_history += cur.rowcount
            for row in cur.fetchall():
                if row[0]:
                    s3_keys.append(row[0])

            # Remove old revisions for reachable objects before pack_time
            if pack_tid is not None:
                # For each reachable object, keep the most recent revision
                # at or before pack_time, delete all older ones
                cur.execute(
                    "DELETE FROM object_history oh "
                    "WHERE oh.tid < %s "
                    "AND oh.zoid IN (SELECT zoid FROM reachable_oids) "
                    "AND EXISTS ("
                    "  SELECT 1 FROM object_history oh2 "
                    "  WHERE oh2.zoid = oh.zoid "
                    "  AND oh2.tid > oh.tid AND oh2.tid <= %s"
                    ")",
                    (pack_tid, pack_tid),
                )
                deleted_history += cur.rowcount

                cur.execute(
                    "DELETE FROM blob_history bh "
                    "WHERE bh.tid < %s "
                    "AND bh.zoid IN (SELECT zoid FROM reachable_oids) "
                    "AND EXISTS ("
                    "  SELECT 1 FROM blob_history bh2 "
                    "  WHERE bh2.zoid = bh.zoid "
                    "  AND bh2.tid > bh.tid AND bh2.tid <= %s"
                    ") "
                    "RETURNING s3_key",
                    (pack_tid, pack_tid),
                )
                deleted_blob_history += cur.rowcount
                for row in cur.fetchall():
                    if row[0]:
                        s3_keys.append(row[0])

        # Phase 5: Clean up transaction_log entries at or before pack_time
        # that are no longer referenced by object_state (FK constraint).
        # These transactions are no longer undoable after pack.
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
        "%d history rows, %d blob history rows, "
        "%d S3 keys to clean",
        deleted_objects,
        deleted_blobs,
        deleted_history,
        deleted_blob_history,
        len(s3_keys),
    )

    return deleted_objects, deleted_blobs, s3_keys
