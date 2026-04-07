#!/usr/bin/env python3
"""Create a small FileStorage with blobs for migration testing.

Usage:
    uv run python tests/make_test_fs.py [output_dir]

Creates Data.fs + blobs/ in output_dir (default: /tmp/test-migration).
~50 transactions with a mix of regular objects and blobs of varying sizes.
"""

from persistent.mapping import PersistentMapping
from ZODB.blob import Blob
from ZODB.FileStorage import FileStorage

import os
import sys
import transaction as txn
import ZODB


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "/tmp/test-migration"
    os.makedirs(out, exist_ok=True)
    fs_path = os.path.join(out, "Data.fs")
    blob_dir = os.path.join(out, "blobs")
    os.makedirs(blob_dir, exist_ok=True)

    if os.path.exists(fs_path):
        print(f"Removing existing {fs_path}")
        for f in os.listdir(out):
            if f.startswith("Data.fs"):
                os.unlink(os.path.join(out, f))

    storage = FileStorage(fs_path, blob_dir=blob_dir)
    db = ZODB.DB(storage)
    conn = db.open()
    root = conn.root()

    # 1. Regular objects (10 txns)
    for i in range(10):
        root[f"item_{i}"] = PersistentMapping({"idx": i, "data": f"value-{i}" * 20})
        txn.commit()

    # 2. Small blobs (10 txns, ~1KB each)
    for i in range(10):
        root[f"small_blob_{i}"] = Blob(os.urandom(1024))
        txn.commit()

    # 3. Mixed: object + blob in same txn (10 txns)
    for i in range(10):
        root[f"mixed_obj_{i}"] = PersistentMapping({"mixed": True, "n": i})
        root[f"mixed_blob_{i}"] = Blob(os.urandom(4096))
        txn.commit()

    # 4. Larger blobs (5 txns, ~100KB each)
    for i in range(5):
        root[f"large_blob_{i}"] = Blob(os.urandom(100_000))
        txn.commit()

    # 5. Update existing objects (10 txns — same OIDs, new revisions)
    for i in range(10):
        root[f"item_{i}"]["updated"] = True
        txn.commit()

    # 6. Update existing blobs (5 txns)
    for i in range(5):
        with root[f"small_blob_{i}"].open("w") as f:
            f.write(os.urandom(2048))
        txn.commit()

    conn.close()
    db.close()

    fs_size = os.path.getsize(fs_path)
    blob_count = sum(
        1
        for dirpath, _, filenames in os.walk(blob_dir)
        for f in filenames
        if f.endswith(".blob")
    )
    print(f"Created {fs_path} ({fs_size / 1024:.0f} KB)")
    print(f"  Blob dir: {blob_dir} ({blob_count} blob files)")
    print("  ~50 transactions, mix of objects and blobs")
    print("\nTest with:")
    print(f"  zodb-convert --source file://{fs_path}?blob_dir={blob_dir} \\")
    print("    --destination pgjsonb://... --workers 4")


if __name__ == "__main__":
    main()
