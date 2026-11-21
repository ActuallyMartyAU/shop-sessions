#!/usr/bin/env python3
"""Order-independent digest of an output folder (both tables). Usage: digest.py OUT"""
import hashlib, sys
from pathlib import Path
import pyarrow.parquet as pq

def digest(folder):
    out = []
    for table in ("user_daily", "campaign_daily"):
        rows = []
        for f in sorted((Path(folder) / table).glob("*.parquet")):
            t = pq.read_table(f)
            cols = sorted(t.column_names)
            rows += ["|".join(str(v) for v in r) for r in zip(*(t.column(c).to_pylist() for c in cols))]
        rows.sort()
        out.append(f"{table}:{len(rows)}:{hashlib.sha256(chr(10).join(rows).encode()).hexdigest()[:16]}")
    return " ".join(out)

print(digest(sys.argv[1]))
