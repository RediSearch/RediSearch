#!/usr/bin/env python3
"""Merge pilot + expand + retry raw CSVs into a single consolidated raw.csv.

- Pilot: all rows included.
- Expand: rows included EXCEPT new/cluster3/1M (cluster was unstable; use retry).
- Retry: all rows included (clean fresh-cluster-per-cell new/cluster3/1M).
"""
import csv
import sys

PILOT = "timeout-bench-results/pilot-20260421T141317Z/raw.csv"
EXPAND = "/tmp/bench-expand-hybrid.csv"
RETRY = "/tmp/bench-cluster3-retry.csv"
OUT = "timeout-bench-results/expand-20260423T095000Z/raw.csv"


def main():
    hdr = None
    rows = []
    pilot_cnt = 0
    for r in csv.DictReader(open(PILOT)):
        hdr = hdr or list(r.keys())
        rows.append(r)
        pilot_cnt += 1
    kept = dropped = 0
    for r in csv.DictReader(open(EXPAND)):
        if (r["version"] == "new" and r["topology"] == "cluster3"
                and r["index_size"] == "1000000"):
            dropped += 1
            continue
        kept += 1
        rows.append(r)
    retry_cnt = 0
    for r in csv.DictReader(open(RETRY)):
        retry_cnt += 1
        rows.append(r)
    print(f"pilot={pilot_cnt} expand_kept={kept} expand_dropped={dropped} "
          f"retry={retry_cnt} total={len(rows)}")

    seen = set()
    dupes = 0
    for r in rows:
        k = (r["version"], r["topology"], r["query_type"],
             r["index_size"], r["timeout_ms"], r["iter"])
        if k in seen:
            dupes += 1
        seen.add(k)
    print(f"dupes={dupes}")
    if dupes:
        print("ERROR: duplicates found", file=sys.stderr)
        sys.exit(1)

    with open(OUT, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=hdr)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
