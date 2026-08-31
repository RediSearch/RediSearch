#!/usr/bin/env python3
"""Two-parameter heatmap of median baseline-vs-candidate ratios for one benchmark group.

Reads the raw pairs dumped by `analyze.py --json` and prints a grid of median ratios
(candidate_ns / baseline_ns; >1 means the baseline is faster) over two chosen parameters.

When the group was analyzed with a pivot that has more than two values, it contains
several candidates; pass `--candidate` to pick which one to chart.

Usage:
    python3 grid.py PAIRS.json GROUP ROW_PARAM COL_PARAM [--candidate VALUE]
Example:
    python3 analyze.py --json /tmp/bench_pairs.json
    python3 grid.py /tmp/bench_pairs.json add/sequential slot_size pop_count
"""
import argparse
import json
import statistics
from collections import defaultdict

META_KEYS = {"group", "candidate", "baseline", "baseline_ns", "candidate_ns", "ratio"}


def sortkey(values):
    """Numeric sort if all values parse as float, else lexical."""
    try:
        [float(v) for v in values]
        return float
    except ValueError:
        return str


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pairs_json", help="output of `analyze.py --json`")
    ap.add_argument("group", help="benchmark group, either group_id (add/sequential) "
                                  "or its folder form (add_sequential)")
    ap.add_argument("row_param")
    ap.add_argument("col_param")
    ap.add_argument("--candidate",
                    help="which non-baseline pivot value to chart (required only when the "
                         "group has more than one candidate)")
    args = ap.parse_args()

    # Accept either the group_id (`add/sequential`) stored in the dump or the folder
    # form (`add_sequential`) the user sees via `ls` and passes to `analyze.py --groups`.
    all_pairs = json.load(open(args.pairs_json))
    pairs = [p for p in all_pairs
             if args.group in (p["group"], p["group"].replace("/", "_"))]
    if not pairs:
        groups = sorted({p["group"] for p in all_pairs})
        raise SystemExit(f"No pairs for group {args.group!r} in {args.pairs_json!r}. "
                         f"Available: {', '.join(groups) or '(none)'}")

    candidates = sorted({p["candidate"] for p in pairs}, key=sortkey({p["candidate"] for p in pairs}))
    if args.candidate:
        if args.candidate not in candidates:
            raise SystemExit(f"Candidate {args.candidate!r} not in group {args.group!r}. "
                             f"Available: {', '.join(candidates)}")
        candidate = args.candidate
    elif len(candidates) == 1:
        candidate = candidates[0]
    else:
        raise SystemExit(f"Group {args.group!r} has multiple candidates: "
                         f"{', '.join(candidates)}. Pass --candidate to pick one.")
    pairs = [p for p in pairs if p["candidate"] == candidate]
    baseline = pairs[0]["baseline"]

    for param in (args.row_param, args.col_param):
        if param not in pairs[0]:
            raise SystemExit(f"Parameter {param!r} not found. Available: "
                             f"{sorted(k for k in pairs[0] if k not in META_KEYS)}")

    rows = sorted({p[args.row_param] for p in pairs}, key=sortkey({p[args.row_param] for p in pairs}))
    cols = sorted({p[args.col_param] for p in pairs}, key=sortkey({p[args.col_param] for p in pairs}))
    d = defaultdict(list)
    for p in pairs:
        d[(p[args.row_param], p[args.col_param])].append(p["ratio"])

    label = f"{args.row_param} \\ {args.col_param}"
    print(f"{args.group} — median ratio {candidate.lower()}/{baseline.lower()}  "
          f"(>1 = {baseline} faster)")
    print(f"{label:>18}|" + "".join(f"{c:>12}" for c in cols))
    for r in rows:
        cells = "".join(
            f"{statistics.median(d[(r, c)]):>12.2f}" if d.get((r, c)) else f"{'-':>12}"
            for c in cols)
        print(f"{r:>18}|" + cells)


if __name__ == "__main__":
    main()
