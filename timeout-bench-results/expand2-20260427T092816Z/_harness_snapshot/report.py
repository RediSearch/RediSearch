"""Print a side-by-side comparison table of old vs new per cell.

Also emits a compact Markdown table per (topology, query_type, index_size)
suitable for pasting into conclusion.md.
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict


def fmt(v: str, digits: int = 1) -> str:
    try:
        return f"{float(v):.{digits}f}"
    except Exception:
        return v


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.summary)))
    has_server = bool(rows and "server_mean_ms" in rows[0])
    # Group rows by everything except version.
    buckets: dict[tuple, dict[str, dict]] = defaultdict(dict)
    for r in rows:
        k = (r["topology"], r["query_type"], r["index_size"], int(r["timeout_ms"]))
        buckets[k][r["version"]] = r

    # Print one MD table per (topology, qtype, size).
    groups: dict[tuple, list] = defaultdict(list)
    for k in sorted(buckets, key=lambda x: (x[0], x[1], int(x[2]), x[3])):
        groups[(k[0], k[1], k[2])].append(k)

    for (topo, qt, size), keys in groups.items():
        print(f"\n### {topo} / {qt} / {int(size):,} docs")
        if has_server:
            print("| timeout | N | v8.6 tmo% | master tmo% |"
                  " v8.6 mean | master mean |"
                  " v8.6 server | master server |"
                  " v8.6 p95 | master p95 |"
                  " v8.6 over_mean | master over_mean |"
                  " v8.6 over_p95 | master over_p95 |")
            print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        else:
            print("| timeout | N | v8.6 tmo% | master tmo% |"
                  " v8.6 mean | master mean |"
                  " v8.6 p95 | master p95 |"
                  " v8.6 over_mean | master over_mean |"
                  " v8.6 over_p95 | master over_p95 |")
            print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        for k in keys:
            old = buckets[k].get("old", {})
            new = buckets[k].get("new", {})
            if not old or not new:
                continue
            base = {
                "to": k[3], "n": old["n"],
                "otp": fmt(float(old["timeout_frac"]) * 100, 0),
                "ntp": fmt(float(new["timeout_frac"]) * 100, 0),
                "om": fmt(old["observed_mean_ms"], 1),
                "nm": fmt(new["observed_mean_ms"], 1),
                "op": fmt(old["observed_p95_ms"], 1),
                "np": fmt(new["observed_p95_ms"], 1),
                "oom": fmt(old["overshoot_mean_ms"], 1),
                "nom": fmt(new["overshoot_mean_ms"], 1),
                "oop": fmt(old["overshoot_p95_ms"], 1),
                "nop": fmt(new["overshoot_p95_ms"], 1),
            }
            if has_server:
                print("| {to} | {n} | {otp}% | {ntp}% |"
                      " {om} | {nm} |"
                      " {os} | {ns} |"
                      " {op} | {np} |"
                      " {oom} | {nom} |"
                      " {oop} | {nop} |".format(
                          **base,
                          os=fmt(old.get("server_mean_ms", ""), 1),
                          ns=fmt(new.get("server_mean_ms", ""), 1),
                      ))
            else:
                print("| {to} | {n} | {otp}% | {ntp}% |"
                      " {om} | {nm} |"
                      " {op} | {np} |"
                      " {oom} | {nom} |"
                      " {oop} | {nop} |".format(**base))


if __name__ == "__main__":
    main()
