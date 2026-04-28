"""Compute a few headline deltas from summary.csv for the conclusion."""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()
    rows = list(csv.DictReader(open(args.summary)))
    by_cell = defaultdict(dict)
    for r in rows:
        k = (r["topology"], r["query_type"], r["index_size"], r["timeout_ms"])
        by_cell[k][r["version"]] = r

    print("Cells where both versions observed 100% timeouts (apples-to-apples):")
    print("-" * 86)
    print(f"{'topo':9s} {'qt':10s} {'size':>8s} {'to':>5s} "
          f"{'v8.6 over':>10s} {'master over':>12s} {'delta':>8s} {'ratio':>6s}")
    diffs = []
    for k, v in sorted(by_cell.items()):
        if "old" not in v or "new" not in v:
            continue
        if float(v["old"]["timeout_frac"]) < 0.99 or float(v["new"]["timeout_frac"]) < 0.99:
            continue
        o = float(v["old"]["overshoot_mean_ms"])
        n = float(v["new"]["overshoot_mean_ms"])
        delta = n - o
        ratio = n / o if o > 0 else float("nan")
        diffs.append((k, o, n, delta, ratio))
        print(f"{k[0]:9s} {k[1]:10s} {int(k[2]):>8d} {int(k[3]):>5d} "
              f"{o:>10.1f} {n:>12.1f} {delta:>+8.1f} {ratio:>6.2f}")
    if diffs:
        mean_o = sum(d[1] for d in diffs) / len(diffs)
        mean_n = sum(d[2] for d in diffs) / len(diffs)
        print(f"{'overall':9s} {'':10s} {'':>8s} {'':>5s} "
              f"{mean_o:>10.1f} {mean_n:>12.1f} {mean_n - mean_o:>+8.1f} "
              f"{mean_n / mean_o if mean_o > 0 else float('nan'):>6.2f}")

    print()
    print("Biggest overshoot reductions (master vs v8.6) for same cell:")
    print("-" * 86)
    diffs.sort(key=lambda d: (d[2] - d[1]))  # most negative delta first
    for k, o, n, delta, ratio in diffs[:8]:
        print(f"  {k[0]:9s} {k[1]:10s} size={int(k[2]):>7d} to={int(k[3]):>5d}ms "
              f"old_over={o:>7.1f}ms  new_over={n:>7.1f}ms  "
              f"delta={delta:>+7.1f}ms  ratio={ratio:.2f}")


if __name__ == "__main__":
    main()
