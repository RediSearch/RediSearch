"""Aggregate a raw pilot CSV into a per-cell summary CSV.

Inputs : <run>/raw.csv (one row per query)
Outputs: <run>/summary.csv (one row per cell, with mean/p50/p95/p99, timeout rate,
         server-side commandstats mean, and overshoot statistics).
"""
from __future__ import annotations

import argparse
import csv
import os
import statistics
from collections import defaultdict


CELL_KEYS = ("version", "topology", "query_type", "index_size", "timeout_ms")


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    xs = sorted(xs)
    n = len(xs)
    k = min(n - 1, int(p * n))
    return xs[k]


def maybe_float(v: str | None) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def fmt_optional(v: float | None) -> str:
    return "" if v is None else f"{v:.2f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    buckets: dict[tuple, list[dict]] = defaultdict(list)
    for r in csv.DictReader(open(args.raw)):
        key = tuple(r[k] for k in CELL_KEYS)
        buckets[key].append(r)

    cols = [
        "version", "topology", "query_type", "index_size", "timeout_ms",
        "n", "timeout_frac",
        "observed_mean_ms", "observed_p50_ms", "observed_p95_ms", "observed_p99_ms",
        "observed_max_ms",
        "server_mean_ms", "server_overshoot_mean_ms",
        "overshoot_mean_ms", "overshoot_p50_ms", "overshoot_p95_ms", "overshoot_p99_ms",
        "overshoot_max_ms",
    ]
    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for key in sorted(buckets):
            rows = buckets[key]
            to_ms = int(key[-1])
            obs = [float(r["observed_ms"]) for r in rows]
            # overshoot is defined against the server's configured timeout.
            over = [max(0.0, x - to_ms) for x in obs]
            server_vals = [
                v for v in (maybe_float(r.get("server_mean_ms")) for r in rows)
                if v is not None
            ]
            server_mean = statistics.fmean(server_vals) if server_vals else None
            server_over = (
                max(0.0, server_mean - to_ms)
                if server_mean is not None else None
            )
            n = len(rows)
            t_frac = sum(1 for r in rows if int(r["timed_out"]) == 1) / n
            w.writerow([
                *key,
                n, f"{t_frac:.3f}",
                f"{statistics.fmean(obs):.2f}",
                f"{pct(obs, 0.50):.2f}",
                f"{pct(obs, 0.95):.2f}",
                f"{pct(obs, 0.99):.2f}",
                f"{max(obs):.2f}",
                fmt_optional(server_mean),
                fmt_optional(server_over),
                f"{statistics.fmean(over):.2f}",
                f"{pct(over, 0.50):.2f}",
                f"{pct(over, 0.95):.2f}",
                f"{pct(over, 0.99):.2f}",
                f"{max(over):.2f}",
            ])
    print(f"wrote {args.out} with {len(buckets)} cells")


if __name__ == "__main__":
    main()
