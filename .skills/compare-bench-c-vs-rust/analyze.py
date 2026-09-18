#!/usr/bin/env python3
"""Pair criterion benchmark variants along a pivot parameter and report where the
baseline variant beats the others.

Reads a criterion output tree (default: bin/redisearch_rs/criterion/) and compares the
variants of a single *pivot* parameter. By default the pivot is `lang` and the baseline
is `C`, so this reproduces the original C-vs-Rust workflow with zero extra flags. Pass
`--pivot`/`--baseline` to compare any other parameter (e.g. `--pivot impl --baseline old`).

Benchmarks are paired by their remaining parameters (everything in `value_str` except the
pivot). For each pairing, the baseline value is the denominator and every other pivot value
is a *candidate*: the reported ratio is candidate_ns / baseline_ns (>1 means the baseline is
faster). When the pivot has more than two values, each non-baseline value gets its own
comparison block (N-way).

Names come from each `new/benchmark.json` (`group_id`, `value_str`); folder names are
ignored because criterion truncates them and distinct configs can collide. Timings come
from `mean.point_estimate` in each `new/estimates.json`.

Usage:
    python3 analyze.py [CRITERION_DIR] [--pivot lang] [--baseline C]
                       [--groups g1,g2,...] [--threshold 1.10] [--json OUT.json]

`--groups` is a comma-separated list of top-level bench-group folders (as shown by
`ls CRITERION_DIR`, e.g. `add_sequential,verify_doc_and_field_doc_default`). Only those
folders are analyzed; the rest are ignored. A requested folder that does not exist is a
hard error listing the available groups.
"""
import argparse
import glob
import json
import os
import statistics
from collections import defaultdict
from itertools import combinations


def group_folder(criterion_dir, benchmark_json):
    """Top-level bench-group folder a leaf lives under (the first `*` in the glob)."""
    return os.path.relpath(benchmark_json, criterion_dir).split(os.sep)[0]


def load_pairs(criterion_dir, pivot, baseline, groups=None):
    """Return (pairs, skipped_no_pivot, skipped_no_baseline).

    Each pair: dict of the non-pivot params plus `candidate`, `baseline`, `baseline_ns`,
    `candidate_ns`, `ratio` (candidate_ns / baseline_ns; >1 means the baseline is faster).

    Benchmarks are paired on every `value_str` param except `pivot`. Within a pairing the
    `baseline` pivot value is the denominator and every other pivot value emits one row.

    If `groups` is a set of top-level folder names, only those folders are analyzed; a
    requested folder absent from the tree is a hard error listing what is available.
    """
    rows = []
    no_pivot = 0
    all_param_names = set()
    pivot_values = set()
    pattern = os.path.join(criterion_dir, "*", "*", "new", "benchmark.json")
    leaves = glob.glob(pattern)

    if groups:
        available = {group_folder(criterion_dir, bj) for bj in leaves}
        missing = [g for g in groups if g not in available]
        if missing:
            raise SystemExit(
                f"Bench group(s) not found: {', '.join(sorted(missing))}\n"
                f"Available groups under {criterion_dir!r}: "
                f"{', '.join(sorted(available)) or '(none)'}")

    for bj in leaves:
        if groups and group_folder(criterion_dir, bj) not in groups:
            continue
        est = os.path.join(os.path.dirname(bj), "estimates.json")
        if not os.path.exists(est):
            continue
        b = json.load(open(bj))
        e = json.load(open(est))
        params = dict(p.split("=", 1) for p in b["value_str"].split("/") if "=" in p)
        all_param_names.update(params)
        if pivot not in params:
            no_pivot += 1  # group-summary folder or a leaf that lacks the pivot param
            continue
        pivot_values.add(params[pivot])
        rows.append((b["group_id"], params, e["mean"]["point_estimate"]))

    if not rows:
        raise SystemExit(
            f"No benchmark leaf carries a {pivot!r} parameter"
            + (f" in groups {sorted(groups)}" if groups else "") + ".\n"
            f"Parameters seen: {', '.join(sorted(all_param_names)) or '(none)'}\n"
            f"Pass --pivot with one of those (the pivot is the comparison axis).")

    by = defaultdict(dict)
    for group, params, mean in rows:
        key = (group, tuple(sorted((k, v) for k, v in params.items() if k != pivot)))
        by[key][params[pivot]] = mean

    pairs = []
    no_baseline = 0
    for (group, kp), d in by.items():
        if baseline not in d:
            no_baseline += 1
            continue
        base_ns = d[baseline]
        for pv, mean in d.items():
            if pv == baseline:
                continue
            row = {"group": group, **dict(kp)}
            row["candidate"] = pv
            row["baseline"] = baseline
            row["baseline_ns"] = base_ns
            row["candidate_ns"] = mean
            row["ratio"] = mean / base_ns
            pairs.append(row)

    if not pairs:
        raise SystemExit(
            f"Pivot {pivot!r} has values {sorted(pivot_values)} but none could be paired "
            f"against baseline {baseline!r}.\n"
            f"Pass --baseline with one of: {', '.join(sorted(pivot_values))}.")

    return pairs, no_pivot, no_baseline


# Bookkeeping keys that are never benchmark parameters.
META_KEYS = {"group", "candidate", "baseline", "baseline_ns", "candidate_ns", "ratio"}


def param_names(pairs, group):
    names = []
    for p in pairs:
        if p["group"] != group:
            continue
        for k in p:
            if k not in META_KEYS and k not in names:
                names.append(k)
    return names


def dist(ratios, baseline, candidate):
    b = {f"{candidate} faster (<0.95)": 0, "~tie (0.95-1.05)": 0, f"{baseline} win 5-10%": 0,
         f"{baseline} win 10-25%": 0, f"{baseline} win 25-50%": 0, f"{baseline} win >50%": 0}
    keys = list(b)
    for r in ratios:
        if r < 0.95: b[keys[0]] += 1
        elif r < 1.05: b[keys[1]] += 1
        elif r < 1.10: b[keys[2]] += 1
        elif r < 1.25: b[keys[3]] += 1
        elif r < 1.50: b[keys[4]] += 1
        else: b[keys[5]] += 1
    return b


def sortkey(values):
    """Numeric sort if all values parse as float, else lexical."""
    try:
        [float(v) for v in values]
        return lambda x: float(x)
    except ValueError:
        return str


def worst_regions(pairs, baseline, candidate, top):
    """Rank the parameter regions where the candidate is slowest, 1-D and 2-D.

    This is the "which direction is worst" answer: instead of leaving the agent to
    guess which dimensions to cross, it scans *every* single dimension and *every*
    pair of dimensions automatically and ranks them by median ratio (candidate_ns /
    baseline_ns; >1 means the baseline is faster, i.e. the candidate is worse).

    The 2-D block flags **interactions** with `✦` — corners whose median ratio
    exceeds, by a clear margin, the worse of the two single-dimension medians that
    make them up. An interaction is a slowdown born of the *combination* of two
    parameter values, which a per-dimension (marginal) view averages away and
    misses. Absolute candidate ns is shown so sub-microsecond, noise-prone regions
    are easy to discount.
    """
    groups = sorted({p["group"] for p in pairs})

    # 1-D marginals: (group, param, value) -> [(ratio, candidate_ns), ...]
    one = defaultdict(list)
    for p in pairs:
        for k in param_names(pairs, p["group"]):
            if k in p:
                one[(p["group"], k, p[k])].append((p["ratio"], p["candidate_ns"]))
    med1 = {k: statistics.median([r for r, _ in v]) for k, v in one.items()}

    print(f"\n=== worst regions: single dimension "
          f"(top {top} by median ratio; >1 = {baseline} faster) ===")
    for key in sorted(one, key=lambda k: -med1[k])[:top]:
        g, k, v = key
        ns = statistics.median([n for _, n in one[key]])
        print(f"   {med1[key]:5.2f}x  {candidate.lower()}={ns:12.0f}ns  "
              f"{g}  {k}={v}  n={len(one[key])}")

    # 2-D corners: every pair of dimensions, per group.
    two = defaultdict(list)  # (group, pa, va, pb, vb) -> [(ratio, candidate_ns), ...]
    for g in groups:
        names = param_names(pairs, g)
        ps = [p for p in pairs if p["group"] == g]
        for a, b in combinations(names, 2):
            for p in ps:
                if a in p and b in p:
                    two[(g, a, p[a], b, p[b])].append((p["ratio"], p["candidate_ns"]))
    if not two:
        return  # groups have <2 non-pivot params; nothing to cross

    med2 = {k: statistics.median([r for r, _ in v]) for k, v in two.items()}
    print(f"\n=== worst regions: dimension pairs "
          f"(top {top} by median ratio; ✦ = interaction, worse than either dim alone) ===")
    for key in sorted(two, key=lambda k: -med2[k])[:top]:
        g, a, va, b, vb = key
        m, ma, mb = med2[key], med1[(g, a, va)], med1[(g, b, vb)]
        ns = statistics.median([n for _, n in two[key]])
        flag = "  ✦" if m > max(ma, mb) * 1.10 else ""
        print(f"   {m:5.2f}x  {candidate.lower()}={ns:12.0f}ns  {g}  "
              f"{a}={va} & {b}={vb}  (alone: {a}={ma:.2f}x {b}={mb:.2f}x)  "
              f"n={len(two[key])}{flag}")


def print_blocks(pairs, baseline, candidate, threshold, top):
    """Distribution, biggest wins, and per-parameter correlation for one candidate."""
    groups = sorted({p["group"] for p in pairs})
    cwins = [p for p in pairs if p["ratio"] > 1.0]
    pct = 100 * len(cwins) / len(pairs)
    print(f"{baseline} faster: {len(cwins)} ({pct:.0f}%)  |  "
          f"{candidate} faster-or-tied: {len(pairs)-len(cwins)}")

    print(f"\n=== ratio distribution ({candidate.lower()}/{baseline.lower()}; "
          f">1 = {baseline} faster) ===")
    overall = dist([p["ratio"] for p in pairs], baseline, candidate)
    print(f"{'OVERALL':>40} | " + " ".join(f"{k}={v}" for k, v in overall.items()))
    for g in groups:
        rs = [p["ratio"] for p in pairs if p["group"] == g]
        print(f"{g:>40} | " + " ".join(f"{k}={v}" for k, v in dist(rs, baseline, candidate).items()))

    print(f"\n=== top {top} biggest {baseline} wins ===")
    bl, cl = baseline.lower(), candidate.lower()
    for p in sorted(pairs, key=lambda x: -x["ratio"])[:top]:
        extra = " ".join(f"{k}={p[k]}" for k in param_names(pairs, p["group"]))
        print(f"   {p['ratio']:5.2f}x  {bl}={p['baseline_ns']:12.0f} "
              f"{cl}={p['candidate_ns']:12.0f}  {p['group']}  {extra}")

    print(f"\n=== per-parameter correlation (median ratio, % meaningful {baseline}-wins) ===")
    for g in groups:
        ps = [p for p in pairs if p["group"] == g]
        print(f"\n{g}  (n={len(ps)})")
        for param in param_names(pairs, g):
            d = defaultdict(list)
            for p in ps:
                d[p[param]].append(p["ratio"])
            if len(d) < 2:
                continue  # constant param, no signal
            print(f"  by {param}:")
            for v in sorted(d, key=sortkey(d.keys())):
                rsv = d[v]
                cwin = 100 * sum(1 for r in rsv if r >= threshold) / len(rsv)
                print(f"     {param}={v:>14}: median={statistics.median(rsv):5.2f}x  "
                      f"{baseline}-wins(>={threshold})={cwin:3.0f}%  n={len(rsv)}")

    worst_regions(pairs, baseline, candidate, top)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("criterion_dir", nargs="?",
                    default="bin/redisearch_rs/criterion")
    ap.add_argument("--pivot", default="lang",
                    help="the value_str parameter that is the comparison axis "
                         "(default: lang)")
    ap.add_argument("--baseline", default="C",
                    help="the pivot value used as the denominator; every other pivot "
                         "value is compared against it (default: C)")
    ap.add_argument("--groups",
                    help="comma-separated top-level bench-group folders to analyze "
                         "(e.g. add_sequential,verify_doc_and_field_doc_default); "
                         "other folders are ignored")
    ap.add_argument("--threshold", type=float, default=1.10,
                    help="ratio at/above which a baseline win counts as 'meaningful'")
    ap.add_argument("--json", help="dump the raw pairs to this path")
    ap.add_argument("--top", type=int, default=15,
                    help="how many biggest baseline wins to list")
    args = ap.parse_args()

    groups = {g.strip() for g in args.groups.split(",") if g.strip()} if args.groups else None
    pairs, no_pivot, no_baseline = load_pairs(
        args.criterion_dir, args.pivot, args.baseline, groups)
    if args.json:
        json.dump(pairs, open(args.json, "w"))

    candidates = sorted({p["candidate"] for p in pairs}, key=sortkey({p["candidate"] for p in pairs}))
    skip_note = f"skipped {no_pivot} leaves without {args.pivot!r}"
    if no_baseline:
        skip_note += f", {no_baseline} pairings lacking a {args.baseline!r} sample"
    print(f"Pairs: {len(pairs)}  (pivot={args.pivot!r} baseline={args.baseline!r}; {skip_note})")

    if len(candidates) == 1:
        print_blocks(pairs, args.baseline, candidates[0], args.threshold, args.top)
    else:
        print(f"Candidates vs baseline {args.baseline!r}: {', '.join(candidates)}")
        for cand in candidates:
            cps = [p for p in pairs if p["candidate"] == cand]
            print(f"\n########## candidate = {cand}  (vs baseline {args.baseline}) ##########")
            print_blocks(cps, args.baseline, cand, args.threshold, args.top)

    print("\nHint: parameters whose every value sits near median 1.00x are noise "
          f"dimensions for the {args.baseline}-vs-candidate gap; the ones that swing the "
          "median locate the win. Re-run with a different --pivot to check other dimensions.")


if __name__ == "__main__":
    main()
