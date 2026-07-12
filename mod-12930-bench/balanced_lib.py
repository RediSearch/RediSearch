"""Balanced full suite: at every (size, depth) cell, calibrate text selectivity until the
SEARCH mirror's p50 matches the VSIM mirror's, then run the full matrix
(workers x fields x contenders) on those balanced queries.

Balance makes the overhead identifiable: with equal branches, sum and max differ maximally
(2x), so "branches overlap" and "serial overhead C" can be separated and C extracted from
w=0 and w=6 independently. Depth as an axis yields the C(WINDOW) curve.

Reuses bench_lib for server management, contenders, oracle and gates.
"""

from collections import Counter
from itertools import cycle
from time import perf_counter, sleep

import numpy as np

import bench_lib as B

SIZES = [10_000, 100_000, 500_000]
DEPTHS = [dict(k=10, window=20), dict(k=50, window=100), dict(k=250, window=500)]
CAL_TOL = 0.08              # calibration targets +-8% of the vsim mirror p50
CAL_ITERS = 12
CAL_REPS = (64, 400)        # warmup, timed reps per calibration probe (tight tolerance needs a stable probe)
N_QUERY_SET = 256


def _df_pool(texts, n):
    df = Counter()
    for i in range(n):
        df.update(set(B.tok(texts[i])))
    return df, [t for t, c in df.most_common() if c >= 5]


def _build_query(df, pool, target, rng, max_tokens=12):
    """OR tokens together until the estimated union reaches ~target matches
    (assumes ~20% token overlap; the measured count is what gets reported)."""
    picked, est = [], 0.0
    for t in pool:
        if len(picked) >= max_tokens or est >= 0.85 * target:
            break
        c = df[t]
        if c <= (target - est) * 1.2 and rng.random() < 0.5:
            picked.append(t)
            est += 0.8 * c
    if not picked:
        picked = [pool[-1]]
    return "@text:(" + "|".join(picked) + ")"


def _gen_qset(df, pool, target, q_emb, seed):
    rng = np.random.default_rng(seed)
    return [(_build_query(df, pool, target, rng), q_emb[i].tobytes())
            for i in range(N_QUERY_SET)]


def _probe_p50(r, args_list, n_warm, n_timed):
    it = cycle(args_list)
    for _ in range(n_warm):
        r.execute_command(*next(it))
    lat = np.empty(n_timed)
    for i in range(n_timed):
        a = next(it)
        t0 = perf_counter()
        r.execute_command(*a)
        lat[i] = perf_counter() - t0
    return float(np.percentile(lat, 50) * 1000)


def calibrate(r, df, pool, q_emb, n, depth):
    """Geometric bisection on the target match count until the SEARCH mirror p50 lands
    within CAL_TOL of the VSIM mirror p50 at this depth. Balance may be unreachable
    (text maxes out below the vector branch, e.g. 10K at k=1000) — recorded, not fatal."""
    vsim_args = [B.vsim_branch(None, q_emb[i].tobytes(), fields=False, **depth)
                 for i in range(64)]
    vsim_p50 = _probe_p50(r, vsim_args, *CAL_REPS)
    print(f"n={n} k/w={depth['k']}/{depth['window']}: vsim p50={vsim_p50:.2f}ms — calibrating")

    lo, hi = min(200.0, 0.002 * n), 1.0 * n
    target, search_p50 = hi, None
    best = None  # (deviation, target, search_p50) — keep the closest-to-balanced probe,
                 # not the last one: at sub-ms targets probe jitter is the same order as
                 # CAL_TOL, so the final bisection step is often not the best seen.
    for it in range(CAL_ITERS):
        target = (lo * hi) ** 0.5
        qs = _gen_qset(df, pool, target, q_emb, seed=1000 + it)[:64]
        args = [B.search_branch(q, v, fields=False, **depth) for q, v in qs]
        search_p50 = _probe_p50(r, args, *CAL_REPS)
        print(f"  target≈{target:,.0f} -> search p50 {search_p50:.2f}ms")
        dev = abs(search_p50 - vsim_p50)
        if best is None or dev < best[0]:
            best = (dev, target, search_p50)
        if dev <= CAL_TOL * vsim_p50:
            break
        if search_p50 < vsim_p50:
            lo = target
        else:
            hi = target
        if hi / lo < 1.05:  # bisection exhausted
            break
    _, target, search_p50 = best
    qset = _gen_qset(df, pool, target, q_emb, seed=42)
    counts = [B.count_matches(r, q) for q, _ in qset[:32]]
    ratio = search_p50 / vsim_p50
    info = dict(size=n, **depth, vsim_p50_ms=round(vsim_p50, 2),
                search_p50_ms=round(search_p50, 2), balance_ratio=round(ratio, 2),
                balanced=abs(ratio - 1) <= CAL_TOL, target=int(target),
                matches_mean=float(np.mean(counts)))
    print(f"  -> |matches|≈{info['matches_mean']:,.0f} ratio={ratio:.2f} "
          f"balanced={info['balanced']}")
    return qset, info


def run_balanced_full(titles, texts, emb, corpus_max):
    q_emb = emb[corpus_max:]
    results, gates, cal_infos = [], [], []

    for n in SIZES:
        print(f"\n===== balanced full, dataset size {n} =====")
        r = B.start_redis(workers=6)
        B.load_corpus(r, titles, texts, emb, n)
        df, pool = _df_pool(texts, n)
        B.set_workers(r, 0)

        for depth in DEPTHS:
            qset, cal = calibrate(r, df, pool, q_emb, n, depth)
            cal_infos.append(cal)

            ok_lin = ok_rrf = 0
            for q, vec in qset[:B.GATE_QUERIES]:
                full_lin, full_rrf = B.oracle_results(r, q, vec, **depth)
                got_lin = {row["__key"]: float(row["combined_score"])
                           for row in B.parse_rows(r.execute_command(*B.hybrid_linear(q, vec, **depth)))
                           if "__key" in row}
                got_rrf = {row["__key"]: float(row["combined_score"])
                           for row in B.parse_rows(r.execute_command(*B.hybrid_rrf(q, vec, **depth)))
                           if "__key" in row}
                ok_lin += B.gate_check(got_lin, full_lin)
                ok_rrf += B.gate_check(got_rrf, full_rrf)
            g = dict(size=n, **depth, matches_mean=cal["matches_mean"],
                     balance_ratio=cal["balance_ratio"],
                     gate_linear=f"{ok_lin}/{B.GATE_QUERIES}",
                     gate_rrf=f"{ok_rrf}/{B.GATE_QUERIES}")
            gates.append(g)
            print(g)

            for w in B.WORKERS_VALUES:
                B.set_workers(r, w)
                for f in [False, True]:
                    fname = "title+text" if f else "none"
                    for name, builder in B.CONTENDERS.items():
                        args_list = [builder(q, vec, fields=f, **depth) for q, vec in qset]
                        sleep(2)
                        stats = B.bench(r, args_list)
                        results.append(dict(size=n, workers=w, fields=fname, contender=name,
                                            matches_mean=cal["matches_mean"],
                                            balance_ratio=cal["balance_ratio"],
                                            balanced=cal["balanced"], **depth, **stats))
                        print(f"n={n} k/w={depth['k']}/{depth['window']} w={w} "
                              f"fields={fname:10s} {name:14s} "
                              f"p50={stats['p50_ms']:.2f}ms qps={stats['qps']:8.1f}")
            B.set_workers(r, 0)
        B.stop_redis()

    meta = dict(depths=DEPTHS, out_k=B.OUT_K, n_query_set=N_QUERY_SET,
                n_warmup=B.N_WARMUP, n_timed=B.N_TIMED, cal_tol=CAL_TOL,
                calibration=cal_infos)
    return results, gates, meta
