"""Balanced-branches addendum: tune text selectivity until the SEARCH mirror's latency
matches the VSIM mirror's, then measure hybrid vs branches in that regime.

The balanced regime is where concurrent depletion has maximal headroom (t(w0)/t(w6) → 2
if branches truly overlap) and where merger overhead is not masked by a dominant branch.
Reuses bench_lib for server management, contenders, oracle and gates. fields=none only.
"""

from collections import Counter
from time import sleep

import numpy as np

import bench_lib as B

SIZES = [100_000, 500_000]  # at 10K the text branch cannot reach vector-branch latency
DEPTH = dict(k=1000, window=2000)
CAL_TOL = 0.20              # calibration stops within ±20% of the vsim mirror p50
CAL_REPS = (64, 200)        # warmup, timed reps per calibration probe
N_QUERY_SET = 256


def _df_pool(texts, n):
    df = Counter()
    for i in range(n):
        df.update(set(B.tok(texts[i])))
    # frequent-first pool of usable tokens
    return df, [t for t, c in df.most_common() if c >= 5]


def _build_query(df, pool, target, rng, max_tokens=12):
    """OR tokens together until the estimated union reaches ~target matches.
    Estimation assumes ~20% overlap between tokens; the measured count is what's reported."""
    picked, est = [], 0.0
    # candidates no bigger than the remaining gap (with slack), frequent first
    for t in pool:
        if len(picked) >= max_tokens or est >= 0.85 * target:
            break
        c = df[t]
        if c <= (target - est) * 1.2 and rng.random() < 0.5:
            picked.append(t)
            est += 0.8 * c
    if not picked:
        picked = [pool[0]]
    return "@text:(" + "|".join(picked) + ")"


def _gen_qset(df, pool, target, q_emb, seed):
    rng = np.random.default_rng(seed)
    return [( _build_query(df, pool, target, rng), q_emb[i].tobytes())
            for i in range(N_QUERY_SET)]


def _probe_p50(r, args_list, n_warm, n_timed):
    from itertools import cycle
    from time import perf_counter
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


def calibrate(r, texts, q_emb, n):
    """Geometric bisection on the target match count until the SEARCH mirror p50
    lands within CAL_TOL of the VSIM mirror p50. Returns (qset, calibration_info)."""
    df, pool = _df_pool(texts, n)
    vsim_args = [B.vsim_branch(None, q_emb[i].tobytes(), fields=False, **DEPTH)
                 for i in range(64)]
    vsim_p50 = _probe_p50(r, vsim_args, *CAL_REPS)
    print(f"n={n}: vsim mirror p50 = {vsim_p50:.2f}ms — calibrating text to match")

    lo, hi = 0.002 * n, 1.0 * n
    target, search_p50 = hi, None
    for it in range(7):
        target = (lo * hi) ** 0.5
        qs = _gen_qset(df, pool, target, q_emb, seed=1000 + it)[:64]
        args = [B.search_branch(q, v, fields=False, **DEPTH) for q, v in qs]
        search_p50 = _probe_p50(r, args, *CAL_REPS)
        print(f"  target≈{target:,.0f} matches -> search p50 {search_p50:.2f}ms")
        if abs(search_p50 - vsim_p50) <= CAL_TOL * vsim_p50:
            break
        if search_p50 < vsim_p50:
            lo = target
        else:
            hi = target
    qset = _gen_qset(df, pool, target, q_emb, seed=42)
    counts = [B.count_matches(r, q) for q, _ in qset[:32]]
    info = dict(size=n, vsim_p50_ms=round(vsim_p50, 2), search_p50_ms=round(search_p50, 2),
                target=int(target), matches_mean=float(np.mean(counts)),
                matches_median=float(np.median(counts)))
    print(f"  calibrated: |matches| mean={info['matches_mean']:,.0f} "
          f"(search {search_p50:.2f}ms vs vsim {vsim_p50:.2f}ms)")
    return qset, info


def run_balanced(titles, texts, emb, corpus_max):
    q_emb = emb[corpus_max:]
    results, gates, cal_infos = [], [], []

    for n in SIZES:
        print(f"\n===== balanced, dataset size {n} =====")
        r = B.start_redis(workers=6)
        B.load_corpus(r, titles, texts, emb, n)
        B.set_workers(r, 0)
        qset, cal = calibrate(r, texts, q_emb, n)
        cal_infos.append(cal)

        # gates against the oracle at the same depth (untimed)
        ok_lin = ok_rrf = 0
        for q, vec in qset[:B.GATE_QUERIES]:
            full_lin, full_rrf = B.oracle_results(r, q, vec, **DEPTH)
            got_lin = {row["__key"]: float(row["combined_score"])
                       for row in B.parse_rows(r.execute_command(*B.hybrid_linear(q, vec, **DEPTH)))
                       if "__key" in row}
            got_rrf = {row["__key"]: float(row["combined_score"])
                       for row in B.parse_rows(r.execute_command(*B.hybrid_rrf(q, vec, **DEPTH)))
                       if "__key" in row}
            ok_lin += B.gate_check(got_lin, full_lin)
            ok_rrf += B.gate_check(got_rrf, full_rrf)
        g = dict(size=n, matches_mean=cal["matches_mean"],
                 gate_linear=f"{ok_lin}/{B.GATE_QUERIES}", gate_rrf=f"{ok_rrf}/{B.GATE_QUERIES}")
        gates.append(g)
        print(g)

        for w in B.WORKERS_VALUES:
            B.set_workers(r, w)
            for name, builder in B.CONTENDERS.items():
                args_list = [builder(q, vec, fields=False, **DEPTH) for q, vec in qset]
                sleep(2)
                stats = B.bench(r, args_list)
                results.append(dict(size=n, workers=w, contender=name,
                                    matches_mean=cal["matches_mean"], **DEPTH, **stats))
                print(f"n={n} w={w} {name:14s} qps={stats['qps']:8.1f} "
                      f"p50={stats['p50_ms']:.2f}ms p99={stats['p99_ms']:.2f}ms")
        B.stop_redis()

    meta = dict(depth=DEPTH, out_k=B.OUT_K, n_query_set=N_QUERY_SET,
                n_warmup=B.N_WARMUP, n_timed=B.N_TIMED, calibration=cal_infos)
    return results, gates, meta
