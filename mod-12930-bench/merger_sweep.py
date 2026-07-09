"""Merger scenario sweep: C = hybrid − max(search, vsim) with window, dataset size and
selectivity varied INDEPENDENTLY (the other suites tie or calibrate selectivity).

Grid: size x window x |matches| target (1% / 10% / 50% of corpus), workers=4, fields=none.
Checks whether merger latency is connected to the scenario (|matches|, size) or only to
WINDOW. Contenders: hybrid_linear + the two branch mirrors. Writes results_merger_sweep.json.
"""

import json
from time import sleep

import numpy as np

import bench_lib as B
import balanced_lib as BAL

SIZES = [10_000, 100_000, 500_000]
WINDOWS = [dict(k=10, window=20), dict(k=50, window=100), dict(k=250, window=500)]
MATCH_FRACS = [0.01, 0.10, 0.50]
WORKERS = 4
N_WARMUP, N_TIMED = 200, 2000

CONTENDERS = {
    "hybrid_linear": B.hybrid_linear,
    "search_branch": B.search_branch,
    "vsim_branch": B.vsim_branch,
}


def main():
    titles, texts, emb, corpus_max = B.load_data()
    q_emb = emb[corpus_max:]
    results = []

    for n in SIZES:
        print(f"\n===== merger sweep, dataset size {n} =====")
        r = B.start_redis(workers=6)
        B.load_corpus(r, titles, texts, emb, n)
        df, pool = BAL._df_pool(texts, n)
        B.set_workers(r, WORKERS)

        for frac in MATCH_FRACS:
            target = frac * n
            qset = BAL._gen_qset(df, pool, target, q_emb, seed=42)
            counts = [B.count_matches(r, q) for q, _ in qset[:32]]
            matches_mean = float(np.mean(counts))
            print(f"target {target:,.0f} ({frac:.0%}) -> measured |matches|≈{matches_mean:,.0f}")

            for depth in WINDOWS:
                cell = {}
                for name, builder in CONTENDERS.items():
                    args_list = [builder(q, vec, fields=False, **depth) for q, vec in qset]
                    sleep(2)
                    stats = B.bench(r, args_list, n_warm=N_WARMUP, n_timed=N_TIMED)
                    cell[name] = stats
                    print(f"n={n} frac={frac:.0%} k/w={depth['k']}/{depth['window']} "
                          f"{name:14s} p50={stats['p50_ms']:.2f}ms")
                c = cell["hybrid_linear"]["mean_ms"] - max(cell["search_branch"]["mean_ms"],
                                                           cell["vsim_branch"]["mean_ms"])
                results.append(dict(size=n, match_frac=frac, matches_mean=matches_mean,
                                    **depth, workers=WORKERS, fields="none",
                                    C_ms=round(c, 3),
                                    hybrid_mean_ms=round(cell["hybrid_linear"]["mean_ms"], 3),
                                    search_mean_ms=round(cell["search_branch"]["mean_ms"], 3),
                                    vsim_mean_ms=round(cell["vsim_branch"]["mean_ms"], 3),
                                    hybrid_p50_ms=round(cell["hybrid_linear"]["p50_ms"], 3),
                                    search_p50_ms=round(cell["search_branch"]["p50_ms"], 3),
                                    vsim_p50_ms=round(cell["vsim_branch"]["p50_ms"], 3)))
                print(f"  -> C = {c:.2f}ms")
        B.stop_redis()

    meta = dict(workers=WORKERS, fields="none", n_warmup=N_WARMUP, n_timed=N_TIMED,
                match_fracs=MATCH_FRACS, windows=WINDOWS,
                definition="C_ms = hybrid_linear mean - max(search_branch, vsim_branch) mean")
    with open("results_merger_sweep.json", "w") as f:
        json.dump(dict(meta=meta, results=results), f, indent=2)
    print("saved results_merger_sweep.json")


if __name__ == "__main__":
    main()
