"""Recalibrate and re-time only the cells whose calibration failed to converge,
splicing them into results_balanced_full.json (everything else untouched)."""

import json
from time import sleep

import bench_lib as B
import balanced_lib as BAL

# 500K cells re-calibrated with a slight high-side bias (search aims ~5% above vsim)
# so the largest corpus does not undercut 100K's text workload.
CELLS = [(500_000, dict(k=10, window=20)),
         (500_000, dict(k=50, window=100)),
         (500_000, dict(k=250, window=500))]
BIAS = 1.05

titles, texts, emb, corpus_max = B.load_data()
q_emb = emb[corpus_max:]
out = json.load(open("results_balanced_full.json"))

loaded_n = None
for n, depth in CELLS:
    print(f"===== patching size={n} k/w={depth['k']}/{depth['window']}")
    if loaded_n != n:
        r = B.start_redis(workers=6)
        B.load_corpus(r, titles, texts, emb, n)
        df, pool = BAL._df_pool(texts, n)
        B.set_workers(r, 2)
        loaded_n = n
    qset, cal = BAL.calibrate(r, df, pool, q_emb, n, depth, bias=BIAS)

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
    gate = dict(size=n, **depth, matches_mean=cal["matches_mean"],
                balance_ratio=cal["balance_ratio"],
                gate_linear=f"{ok_lin}/{B.GATE_QUERIES}", gate_rrf=f"{ok_rrf}/{B.GATE_QUERIES}")
    print(gate)

    rows = []
    for f in [False, True]:
        fname = "title+text" if f else "none"
        for name, builder in B.CONTENDERS.items():
            args_list = [builder(q, vec, fields=f, **depth) for q, vec in qset]
            sleep(2)
            stats = B.bench(r, args_list)
            rows.append(dict(size=n, workers=2, fields=fname, contender=name,
                             matches_mean=cal["matches_mean"],
                             balance_ratio=cal["balance_ratio"], balanced=cal["balanced"],
                             **depth, **stats))
            print(f"{name:14s} fields={fname:10s} p50={stats['p50_ms']:.2f}ms")
    key = lambda x: (x["size"], x["window"])
    out["results"] = [x for x in out["results"] if key(x) != (n, depth["window"])] + rows
    out["gates"] = [x for x in out["gates"] if key(x) != (n, depth["window"])] + [gate]
    out["meta"]["calibration"] = [x for x in out["meta"]["calibration"]
                                  if key(x) != (n, depth["window"])] + [cal]

out["results"].sort(key=lambda x: (x["size"], x["window"], x["fields"], x["contender"]))
out["gates"].sort(key=lambda x: (x["size"], x["window"]))
out["meta"]["calibration"].sort(key=lambda x: (x["size"], x["window"]))
B.stop_redis()
with open("results_balanced_full.json", "w") as f:
    json.dump(out, f, indent=2, default=str)
print("patched results_balanced_full.json")
