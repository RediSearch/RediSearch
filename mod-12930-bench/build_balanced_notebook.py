"""Generate mod12930_balanced.ipynb — the balanced-branches addendum."""

import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s))
code = lambda s: cells.append(nbf.v4.new_code_cell(s))

md("""# MOD-12930 addendum — balanced branches

The main matrix never balances the two subqueries: text is either near-zero (selective)
or dominant (broad). This addendum **calibrates text selectivity until the SEARCH mirror's
p50 matches the VSIM mirror's** (±20%, geometric bisection on the target match count),
then measures the same four contenders in that regime. fields=none, depth K/W=1000/2000,
sizes 100K/500K (at 10K the text branch can't reach vector-branch latency even matching
the whole corpus).

**What this regime uniquely shows:**
- **Parallelism gain** `t(w0)/t(w6)` — with equal branches, truly concurrent depletion
  approaches 2×; anything well below that bounds what workers actually buy.
- **ε with no dominant branch** — merger + YIELD overhead, not masked by branch skew.""")

code("""import json
import pandas as pd
import bench_lib as B
import balanced_lib as BAL

titles, texts, emb, corpus_max = B.load_data()
results, gates, meta = BAL.run_balanced(titles, texts, emb, corpus_max)

with open('results_balanced.json', 'w') as f:
    json.dump(dict(meta=meta, results=results, gates=gates), f, indent=2, default=str)
print('saved results_balanced.json')""")

md("## Gates & calibration")

code("""print(json.dumps(meta['calibration'], indent=1))
pd.DataFrame(gates)""")

md("## Results — p50 (ms) and parallelism gain\n\n"
   "`gain = p50(workers=0) / p50(workers=6)` per contender. For the hybrids, the ceiling "
   "with perfectly overlapping equal branches is ≈2× (minus merger/YIELD, which don't "
   "parallelize); the mirrors are single queries, so their gain should be ≈1.")

code("""df = pd.DataFrame(results)
p = df.pivot_table(index=['size', 'contender'], columns='workers', values='p50_ms', sort=False)
p.columns = [f'w{c}_p50_ms' for c in p.columns]
p['gain_w0/w6'] = (p['w0_p50_ms'] / p['w6_p50_ms']).round(2)
order = ['hybrid_linear', 'hybrid_rrf', 'search_branch', 'vsim_branch']
p = p.reindex([(s, c) for s in sorted(df['size'].unique()) for c in order])
p.round(2)""")

md("### ε in the balanced regime")

code("""m = df.pivot_table(index=['size', 'workers'], columns='contender', values='mean_ms', sort=False)
eps = m['hybrid_linear'] - m[['search_branch', 'vsim_branch']].max(axis=1)
pd.DataFrame({'hybrid_mean_ms': m['hybrid_linear'].round(2),
              'slowest_branch_ms': m[['search_branch', 'vsim_branch']].max(axis=1).round(2),
              'sum_branches_ms': m[['search_branch', 'vsim_branch']].sum(axis=1).round(2),
              'eps_vs_max_ms': eps.round(2)})""")

nb["cells"] = cells
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, "mod12930_balanced.ipynb")
print("wrote mod12930_balanced.ipynb")
