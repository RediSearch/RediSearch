"""Generate mod12930_balanced.ipynb. Machinery lives in bench_lib.py / balanced_lib.py /
merger_sweep.py; the notebook is the narrative and the result tables."""

import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s))
code = lambda s: cells.append(nbf.v4.new_code_cell(s))

md("""# MOD-12930 — FT.HYBRID vs its subqueries (balanced suite)

FT.HYBRID is timed alongside its two subqueries run standalone (same K/WINDOW, same
reply size). Per (size, depth) cell, the text query is tuned so the SEARCH subquery's
latency is similar to the VSIM subquery's — with heavily skewed subqueries the comparison
says little about concurrency or merge cost. Note the SEARCH numbers are therefore *not*
the native scaling of one fixed text query.

**Matrix:** size (10K/100K/500K) × depth K/W (10/20, 50/100, 250/500) × workers=2 ×
loader (keyspace access) on/off × 4 contenders. Dataset: dbpedia, 512-dim embeddings,
HNSW cosine. Before timing, every configuration must pass a correctness gate: FT.HYBRID's
output must equal an offline fusion of the two raw queries (tie-aware).

Set `REUSE_RESULTS=1` to re-render tables from saved results without re-running.""")

code("""import json, os
import pandas as pd

if os.environ.get('REUSE_RESULTS') and os.path.exists('results_balanced_full.json'):
    d = json.load(open('results_balanced_full.json'))
    results, gates, meta = d['results'], d['gates'], d['meta']
    print('reusing saved results_balanced_full.json')
else:
    import bench_lib as B
    import balanced_lib as BAL
    results, gates, meta = BAL.run_balanced_full(*B.load_data())
    with open('results_balanced_full.json', 'w') as f:
        json.dump(dict(meta=meta, results=results, gates=gates), f, indent=2, default=str)
    pd.DataFrame(results).to_csv('results_balanced_full.csv', index=False)
    print('saved results_balanced_full.json / .csv')""")

md("## Calibration & gates\n\n`balance_ratio` = search p50 / vsim p50 at calibration.")

code("pd.DataFrame(gates)")

md("## p50 latency (ms) by contender")

code("""df = pd.DataFrame(results)
df.pivot_table(index=['size', 'window', 'fields'], columns='contender',
               values='p50_ms', sort=False).round(2)[
    ['hybrid_linear', 'hybrid_rrf', 'search_branch', 'vsim_branch']]""")

md("""## Merger sweep

Same contenders with window, dataset size and text selectivity varied independently
(match-fraction 1% / 10% / 50% of the corpus; the calibrated matrix above cannot separate
these axes). Raw p50 per contender.""")

code("""if os.environ.get('REUSE_RESULTS') and os.path.exists('results_merger_sweep.json'):
    sw = json.load(open('results_merger_sweep.json'))
    sweep_results = sw['results']
    print('reusing saved results_merger_sweep.json')
else:
    import bench_lib as B
    from merger_sweep import run_sweep
    sweep_results, _ = run_sweep(*B.load_data())

sdf = pd.DataFrame(sweep_results)
sdf.pivot_table(index=['size', 'match_frac'], columns='window',
                values=['hybrid_p50_ms', 'search_p50_ms', 'vsim_p50_ms']).round(2)""")

nb["cells"] = cells
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, "mod12930_balanced.ipynb")
print("wrote mod12930_balanced.ipynb")
