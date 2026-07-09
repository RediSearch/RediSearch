"""Generate mod12930_balanced.ipynb — the balanced FULL suite."""

import nbformat as nbf

nb = nbf.v4.new_notebook()
cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s))
code = lambda s: cells.append(nbf.v4.new_code_cell(s))

md("""# MOD-12930 — balanced full suite

Same contenders and axes as the main (imbalanced) suite, but the text query of every
(size, depth) cell is **calibrated** so the SEARCH mirror's p50 matches the VSIM mirror's
(±20%, geometric bisection on the target match count; the achieved ratio is recorded).

**Why balance:** with skewed branches, `hybrid ≈ max + C` and `hybrid ≈ sum + C'` fit the
same measurement (sum − max = the small branch ≈ noise), so neither concurrency nor
overhead is identifiable. Equal branches maximize sum − max, so the workers-0/workers-N
pair separates "branches overlap" from "serial overhead C" and yields C twice, independently.

**Matrix:** size (10K/100K/500K) × depth K/W (10/20, 50/100, 250/500 — giving the
C(WINDOW) curve) × workers (0/4) × fields (none/title+text) × 4 contenders.
A cell whose calibration cannot reach balance is recorded with `balanced=False`.""")

code("""import json, os
import pandas as pd

# Set REUSE_RESULTS=1 to re-render the tables from a previously saved run
# without re-executing the benchmark.
if os.environ.get('REUSE_RESULTS') and os.path.exists('results_balanced_full.json'):
    d = json.load(open('results_balanced_full.json'))
    results, gates, meta = d['results'], d['gates'], d['meta']
    print('reusing saved results_balanced_full.json')
else:
    import bench_lib as B
    import balanced_lib as BAL
    titles, texts, emb, corpus_max = B.load_data()
    results, gates, meta = BAL.run_balanced_full(titles, texts, emb, corpus_max)
    with open('results_balanced_full.json', 'w') as f:
        json.dump(dict(meta=meta, results=results, gates=gates), f, indent=2, default=str)
    pd.DataFrame(results).to_csv('results_balanced_full.csv', index=False)
    print('saved results_balanced_full.json / .csv')""")

md("## Calibration & gates\n\n`balance_ratio` = search p50 / vsim p50 at calibration "
   "(1.0 = perfect balance; a cell counts as balanced within ±20%).")

code("pd.DataFrame(gates)")

md("## p50 latency (ms)")

code("""df = pd.DataFrame(results)
pivot = df.pivot_table(index=['size', 'window', 'workers', 'fields'], columns='contender',
                       values='p50_ms', sort=False).round(2)
pivot[['hybrid_linear', 'hybrid_rrf', 'search_branch', 'vsim_branch']]""")

md("""## C — the serial overhead, and the C(WINDOW) curve

fields=none. `C_w0 = hybrid − (search+vsim)`, `C_w6 = hybrid − max(search,vsim)`
(mean latency). If depletion truly overlaps and C is genuinely serial, the two agree.
`C/max` says how the overhead compares to running one entire branch.""")

code("""m = df[df.fields == 'none'].pivot_table(index=['size', 'window', 'workers'],
        columns='contender', values='mean_ms', sort=False)
mx = m[['search_branch', 'vsim_branch']].max(axis=1)
sm = m[['search_branch', 'vsim_branch']].sum(axis=1)
c = pd.DataFrame({'hybrid': m['hybrid_linear'], 'max_branch': mx, 'sum_branch': sm})
c['C_ms'] = (c['hybrid'] - c['sum_branch']).where(
    c.index.get_level_values('workers') == 0, c['hybrid'] - c['max_branch'])
c['C_pct_of_max'] = (100 * c['C_ms'] / c['max_branch'])
c['gain_hint'] = None
c.round(2)""")

md("### Parallelism gain per depth (`p50(w0)/p50(w_max)`, fields=none)")

code("""wmax = df['workers'].max()
p = df[df.fields == 'none'].pivot_table(index=['size', 'window', 'contender'],
        columns='workers', values='p50_ms', sort=False)
p.columns = [f'w{c}' for c in p.columns]
p['gain'] = (p['w0'] / p[f'w{wmax}']).round(2)
p.round(2)""")

nb["cells"] = cells
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, "mod12930_balanced.ipynb")
print("wrote mod12930_balanced.ipynb")
