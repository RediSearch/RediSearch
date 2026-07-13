# MOD-12930 — FT.HYBRID benchmark

Times `FT.HYBRID` alongside its two subqueries run standalone, on queries tuned per cell
so the two subqueries have similar latency. Full context and conclusions: `SUMMARY.md`;
results presentation: `mod12930_balanced_report.html`.

## Files

| file | role |
|---|---|
| `download_data.py` | fetch the dbpedia rows + embeddings into `data.npz` (run once) |
| `bench_lib.py` | server management, corpus load + warmup, contender commands, correctness oracle, timing loop |
| `balanced_lib.py` | per-cell calibration (text latency ≈ vector latency) and the matrix runner |
| `merger_sweep.py` | sweep with window / size / selectivity varied independently |
| `build_balanced_notebook.py` | generates `mod12930_balanced.ipynb` |
| `run_balanced.sh` | entrypoint: executes the notebook, regenerates the report |
| `make_balanced_report.py` | renders `mod12930_balanced_report.html` from the results JSON |

## Running

```bash
python download_data.py          # once (~1.2GB)
./run_balanced.sh                # full run (~1h on a laptop); REUSE_RESULTS=1 re-renders only
```

Requires a release build of the module (`../bin/*release*/search-community/redisearch.so`)
and `redis-server` on PATH. Keep the machine awake for the duration.
