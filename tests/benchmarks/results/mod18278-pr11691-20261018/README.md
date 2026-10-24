# MOD-18278: PR #11691 vector backfill comparison

Run date: 2026-10-18. Host: dorer-intel (Intel Xeon Platinum 8375C, 16 logical CPUs).

The optimized build had slightly lower observed median scan times in this configuration. The differences are small compared with the variation observed in the identical-binary control; these three-pair samples do not establish a reliable performance gain or regression. The concurrent-write counters do confirm that the PR performs extra insertions in place of relabels.

## Scan completion results

100,000 HASH documents; HNSW FLOAT32, 128 dimensions, L2, M=16, EF_CONSTRUCTION=200. One existing vector field, adding a second vector field whose value is present in every document. WORKERS=0 and OPTIMIZE_PARTIAL_UPDATE=true. Three repetitions per build and write rate; build order alternates between repetitions.

Change is derived as `(after median / before median - 1) × 100`; a negative value means shorter elapsed time.

| Target writes/s | Before median (s) | After median (s) | Median change | Before CPU median (s) | After CPU median (s) |
|---:|---:|---:|---:|---:|---:|
| 0 | 45.425 | 44.916 | -1.12% | 46.241 | 45.833 |
| 10 | 46.227 | 45.328 | -1.94% | 46.297 | 46.413 |

CPU is total Redis user + system CPU during the measured scan and writer shutdown, not the Python driver CPU. Elapsed time ends at the first FT.INFO observation of indexing=0 and percent_indexed=1; polling interval is 10 ms. Dataset load is outside the timed interval.

### Individual paired runs

| Target writes/s | Repetition | Before (s) | After (s) | Paired change |
|---:|---:|---:|---:|---:|
| 0 | 1 | 46.372 | 44.761 | -3.48% |
| 0 | 2 | 45.425 | 45.443 | +0.04% |
| 0 | 3 | 45.084 | 44.916 | -0.37% |
| 10 | 1 | 46.227 | 45.328 | -1.94% |
| 10 | 2 | 45.736 | 45.053 | -1.49% |
| 10 | 3 | 47.113 | 45.927 | -2.52% |

## Identical-binary A/A control

Six fresh-process trials use the exact same baseline .so, labeled A/B, with no writer. This is an observed control sample, not a confidence interval or a universal noise bound.

| Repetition | A (s) | B (s) | Paired change |
|---:|---:|---:|---:|
| 1 | 46.340 | 44.622 | -3.71% |
| 2 | 45.101 | 46.214 | +2.47% |
| 3 | 45.103 | 44.944 | -0.35% |

The control was run before the main comparison; it does not characterize all possible drift during the later writer trials.

## Counter evidence and achieved write rates

All no-writer trials on both builds recorded exactly 100,000 vector inserts and 100,000 vector relabels.

| Build | Repetition | Completed writes | Achieved writes/s | Missed slots | Vector inserts | Vector relabels | Inserts above 100,000 |
|---|---:|---:|---:|---:|---:|---:|---:|
| before | 1 | 463 | 10.016 | 0 | 100,000 | 100,926 | 0 |
| after | 1 | 454 | 10.016 | 0 | 100,217 | 100,691 | 217 |
| before | 2 | 458 | 10.014 | 0 | 100,000 | 100,916 | 0 |
| after | 2 | 451 | 10.010 | 0 | 100,189 | 100,713 | 189 |
| before | 3 | 472 | 10.018 | 0 | 100,000 | 100,944 | 0 |
| after | 3 | 460 | 10.016 | 0 | 100,218 | 100,702 | 218 |

The writer changes only the indexed TEXT field and retains the vector bytes. With no vector-value changes, inserts above N in the optimized build demonstrate that the added-vector reinsertion path was exercised. Counters include scan work and writer work, including a possible in-flight write after scan completion was observed; they are not backfill-only counters. Every trial satisfies `inserts + relabels = 2N + 2 × completed_writes`.

The writer is synchronous and paced at 10 requests/s. No slots were missed. Actual totals differ because each writer stops when its trial completes. The very small achieved-rate overshoot comes from issuing the first write immediately; these are matched rates, not identical total write counts.

## Memory and correctness checks

| Target writes/s | Before median used-memory increase (MiB) | After median used-memory increase (MiB) | Before median peak Redis used_memory (MiB) | After median peak Redis used_memory (MiB) |
|---:|---:|---:|---:|---:|
| 0 | 100.49 | 100.28 | 378.09 | 376.98 |
| 10 | 100.69 | 100.33 | 377.39 | 377.12 |

The peak metric is Redis allocator-accounted used_memory_peak, not process RSS; it includes initial dataset loading, not just ALTER. All 18 trials passed the document-count, vector-label-count, and indexing-failure checks. No-writer operation-count assertions passed. These are benchmark sanity checks, not the full correctness suite.

## Provenance and reproduction

The reusable driver is [benchmark_alter_vectors.py](../../scripts/benchmark_alter_vectors.py). Binaries are retained on the benchmark host; they are not committed to the repository.

- Before: `6fa99d7e9eee0b60fc59117c7120feb66972bf5e`.
- After: `af82b359e2a293a1824d4dc32f841e7c440d5f30`, the merge of PR #11691.
- VecSim: `fcdeb37b1c264c00ad2e8176d91cc2c0d071625a` on both builds.
- Search release build: `./build.sh`, GCC 13.3.0, no LTO flag supplied.
- Redis: `d3abc02fed6664cc0afaf9ffe4e172a99e274f44`, version 8.9.241, built with `make -j16 BUILD_TLS=no`.
- Seed: 18278. Each trial starts a new private Redis process with persistence disabled and a private Unix socket.
- Artifacts are copied out of build directories and checksum-verified before the series.
- Host CPU-load snapshots and checks during the run showed no competing heavy workload.

This historical pair isolates skipping the comparison for newly added vector fields. Both builds already include pre-existing-vector relabeling and selective ALTER scanning. These results do not compare the broader optimization against full replacement, and they do not describe current master. [Later runtime commits](later-runtime-commits.txt) include geometry ID moves, a VecSim bump, and other changes excluded from the pair.

Commands, from the remote artifact directory:

```bash
python3 benchmark_alter_vectors.py --redis-server ./redis-server \
  --module control_A=./before.so --module control_B=./before.so \
  --documents 100000 --dim 128 --writer-rates 0 --repetitions 3 \
  --output control-100k-128.jsonl

python3 benchmark_alter_vectors.py --redis-server ./redis-server \
  --module before=./before.so --module after=./after.so \
  --documents 100000 --dim 128 --writer-rates 0 10 --repetitions 3 \
  --output comparison-100k-128.jsonl
```

The driver creates output files exclusively; use new filenames to reproduce without overwriting existing results.

## Artifacts and remaining scope

- [Raw A/A records](control-100k-128.jsonl)
- [Raw before/after records](comparison-100k-128.jsonl)
- [Calibration](calibration-10k.jsonl): 10,000 docs, baseline only, 10 writes/s, zero missed slots; excluded from the main aggregates.
- [Binary checksums](SHA256SUMS)
- [Before build manifest](before-build-manifest.txt)
- [After build manifest and hardware](after-build-manifest.txt)

Remote binaries, build logs, driver, raw data, and manifests are under `/home/ubuntu/mod18278-pr11691-20261018/` on dorer-intel. The pinned runtime build checkout is `/home/ubuntu/wt/redis-mod18278/`.

Remaining MOD-18278 coverage includes higher write pressure, additional dimensions/dataset sizes, multiple pre-existing fields at benchmark scale, worker-enabled tiered indexes, sparse/dense HASH and JSON selective skipping, and geometry move/fallback cases. Three paired repetitions do not establish non-regression for other workloads.
