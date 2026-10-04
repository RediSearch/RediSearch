# MOD-19005 allocation experiments


## Experiment scope and decision

These experiments are stacked on the force-pushed row-block coverage tip
`fb860a3d5bcc5679cd09f10eab455c55ed91681d` (PR 11753), with row blocks enabled
by default in both benchmark arms. The upstream NUL-terminator allocation fix
is already in that baseline. PR 11652's linear-scan/inline-set implementation
is not applied; all variants retain hash-based TOLIST membership.

**This is an experimental branch, not a production merge recommendation.**
The requested default-on control is unsafe during a mixed-version rolling
upgrade: coordinators can send `_ROW_BLOCK` to shards that do not support it.
A production follow-up must keep the default off or negotiate support.

### What each experiment removes

| Experiment | Work removed | Work that remains |
|---|---|---|
| Stack TOLIST iterator | One iterator allocation/free per finalized TOLIST instance | Membership hashing, dict entries/buckets, reference-count round trip |
| Compact group keys | One dynamic lookup-row allocation/free per retained keyed group; key slots share the existing group arena | Group-key hashing, reducers, reference destruction, output-row storage |
| TOLIST ownership drain | Iterator allocation/free, finalization incref/decref round trip, and the later second dictionary walk | Membership hashing, per-entry allocation/free, bucket allocation/free, output array |

The final source contains compact keys and the ownership drain. The drain
supersedes the iterator helper; the iterator-only experiment remains in history
with its own results. Drain moves entry/bucket frees into finalization rather
than eliminating them. Compact keys use the existing group block allocator;
they do not change string ownership or row-block pinning.

These are structural operation reductions, not a new instrumented allocation
census. Byte estimates from earlier profiling are not used as measured memory
savings here. No experiment changes the 8 MiB pinning budget.

### Sequential experiment results

Percentages are the median of paired round-p50 percentage changes; negative
means faster. Each experiment is compared with its immediate parent. Do not
add the percentages: the direct baseline comparison below measures the final
combination. Standalone client timings include reply decoding.

| Shape | Stack iterator | Compact keys | Ownership drain | Combined vs original baseline |
|---|---:|---:|---:|---:|
| Four distinct values (`tiny_4`) | -1.28% | -0.31% | +0.17% | -0.33% |
| 16 distinct short values / 256 rows | +2.96% | +0.21% | +1.02% | +4.74% |
| 16 distinct long values / 256 rows | +4.01% | +0.52% | -1.61% | +0.97% |
| 256 distinct values | -0.45% | +1.95% | -1.49% | +0.54% |
| Singleton guardrail | -0.50% | -1.60% | -1.15% | -4.19% |
| 17 distinct long values guardrail | +3.80% | +3.29% | -2.98% | +2.58% |
| 4096 distinct values guardrail | +0.34% | +0.20% | -2.14% | +0.87% |

The identical-binary A/A calibration has shape medians from -1.20% to +1.17%,
with individual rounds from -8.70% to +6.54%. Small changes are inconclusive;
repeated slowdowns are retained as guardrails rather than averaged away.

The two-round six-shard screening comparisons were:

- Stack iterator: -1.17%, -0.29%. Net win not established.
- Compact keys: -3.16%, -3.55%. A distributed workload win, with a
  +3.29% median slowdown on the 17-long-value guardrail.
- Ownership drain: -0.51%, -0.38%. Small distributed gains, mixed shapes;
  broad net latency win not established.

### Final combined six-shard result

The direct original-baseline versus compact-keys + drain comparison improves
ordinary-query p50 in all four alternating pairs: **-5.43%, -4.94%, -4.63%,
-4.28%**, median **-4.79%**. Median round p50 is 1770.8 ms versus 1685.1 ms.
All-node server CPU falls in every pair, median **-5.25%**. Every result and
profile warning check passes. This is a measured win on this distributed
workload, with mixed standalone results; it is not a universal net win.

Separate FT.PROFILE runs report these median times across four profiles per arm:

| Coordinator processor / component | Baseline ms | Combined ms |
|---|---:|---:|
| Network total | 1011.5 | 1001.0 |
| Shard wait (within Network) | 763.9 | 747.4 |
| Row conversion (within Network) | 199.1 | 204.2 |
| Reply freeing (within Network) | 1.05 | 1.00 |
| Grouper | 985.7 | 920.5 |
| Sorter | 144.3 | 193.3 |

Profile queries are instrumented and have different elapsed time from ordinary
queries. These medians are not additive and the Network subfields are not an
exhaustive partition. Sorter time rises while Grouper time falls; processor
accounting alone does not identify where an end-to-end saving comes from.
Grouper remains the largest coordinator computation target. This run does not
provide a fresh Accumulate/Finalize/Clear or allocation call-site census.

### Measurement and provenance

All seven standalone cases use 32,768 documents, four alternating rounds,
40 measured queries per arm/case/round, and five warmups. Six-shard screening
uses two alternating rounds and five measured queries per arm/round. The final
combined cluster comparison uses four alternating rounds and eight measured
queries per arm/round. Ordinary queries are timed separately from FT.PROFILE.
INFO CPU is per Redis process: the coordinator node also executes its local
shard, so its CPU delta is not coordinator-only CPU. Network wait in FT.PROFILE
is elapsed waiting time, not CPU consumption.

The host has 16 logical CPUs / 8 physical cores. Redis is 8.9.241; redis-py is
6.4.0. Module builds are release/RelWithDebInfo with O3. Builds and benchmarks
ran sequentially. CPU affinity was not pinned, and this is a shared host;
these runs do not establish production tail latency or concurrency behavior.

Every arm validates results and records binary SHA-256 hashes. The module
snapshots were built with each experiment's source changes before its result
commit was finalized, so embedded Git build metadata may name the parent.
Hashes and saved source patches identify the measured artifacts.
`results/raw-results.tar.gz` preserves every raw timing JSON, full profile,
source patch, build log, binary manifest and the local cluster controls/reference.
Extract it into a scratch directory for inspection; its control script contains
machine-specific paths and must be reviewed before reuse. Compact JSON summaries
beside it are directly reviewable in GitHub. Source-equivalent
commits are the default-on control, iterator experiment, compact-key experiment,
and ownership-drain experiment respectively.

The saved synthetic cluster has 5.7M indexed documents, 273,282 groups and 400
returned rows. Validation compares group identities/fields and sorted TOLIST
values, allowing only small floating-point differences in numeric outputs.
`cluster-query.json` preserves the exact query. `cluster-seed-config.toml`
preserves the generator parameters; the local generator checkout is
`search-workload-repro` at `da14010c8401e055cb8ffba279e3c3f5f442aa09` with local
changes. The existing RDB snapshot, not a fresh seed, was reused across all
arms. These files alone do not recreate that exact RDB on another machine.

Builds and benchmark result checks passed. Unit, flow and sanitizer suites
were not run as part of these experiments. In particular, production promotion
needs focused coverage for dictionary ownership transfer, active rehashing,
empty dictionaries and reuse. Before/after INFO memory snapshots are not peak
memory measurements and cannot establish a memory-saving percentage.

### Next decision

Compact group storage is the strongest candidate from the incremental cluster
measurements. Isolate it in a production follow-up, retain the duplicate-heavy
guardrails, and extend the comparison to 20 shards and concurrent queries.
The drain should remain separately selectable until its smaller gain survives
those checks. Neither the iterator-only result nor reduced allocation counts
justify a blanket performance claim.

If allocations remain dominant, the next experiment should pool TOLIST
container/entry storage while retaining hash membership. Measure allocation
calls, allocated bytes, peak live bytes, hash/equality calls and cleanup CPU
separately. An arena still needs to release owned RSValue references; deleting
entry frees alone does not remove payload destruction. Row-block retention is
a separate experiment requiring pinned/live bytes and copied-byte measurements,
not simply a larger hard cap. The existing NUL-copy fix stays in every baseline.

## Harness

This standalone synthetic benchmark compares TOLIST implementations over four
primary value shapes by default:

- `tiny_4`: four distinct values in a four-row group.
- `duplicates_16_short`: 16 distinct 16-byte values repeated over 256 rows.
- `duplicates_16_long`: 16 distinct 512-byte values repeated over 256 rows.
- `distinct_256`: 256 distinct 16-byte values in a 256-row group.

Three extended guardrails are available: `tiny_1`, `duplicates_17_long` (crosses
the inline-to-dictionary boundary), and `distinct_4096`. Select explicit cases
with `--cases tiny_4,duplicates_16_short`; use `--cases all` for every case.

The harness starts one standalone Redis process at a time, loads identical
synthetic data for each case, alternates baseline/candidate order between
rounds, warms each query, checks complete group and value sets, and records all
client timing samples. It also records Redis CPU and memory INFO snapshots
immediately before and after each timing batch. These are batch deltas and
before/after memory readings; memory is **not** sampled as a peak. Client timing
includes decoding the complete reply in redis-py.

Standalone runs do not exercise shard-to-coordinator row-block transport. The
harness records `CONFIG GET search-internal-row-block-format` per arm so a
benchmark module built with the row-block default enabled can be confirmed, but
that setting does not change the standalone query protocol. Use the distributed
5.7M-document workload for transport measurements.

## Run

Keep raw per-sample output outside the repository. Write the compact summary to
the experiment directory after comparing module hashes and results:

```sh
rtk proxy /home/ubuntu/workspace/redisearch/.venv/bin/python \
  tests/benchmarks/mod19005/benchmark_tolist_cardinality.py \
  --redis-server /home/ubuntu/workspace/redis-unstable/src/redis-server \
  --baseline-module /path/to/baseline/redisearch.so \
  --candidate-module /path/to/candidate/redisearch.so \
  --output /tmp/mod19005-tolist-raw.json \
  --summary-output tests/benchmarks/mod19005/summary.json
```

The default is `tiny_4,duplicates_16_short,duplicates_16_long,distinct_256`.
To run the extended cases too, pass `--cases all`.

The default is four repetitions and 40 timed queries per case and arm, with five
warmup queries. Use the `--help` output for sample and repetition controls. Run
without competing builds, benchmarks, or Redis load. Do not infer distributed
latency, production tail latency, or peak memory from this standalone harness.

## Six-shard workload

`benchmark_cluster.py` uses an explicitly supplied cluster control script and
working directory. The control script must implement `stop` and `start
<module-path>`. It restarts only the cluster managed by that script. The runner
checks every node for the expected module and `search-internal-row-block-format
yes`, verifies the 5.7M indexed documents and 400 returned rows against the
saved result, runs five warmups, waits for host CPU busy time to fall below 20%
over a three-second `/proc/stat` window, then records at least five ordinary
queries. It runs one `FT.PROFILE ... LIMITED` query separately and saves the
profile reply beside the raw JSON. Memory values are before/after snapshots, not
peak memory.

Example for the preserved local cluster controls and dataset:

```sh
rtk proxy /home/ubuntu/workspace/redisearch/.venv/bin/python \
  tests/benchmarks/mod19005/benchmark_cluster.py \
  --cluster-control /tmp/mod19005-tip-profile/cluster.py \
  --cluster-workdir /tmp/mod19005-tip-profile \
  --ports 7401,7402,7403,7404,7405,7406 \
  --baseline-module /path/to/baseline/redisearch.so \
  --candidate-module /path/to/candidate/redisearch.so \
  --query /tmp/mod19005-tip-profile/query.json \
  --reference-rows /tmp/mod19005-tip-profile/logs/plain-0-rows.json \
  --output /tmp/mod19005-cluster-raw.json
```

The control script itself must enforce its own data-directory/port ownership
checks. Confirm its configured node directories and ports before running it.
