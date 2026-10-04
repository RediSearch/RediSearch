# MOD-19005 TOLIST cardinality harness

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
