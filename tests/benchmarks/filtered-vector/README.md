# Filtered-vector POC comparison

This runner measures real `FT.SEARCH` filtered HNSW queries, including the production
AUTO policy, against the frozen artifacts and independent FP32 oracle in
`VectorSimilarity/tests/benchmark/filtered`. It changes no engine behavior.
The same runner supports RAM and disk; Enterprise supplies disk setup and cache protocols.

Start a VecSim-only algorithm POC with the native runner in the
[shared benchmark draft](https://github.com/RedisAI/VectorSimilarity/pull/1093).
Its `tests/benchmark/filtered/POC-GUIDE.md` connects the native, Redis, disk and
customer stages. Native scan/batch baselines use precomputed eligibility and a
checksummed HNSW graph; they do not include this runner's Redis command costs.

## First run

Use Python 3.12 or newer; dependencies are pinned with wheel hashes.

Use a dedicated standalone server with `search-on-timeout fail`, workers enabled,
and debug commands available for index readiness and backend verification. Redis 8.6+
is needed by current Search. The runner creates a new index and document prefix,
refuses existing data, and never flushes a database. It leaves the index for subsequent
policy and algorithm runs. Use `REDIS_PASSWORD`, `--username`, and `--tls` if needed;
the password is not written to artifacts.

```sh
python -m pip install --only-binary=:all: --require-hashes -r tests/benchmarks/filtered-vector/requirements.txt
SHARED=deps/VectorSimilarity/tests/benchmark/filtered
# Before the shared benchmark dependency lands, set SHARED to that PR's checkout.
python "$SHARED/filtered_bench.py" prepare --output /tmp/filtered-synthetic \
  --documents 10000 --dimensions 128 --queries 32 --seed 20221 --metric cosine
python "$SHARED/filtered_bench.py" oracle /tmp/filtered-synthetic
python tests/benchmarks/filtered-vector/run.py \
  --shared-root "$SHARED" --bundle /tmp/filtered-synthetic \
  --output /tmp/baseline-ef100 --port 6379 --prepare \
  --run-label baseline --source-revision SERVER_MODULE_COMMIT \
  --module-artifact /path/to/loaded/redisearch.so --ef-runtime 100 --repeats 3
python "$SHARED/filtered_bench.py" score /tmp/filtered-synthetic \
  /tmp/baseline-ef100/results.jsonl --policies AUTO BATCHES ADHOC_BF --repeats 3 \
  --target-recall .95 --output /tmp/baseline-ef100/report.json
```

The shared dataset registry describes importing DBpedia FP32 cosine (100K quick,
1M selection) and Deep FP32 L2 (1M then 10M disk). Dataset binaries and results stay
outside Git. Synthetic data verifies the plumbing; it does not decide the winner.

## Freeze the comparison before branching algorithms

1. Record the benchmark commits in all participating repos, the dataset bundle ID,
   and the server's Redis/Search/VecSim/Enterprise revisions. Create algorithm branches
   from each repository's benchmark base. Keep the harness identical across contenders.
2. Load once in ascending document-ID order, wait for all vectors to reach the HNSW
   backend, and retain the index. The runner records a graph ID in a separate Redis key.
   Save a baseline RDB/disk snapshot and its checksum when comparing separate processes.
   A graph ID is a tracking label, not a graph checksum or proof of equivalent topology.
3. For traversal/policy changes, restore that same snapshot and omit `--prepare`.
   Bundle/prefix/backend mismatches are rejected. For graph-construction changes,
   build separate graphs and report construction time/size and repeat variability.
4. Sweep `--ef-runtime` (for example 10, 50, 100, 200) into separate output directories,
   with the same cases and three repetitions. ADHOC_BF receives no incompatible EF_RUNTIME
   query argument. AUTO omits HYBRID_POLICY; the runner does not emulate its heuristic.
5. Compare latency at matched recall, per selectivity/correlation/k cohort. Use a 95%
   recall selection target and report cohorts that miss it, failed queries, and incomplete
   result counts. Do not average away low-selectivity failures or use latency alone.

This first foundation includes a fixed filter bank, independently generated queries,
random membership and anchor-near/far correlation stress cases, k=10/100 where population
permits, and zero/fewer-than-k semantics. Filters are loaded once as TAG memberships;
there are no per-query metadata writes. Anchor filters are extreme controlled stress
cases, not estimates of customer filter distributions. Actual eligible counts live in
the oracle, including rounding in small fixtures.

## Reading results

`results.jsonl` contains every case/policy/repetition with returned IDs, scores, latency,
and status. The exact oracle runs separately. `run.json` records bundle/graph identities,
declared server revision, optional module SHA256, runner/dependency revisions, schema,
server/module/index metadata, client hardware and server memory. Supply controlled server host
settings with `--environment-json`; keep credentials out of that file.

The timing scope is one client round trip with one outstanding request, including Redis
parsing, filtering, vector search, sorting and serialization. It is not a VecSim kernel
timer or a concurrent throughput result. Policy order rotates between repetitions;
cache state is recorded as the state at the start of the run. No cache is evicted by
this script. Use a separate warm-up pass before labeling a measured run warm.

The requested policy is not proof of the effective strategy. Effective policy remains
null in per-query results; untimed `FT.PROFILE` samples are saved in `profiles.json` after
measurement for inspection. The initial implementation does not extract every strategy
transition. Timeouts fail because the server must use `search-on-timeout fail`.

Disk queries require explicit policies on the current Enterprise implementation; AUTO
is rejected by the runner. The backend's `IS_DISK` flag is checked, and the shared full
precision oracle also evaluates quantized/reranked results. Disk ADHOC_BF is not ground
truth. Follow the Enterprise wrapper for bounded-memory and cold-start campaigns.

## Validation and next layers

```sh
python -m unittest discover -s tests/benchmarks/filtered-vector -p 'test_*.py' -v
```

The small live smoke uses an existing module artifact and verifies result parsing,
all three policies, zero/fewer-than-k cases, and scoring against the independent oracle.
It is an adapter compatibility check, not a performance measurement of this source tip.
Run the 10K fixture against each candidate's freshly built module before full campaigns.

Subsequent layers are a native disk kernel adapter with the same bundle, richer
numeric/text predicates, concurrent load, mutation-aware correctness, multi-value docs,
and instrumented strategy/I/O counters. Customer replay belongs in its source repository;
business-score/group-by output must not be scored as filtered vector top-k recall.

For a new native algorithm such as NaviX, add its actual RQE/VecSim integration
before extending this runner's accepted policies. The current batch API does not
receive filter eligibility. Use an explicit strategy route and diagnostic marker
to prove the candidate ran; AUTO can otherwise choose ADHOC and bypass it.
