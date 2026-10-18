# FT.ALTER vector backfill benchmark

`benchmark_alter_vectors.py` measures background scan completion for
[MOD-18278](https://redislabs.atlassian.net/browse/MOD-18278). It runs against a real
Redis server and release Search module, creating a fresh private process and Unix
socket for every trial. It needs Python with `redis` and `numpy` installed.
Use a Redis runtime compatible with the selected Search revision. For the pinned
PR #11691 modules below, use the repository benchmark workflow's Redis commit
`d3abc02fed6664cc0afaf9ffe4e172a99e274f44`; the host's default Redis 8.4 lacks the
required key-metadata API. Use the pinned runtime rather than relying on an
unstable version string; an older unstable binary on the host accepted this
module at startup but crashed during document loading.

The initial implementation covers HASH documents with HNSW FLOAT32 vectors:

- `add-vector`: documents already contain an unindexed vector field. ALTER adds
  it to the schema. This covers PR #11691 with and without concurrent writes.
- `add-tag`: ALTER adds a TAG field present in every document, exercising relabeling
  of the pre-existing vectors.

Both scenarios support multiple pre-existing vector fields and configurable vector
dimensions, document counts, and random seed. Workers are fixed at zero, so vector
inserts finish synchronously and scan completion does not leave tiered ingest work
outstanding. JSON, sparse field presence, geometry, and worker-enabled configurations
are not covered by this driver yet.

## Run

Build release modules at the desired revisions and copy them to distinct paths.
Retain each build log, source SHA, submodule SHAs, compiler/options, and module
SHA256 next to the results. Labels identify the builds; the driver records module
checksums and Redis version/git SHA but cannot verify a module's source revision.

For PR #11691, use its immediate parent
`6fa99d7e9eee0b60fc59117c7120feb66972bf5e` and merged revision
`af82b359e2a293a1824d4dc32f841e7c440d5f30`. That pair isolates the added-field
comparison change; both already include pre-existing-vector relabeling. Comparing
the gate enabled/disabled on one module measures the broader replacement path.

```bash
python3 tests/benchmarks/scripts/benchmark_alter_vectors.py \
  --redis-server /absolute/path/to/redis-server \
  --module before=/absolute/path/to/before/redisearch.so \
  --module after=/absolute/path/to/after/redisearch.so \
  --documents 100000 --dim 128 --vector-fields 1 \
  --scenario add-vector --writer-rates 0 1000 --repetitions 3 \
  --output /tmp/alter-added-vector.jsonl
```

Use `--scenario add-tag` for pre-existing vectors plus a new non-vector field.
Use `--optimize-partial-update false` in a separate invocation to measure the full
replacement fallback. Use a new output path for each invocation; existing files
are never overwritten. A smoke run can use `--documents 1000 --dim 16
--writer-rates 0 --repetitions 1`.

Each output line is one completed trial. Trials alternate build order between
repetitions. Check the machine for competing workloads before running; build and
benchmark sequentially. Use identical-binary A/A controls before attributing small
differences to the optimization.

## Measurements and checks

- `scan_observed_seconds`: elapsed time from issuing ALTER until the first
  observation of `indexing=0` and `percent_indexed=1`. This includes command time,
  background scheduling, polling, and starting the optional writer thread. The
  default polling interval is 10 ms; small timing differences near this scale are
  not meaningful. An incomplete or timed-out scan fails the run.
- `alter_command_seconds`: time until the command reply, recorded separately.
- `counter_deltas`: INFO MODULES vector indexing/relabel counts taken before ALTER
  and after the writer stops. They include both scan work and concurrent writes,
  including a possible write in flight when completion is observed. They do not
  measure vector deletions or isolate backfill-only operations under writes.
- `writer_completed`, `writer_missed_slots`: actual completed writes and offered
  rate slots skipped under overload. Writes choose seeded random documents and
  change the indexed TEXT field while retaining the vector bytes. The writer starts
  after ALTER replies. A concurrent trial requires at least one write to complete
  before scan completion is observed; this does not prove the scan revisited a
  document already rewritten by the writer.
  This is a paced synchronous writer, so the target rate is not guaranteed under
  load. Check `writer_effective_ops_per_second` and missed slots before comparing
  trials as equivalent write workloads; reduce the target to a sustainable rate
  if the question requires a matched achieved rate.
- CPU deltas cover the Redis process during the scan and writer shutdown; memory
  reports Redis allocator-accounted usage and `used_memory_peak`, including initial
  dataset load. Process RSS is not measured.

Without concurrent writes, the driver asserts the expected insert/relabel counts
for the selected gate setting. After timing, it checks the document count, indexing
failure count, vector label counts, and (for `add-tag`) the added field's match count.
These are benchmark sanity checks, not a replacement for the correctness suite.
Absent relabel counters are preserved as `null` in snapshots and treated as zero in
deltas for older/gate-disabled modules.

## Repository benchmark integration

The driver lives alongside the repository's other benchmark scripts. The
`FT.ALTER Vector Comparison` workflow builds a pinned baseline and candidate, then
runs the control and comparison sequentially on one Ubuntu 24.04 runner. It publishes
a job summary and an artifact containing binaries, hashes, manifests, build logs,
and raw JSONL. Build and benchmark phases are sequential.

PRs modifying the workflow, driver, or summary scripts trigger the default comparison
automatically: the immediate parent of PR #11691 versus its merged revision, 100K
documents, 128 dimensions, three repetitions, and zero/10 writes per second. Manual
dispatch allows compatible Search revisions and dataset parameters to be selected
once the workflow is registered on the default branch:

```bash
gh workflow run benchmark-alter-vectors.yml --ref master \
  -f baseline_sha=6fa99d7e9eee0b60fc59117c7120feb66972bf5e \
  -f candidate_sha=af82b359e2a293a1824d4dc32f841e7c440d5f30 \
  -f documents=100000 -f dimensions=128 -f repetitions=3 -f writer_rate=10
```

The summary validates complete pairs, the planned repetition/rate counts, and matching
runtime/configuration before reporting medians. Its A/A control covers the no-write
case; it is not a noise bound for all loads. The benchmark machine differs from the
original dorer-intel run, whose [report and raw data](../results/mod18278-pr11691-20261018/README.md)
are retained separately.

This is a direct CI job, not a redisbench-admin
YAML tool: that runner dispatches to supported tool adapters, and FT.ALTER command
latency alone does not capture background scan duration. Integrating these JSONL
metrics into the standard runner and its comparison/export pipeline requires a
runner adapter. No YAML that silently measures only command latency is supplied.
