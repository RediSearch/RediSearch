# Numeric range KNN decision tree

The experiment selects a numeric filter access path for filtered KNN. Redis
normally walks and merges postings from the numeric ranges each time it
intersects an HNSW batch. The alternative seeks directly through the exact
numeric values in a document-ID-ordered snapshot. It can seek near each
vector candidate without materializing the full set of matching IDs.
Single ranges, two-range intersections and two-range unions can use the probe.

The current decision tree requires an in-memory dense index, 50,000 to
1,000,000 documents, K from 1 to 128, numeric fields with one value per
document, and at least four matching tree ranges per predicate. The query must
allow trimming rich results and the index must be free of field expiration.
The numeric postings path handles every other shape. Bounds use the same
comparison and decoded numeric representation as the postings path. An
insertion invalidates the snapshot. Each snapshot costs approximately eight
bytes per document per participating field. The first eligible query after
an insertion may pay to rebuild it.

The tree preserves the legacy match estimate. For single-value FP32 RAM HNSW
with 64 to 256 dimensions, it can also choose the following vector paths.

- For an automatic two-range AND with K up to 32, the planner samples the
  combined predicate at up to 1,024 document IDs. It chooses exact search for
  an uncompressed vector index when at most 10% of the sample matches. This
  avoids starting graph traversal on selective conjunctions that often fall
  back to exact search.
- When the query uses batches, the iterator prefetches newly discovered
  vectors, defers ordering distant candidates, and transfers saved results in
  bulk. This preserves candidate ordering, tie-breaking, and search effort.

The tree honors an explicit `HYBRID_POLICY` or `BATCH_SIZE`. HNSW graph
traversal uses the requested `EF_RUNTIME`. The numeric filter probes query
deadlines during seeks and checks its document domain on revalidation.
`FT.PROFILE` shows `DENSE-NUMERIC-FILTER` when the dense filter is selected.

On x86-64 with AVX, sequential reads compare the numeric bounds in groups of
64 document IDs and cache the matching IDs as a bit mask. Sparse seeks and
the final partial block use the scalar lookup.

## Run the Redis comparison

Use a disposable server with the candidate module, one shard, six search
workers, and an empty database. Start it with `--enable-debug-command local`
so the loader can check the vector indexing queue over the Unix socket.
Install the Python Redis client. Download
`random_float_1m.tgz` from the
[pinned dataset registry](https://github.com/redis/vector-db-benchmark/blob/d3f4cc350b24b4cdc71432784e524b4ff76dec4d/datasets/datasets.json)
and extract it into a directory containing `vectors.npy`, `payloads.jsonl`
and `tests.jsonl`. The archive SHA-256 is
`8ae0aae24fd8eda4d265a3968b1d7b0c5a1b3dc504ec4e6d970a100655ea4baa`.

```sh
python3 scripts/numeric_knn_benchmark/replay.py build \
  --socket /tmp/redis-bench.sock --data /path/to/data

for policy in legacy dense dense legacy; do
  python3 scripts/numeric_knn_benchmark/replay.py measure \
    --socket /tmp/redis-bench.sock --data /path/to/data \
    --ef 2048 --queries 10000 --clients 100 --policy "$policy" \
    --warmups 1
done
```

Repeat at one client and with independent index builds. Use the same module
and graph for the two Redis settings in each trial. `--profile` runs untimed
samples for the three filter shapes. Keep the p50, p95, p99 and recall along
with QPS and record any dropped requests. The replay computes filtered
recall from the published exact IDs and aborts on incomplete result counts.

## Compare with OpenSearch

Replay the same range queries and exact ground truth on OpenSearch 3.7.0
(Lucene 10.4.0, one shard, HNSW `M=64`, `ef_construction=512`, K=25).
Use query-level effort sweeps to compare measured points at similar recall,
starting around 99.6–99.7% at one and 100 clients. Run all arms with the same
dataset, query list and driver. Record resource allocation and repeat across
independent trials.
