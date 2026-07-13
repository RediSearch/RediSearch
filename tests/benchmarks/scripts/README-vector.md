# Vector Macro-Benchmark Dataset

The `vecsim-glove-100-angular-1183514-*.yml` benchmarks measure vector search through a
real Redis server and the RediSearch command layer. They complement the VecSim library's
micro-benchmarks rather than replacing them.

## Dataset

The benchmarks use the public ANN-Benchmarks `glove-100-angular` corpus:

- 1,183,514 real 100-dimensional GloVe embeddings
- 10,000 real query vectors
- FLOAT32 values with cosine distance
- Ground-truth neighbors and distances for every query

The HNSW configuration (`M=36`, `EF_CONSTRUCTION=250`, `EF_RUNTIME=300`) matches the
configuration used by `deps/VectorSimilarity/tests/benchmark/bm_datasets.py`.

## Generate

Install the benchmark requirements and generate the full input set:

```bash
uv pip install -r tests/benchmarks/scripts/requirements-vector.txt
uv run python tests/benchmarks/scripts/generate_ann_vector_dataset.py \
  --output-dir /tmp/glove-100-angular-1183514
```

For a quick generator or local-run smoke test, use a bounded subset:

```bash
uv run python tests/benchmarks/scripts/generate_ann_vector_dataset.py \
  --source /path/to/glove-100-angular.hdf5 \
  --output-dir /tmp/glove-smoke \
  --dataset-name glove-smoke \
  --max-vectors 1000 \
  --max-queries 20
```

The generator writes binary-safe FTSB CSV files. It does not replace vectors with text or
base64, so Redis receives the same little-endian FLOAT32 blobs used by normal clients.
Every query uses a distinct vector from the ANN test set. Range-query radii come from each
query's 100th ground-truth neighbor, producing a stable, realistic result cardinality.

The generated manifest records counts, dimensions, file sizes, and SHA-256 hashes. Keep it
with the dataset so local and CI inputs can be compared exactly.

## Publish For CI

Upload the generated files under the benchmark dataset prefix used by the YAML files:

```bash
aws s3 cp /tmp/glove-100-angular-1183514/ \
  s3://benchmarks.redislabs/redisearch/datasets/glove-100-angular-1183514/ \
  --recursive \
  --acl public-read
```

The expected objects are:

```text
glove-100-angular-1183514.redisearch.commands.SETUP.csv
glove-100-angular-1183514.redisearch.commands.BENCH.KNN.csv
glove-100-angular-1183514.redisearch.commands.BENCH.RANGE.csv
glove-100-angular-1183514.manifest.json
```

After publishing, all three YAMLs work with `redisbench-admin run-local` and are selected by
the existing `vecsim*.yml` CI benchmark glob.

## Measurements

- `vecsim-glove-100-angular-1183514-hnsw-knn.yml`: HNSW KNN throughput and latency with
  realistic query-vector variation.
- `vecsim-glove-100-angular-1183514-hnsw-range.yml`: HNSW range-query throughput and
  latency at approximately 100 results per query.
- `vecsim-glove-100-angular-1183514-flat-knn.yml`: 200 exact FLAT scans that isolate the
  100-dimensional FLOAT32 cosine distance kernel through the Redis server.
- `build.vector_index_sz_mb`: exported by the existing benchmark framework for both HNSW
  query cases, making per-node HNSW memory changes visible at million-vector scale.
