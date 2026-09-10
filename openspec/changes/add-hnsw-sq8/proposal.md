# HNSW SQ8 integration (MOD-14958)

## Why

Expose VectorSimilarity's SQ8 compression to HNSW users who need to reduce vector
storage memory. This continues the existing integration described in the
[SQ HLD](https://redislabs.atlassian.net/wiki/spaces/DX/pages/6153601069).

## What changes

- Accept `COMPRESSION SQ8` and `TRAINING_THRESHOLD` on HNSW vector fields.
- Report the compression configuration in `FT.INFO`.
- Persist the configuration and rebuild the index from source documents on reload.
- Preserve loading of older RDB versions with compression disabled.

The dependency is [VectorSimilarity #1029](https://github.com/RedisAI/VectorSimilarity/pull/1029).
Benchmarks and broader concurrency testing belong to the sibling SQ8 tasks.
