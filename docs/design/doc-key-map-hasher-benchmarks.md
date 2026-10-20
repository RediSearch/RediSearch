# Document key map hasher comparison

Measured on 2026-10-20, before replacing the standard-library hasher with
`ahash::RandomState`. The benchmark compares those two implementations explicitly,
so it remains usable after that production change.

## Decision

Use randomized AHash. In the 1.6M-document fixture, the median paired speedups were
3.07–4.84x for hashing, 1.90–2.33x for successful lookup, 1.36–1.60x for misses,
and 1.44–1.62x for initial population with growth. Replacement improved for short
and medium keys; the 256-byte replacement case was effectively unchanged (1.00x).
The smaller 100K-document/64-byte replacement case had a 0.93x paired median
with substantial variation (0.75–4.04x), so this run does not establish a blanket
write-speedup claim. These are isolated model results, not end-to-end Redis
throughput gains.

The existing full-key comparison through DMDs still determines equality. Hashes
are process-local and not persisted. AHash uses a private randomized seed for each
map; this change introduces no deterministic shared seed or custom hash algorithm.
AHash is non-cryptographic and is intended for in-memory hash tables; see its
[design documentation](https://github.com/tkaitchuck/aHash#design).

## Environment and procedure

- Intel Core Ultra 7 255U, Linux x86_64, pinned to logical CPU 0 (a P-core).
- Rust 1.94.0, AHash 0.8.12, hashbrown 0.17.1.
- Workspace release/bench profile: optimized, fat LTO, one codegen unit.
- Generic Rust target, with no `target-cpu=native` or AES target feature. Although
  the CPU supports AES, AHash's software path was selected (`aes_target_feature=false`).
  AES-enabled builds and other architectures were not measured.
- CPU frequency scaling remained enabled (`powersave`); background system activity
  was not controlled. No other agent build/test/benchmark ran concurrently.
- One invocation, six datasets, two warmup pairs followed by eleven measured pairs
  per operation. Each pair alternates which hasher runs first. Each fixture uses
  a normally randomized seed; this is not a multi-seed distribution study.
- 100K and 1.6M documents, with 16-byte sequential document names, 64-byte tenant/
  catalog names, and 256-byte binary keys. Query IDs are deterministically mixed.
- Both implementations use the same 16-byte `{hash, id}` payload, hashbrown growth
  policy, ID-to-owned-key indirection, and full byte comparison. Query strings
  are separately allocated, preventing same-address equality shortcuts.
- A query bank contains 57,600 keys (`900 * 64`). `hash` measures eight passes;
  `hit` and `miss` measure two passes. Missing keys use a prefix absent from all
  indexed keys. `replace` removes and reinserts each query's ID, hashing twice.
  `build` populates from empty through normal growth; destruction is outside timing.
- Fixtures use Rust's system allocator and model key access with a vector. They
  exclude the module's Redis allocator, C/FFI calls, DMD hash-bucket chain traversal,
  GIL/spec locks, sorting/deduplication, and iterator/result processing. Compiler
  inlining also differs from the C/FFI path. Consequently the map lookup factors
  must not be presented as measured module or command speedups. Full workload
  benchmarking remains required before broader rollout or default enablement.

The hasher comparison uses one generic fixture rather than making the production
FFI type generic. This holds the algorithm constant without expanding its API.
Existing production collision/lifecycle tests validate the selected hasher separately.

## Allocation

Each populated fixture had the same bucket count for both hashers. At 1.6M documents,
2,097,152 buckets plus control bytes and the map object occupied 35,651,648 bytes
with the standard seed and 35,651,664 bytes with AHash. This is a 16-byte increase
per index, with no per-document payload change. These counts exclude indexed key
storage, allocator rounding/metadata, transient growth allocations, and RSS.

## Results

Times are medians in nanoseconds per operation (per inserted document for `build`).
Speedup is the median of the eleven *paired* Std/AHash ratios, so it need not equal
the ratio of the two displayed medians. Ranges show the observed paired minimum
and maximum; they are not confidence intervals. All 330 samples are in the
[raw CSV](doc-key-map-hasher-benchmarks.csv).

| Documents | Key bytes | Operation | Std ns/op | AHash ns/op | Paired speedup | Observed range |
| ---: | ---: | --- | ---: | ---: | ---: | ---: |
| 100,000 | 16 | hash | 7.18 | 1.54 | 4.67 | 3.45–5.11 |
| 100,000 | 16 | hit | 367.58 | 172.69 | 2.31 | 1.06–2.92 |
| 100,000 | 16 | miss | 14.56 | 8.95 | 1.67 | 1.36–1.79 |
| 100,000 | 16 | replace | 84.06 | 46.40 | 1.37 | 0.95–3.45 |
| 100,000 | 16 | build | 34.12 | 22.14 | 1.54 | 1.48–1.63 |
| 100,000 | 64 | hash | 14.59 | 4.43 | 3.34 | 2.93–3.78 |
| 100,000 | 64 | hit | 410.90 | 243.18 | 1.72 | 1.39–2.05 |
| 100,000 | 64 | miss | 29.35 | 18.17 | 1.75 | 1.27–2.18 |
| 100,000 | 64 | replace | 90.91 | 65.57 | 0.93 | 0.75–4.04 |
| 100,000 | 64 | build | 27.66 | 17.05 | 1.56 | 1.42–1.86 |
| 100,000 | 256 | hash | 71.01 | 19.70 | 3.68 | 3.20–5.43 |
| 100,000 | 256 | hit | 1053.19 | 531.52 | 1.91 | 1.64–3.31 |
| 100,000 | 256 | miss | 90.39 | 48.68 | 1.83 | 1.66–2.13 |
| 100,000 | 256 | replace | 160.76 | 125.48 | 1.07 | 0.82–3.76 |
| 100,000 | 256 | build | 65.10 | 43.84 | 1.49 | 1.40–1.81 |
| 1,600,000 | 16 | hash | 6.87 | 1.43 | 4.84 | 4.55–5.16 |
| 1,600,000 | 16 | hit | 815.63 | 354.17 | 2.33 | 2.20–2.75 |
| 1,600,000 | 16 | miss | 54.15 | 29.03 | 1.54 | 1.15–3.10 |
| 1,600,000 | 16 | replace | 594.11 | 242.76 | 2.22 | 2.03–3.27 |
| 1,600,000 | 16 | build | 52.64 | 31.00 | 1.62 | 1.40–1.77 |
| 1,600,000 | 64 | hash | 15.62 | 5.22 | 3.07 | 2.07–3.87 |
| 1,600,000 | 64 | hit | 1036.54 | 546.30 | 1.98 | 1.71–2.27 |
| 1,600,000 | 64 | miss | 83.89 | 56.15 | 1.36 | 1.08–2.82 |
| 1,600,000 | 64 | replace | 591.33 | 300.32 | 1.84 | 1.67–2.53 |
| 1,600,000 | 64 | build | 60.92 | 42.17 | 1.44 | 1.36–1.52 |
| 1,600,000 | 256 | hash | 77.78 | 20.12 | 4.41 | 3.80–6.09 |
| 1,600,000 | 256 | hit | 2105.86 | 1083.37 | 1.90 | 1.59–2.07 |
| 1,600,000 | 256 | miss | 187.80 | 105.22 | 1.60 | 1.33–3.27 |
| 1,600,000 | 256 | replace | 637.76 | 638.19 | 1.00 | 0.87–1.50 |
| 1,600,000 | 256 | build | 106.68 | 74.55 | 1.44 | 1.28–1.63 |

## Reproduction

From the repository root, with no concurrent builds/tests/benchmarks:

```bash
set -o pipefail
taskset -c 0 cargo bench --manifest-path src/redisearch_rs/Cargo.toml \
  -p doc_key_map_ffi --bench hashers 2>&1 | tee /tmp/doc-key-hash-bench.log
```

The raw run log was `/tmp/doc-key-hash-bench.log`. The benchmark asserts hit IDs,
missing-key results, and post-replacement IDs for both fixtures outside timed regions.
