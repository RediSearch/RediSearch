# MOD-18033: INFO audit and first implementation

Date: 2026-09-22. Scope: the smaller design, retaining per-index aggregation and
caching expensive storage reads. Background caches are not implemented by this
change.

## Findings

| Path reached from `IndexesInfo_TotalInfo` | Actual work | Cache implementation action |
|---|---|---|
| Registry iteration and `pthread_rwlock_rdlock` | One blocking index read lock per index | Keep initially; measure contention under indexing/GC. Do not claim bounded latency. |
| `IndexSpec_collect_text_overhead` | `TrieType_MemUsage` computes an estimate from the stored trie size | Keep; no trie traversal or filesystem access. |
| `IndexSpec_collect_tags_overhead` | Field loop, then `TrieMap_MemUsage` reads the Rust trie's cached memory usage | Keep; O(fields), not O(tag values). |
| `IndexSpec_collect_numeric_overhead` | Field loop, lookup of existing numeric/geo index, fixed base-size accounting | Keep; no range-tree traversal. |
| `IndexSpec_GetVectorIndexesStats` | Field loop and `VecSimIndex_StatsInfo` | Keep the statistics API, not the debug-info API. The checked tiered HNSW path reads allocation/counters, not the graph; qualify other deployed vector backends in the scale test. |
| `IndexSpec_TotalMemUsage` disk branch | `SearchDisk_CollectIndexMetrics` collects properties across CFs | Replace only the INFO caller's disk collection with a published sample. Preserve FT.INFO behavior in this alternative. |
| `IndexSpec_TotalBlockCount` disk branch | Enterprise sums `estimate-num-keys` across text/tag CFs, with a tag-map read lock | Include block count in the INFO snapshot. This is an additional engine-property path beyond memory collection. |
| `GCContext_GetStats` | Disk GC reads atomic counters; fork GC copies statistics | Keep. |
| Activity, errors, document count, min/max | Scalar accounting/comparisons during the existing loop | Keep; no incremental global accounting rewrite justified yet. |
| `getDiskUsageCallback` | Index walk plus potentially synchronous per-index TTL refresh | Separate the INFO snapshot callback from quota enforcement as planned. |
| Enterprise `output_info_metrics` | Sums the existing per-index map | Keep initially; measure the loop rather than introducing a global snapshot immediately. |

Conclusion: after caching **all** storage-property paths, the retained work is
mostly field/index loops, scalar reads, and locks. The source audit supports the
smaller approach. It does not establish a latency bound, nor prove that every
engine property is free of I/O simply because it returns a number.

## Background ownership

The Enterprise database abstractions already provide most of the needed lifetime
machinery:

* `Database` requires `Send + Sync + 'static`.
* `IndexSpec::database()` clones the database handle.
* `RegisteredDatabase` wraps `Arc<OwnedDatabase>` and is cloneable.
* `OwnedDatabase::mark_for_deletion` defers DB destruction until the last owner
  drops. A worker's owned DB reference can therefore outlive index unregister.
* `owned_cf_handle` provides an owned CF guard, with the explicit requirement
  that guards die before the last DB reference.

Build a small metrics target with CF guards declared before its owned database
field. Prove the concrete target is `Send` at compile time, and use owned scalar
counter references where needed. Do not move or borrow the whole C/Rust index
object into the worker. Maintain CF membership at schema/CF lifecycle hooks.

Keep `MainThreadContext` and registration APIs on the main thread. Use generation
IDs to discard stale completion results, and separately test DB/CF retirement,
drop/recreate under the same name, fork, and shutdown. Existing Arc ownership
helps avoid a database lifetime rewrite; it does not replace those tests.

## First patch: section selection

Core now selects each ordinary INFO section before obtaining its data. Sections
that need index statistics share one lazily collected `TotalIndexesInfo` per
request. Fields, threading, and coordinator errors use existing global statistics
and need no index collection. Version/configuration-only requests avoid it too.

Disk-only requests still require collection in this first patch: Enterprise's
output currently reads the map populated by that collection. Core selects and
opens `disk`, primes the metrics once, then delegates field output. Enterprise's
matching change removes its second opening of the disk section. The function
table layout is unchanged, but its section-ownership contract changes: ship the
core and Enterprise changes together. Mixing old/new sides can duplicate a disk
header or emit fields into the wrong section.

Core files:

* `src/info/info_redis/info_redis.c`
* `src/search_disk_api.h`
* `tests/cpptests/test_cpp_info_sections.cpp`
* `tests/pytests/test_info_modules.py`

Enterprise files:

* `redisearch_disk/src/disk_context.rs`
* `redisearch_disk/tests/integration/disk_context.rs`

The Enterprise change lives in the separate checkout
`.worktree/mod-18033-enterprise`, on `guy-mod-18033-info-sections`, based on
Enterprise `origin/main`. Its original checkout and unrelated edits were left
intact. Integration must update Enterprise's `deps/RediSearch` to the matching
core revision before shipping.

## Next cache implementation

1. Flex: background physical scan/internal property cache; keep the DB loop over
   cached values and preserve the current directory boundary/formulas.
2. Enterprise: background per-index/CF metrics, including the block-count
   property; reuse owned database handles and existing metric structures.
3. Core: INFO-specific cached memory/block-count reads and the separate cached
   logical-usage callback. Keep enforcement and FT.INFO on their current paths.
4. Test blocked/failed refreshes and lifecycle races, then repeat management
   polling at 100/1,000 indexes and increasing SST counts. Measure the remaining
   loops, locks, and background CPU/I/O before expanding the implementation.

Before enabling asynchronous output, resolve the smaller design's cold-start
trade-off: zero placeholders with readiness metadata are compatible with numeric
consumers but can look like measured zero to legacy consumers. This first patch
does not introduce that behavior or any new freshness fields.

## Verification

* Debug build with assertions: passed.
* Full C/C++ suite: 1,091 passed, zero failures (including six new INFO dispatch
  tests using a fake disk backend to count actual collection calls).
* `test_info_modules` behavioral suite: 36 passed, zero failures, using the rebuilt
  module and the existing RedisJSON test module.
* Enterprise `integration` tests filtered to `disk_context::`: seven passed,
  zero failures. Empty/populated output tests now assert that the backend emits
  no additional section header.
* Changed-file whitespace checks and Enterprise Rust formatting: passed.

Enterprise tests reused existing local native libraries and dependency checkouts
through temporary links, removed after the run. The dependency revisions were
core `3c021835dab3282aa97ddbdf4a61a72a859df559`, rust-speedb
`3579cd20aec55f1a558ec8e9c73123a801477f80`, and speedb-ent
`4c680acb2256a2a1cb572bc458554a91cb983278`. This validates the changed Rust output
logic; it is not a full matched Flex build or a new scale measurement. There
were two existing unused-import warnings in the unrelated doc-table test module.

Run logs: `/tmp/mod-18033-build-tests.log`, `/tmp/mod-18033-unit.log`,
`/tmp/mod-18033-pytest-final.log`, and `/tmp/mod-18033-enterprise-tests.log`.
