# Lazy document key map

This change builds on PR #11945 and replaces its eager map allocation with
activation on first INKEYS use. Indexes that never use INKEYS retain Redis document
metadata for other consumers but allocate and maintain no additional key map.
No configuration, command syntax, or persistence format changes.

## Activation and execution

A memory index starts inactive. Its first INKEYS request allocates an empty map
under the spec write lock and schedules work on the existing single-threaded
reindex pool, independently of query WORKERS. An empty index becomes ready
immediately. Disk mode retains admission-time resolution and never activates a map.

Each task promotes a weak index reference, takes the spec write lock without the
GIL, and visits at most 1,024 documents or bucket boundaries. It then unlocks,
releases its strong reference, and requeues at low priority if work remains. Index
deletion cancels subsequent batches. Shutdown drains the reindex pool before the
cleanup pool because a finishing batch can release the last index reference and
enqueue its destruction.

Until ready, INKEYS resolves through Redis document metadata during ordinary AST
evaluation. A query worker checks state under the spec read lock; when necessary,
it releases that lock, acquires the GIL, and reacquires the read lock. It holds the
GIL through execution-plan construction, includes that interval in FT.PROFILE's
GIL time, and releases it before execution. Inline and EXPLAIN execution already
own the GIL. There is no separate AST traversal or cached INKEYS node. Once ready,
resolution uses the Rust map directly.

Metadata lookup uses NOEFFECTS, which Redis defines to include NOEXPIRE. An expired
key is not unlinked while the caller holds the spec read lock. A regression test
covers SEARCH and EXPLAIN with active expiry disabled and checks that the expired
key remains physically present after fallback resolution.

## Backfill and concurrent mutation

The task owns the cursor and retains its parked DMD across batches. Deletion cannot
free that DMD while it is retained; restarting the bucket avoids following its
cleared chain link. The spec lock protects traversal and mutations, and task cleanup
releases the retained reference even after index drop. See
[DocTable_BuildKeyMapBatch](../../src/doc_table.c) for traversal mechanics.

Writes maintain the map from activation onward, including documents added beyond
the initial bucket snapshot. Rename and deletion tolerate entries not yet
backfilled; idempotent insertion prevents duplicates when backfill revisits a
document already maintained by a writer. Full-hash collisions retain distinct IDs.

The builder reserves capacity for the current live-document count while processing
its first bucket. This runs in the background and avoids the large repeated
rehashes observed with incremental growth. Concurrent writes can still require
ordinary map growth if they exhaust that reservation.

The map remains allocated after activation, including when all documents are
deleted. Reload starts with an inactive map. The lazy state adds 8 bytes per index on
the tested 64-bit build; indexes that never use INKEYS allocate no hash table or
private hash seed. Cached hashes are retained: the earlier hash-recomputation
experiment saved memory but imposed unacceptable resize cost.

## Accepted tradeoffs

- Batch size bounds traversal work, not wall-clock lock time. Allocation, page
  faults, scheduling, and concurrent growth prevent a strict latency guarantee.
- A sparse bucket array still requires visiting its allocated buckets, bounded
  by the document-table capacity rather than all historical IDs.
- Low-priority continuation jobs bound queue growth, but do not throttle CPU or
  spec-lock duty cycle. Timer-based pacing would trade slower activation for lower duty cycle.
- A full reindex scan can occupy the sole pool thread until it finishes. Queries
  continue through the metadata/GIL fallback while activation waits.
- Fallback holds the GIL for all plan construction. Narrowing this would require
  a separate resolution phase or a different evaluation contract.
- Allocation follows the existing fail-fast policy. This change introduces no
  eviction policy or persistent map state.

The measured costs and procedure are recorded in the
[benchmark report](inkeys-lazy-benchmarks.md). They are workload measurements, not latency guarantees.

## Validation

[INKEYS flow tests](../../tests/pytests/test_inkeys.py) cover query behavior across
activation, mutation, reload, and teardown, including fallback profiling.
[Document-table tests](../../tests/cpptests/test_cpp_index.cpp) cover cursor
lifetime and mutation safety; [legacy-load tests](../../tests/cpptests/test_cpp_rdb.cpp)
cover reconstruction. Rust [map tests](../../src/redisearch_rs/c_entrypoint/doc_key_map_ffi/src/lib.rs)
and [FFI tests](../../src/redisearch_rs/c_entrypoint/doc_key_map_ffi/tests/integration.rs)
cover collisions, idempotency, and borrowed binary keys.
