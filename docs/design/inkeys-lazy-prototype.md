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

The background task owns the traversal state, so indexes retain no iterator fields.
Its first nonempty-budget batch snapshots the bucket-array capacity. Within each
batch the spec lock pins the chain without per-document reference-count changes.
At the end it retains only the next DMD; at the start of the next batch it checks
that DMD for deletion and releases the retained reference. If deleted, traversal
restarts the current bucket because unlinking clears the DMD's chain link.
Completion and cancellation release any parked reference, even after index drop.

Array growth preserves bucket membership, and newly created buckets contain only
new documents maintained by the normal write path. Without cursor deletions,
traversal costs O(snapshot bucket capacity + visited chain entries), independent
of the historical maximum document ID and chain ordering. Cursor deletions can
cause rescans, but every batch still bounds documents and empty buckets visited.

Writes maintain the map from activation onward. During construction, deletion
and rename tolerate an entry not having been backfilled yet; rename inserts the
new key. Backfill uses idempotent insertion by hash and document ID to avoid
creating a second entry for a document already maintained by a writer. Full-hash
collisions between different IDs remain distinct. State, cursor, map, and DMD
mutations are all protected by the spec lock.

The builder reserves capacity for the current live-document count while processing
its first bucket. This runs in the background and avoids the large repeated
rehashes observed with incremental growth. Concurrent writes can still require
ordinary map growth if they exhaust that reservation.

The map remains allocated after activation, including when all documents are
deleted. Reload starts with an inactive map. The state adds 24 bytes per index on
the tested 64-bit build; indexes that never use INKEYS allocate no hash table or
private hash seed. Cached hashes are retained: the earlier hash-recomputation
experiment saved memory but imposed unacceptable resize cost.

## Accepted tradeoffs

- Batch size bounds traversal work, not wall-clock lock time. Allocation, page
  faults, scheduling, and concurrent growth prevent a strict latency guarantee.
- A sparse bucket array still requires visiting its allocated buckets, bounded
  by the document-table capacity rather than all historical IDs.
- A full reindex scan can occupy the sole pool thread until it finishes. Queries
  continue through the metadata/GIL fallback while activation waits.
- Fallback holds the GIL for all plan construction. Narrowing this would require
  a separate resolution phase or a different evaluation contract.
- Allocation follows the existing fail-fast policy. This change introduces no
  eviction policy or persistent map state.

The measured costs and procedure are recorded in the
[benchmark report](inkeys-lazy-benchmarks.md). They are workload measurements, not latency guarantees.

## Validation

Tests cover absent allocation, activation with WORKERS 0/2, queries while the
builder is parked, rename/delete/insert during construction, cursor deletion,
bucket-array growth, unordered chains, completion, queued reindex, timeout, index
drop, binary keys, inline/Lua/EXPLAIN, reload, legacy loading, and memory accounting.
Rust tests cover reservation with existing hash collisions and deduplication by ID.

The shutdown regression holds the builder's final strong reference until reindex
pool teardown explicitly signals it; it uses no wall-clock auto-release and checks
the server exit code. The profiling test deliberately holds the fallback GIL
interval open so admission-time accounting cannot hide a missing contribution.
