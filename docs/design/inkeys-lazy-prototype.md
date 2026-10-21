# Lazy document key map: local prototype

This prototype branches from the always-on implementation in PR #11945. It is
local exploration, not a change to the published PR or its design decision.

## Activation and execution

A memory index starts with no map allocation or map maintenance. Its first INKEYS
request allocates an empty map under the spec write lock and schedules one task
on the existing single-threaded reindex pool, independent of query WORKERS.
Disk mode retains admission-time resolution and never activates a map.

The task visits at most 1,024 document IDs per invocation, taking the spec write
lock without the GIL. It then releases the lock and requeues at low priority.
Each invocation promotes a weak index reference; index deletion cancels the next
batch without retaining a dropped index for the whole build. Pool destruction
already drains queued work, including these continuations.

Until ready, INKEYS resolves through Redis document metadata during ordinary AST
evaluation. A query worker checks state under the spec read lock; when necessary,
it releases that lock, acquires the GIL, and reacquires the read lock. It holds the
GIL through execution-plan construction, then releases it before execution.
Inline and EXPLAIN execution already own the GIL. No separate AST traversal or
cached INKEYS node is introduced. Once ready, resolution uses the Rust map directly.

## Concurrent mutation rule

Construction snapshots maxDocId and maintains a next-ID cursor. Map maintenance
covers the completed prefix and IDs newer than the snapshot. Backfill reads the
current DMD for each remaining ID. This gives each ID one insertion owner, so
insert/delete/rename remain correct without idempotent map insertion or a per-DMD
build marker. State and mutation are protected by the spec lock.

The map remains allocated after activation, including when all documents are
deleted. Reload rebuilds documents with an inactive map; the next INKEYS activates
it again. The prototype adds 24 bytes of state per index on this 64-bit build;
indexes that never use INKEYS allocate no hash table or private hash seed.

## Complexity exposed by the prototype

- The scan visits historical IDs, not only live documents. Heavy churn can make
  it expensive, and repeated document-table bucket-chain searches can amplify the
  cost. A production version needs a better resumable traversal or measurements
  that justify this choice.
- A batch limits IDs visited, not elapsed lock time. Long document-table chains
  and a hash-table resize can exceed the intended time slice. The prior resize
  benchmarks still apply; a strict latency bound needs additional work.
- Existing reindex scans occupy the sole pool thread until their entire scan
  completes. Activation can wait behind initial indexing or ALTER; queries keep
  working through the GIL fallback during that wait.
- Fallback holds the GIL for all plan construction, which can be expensive for
  complex queries. Narrowing this requires a separate fallback-resolution phase
  or a different evaluation contract.
- Scheduling uses the existing fail-fast allocation policy. No new persistence
  format, user configuration, or eviction policy is added.

## Validation

Focused tests cover absent allocation, activation with WORKERS 0/2, queries while
construction is parked, concurrent rename/delete/insert, completion, queued
reindex, timeout, index drop, binary keys, inline/Lua/EXPLAIN, reload, legacy load,
and memory accounting. C tests force document-table bucket collisions while
mutating both sides of the build cursor. Performance and full disk-engine runtime
validation are outside this prototype's evidence.
