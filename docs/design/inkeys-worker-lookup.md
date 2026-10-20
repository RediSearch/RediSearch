# INKEYS resolution at execution time with an optional compact lookup table

Status: implementation under validation; workload benchmark results pending.

Related issue: [INKEYS worker/main-thread regression](https://github.com/RediSearch/RediSearch/issues/11935).
Historical change: [remove the DocTable key trie](https://github.com/RediSearch/RediSearch/pull/10561),
commit `f3efd437fcc91704e2ac5a697cf893a02775fd17`.

## Problem and scope

Memory-mode INKEYS used to resolve keys during query-node evaluation under the
spec read lock. Removing the DocTable trie moved resolution to admission on the
main thread because Redis key-metadata lookup requires the GIL. This serializes
lookup work and leaves a window in which reindexing invalidates the captured IDs
before execution begins.

Restore resolution against the index state used to construct the query's
iterators. Offer an optional per-index compact table to perform that resolution
on workers without the GIL.

Redis key metadata remains unchanged. It serves other features and is not a
candidate for replacement. Its memory cost is outside this change's accounting.
The proposed table is additional memory, with INKEYS as its only planned lookup
consumer. Existing metadata consumers continue using their current interfaces.

This plan targets memory mode. Disk mode retains its existing lookup mechanism;
any claim about correcting its snapshot behavior requires a separate audit of
the relationship between key metadata and the disk snapshot.

## Behavior contract

- Borrow binary-safe key arguments from the request's retained argv; do not copy
  their bytes or retain raw command-handler argv beyond its lifetime.
- Resolve memory-mode INKEYS when establishing execution state, not at admission.
  A completed reindex before that point must make its new doc ID eligible.
- Filter unknown keys, sort IDs, and deduplicate before constructing the existing
  sorted ID iterator. Preserve INKEYS parsing, errors, and duplicate semantics.
- Do not promise a transaction-wide snapshot or change existing expiration and
  result-loading behavior after iterator construction.
- Optional acceleration must produce the same results as the corrected metadata
  path. Disabling acceleration must not restore the admission/execution race.
- Do not change query wire formats, RDB/AOF formats, or replicated commands.

## Configuration and rollout

Propose a dedicated default-off, startup-only setting, provisionally named
`search-inkeys-lookup-map`. Final spelling and exposure require normal design
review. It selects an execution optimization, not a new query syntax.

The initial implementation uses one process-level choice:

| Setting | Memory-mode indexes | INKEYS resolution |
| --- | --- | --- |
| Off | No table allocation or population | Metadata under the GIL at execution handoff |
| On | Each index maintains a complete table | Table under the spec read lock |

This intentionally simple first version avoids partial population and runtime
transition states. An enabled process pays for every memory-mode index, even an
index that never receives INKEYS. Document that cost clearly. Operators should
enable it only for workloads where the measured benefit justifies the memory.
Disk indexes never allocate the table.

The shared ENABLE_UNSTABLE_FEATURES flag is not the proposed switch:
[the unstable-feature policy](../CONTRIBUTING-unstable-features.md) excludes bug
fixes, refactors, and performance changes. Use the normal review process for the
dedicated tuning option. Experimental rollout does not waive correctness tests.

Do not initially support runtime enable/disable, automatic first-query
population, or persistent per-index options. Runtime population would require
bounded backfill, concurrent mutation reconciliation, atomic publication, and
safe teardown. Building the whole table synchronously on the first INKEYS query
would introduce an unacceptable surprise latency.

If measurements show that process-wide allocation is too broad, revisit an
explicit per-index activation mechanism before shipping. That is a separate
design decision with lifecycle and persistence consequences, not an implicit
extension of this plan.

## Table representation and ownership

Use hashbrown HashTable behind a narrow Rust/C interface. Each table uses a
standard-library RandomState seed and SipHash; no custom probing algorithm is needed.

Each occupied bucket stores:

```text
key_hash: u64
doc_id:   u64
```

Control bytes describe bucket state. Store neither owned key bytes nor a DMD
pointer. The table must support multiple entries with the same full hash:
a plain map keyed only by u64 would incorrectly conflate different keys.

Lookup computes the key hash, visits matching candidates, borrows each candidate
DMD by ID, and compares its key length and bytes. The spec read lock protects
both the table and this access. No DMD or key reference escapes that protected
lookup. Verify the exact DMD borrowing contract in the implementation.

Use a seeded hash suitable for untrusted key names. Evaluate throughput and
collision resilience together; do not choose a predictable fast hash solely on
microbenchmarks. Tests need an injectable hash to force full-hash collisions.

Keep payload size, bucket capacity, control allocation, and resize behavior
observable. Use repository allocation integration for the Rust implementation
and account for actual allocated capacity rather than only live entries.

## Lifecycle and synchronization

Maintain the table within the same spec write-lock critical sections that publish
or remove searchable DMDs. Establish these invariants:

1. Every live searchable DMD has exactly one corresponding table entry.
2. Every entry resolves to a live DMD with the same key hash.
3. No entry survives removal of the DMD needed for collision comparison.
4. Readers never observe half of a replacement or rename.

Audit the mutation paths rather than updating only ordinary HSET indexing:

| Operation | Required maintenance |
| --- | --- |
| New indexed document | Insert before publishing the complete state to readers |
| Reindex/replacement | Remove old entry and publish new entry with the DMD change |
| Delete or logical de-index | Remove entry while the old key/DMD is still available |
| Rename | Remove using the old key/hash before changing the stored key; insert the new mapping |
| Expiry, eviction, trimming, move | Follow the actual document-removal/publication paths |
| Initial scan and legacy load | Populate alongside DMD reconstruction before queries see that state |
| Drop/recreate, flush, load failure | Free table with its owning spec; never reuse another incarnation's table |

Preserve existing locking and ownership rules for Redis metadata callbacks.
Do not take the GIL from a table operation or while holding the spec read lock.
Audit error paths so indexing failure cannot leave a table/DMD mismatch.
Debug assertions should enforce the locally checkable invariants.

No new serialized representation is needed: the table is derived from live
DMDs during normal index construction or load. Account explicitly for any
legacy load path that creates DMDs without ordinary indexing.

## Query execution

### Accelerated path

Admission records only the borrowed keys in the ID-filter AST node. Once the
worker holds the spec read lock, execution-plan construction resolves them
through the table immediately before node evaluation. The existing Rust evaluator
sorts/deduplicates the IDs and creates the ID iterator.

Deferral to node evaluation is sufficient; a new iterator that initializes on
its first Read is not required. The full resolved list is needed for ordering
and SkipTo, so resolving one argv key per Read would not preserve its contract.

Audit standalone, coordinator/shard, synchronous, cursor, hybrid if reachable,
and explain paths. In particular, explain currently expects pre-resolved IDs;
it must use the appropriate protected resolution path without changing output.
Cursor continuation must not rebuild the filter against a different index state.

### Corrected metadata fallback

Before taking the execution spec read lock, acquire the GIL using the worker's
valid thread-safe context. Establish the spec read lock while still holding the
GIL, resolve the key metadata, then release the GIL while retaining the spec lock
for iterator construction. Keep the same execution spec reference throughout.

This is a proposed lock sequence, not a verified implementation. Audit all
relevant lock acquisition paths and callbacks before adopting it. Never block
for the GIL while already holding the spec lock. If the sequence conflicts with
an existing invariant, redesign the handoff rather than release both locks and
reintroduce the update window.

Handle timeout, dropped index, cancellation, and construction errors with exactly
balanced locks and context ownership. Synchronous execution already has the GIL;
it must not acquire it recursively.

The fallback fixes consistency but does not remove GIL serialization. Its
performance is a separate baseline from both the admission-time implementation
and the accelerated path.

## Memory model

All numbers below assume 64-bit pointers and IDs. They are layout estimates,
not measured allocator usage or RSS.

For a table with 16-byte payloads and approximately one control byte per bucket:

```text
table bytes ~= 17 * bucket_count
bytes per live document ~= 17 / occupancy
```

At 1.6 million documents and 2^21 buckets, this is approximately 34 MiB, or
22.3 bytes per document. Capacity rounding, deletion tombstones, shrinking policy,
allocator rounding, and old/new tables coexisting during growth must be measured.

The historical trie used a 4-byte node header, a 16-byte optional raw pointer
payload, an 8-byte pointer plus one discriminator byte per child, and a separate
8-byte doc-ID allocation per document. Its actual raw-pointer payload does not
have the smaller optional-NonNull layout.

Giving labels zero cost, a node with c children occupies:

```text
align_up(4 + c, 8) + 8*c + 16
```

For T nodes and N documents, T >= N and there are T-1 edges. Structural allocation
is therefore at least 24*T + 8*(T-1) + 8*N >= 40*N-8 bytes, excluding the small
top-level object and allocator rounding. This is an optimistic floor, not a
claim that real keys achieve it.

| Representation or trie shape | Approximate bytes/document | At 1.6M documents |
| --- | --- | --- |
| Historical trie structural lower bound | 40 | 61.0 MiB |
| Full 255-way branching, documents at leaves, labels free | 41.1 | 62.8 MiB |
| Full binary branching, documents at leaves, labels free | 72 | 109.9 MiB |
| Proposed table with 2^21 buckets | 22.3 | 34.0 MiB |

Do not present this comparison as a memory saving against today's implementation:
today has no such module-side table. Report incremental bytes with acceleration
enabled. Keep metadata overhead out of both the benefit calculation and scope.

## Validation and acceptance

### Correctness

- Unit-test insert, lookup, replacement, deletion, growth, and collision chains.
- Force identical full hashes for different keys; delete one and verify the
  others remain accessible, including after a resize.
- Cover binary/embedded-NUL keys, empty keys where accepted, unknown keys,
  duplicates, and existing INKEYS count validation.
- Exercise every lifecycle row above, including initial/legacy load, rename into
  and out of an index prefix, index filters, and multiple indexes on one key.
- Use a deterministic pause before worker spec-lock acquisition: reindex an
  INKEYS document, resume execution, and assert that it remains eligible.
  Run with acceleration both off and on.
- Cover concurrent indexing and querying, queued timeouts, drop/recreate,
  cursor continuation, and explain output.
- Verify the off state allocates no table; disk mode remains unaffected.
- Run standalone and cluster flow coverage, Rust tests, C/FFI tests, formatting,
  lint, and the relevant sanitizers. Test configuration isolation between cases.

### Performance and memory

Compare four clearly identified builds/configurations:

1. Historical trie implementation.
2. Current admission-time metadata resolution.
3. Corrected execution-time metadata fallback.
4. Corrected execution-time compact table.

Use identical datasets and controlled runs; historical source differences must
be disclosed. Include representative production-like key names, short numeric
suffixes with common prefixes, and long/random keys. Start with the reported
1.6M-document workload, INKEYS sizes around 1, 100, and 900, and worker counts
0, 1, and 3, then scale where useful.

Measure throughput, tail latency, main-thread CPU, worker utilization, GIL wait/
hold time, indexing throughput, table steady-state and peak allocation, and
process memory deltas. Include read-only and concurrent-update workloads,
multiple indexes, and workloads without INKEYS.

Before enabling by default or widening rollout, require demonstrated improvement
for the affected workload, no correctness divergence between paths, quantified
write overhead, and an acceptable measured memory budget. Decide numerical
performance thresholds from the baseline; do not invent them in advance.

## Reviewable implementation tasks

- [ ] Audit execution/lock order, DMD lifetimes, all mutation paths, and legacy load.
      Finalize configuration semantics and table library/hash selection.
- [ ] Add a deterministic admission/execution race regression and correct the
      metadata fallback without gating the fix.
- [ ] Implement the compact table with allocation reporting and collision tests.
- [ ] Integrate optional ownership and all document lifecycle maintenance.
- [ ] Route accelerated INKEYS evaluation and explain through the protected table.
- [ ] Add configuration, lifecycle, concurrency, standalone, and cluster coverage.
- [ ] Run paired performance/memory experiments and record results and limitations.
- [ ] Obtain independent review before a PR; complete required validation and
      document rollout, operational memory cost, and release-note implications.

## Deferred alternatives

- GIL-free Redis key-metadata API: requires Redis keyspace and metadata lifetime
  synchronization, beyond this change.
- Owned-string hash map: unnecessary duplicate key ownership for this reader.
- Restoring the generic trie: retains the traversal cost motivating the issue.
- Runtime/per-index lazy activation: potentially useful to constrain memory,
  but requires a separate backfill/publication design.
- Removing metadata or redirecting other metadata consumers: explicitly out of scope.
