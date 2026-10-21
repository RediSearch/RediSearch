# INKEYS resolution at execution time

Related issue: [INKEYS worker/main-thread regression](https://github.com/RediSearch/RediSearch/issues/11935).
Historical change: [remove the DocTable key trie](https://github.com/RediSearch/RediSearch/pull/10561).

## Problem and scope

Removing the DocTable trie moved INKEYS resolution to admission on the main
thread because Redis key-metadata lookup requires the GIL. This serializes lookup
work and leaves a window in which reindexing invalidates the captured IDs before
execution begins.

Every in-memory index now maintains a compact document key map. INKEYS resolves
against the index state used to construct its iterators, under the spec read
lock, without acquiring the GIL. The map is always enabled for in-memory indexes;
there is no new configuration or FT.CREATE option. Indexes that never use
INKEYS still pay its storage and maintenance cost.

Disk indexes never allocate the map. Their existing main-thread metadata
resolution remains unchanged, including for documents whose contents must be
fetched from disk. This change does not claim to alter disk snapshot semantics.

Redis per-key metadata remains unchanged because other features use it. Its
memory cost is outside this fix's accounting. DocKeyMap describes the generic
key-to-document-ID capability; INKEYS is its initial consumer.

## Behavior and ownership

- Borrow binary-safe key arguments from the request's retained argv; do not copy
  their bytes or retain command-handler argv beyond its lifetime.
- Resolve in-memory keys during normal ID-node evaluation, after acquiring the
  execution spec read lock. A completed reindex before that point contributes
  its new document ID.
- Filter unknown keys, sort IDs, and deduplicate before constructing the existing
  sorted ID iterator. Parsing, duplicate semantics, and errors are unchanged.
- Preserve existing expiration and result-loading behavior after iterator
  construction; do not promise a transaction-wide snapshot.
- Preserve query wire formats, RDB/AOF formats, and replicated commands.

The AST carries no cached INKEYS-node pointer and needs no separate resolution
walk. Inline execution uses the same node evaluator as workers. EXPLAIN resolves
in-memory IDs while printing the ID node during its existing AST walk, under the
spec lock; disk nodes already contain IDs. Cursor continuation retains its
constructed iterator rather than resolving a new filter.

Deferring to node evaluation is sufficient. Resolving one argv key per iterator
Read would not preserve ordering and SkipTo, which need the full sorted ID list.

## Table representation and synchronization

A Rust hashbrown HashTable stores a 64-bit hash and 64-bit document ID in each
occupied bucket, with control bytes describing bucket state. It owns neither
key bytes nor DMD pointers. Each table uses private randomized ahash seeds;
local testing found AHash faster than the standard-library hasher.

Full-hash collisions remain separate entries. Lookup hashes the borrowed query
key, visits candidates, and compares key length and bytes through the candidate
DMD. The spec read lock pins the table and DMD keys; no borrowed reference escapes
lookup. Rust node evaluation probes the map directly, calling C only for candidate
comparison. C consumers use DocTable_GetIdByKey.

Table maintenance shares the spec write-lock critical sections that publish or
remove searchable DMDs. Every live searchable DMD has one entry, every entry
resolves to the matching live DMD, and readers cannot observe partial replacement
or rename. Removal occurs while the old key is still available. Table operations
do not acquire the GIL.

| Operation | Maintenance |
| --- | --- |
| Insert and reindex | Publish the entry with the new DMD; remove replaced IDs |
| Delete, expiry, eviction, trim, move | Remove alongside the DMD |
| Rename | Remove before changing the DMD key, then insert its new hash |
| Initial scan and legacy load | Populate alongside DMD reconstruction |
| Drop, flush, failed load | Free with the owning DocTable |

The table is derived state, rebuilt during indexing or load rather than serialized.
Its allocated bucket/control capacity and object size contribute to
`key_table_size_mb` and total index memory. Redis metadata remains a separate cost.

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
| Document key map with 2^21 buckets | 22.3 | 34.0 MiB |

Do not present this comparison as a memory saving against today's implementation:
today has no such module-side table. Report the map as incremental index memory. Keep metadata overhead out of both the benefit calculation and scope.

## Benchmark decision: retain the stored hash

Tested both the current `{hash, id}` representation and an ID-only variant that
recomputed hashes from DMD keys when hashbrown resized. Both used ahash and the
same enabled map. Three alternating-order repetitions covered 1.6 million
documents, 900-key queries with workers 0/3, updates, bounded delete/insert churn,
a non-INKEYS control, and 64/256-byte keys. Fresh ingestion crossed exact growth
boundaries up to one million documents; separate flame graphs covered lookup and
growth. The experimental variant passed collision/growth tests and live lifecycle
checks. All 60 timed cases and 12 ingestion runs completed without result errors.

| Metric | Stored hash | Recompute |
| --- | ---: | ---: |
| Logical table storage at 1.6M documents | 34 MiB | 18 MiB |
| 900-key query throughput, workers 0 | 1,713 QPS | 1,671 QPS |
| 900-key query throughput, workers 3 | 4,997 QPS | 5,017 QPS |
| Growth insertion at 917,504 documents, 64-byte keys | 18.9 ms | 404 ms |
| Same growth insertion, 256-byte keys | 18.5 ms | 488 ms |

Values are three-run medians. ID-only saved about 20 MiB of actual Redis
allocation at 1.6M documents, while ingestion CPU/document increased by roughly
6%. Steady updates and churn were close. Shared-host variability limits conclusions
about small throughput or tail-latency differences, but the growth-pause gap was
consistent across repetitions and key lengths. Growth was measured on the main
thread with workers disabled; worker query execution does not make resizing
incremental.

The six CPU profiles support the cause: recomputation's growth cost is dominated
by DMD lookup and key access, rather than ahash arithmetic. The largest measured
resize takes 21–26 times longer. The memory saving is **not worth that synchronous
pause today**, so the always-on map retains its stored hash. Revisiting ID-only
entries would require addressing growth latency as a separate design problem.
The benchmark variant is not part of the implementation.

## Validation and alternatives

Coverage includes forced full-hash collisions across growth/removal, binary and
empty keys, duplicates and unknown keys, queued reindex before spec-lock acquisition,
inline/Lua execution, EXPLAIN, timeout callbacks, initial scan, rename, deletion,
expiry, filtered/multiple indexes, reload, legacy load, allocation accounting, and
disk exclusion. Run C/Rust tests, standalone and cluster flow tests, formatting,
lint, and CI for the final implementation. A disk-exclusion unit test does not
substitute for full disk-engine runtime validation.

Alternatives intentionally left out:

- GIL-free Redis metadata lookup requires separate keyspace/lifetime synchronization.
- An owned-string map adds unnecessary duplicate ownership.
- Restoring the generic trie retains its traversal and structural-memory costs.
- Startup, unstable, or per-index switches add rollout/API states without addressing
  the underlying fix. Runtime lazy activation would also require bounded backfill
  and atomic publication; synchronous first-query population is unsuitable.
- Removing metadata or redirecting its other consumers is outside this change.
