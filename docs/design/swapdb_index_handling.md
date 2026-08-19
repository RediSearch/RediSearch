# Design: Tracking `SWAPDB` in per-DB indexes

## Status

Implemented (`Indexes_SwapDb` / `onSwapDb` in `src/indexes.c`). Companion to the
per-DB index work in PR #10175 (indexes bound to `sp->dbid`;
`FT.CREATE`/query/notifications/`FLUSHDB`/`MOVE` all routed by DB). This document
records the design; the on-disk (SearchDisk/Flex) variant remains unverified
(see open questions).

## Problem

`SWAPDB id1 id2` swaps the entire contents of two logical databases in O(1) by
swapping the two `redisDb` structures. After the swap, every key that was in
`id1` is in `id2` and vice versa.

RediSearch indexes are now bound to a logical DB via `IndexSpec.dbid`, and that
binding is the sole source of truth for every DB-scoped behavior:

- registry lookup key `(dbid, name)` in `specDict_g`,
- keyspace-notification routing (`eventDb == spec->dbid` in
  `Indexes_FindMatchingSchemaRules`),
- background initial scan (`RedisModule_SelectDb(ctx, spec->dbid)`),
- `FT._LIST` / cross-DB visibility,
- `FLUSHDB` scoping (`Indexes_FreeByDb`).

`SWAPDB` moves the *documents* between DBs but leaves `sp->dbid` unchanged, so
after a swap an index points at a DB that no longer holds its documents:

- An index on `id1` keeps serving stale doc-ID→key mappings while its documents
  now live in `id2`; content loads on the index open keys in `id1` and miss.
- A write to `id2` (which now holds that index's documents) is routed to indexes
  bound to `id2`, not to the index that actually owns those documents.

The index is silently inconsistent with the keyspace until it is rebuilt
(`FT.DROPINDEX` + `FT.CREATE`, or an RDB reload that re-runs the initial scan).

## Key insight: the fix is metadata-only

An index's *content* is DB-independent. The inverted index, the numeric/tag/
vector indexes, and the doc table all key on the **document key name** and an
internal **doc-ID**, never on a DB number. `SWAPDB` does not rename keys — it
only changes which DB a given key name resides in. So none of the indexed data
needs to move or be rebuilt.

Everything that *is* DB-specific is derived from `sp->dbid` at runtime. Therefore
the only state that must change on `SWAPDB id1 id2` is the binding itself:

> For every spec on `id1`, set `dbid = id2`; for every spec on `id2`, set
> `dbid = id1`; and re-key those specs in `specDict_g` accordingly.

This is exactly the "switch the dbid and move it in `specDict`" idea — it is
sufficient, with one ordering subtlety (below).

## The hook

Mirror `onFlush` (`src/indexes.c`), which already subscribes to a server event
for `FLUSHDB`/`FLUSHALL`:

```c
RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_SwapDB, onSwapDb);
```

The callback receives a `RedisModuleSwapDbInfo` (`src/redismodule.h`):

```c
typedef struct RedisModuleSwapDbInfo {
    uint64_t version;
    int32_t dbnum_first;   /* one of the swapped DBs  */
    int32_t dbnum_second;  /* the other swapped DB    */
} RedisModuleSwapDbInfoV1;
```

The event fires synchronously under the GIL as part of `SWAPDB`, on the main
thread — the same execution context as `onFlush` — so it can mutate the global
registry directly without extra locking. It is delivered on both primary and
replica (the replica replays the `SWAPDB`), and on AOF load, which keeps all
copies deterministic. There is a single subevent today; we should still guard on
it defensively (as `onFlush` does for `REDISMODULE_SUBEVENT_FLUSHDB_START`).

## Algorithm

```c
static void onSwapDb(RedisModuleCtx *ctx, RedisModuleEvent eid,
                     uint64_t subevent, void *data) {
  const RedisModuleSwapDbInfo *si = data;
  if (!si) return;
  Indexes_SwapDb(si->dbnum_first, si->dbnum_second);
}
```

```c
// Re-bind every index on db_a to db_b and vice versa. Metadata only: the
// indexed data is keyed by document name/doc-ID and does not move.
void Indexes_SwapDb(int db_a, int db_b) {
  if (db_a == db_b || !specDict_g || dictSize(specDict_g) == 0) return;

  // Phase 1: collect the affected specs. We must NOT re-key while iterating,
  // and we must remove all old entries before adding any new one (see below).
  arrayof(StrongRef) affected = array_new(StrongRef, 8);
  dictIterator *it = dictGetIterator(specDict_g);
  for (dictEntry *e; (e = dictNext(it)); ) {
    StrongRef ref = dictGetRef(e);
    IndexSpec *sp = StrongRef_Get(ref);
    if (sp && (sp->dbid == db_a || sp->dbid == db_b)) {
      array_append(affected, ref);
    }
  }
  dictReleaseIterator(it);

  // Phase 2: detach all affected specs from specDict_g under their OLD key.
  // valDestructor is NULL, so dictDelete frees only the (dbid,name) key and
  // leaves the RefManager value intact.
  for (size_t i = 0; i < array_len(affected); i++) {
    IndexSpec *sp = StrongRef_Get(affected[i]);
    dictDelete(specDict_g, DB_SPEC_KEY(sp->dbid, sp->specName));
  }

  // Phase 3: flip dbid and re-insert under the NEW key.
  for (size_t i = 0; i < array_len(affected); i++) {
    StrongRef ref = affected[i];
    IndexSpec *sp = StrongRef_Get(ref);
    sp->dbid = (sp->dbid == db_a) ? db_b : db_a;
    dictAdd(specDict_g, DB_SPEC_KEY(sp->dbid, sp->specName), ref.rm);
  }
  array_free(affected);
}
```

### Why three phases (the ordering subtlety)

The registry key is `(dbid, name)`. If `id1` and `id2` each hold an index named
`idx`, re-keying one at a time would transiently create a duplicate key: moving
`id1/idx` to `(id2, idx)` collides with the still-present `id2/idx`. Removing
**all** affected entries first, then re-adding, avoids any transient collision —
the swap of a same-named pair is itself a clean permutation. Collecting refs in
phase 1 also avoids mutating the dict while iterating it.

## What does *not* change

- **`specIdDict_g`** — keyed by the globally-unique `specId`, not by DB. The
  `RefManager`/`StrongRef` objects are untouched; only `specDict_g` keys move.
- **`SchemaPrefixes_g`** — maps prefix → spec refs and is consulted *before* the
  `eventDb == sp->dbid` filter, so the refs stay valid and routing self-corrects
  from the updated `dbid`.
- **Aliases** — the global alias table points at the spec object (a `StrongRef`),
  not at a DB. After the swap, `Indexes_LoadIndexSpecUnsafeEx`'s alias path still
  filters by `sp->dbid != options->db`, which now reflects the new binding.
- **Inverted index / doc table / numeric / tag / vector data** — keyed by
  document name and doc-ID; DB-independent.
- **GC** (fork and tiered) — operates on in-memory structures with no DB
  selection; DB-agnostic by construction.
- **RDB** — persists the final `dbid` per index; `SWAPDB` is not itself
  persisted, so a reload after a swap restores the already-swapped bindings.

## Edge cases & open questions

1. **In-flight background scan.** A single-index scan selects `spec->dbid` on its
   detached context *once* at scan start (`indexes_scan.c`). If `SWAPDB` flips
   `dbid` mid-scan, the scanner keeps reading the old DB and indexes the wrong
   keyspace. Options: (a) have the scanner re-read `spec->dbid` per `RM_Scan`
   batch and abort/restart on change; (b) cancel and reschedule scans for
   affected specs from `Indexes_SwapDb`. Restart (b) is simplest and matches how
   other lifecycle events invalidate a scan. **Implemented as (b).**

   *Idempotency note:* the restart re-scans from scratch and re-indexes keys the
   cancelled scan had already processed. This is correct because each doc is
   submitted with `DOCUMENT_ADD_REPLACE` (`IndexSpec_UpdateDoc`), which replaces
   an existing same-key doc rather than duplicating it - the same idempotency the
   existing `IndexesScanner_New` cancel-and-restart (e.g. on `FT.ALTER`) already
   relies on. The only cost is re-processing the already-scanned subset (extra CPU
   plus transient GC garbage from the replaced docIds), never wrong/duplicate
   results.

2. **Open cursors.** A cursor created by `FT.AGGREGATE ... WITHCURSOR` holds a
   reference to the spec; `FT.CURSOR READ` re-resolves by name on the caller's
   selected DB. After a swap, a client on the new DB resolves the index fine, but
   a client still on the old DB no longer finds it. This matches `MOVE`/drop
   semantics (a cursor's index can vanish from a DB) and is acceptable; document
   it. Confirm no use-after-free — the cursor's own ref keeps the spec alive.

3. **SearchDisk / Flex (on-disk indexes).** No `dbid` appears in disk paths or
   file names, and on-disk doc deletion is key-name based via `keyMetaOnUnlink`,
   so a metadata-only swap is *likely* sufficient. **Verify** that the disk
   key-meta layer is not separately keyed by DB before relying on this; if it is,
   `SWAPDB` on disk-backed indexes may need extra work or an explicit
   unsupported-error.

4. **Self-swap (`id1 == id2`).** No-op; guarded at the top of `Indexes_SwapDb`.

5. **One-sided swap.** If only one of the two DBs has indexes, the algorithm
   still works — the empty side contributes nothing and the populated side's
   specs simply re-bind to the other DB number.

6. **Cluster.** Cluster is single-logical-DB and `SWAPDB` is not used; the event
   either does not fire or is a no-op. No special handling needed, but the
   callback should be a no-op when `db_a`/`db_b` carry nothing.

7. **Replication / AOF ordering.** Because the swap is metadata-only and driven
   by the same `SWAPDB` the replica/AOF replays, primary and replica converge
   without propagating anything extra from the module.

## Testing plan

End-to-end (`tests/pytests/test_index_db_scoping.py`, `@skip(cluster=True)`):

- **Swap moves the index with its data.** Index `idxA` on DB 1 with docs, no
  index on DB 2. `SWAPDB 1 2`. Assert the index is now visible/queryable on DB 2
  and gone from DB 1, returning the same documents (content loads succeed on
  DB 2).
- **Two indexes swap symmetrically.** `idx` on DB 1 and a different-schema `idx`
  on DB 2 (same name). `SWAPDB 1 2`. Assert each name now resolves on the other
  DB to its original documents — exercises the three-phase same-name re-key.
- **Notifications follow the swap.** After the swap, an `HSET`/`DEL` on DB 2 is
  reflected by the index that moved there (and not by anything on DB 1).
- **Scan races (if restart is chosen).** Create with pending scan, swap, assert
  the scan completes against the correct DB.
- **RDB round-trip after swap.** `SWAPDB`, then `dumpAndReload`, assert bindings
  persist.
- **Disk-backed variant** of the first test if SearchDisk support is confirmed.

C unit coverage for `Indexes_SwapDb` re-keying (including the same-name pair) in
`tests/cpptests` is also warranted.

## Summary

The user's hypothesis is correct: handling `SWAPDB` is fundamentally just
"flip `sp->dbid` for the two DBs and re-key `specDict_g`," because every DB-scoped
behavior derives from `sp->dbid` at runtime and the indexed data is
DB-independent. The only non-trivial pieces are (1) doing the re-key in three
phases to avoid a transient same-name key collision, (2) invalidating/restarting
any in-flight initial scan for the affected specs, and (3) confirming the
on-disk (SearchDisk) layer is not separately DB-keyed.
