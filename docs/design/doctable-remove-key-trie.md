# Design: Remove the DocTable key trie — use DocIdMeta for key→docId in both modes

Status: **Implemented — under test**
Branch: `jk-doctable-remove-key-trie`
Author: Jonathan Keinan

## Implementation notes (as built)

These record where the implementation refined the plan below:

- **INKEYS pre-resolution is now unconditional** (`applyGlobalFilters`,
  `aggregate_request.c`). key→docId resolution opens the Redis key
  (`DocIdMeta_Get`), which requires the GIL, so it must not run on a background
  query-execution thread. INKEYS keys are therefore resolved to docIds on the
  main thread during query construction in **both** modes (previously disk-only),
  and the query evaluator (C `QueryNode_DumpExplain`, Rust `eval_ids`) only ever
  consumes pre-resolved docIds. The Rust `eval_ids` key-lookup path and its C
  `DocTable_GetId` FFI binding were removed. This was the one key→docId site that
  was *not* already main-thread/GIL, so pre-resolving closes that gap.
- **`DocIdMeta_Init` gates on `SearchDisk_IsEnabledForValidation()`** (not
  `IsEnabled()`) so unit tests that set `RSGlobalConfig.simulateInFlex` register
  the rdb_save/rdb_load callbacks. In production `simulateInFlex` is false, so
  this equals `IsEnabled()`.
- **`IndexSpec_DeleteDoc_Unsafe` is the logical de-index path** (key still
  exists but should no longer be indexed: field change, filter no longer
  matches, REPLACE cleanup). It is unified across modes: resolve docId via
  DocIdMeta, delete (DocTable/disk), then `dropDocIdMeta`. Physical key removal
  goes through the `unlink` callback → `IndexSpec_DeleteDocById`.
- **Q1 (legacy RDB) — resolved:** `DocTable_LegacyRdbLoad` no longer populates a
  key→docId structure. During `LOADING_ENDED`, `Indexes_UpgradeLegacyIndexes()`
  drops the old legacy index keyspace state, frees the loaded DocTable, resets
  stats, and only then exposes the upgraded spec for the full keyspace scan. The
  scan repopulates DocIdMeta from live Redis keys.
- **Q3 (FT.GET/FT.MGET) — resolved:** accepted the key-meta lookup; those
  commands open the key anyway to read fields, so the marginal cost is one
  `GetKeyMeta`.
- **INFO/stats:** `key_table_size`(`_mb`) now report 0 (no in-memory trie; the
  mapping lives in Redis key-metadata, not module-tracked memory).
- **DocIdMeta reclamation in memory mode (MOD-17109 review).** Disk mode GCs
  stale DocIdMeta entries through its RDB save/load cycle (the save skips
  entries whose specId is no longer live, and skips a key whose dict is empty).
  Memory mode registers no rdb_save/load, so two paths that leave the Redis key
  in place needed explicit reclamation. `specId` is monotonic and never reused
  (`nextSpecId_g++`), so a stale entry is always inert — it can never alias a
  live index — which makes deferred/partial reclamation safe by construction.
  - *Logical de-index* (surviving key stops matching: FILTER change, rename out
    of prefix, REPLACE) — `DocIdMeta_DeleteWithOpenKey` now detaches
    (`SetKeyMeta(key, 0)`) and frees the per-key dict when its last entry is
    removed, so a de-indexed-but-surviving key retains no empty allocation.
    `SetKeyMeta` does not free the previous value and Redis skips the free
    callback once `meta == reset_value(0)`, so the order is detach-then-free.
  - *KEEPDOCS drop* (the `FT.DROPINDEX` default: index gone, keys kept) —
    nothing else ever removes the dropped spec's entries. `DropIndexCommand`
    marks `sp->pruneKeyMetaOnFree`, and background teardown
    (`IndexSpec_PruneDocIdMeta`, on the `cleanPool` worker before `DocTable_Free`)
    walks the still-intact DocTable and drops this spec's entry from each key,
    reusing the retained `DMD->keyPtr` as the key list. It opens keys under the
    GIL in bounded batches (`DOCID_META_PRUNE_GIL_BATCH`) — the concurrent-search
    / fork-GC yield pattern — so `FT.DROPINDEX` itself stays O(1) on the main
    thread. A delete-docs drop leaves the flag clear (those keys and their
    metadata are removed as the keys are deleted). Disk mode skips this.
  - *Caveat — `_FREE_RESOURCE_ON_THREAD false`:* the eager KEEPDOCS-drop prune
    runs only on the background (`cleanPool`) free path, which needs no GIL held.
    Under the non-default `freeResourcesThread=false` (spec freed inline on an
    unknown thread), the prune is skipped; those entries are reclaimed lazily
    when their keys are eventually deleted (safe — the entries are inert).

### Verification (local, community build)

- Build: green (C + Rust, `redisearch.so`).
- C/C++ unit tests: **967 passed, 0 failed** (DocIdMeta RDB tests run under
  `simulateInFlex`).
- Rust: `query_eval` **49 passed** (IDS test fixtures updated to carry
  pre-resolved docIds).
- Python flow (memory mode): `test_followhashes` (31), `test_expire` (36),
  `test_index` (4), `test_parser` (20), `test_if` (2), `test_rdb_load` (2) — all
  passed. `test_parser` initially surfaced a crash: a stale
  `RS_ASSERT(SearchDisk_IsEnabled())` in the FT.EXPLAIN IDS dump fired once RAM
  also pre-resolved docIds; removed.
- **Flex/disk mode not validated locally** (needs the closed-source enterprise
  disk build). Disk-path changes are behavior-preserving (disk already used
  DocIdMeta + `unlink`); validate in CI.
- **Reclamation follow-up (MOD-17109 review):** `test_cpp_doc_id_meta` extended
  with free-on-empty coverage (`TestDeleteLastEntryDetachesMeta`,
  `TestDeleteNonLastEntryKeepsMeta`, `TestSetAfterLastDeleteRecreatesMeta`) — 28
  DocIdMeta tests pass, no double-free at teardown. `query_eval` still 49 passed
  (the two `optional.rs` IDS fixtures now carry pre-resolved docIds). The
  `key_table_size_mb` expectations that assumed a non-zero trie were updated to 0
  (`test.py::testInfoCommand`, `test_coordinator.py::testInfo`,
  `test_issues.py::testMemAllocated`, `test_resp3.py`).

## 1. Summary

Today the in-memory `DocTable` keeps a `DocIdMap dim` (a `TrieMap`) that maps a
document's **key string → internal docId**. This trie is used *only* in RAM
mode. In disk (Flex) mode the same mapping already lives on the Redis key as
key-metadata via **`DocIdMeta`** (`src/doc_id_meta.c`), and the DocTable trie is
allocated but never populated or read.

This change **removes the `dim` trie entirely** and routes key→docId through
`DocIdMeta` in **both** modes. The `DocTable` collapses to a pure
**docId → `RSDocumentMetadata` (DMD)** store: the in-memory bucket/chain array
stays for RAM (disk continues to fetch DMDs from disk via
`SearchDisk_GetDocumentMetadata`), but the key→docId direction becomes uniform.

Deletion is **unified on the `DocIdMeta` `unlink` callback** in both modes
(the pattern disk already uses), retiring the RAM-only keyspace-notification
delete path.

`DocIdMeta_Init` is called in **both** modes, but the key-meta **callback set is
conditional**: the `rdb_save`/`rdb_load` callbacks (and `unlink`) are registered
only where they are correct. RAM must **not** persist the mapping — a RAM RDB
load rebuilds the index by re-scanning the keyspace and re-indexing, which
re-assigns docIds and repopulates `DocIdMeta` from scratch, so a saved mapping
would be stale.

## 2. Motivation

- **Memory:** the per-index `TrieMap` (`dim`) is pure overhead. In disk mode it
  is already dead weight (allocated, never used). In RAM mode it duplicates
  information now available on the key.
- **Uniformity:** RAM and disk currently have two divergent key↔docId
  mechanisms and two delete paths. Collapsing to one (`DocIdMeta` + `unlink`)
  removes a whole class of "RAM branch vs disk branch" forks scattered across
  the indexer, delete, rename, TTL, FT.GET/FT.MGET and debug paths.

## 3. Current architecture (baseline)

Disk mode is a global, load-time setting (`SearchDisk_IsEnabled()` → `isFlex`);
every spec is all-disk or all-RAM. `isSpecOnDisk(sp)` just wraps it.

| Concern | RAM mode (today) | Disk mode (today) |
|---|---|---|
| key → docId | `DocTable.dim` trie (`DocTable_GetId` → `DocIdMap_Get`) | `DocIdMeta` on the Redis key |
| docId → DMD | in-memory `buckets`/chain (`DocTable_Borrow`) | `SearchDisk_GetDocumentMetadata` |
| add / assign id | `makeDocumentId` → `DocTable_Put` (buckets **and** trie) | `SearchDisk_PutDocument` + `DocIdMeta_Set` |
| delete on key drop | `del`/`set`/`change` **notification** → `IndexSpec_DeleteDoc` → `DocTable_PopR` | `DocIdMeta` **`unlink`** callback → `IndexSpec_DeleteDocById` |
| rename | `DocTable_Replace` (trie + `keyPtr`) | `DocIdMeta_Get` + `SearchDisk_ReplaceKey` |
| RDB of the mapping | not persisted; rebuilt by re-scan/re-index on load | `DocIdMeta` key-meta `rdb_save`/`rdb_load` (gated by `ForgetDocIdMetadata`) |

Key facts confirmed while scoping (file:line at baseline `20b0cd6e3`):

- `DocIdMeta_Init` is called **only** under `SearchDisk_IsEnabled()` —
  `src/module.c:4881`.
- The modern RDB path (`IndexSpec_RdbSave`, `src/spec.c:2940`) never serializes
  the DocTable. `DocTable_LegacyRdbLoad` is the only DocTable (de)serializer and
  its caller asserts `!SearchDisk_IsEnabled()` (`src/spec.c:3179`).
- All RAM key→docId call sites run on the **main thread** or under the **GIL**
  (indexer `doAssignIds` holds the GIL — `src/indexer.c:770`). The multithreaded
  FT.SEARCH path never does key→docId; it goes docId→DMD via buckets. So moving
  key→docId to `DocIdMeta` (which opens a Redis key) adds **no** new GIL exposure
  on the query hot path.
- Deletion notification handler already separates the two worlds:
  `del`/`set` (`src/notifications.c:276`) and the empty-key branch of `change`
  (`:306`) skip the notification delete when `SearchDisk_IsEnabled()` because
  "Deletion handled by keyMetaOnUnlink callback"; `expired`/`evicted`/`trimmed`
  (`:330`) `RS_ASSERT(!SearchDisk_IsEnabled())` — disk does not subscribe to them.
- **`unlink` fires on expiration and eviction (established).** The keyspace
  subscription (`Initialize_KeyspaceNotifications`, `src/notifications.c:731`)
  registers *different* flag sets per mode: disk omits `EXPIRED`/`EVICTED`/
  `TRIMMED` and all delete-causing handlers, with the comment "On Disk we do not
  listen to notifications that lead to deleting the keys as the unlink callback
  of DocIDMeta will handle it" (`:736`). Since disk has **no other** de-index
  path for expired/evicted keys and does not leak them, the key-meta `unlink`
  callback provably fires on active expiry, lazy expiry, and maxmemory eviction.
  This is the evidence that unifying RAM onto `unlink` is safe (resolves Q2).
- On RAM load, `LOADING_ENDED` → `Indexes_EndRDBLoadingEvent` (`src/notifications.c:1112`)
  drives the background re-scan/re-index that rebuilds `spec->docs`.

## 4. Target design

### 4.1 DocTable becomes a docId→DMD store

- Delete the `DocIdMap dim` field from `DocTable` and the `DocIdMap` type +
  `DocIdMap_*` / `NewDocIdMap` helpers from `doc_table.{h,c}`.
- `DocTable_Put` keeps: assign `docId = ++maxDocId`, allocate the DMD (storing
  `keyPtr` for the docId→key direction), insert into `buckets`. It **drops** the
  trie insert and the "already present" trie check. The duplicate/REPLACE check
  moves to the caller (`makeDocumentId`) via `DocIdMeta`.
- `DocTable_GetId` / `DocTable_GetIdR` / `DocTable_BorrowByKey(R)` /
  `DocTable_Pop(R)` / `DocTable_Replace` (all trie-based, key-addressed) are
  **removed**. Callers switch to: resolve key→docId via `DocIdMeta`, then use
  docId-addressed DocTable ops.
- Add a docId-addressed pop used by the unified delete path (name TBD, e.g.
  `DocTable_DeleteById`): unchain the DMD from its bucket, mark
  `Document_Deleted`, drop its TTL entry, adjust `memsize`/`size`. This is
  today's `DocTable_Pop` body minus the `DocIdMap_*` calls and the key lookup.
- `DocTable_GetKey`, `DocTable_Borrow`, `DocTable_Exists`, the DMD lifecycle,
  TTL, sorting-vector and payload APIs are **unchanged** (they are docId- or
  DMD-addressed already).
- `DocTable_LegacyRdbLoad` drops its `DocIdMap_Put`; it stays RAM-only (legacy
  is disk-unsupported) and only needs to consume/rebuild buckets until
  `Indexes_UpgradeLegacyIndexes()` frees the loaded DocTable. Note: legacy RDB
  carried the mapping implicitly via re-population; since modern RAM rebuilds by
  re-index anyway, the legacy loader only needs the DMDs in buckets.

### 4.2 key→docId via DocIdMeta in both modes

Replace the RAM `DocTable_GetId`-family calls with `DocIdMeta_Get(...)`:

- `makeDocumentId` (`src/indexer.c`): before `DocTable_Put`, `DocIdMeta_Get` the
  key. If found and not REPLACE → treat as existing (return existing DMD via
  `DocTable_Borrow(docId)`); if REPLACE → pop the old DMD by that docId. After
  `DocTable_Put`, `DocIdMeta_Set(key, newDocId)`. This mirrors the disk
  `doAssignIds`/`applyDocTable` split but writes buckets instead of disk.
- FT.GET / FT.MGET (`src/module.c:254,294`), `Document_EvalExpression`
  (`src/document.c:1023`), `AddDocumentCtx_UpdateNoIndex` (`src/document.c:1077`),
  FT.EXPLAIN id dump (`src/query.c:1868`), debug `getDocIdFromKey`
  (`src/debug_commands.c:903`) and `DumpDocInfo` (`:1566`): use `DocIdMeta_Get`
  then `DocTable_Borrow(docId)`. Several of these already have a disk branch that
  does exactly this — the two branches merge into one.

### 4.3 Unified deletion on the `unlink` callback

- Register the `unlink` callback in **both** modes.
- Make `IndexSpec_DeleteDocById` (`src/spec.c:3544`) work in RAM: drop the
  `RS_ASSERT(isSpecOnDisk(spec))`; branch internally — disk calls
  `SearchDisk_DeleteDocumentById`, RAM calls the new `DocTable_DeleteById` — then
  both call `indexSpec_OnDocDeleted(spec, docId, docLen)` (vectors/geometry +
  scoring stats) under the write lock.
- `docIdMetaUnlink` already iterates `specId→docId` and calls
  `IndexSpec_DeleteDocById` per spec, then invalidates the entry. It becomes the
  single delete trigger for both modes.
- Retire the RAM notification delete path and **converge RAM's keyspace
  subscription onto disk's set**: in `Initialize_KeyspaceNotifications`
  (`src/notifications.c:731`) RAM stops subscribing to
  `EXPIRED`/`EVICTED`/`TRIMMED`/`KEY_TRIMMED` (the mode branches collapse to a
  single flag set). In `KeySpaceNotificationCallback`, delete GROUPS B/D/E
  collapse: `del`/`set`/empty-`change` delete handling and the
  `expired`/`evicted`/`trimmed` cases are all removed for RAM because `unlink`
  now owns deletion — exactly the disk behavior today. (Q2 confirms `unlink`
  fires on expiry/eviction — see §3.)
- `IndexSpec_DeleteDoc` / `IndexSpec_DeleteDoc_Unsafe` (the key-addressed RAM
  delete) are removed or reduced to a thin `DocIdMeta_Get` + `IndexSpec_DeleteDocById`.

### 4.4 Rename

RAM `DocTable_Replace` is removed. Both modes: `DocIdMeta_Get(to_key)` → docId,
then update the stored key — disk via `SearchDisk_ReplaceKey`, RAM by rewriting
`dmd->keyPtr` (new small helper `DocTable_SetKeyById(docId, to_str, to_len)`).
`DocIdMeta`'s `rename` callback stays NULL (meta rides with the key on rename),
so no re-`Set` is needed.

### 4.5 DocIdMeta_Init: always-init, conditional callbacks

Split registration so RAM and disk register different callback sets:

| callback | RAM | Disk | rationale |
|---|---|---|---|
| `free` | ✅ | ✅ | free the per-key `specId→docId` dict when the key is freed |
| `move` | ✅ | ✅ | docId is meaningless in another DB → drop meta on MOVE |
| `rename` | NULL | NULL | keep meta with the key across RENAME |
| `unlink` | ✅ | ✅ | single unified delete trigger (§4.3) |
| `rdb_save` | **NULL** | ✅ | RAM rebuilds via re-index on load; persisting = stale mapping |
| `rdb_load` | **NULL** | ✅ | same |
| `aof_rewrite`/`defrag`/`mem_usage`/`free_effort` | NULL | NULL | unchanged |

Implementation: `DocIdMeta_Init(ctx)` builds the `RedisModuleKeyMetaClassConfig`
with `rdb_save`/`rdb_load` set only when `SearchDisk_IsEnabled()`. Call it
unconditionally in `src/module.c` (remove the `if (SearchDisk_IsEnabled())`
guard at `:4881`). `ForgetDocIdMetadata` and its `notifications.c` toggles remain
disk-only concerns (they only matter when `rdb_save`/`rdb_load` are registered).

## 5. Concurrency

- key→docId moves onto `DocIdMeta` (opens a Redis key). All callers are
  main-thread or GIL-holding (see §3), including the bulk id-assignment section
  which explicitly holds the GIL. FT.SEARCH is unaffected (docId→DMD only).
- `unlink` fires on the main thread as part of key deletion.
  `IndexSpec_DeleteDocById` takes the spec **write lock** (as it does today for
  disk), so RAM bucket mutation stays serialized against background workers.
- The docId→DMD buckets and DMD refcount/lifecycle are unchanged, so the
  existing lock discipline for reads (`DocTable_Borrow` under the spec read
  lock) is preserved.

## 6. Risks & mitigations

1. **Expiration/eviction semantics.** RAM currently de-indexes expired/evicted
   keys via dedicated notifications; disk relies on `unlink`. Unifying means RAM
   de-indexes them via `unlink`. The precondition — that `unlink` fires on
   expiry/eviction — is **established** by disk's existing exclusive reliance on
   it (Q2). Residual risk is ordering/edge cases (e.g. lazy expiry during a
   query, replica-side expiry). *Mitigation:* dedicated pytests for TTL expiry
   and `maxmemory`-eviction de-indexing, master + replica; bisect against the
   disk suite which already exercises the same `unlink` path.
2. **Write/GET latency.** key→docId becomes a key-meta open-key instead of a
   trie lookup, on add/replace/delete/rename/FT.GET/FT.MGET. Search throughput
   is unaffected. *Mitigation:* reuse open-key handles where the caller already
   holds one (the `*WithOpenKey` variants already exist); micro-benchmark FT.GET
   and ingest.
3. **RDB / replication regressions in RAM.** Getting the conditional callbacks
   wrong could persist a stale mapping or crash load. *Mitigation:* explicit
   assertion that RAM never registers `rdb_save`/`rdb_load`; RDB save→load
   round-trip and replica tests.
4. **Legacy RDB.** Ensure the legacy loader still produces a queryable RAM index
   before the post-load scan repopulates `DocIdMeta` (Q1).
5. **Cross-cutting blast radius.** ~10 files. *Mitigation:* staged commits (§8),
   each independently building and testable.

## 7. Test plan

- **C unit** (`tests/cpptests`): DocTable docId→DMD ops after trie removal;
  `test_cpp_index.cpp:1260` (`DocTable_GetKey`) updated to the docId-addressed API.
- **Rust:** `redis_mock` references `DocIdMeta_Init` — update the mock if the
  signature/registration changes (`src/redisearch_rs/redis_mock/src/lib.rs:356`).
- **Python flow** (`tests/pytests`): add/replace/get/mget; DEL/SET-overwrite
  de-indexing; TTL expiry + `maxmemory` eviction de-indexing; RENAME; RDB
  save/load round-trip (RAM: mapping rebuilt, not loaded); replica consistency.
  Run the existing suites in both default and Flex configurations.
- **Coverage:** `/check-flow-coverage` on `doc_table.c`, `doc_id_meta.c`,
  `indexer.c`, `notifications.c` delete paths.

## 8. Task / commit breakdown

1. **DocIdMeta_Init split** — always-init, conditional `rdb_*` (+ `unlink` in
   both); update `module.c` call site and the Rust mock. Behaviorally inert in
   RAM until callers switch (nothing calls `DocIdMeta` in RAM yet). Verifiable in
   isolation.
2. **RAM `IndexSpec_DeleteDocById` + `DocTable_DeleteById`** — implement the RAM
   delete-by-id; keep the old notification path in place for now.
3. **Switch RAM delete to `unlink`** — flip the `notifications.c` guards, retire
   `IndexSpec_DeleteDoc`'s trie branch. TTL/eviction tests here.
4. **Switch RAM key→docId reads to `DocIdMeta`** — `makeDocumentId`, FT.GET/MGET,
   eval, debug, explain, rename. Each converges its disk/RAM branches.
5. **Remove the trie** — delete `dim`, `DocIdMap`, `NewDocIdMap`, and the now-dead
   key-addressed DocTable functions; fix `DocTable_LegacyRdbLoad`; update C unit
   tests.
6. **Docs/tests/lint** — spec delta if any user-visible behavior shifts (none
   expected), full `/verify`.

## 9. Open questions (need answers before / during implementation)

- **Q1 — Legacy RDB — RESOLVED:** user commands do not observe the
  `DocTable_LegacyRdbLoad` buckets. `Indexes_UpgradeLegacyIndexes()` frees them
  and resets stats before the upgraded spec is scanned/re-indexed, so DocIdMeta
  is populated by the scan that creates the live DocTable entries.
- **Q2 — Expiration/eviction — RESOLVED.** The key-meta `unlink` callback fires
  on active expiry, lazy expiry, and maxmemory eviction: disk mode already
  relies on it exclusively (it does not subscribe to `EXPIRED`/`EVICTED`/
  `TRIMMED` and has no other de-index path — `src/notifications.c:735-744`). RAM
  can safely drop those subscriptions and unify on `unlink`. Still covered by
  dedicated TTL/eviction pytests during bring-up.
- **Q3 — FT.GET/FT.MGET hot path:** acceptable to pay a key-meta lookup here, or
  should FT.GET keep a fast path? (These open the key anyway to read fields, so
  the marginal cost is one `GetKeyMeta` call.)
- **Q4 — `DocIdMeta` value scope:** the per-key value is a `dict specId→docId`
  to support multiple indexes over one key. Confirm RAM multi-index scenarios
  (several FT indexes over the same prefix) are covered by the existing dict
  design (they are, by construction — same as disk).
