# Design: Bind IndexSpecs to their logical DB (per-DB scoping)

Status: Implemented (first pass) / for review
Author: Joan Fontanals Martinez
Related branch: `joan-fix-flush-db-other-than-0`

## As implemented (read this first)

The implementation realizes the per-DB behavior with a single global registry whose
key carries the DB, rather than one container per DB. Specifically:

- adds a single `int dbid` field to `IndexSpec` (`spec.h`), set from
  `RedisModule_GetSelectedDb(ctx)` at `FT.CREATE` (`spec.c` `IndexSpec_CreateNew`);
  the `db != 0` guard in `module.c` is removed;
- keys **`specDict_g` by a composite `(dbid, name)`** (`DbSpecKey` + `dictTypeDbSpec`
  in `indexes.c`), so the **same index name can exist independently on different DBs**.
  `specIdDict_g` stays keyed by the globally-unique spec id;
- scopes everything user-facing to the connection's DB:
  - name lookup (`Indexes_LoadIndexSpecUnsafe[Ex]` fetch by `(options->db, name)`;
    aliases resolve then filter by DB) → cross-DB lookups return "no such index",
  - keyspace-notification dispatch (`Indexes_FindMatchingSchemaRules` skips specs whose
    `dbid != RedisModule_GetSelectedDb(ctx)`),
  - `FT._LIST` (`Indexes_List` filters by the connection's DB);
- frees per DB on `FLUSHDB` (`Indexes_FreeByDb`); `FLUSHALL` still frees everything;
- selects `spec->dbid` on the background initial scan (`indexes_scan.c`);
- fixes `getDocTypeFromString` to open the key on the **event** context instead of
  the DB-0-pinned `RSDummyContext` — required so the per-key `loaded` event reindexes
  documents on non-zero DBs during RDB load;
- persists `dbid` in the RDB (`INDEX_CURRENT_VERSION` 28, new `INDEX_DB_ID_VERSION`);
  older RDBs load with `dbid = 0` (backward compatible).

Background / detached paths were audited (§3.5): cursor-read and aggregate detached
contexts already copy the connection's DB (`aggregate_exec.c`, `hybrid_request.c`);
blocked-client thread-safe contexts inherit the client's DB; the **fork GC opens no
document keys** (pure in-memory index work), so its DB-0 context is harmless. The only
residual is the `copy_to`/hash-field **command filter** (`notifications.c`), which opens
the key on `RSDummyContext` (DB 0) purely to gate hash-field-name collection — on a
non-zero DB it falls back to a full document reindex (correct, just not field-targeted).
Cluster mode remains DB-0 only.

Tests: `tests/pytests/test_index_on_other_db.py` — create/initial-scan/notification
scoping/cross-DB invisibility/same-name-per-DB/content-load+cursor-read/per-DB flush/RDB
reload — plus regression sweeps (follow-hashes, expire, info, acl, aggregate,
rdb-compatibility) and the `RdbMockTest` C++ suite; all green.

---


## 1. Problem & goal

Today every RediSearch index is forced to live in **DB 0**. `FT.CREATE` is hard-rejected
on any other logical DB ([`module.c:616`](../../src/module.c#L616)), and the whole engine
assumes a single keyspace. The current branch added a stopgap so a `FLUSHDB` on a
non-zero DB no longer drops the DB-0 indexes ([`indexes.c onFlush`](../../src/indexes.c#L427)).

**Goal:** allow an index to be created in *any* logical DB and bind it to that DB, so that:

- `FT.CREATE` works on a connection selected on DB N; the index belongs to DB N.
- Index lookup is **scoped to the connection's selected DB**: `FT.SEARCH`/`FT.INFO`/etc. on
  DB N only see indexes created on DB N.
- Keyspace notifications only (re)index documents for the DB the event fired in, against
  indexes bound to that DB.
- `FLUSHDB` on DB N drops only the indexes bound to DB N; `FLUSHALL` drops all.
- All document key access for an index targets the index's own DB.

### Chosen semantics: per-DB scoping (each DB is its own namespace)

- **Lookup is DB-scoped.** `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`, `FT.ALTER`, `FT._LIST`,
  alias commands, etc. resolve indexes only within the connection's selected DB. An index
  created on DB 5 is invisible (and not queryable) from a connection on DB 0.
- **Index names are unique per DB, not globally.** The same name may exist in two DBs as two
  independent indexes — the natural keyspace-namespace model.
- **`dbid` is recorded on each spec** so background tasks (which carry only a spec reference)
  know which DB to operate on.

Governing principle for data access:

> **All document-key access for an index uses `spec->dbid`.** For foreground commands this
> equals the connection's selected DB (scoping guarantees it); for background tasks (scanner,
> GC, cursor reads, indexer) running on a detached context, select `spec->dbid` explicitly.
> The caller's selected DB picks the registry; `spec->dbid` opens the keys.

## 2. Current state (DB-0 assumptions)

| Area | Site | Assumption |
|------|------|-----------|
| Creation guard | [`module.c:616`](../../src/module.c#L616) | rejects `GetSelectedDb(ctx) != 0` |
| Global registries | [`indexes.c:58`](../../src/indexes.c#L58), [`rules.c:23`](../../src/rules.c#L23), [`alias.c:14`](../../src/alias.c#L14) | `specDict_g`, `specIdDict_g`, `SchemaPrefixes_g`, `AliasTable_g`, `legacySpecDict` — single global namespace |
| Lookup | [`indexes.c:144`](../../src/indexes.c#L144) | `Indexes_LoadIndexSpecUnsafe` resolves by name in the one global dict |
| Notification dispatch | [`notifications.c:83`](../../src/notifications.c#L83), [`indexes.c:465`](../../src/indexes.c#L465) | matches all specs globally, ignores event DB |
| `copy_to` dest read | [`notifications.c:309`](../../src/notifications.c#L309) | opens dest key on `RSDummyContext` (pinned DB 0) |
| Flush | [`indexes.c:427`](../../src/indexes.c#L427) | frees all specs when `dbnum` is 0 or -1 |
| RDB persistence | [`indexes.c:336`](../../src/indexes.c#L336) save / [`indexes.c:243`](../../src/indexes.c#L243) load | specs in module **aux data**, global, once per RDB, no DB recorded |
| Scanner | [`indexes_scan.c:278`](../../src/indexes_scan.c#L278) | scans the detached ctx's DB (DB 0) |
| Doc key opens | `document_basic.c`, `indexer.c`, `fork_gc/`, `rlookup_load_document.c`, [`indexes.c:646`](../../src/indexes.c#L646) | `RedisModule_OpenKey` on whatever DB the ctx is on |
| Query detached ctx | [`aggregate_exec.c:1483`](../../src/aggregate/aggregate_exec.c#L1483), [`hybrid_request.c:458`](../../src/hybrid/hybrid_request.c#L458) | copies caller's selected DB (the only places DB is propagated today) |

**Key enabler:** the `ctx` passed to `HashNotificationCallback` already has its selected DB
set to the DB where the event fired, so `RedisModule_GetSelectedDb(ctx)` is the routing
signal we need — we simply don't read it today.

## 3. Design

### 3.1 Data model — per-DB registry buckets

Because lookup is DB-scoped and names may collide across DBs, the global registries become
**per-DB**. Introduce a registry bucket and a container keyed by DB id:

```c
typedef struct {
  dict       *specDict;        // HiddenString name  -> RefManager*
  TrieMap    *schemaPrefixes;  // key prefix         -> SchemaPrefixNode*
  AliasTable *aliases;         // alias name         -> spec
} SpecRegistry;

// dbid (int) -> SpecRegistry*. Lazily created when the first index lands in a DB,
// torn down when the DB's last index is dropped (or on flush).
dict *dbRegistries_g;
```

Notes:
- A `dict` keyed by `dbid` (rather than an array sized by the `databases` config) avoids
  pre-allocating buckets for the common single-DB case and naturally handles a large
  `databases` value.
- **`specIdDict_g` stays global** and `specId` stays globally monotonic/unique — it backs
  cursors and document IDs, which are not DB-scoped. (Flush must still remove the flushed
  DB's ids from it; see §3.4.)
- Each `IndexSpec` gains `int dbid` ([`spec.h:296`](../../src/spec.h#L296)) so background
  tasks can recover their DB from a bare spec reference.

A thin accessor `registryFor(int dbid)` (and `registryForCtx(ctx)` =
`registryFor(GetSelectedDb(ctx))`) replaces direct references to the old globals. This is the
bulk of the mechanical change.

### 3.2 Creation

In `CreateIndexCommand` ([`module.c:616`](../../src/module.c#L616)):
- Remove the `GetSelectedDb(ctx) != 0` rejection.
- `int dbid = RedisModule_GetSelectedDb(ctx)`; set `sp->dbid = dbid`.
- Insert the spec into `registryFor(dbid)` (lazily creating the bucket). Name-collision
  checks (`FT.CREATE` of an existing name, `IF NOT EXISTS`) are evaluated within that bucket.
- In cluster mode, force `dbid = 0` (cluster is DB-0-only; see §3.7).

### 3.3 Lookup and notification routing

- **Lookup:** all by-name resolution (`Indexes_LoadIndexSpec*`, alias resolution, `FT.*`
  command handlers, `FT._LIST`, `Indexes_Count`) goes through `registryForCtx(ctx)`. Indexes
  in other DBs are simply not present in that bucket → "no such index", exactly as if they
  didn't exist.
- **Notifications:** in `HashNotificationCallback`
  ([`notifications.c:83`](../../src/notifications.c#L83)), read
  `int dbid = RedisModule_GetSelectedDb(ctx)` once; do all prefix matching against
  `registryFor(dbid)->schemaPrefixes`. Events for a DB with no bucket return immediately.
- Fix the `copy_to` path ([`notifications.c:306`](../../src/notifications.c#L306)): open the
  destination key on a context selected to `dbid`, not the DB-0-pinned `RSDummyContext`.

### 3.4 Flush

In `onFlush` ([`indexes.c:427`](../../src/indexes.c#L427)):
- `dbnum == -1` (FLUSHALL): free **all** buckets (and `Dictionary_Clear()`, dialect stats) —
  current behavior.
- otherwise (FLUSHDB on DB N): free `registryFor(dbnum)` as a unit — `Indexes_Free` over its
  `specDict`, then drop the bucket. Each freed spec also removes its `specId` from the global
  `specIdDict_g`. Other DBs' buckets are untouched.

This replaces the stopgap `dbnum != -1 && dbnum != 0` early-return. Flush is O(indexes in
that DB) and needs no global scan.

### 3.5 Document-key access uses `spec->dbid`

For foreground commands, scoping already guarantees the connection's DB equals `spec->dbid`,
so existing inline opens are correct. The work is on **detached/background contexts**, which
default to DB 0 and must select `spec->dbid` (saving/restoring the previous DB) before
`RedisModule_OpenKey`:

- **Scanner** ([`indexes_scan.c`](../../src/indexes_scan.c#L278)): select `spec->dbid` so
  `RedisModule_Scan` iterates the index's own DB.
- **Indexer / background indexing**, **fork GC** doc opens, **cursor / query document loads**
  ([`rlookup_load_document.c`](../../src/rlookup_load_document.c#L64)), **expiration checks**
  ([`indexes.c:646`](../../src/indexes.c#L646)).

A small RAII-style `SelectDbGuard` (selects a DB, restores the previous one on scope exit)
keeps these sites tidy. An audit of every `RedisModule_OpenKey` reachable from a spec is part
of this step.

### 3.6 Persistence (RDB)

Specs are serialized through module **aux data**, globally, *before* the keyspace
(`aux_save_triggers = REDISMODULE_AUX_BEFORE_RDB`). On load the aux block is restored before
keys, and documents are then indexed organically through the per-key `loaded` keyspace event
([`notifications.c:115`](../../src/notifications.c#L115)) whose `ctx` carries the right DB.

Required changes:
- **Bump the aux encoding version** to write each spec's `dbid`
  ([`indexes.c:336`](../../src/indexes.c#L336) / [`indexes.c:243`](../../src/indexes.c#L243),
  per-spec in [`spec.c IndexSpec_RdbLoad/Save`](../../src/spec.c#L2944)).
- On load, place each spec into `registryFor(dbid)`.
- **Backward compatibility:** an RDB written by an older version (no `dbid` field) loads every
  spec with `dbid = 0` — byte-for-byte identical behavior to today. This is the riskiest piece
  and must be covered by a load test from a pre-change RDB fixture.
- Multi-DB load then "just works": keys in DB N fire `loaded` events on a DB-N ctx, matched
  only against `registryFor(N)` via §3.3.

### 3.7 Cluster mode

Cluster uses only DB 0. Force `dbid = 0` on creation in cluster mode and keep the
`@skip(cluster=True)` markers on the DB-specific tests. Multi-DB behavior is standalone-only.

### 3.8 `databases` config = 1

If the server is configured with a single DB, only the DB-0 bucket ever exists and there is no
observable behavior change — a useful safety property.

## 4. Plan (phased)

1. **Registry refactor (no behavior change).** Introduce `SpecRegistry` + `dbRegistries_g` and
   route all current access through `registryFor(0)`. Everything still lives in DB 0; the full
   suite should stay green. *Largest but lowest-risk commit.*
2. **Add `dbid` to `IndexSpec`;** set from `GetSelectedDb(ctx)` at creation; remove the
   `db != 0` guard; force 0 in cluster. Insert/lookup via `registryForCtx`.
3. **Notification routing** by event DB; fix the `copy_to` DB-0 pin.
4. **Flush** becomes a per-bucket drop (+ `specIdDict_g` cleanup). Replace the stopgap.
5. **Background DB selection** on scanner / indexer / GC / query-load / expiration
   (`SelectDbGuard`).
6. **RDB aux version bump + `dbid`**, with old-RDB → `dbid 0` load path.
7. **Tests & docs** (§5).

Each step is an independently reviewable commit; step 1 is the largest but lowest-risk.

## 5. Testing

- **Python e2e** (extend [`test_flushdb_behavior.py`](../../tests/pytests/test_flushdb_behavior.py)):
  - Create an index on DB 5, `HSET` a doc on DB 5; `FT.SEARCH` from DB 5 returns it, and from
    DB 0 returns **"no such index"** (scoping).
  - Same index name on DB 0 and DB 5 are independent indexes with independent contents.
  - `FLUSHDB` on DB 5 drops only the DB-5 index; a DB-0 index survives. `FLUSHALL` drops both.
  - Notification isolation: writing a key in DB 5 must not feed an index bound to DB 0 (and
    vice versa).
  - RDB round-trip: indexes on DB 0 and DB 5 with docs, `DEBUG RELOAD`, assert both restore in
    their own DB and re-index correctly.
  - Backward-compat load: load a pre-change RDB fixture; assert all indexes come back on DB 0.
- **C unit tests** for `registryFor` lifecycle, per-bucket flush, and the notification dbid
  routing.
- All `@skip(cluster=True)` retained for DB-specific cases.

## 6. Risks & open items

- **RDB backward compatibility** is the highest-risk item — verify load of an RDB produced
  before this change (and ideally a forward/`replicaof` mixed-version scenario).
- **Completeness of DB scoping:** every by-name lookup and every `RedisModule_OpenKey`
  reachable from a spec must be routed/selected correctly; a missed site leaks an index across
  DBs or opens the wrong key. The §3.1 accessor and §3.5 audit are how we contain this.
- **`specId` global uniqueness vs. per-DB flush:** ids stay global; flush of one DB must remove
  exactly that DB's ids from `specIdDict_g` without disturbing others.
- **Aliases per DB:** `AliasTable` moves into the bucket; confirm alias add/del/resolve and the
  self-removal-on-drop path ([`alias.c`](../../src/alias.c)) all operate on the spec's bucket.
- This is a **behavior + persistence-format change** — per `CLAUDE.md` it should go through the
  spec-driven workflow gated on maintainer review (GitHub issue first). This document is the
  design artifact for that review.
