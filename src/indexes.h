/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEXES_H__
#define __INDEXES_H__

// indexes.h -- the global index registry and keyspace dispatch API.
//
// Owns the global spec dictionaries (specDict_g / specIdDict_g), defined in
// indexes.c and declared here. The shared SpecOp* / TimerOp types live in
// spec.h, which this header includes.

#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

// The global index registry. specDict_g is keyed by a composite (logical DB,
// name) so the same index name can exist independently on different DBs;
// specIdDict_g is keyed by the globally-unique spec id. Both are defined in
// indexes.c.
extern dict *specDict_g;
extern dict *specIdDict_g;  // Maps specId (uint64_t) -> RefManager* (same as specDict_g values)

// Composite key for specDict_g: an index is identified by (logical DB, name).
// `name` is borrowed when building a lookup/delete key (the dict dups it on add).
typedef struct {
  int dbid;
  const HiddenString *name;
} DbSpecKey;

// Build a pointer to a transient DbSpecKey for a dictAdd/dictFetchValue/dictDelete
// call on specDict_g. The compound literal lives for the enclosing block, which
// covers the dict call (dictAdd dups the key; fetch/delete only read it).
#define DB_SPEC_KEY(db, nm) (&(DbSpecKey){.dbid = (db), .name = (nm)})

//---------------------------------------------------------------------------------------------

// Create a new index from FT.CREATE arguments, register it in the global
// registry, and (unless SKIPINITIALSCAN was given) schedule its initial
// background scan. This is the registry-level entry point that composes the
// IndexSpec core (IndexSpec_CreateNew) with the scanner (IndexSpec_ScanAndReindex);
// callers should use it rather than IndexSpec_CreateNew directly so the initial
// scan is not silently skipped.
// Returns the new spec, or NULL on error (with `status` set).
IndexSpec *Indexes_CreateNewSpec(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                             QueryError *status);

/**
 * Add an already-built spec to the global registry directly. For testing only.
 */
void Spec_AddToDict(RefManager *w_spec);

/**
 * @brief Remove the spec from the global registry (specDict_g/specIdDict_g) and
 * tear down its remaining global state. Delegates the non-registry teardown to
 * IndexSpec_Unlink. This function consumes the strong reference it gets.
 *
 * @param ref a strong reference to the spec
 * @param removeActive - should we call CurrentThread_ClearIndexSpec on the released spec
 */
void Indexes_RemoveSpecFromGlobals(StrongRef spec_ref, bool removeActive);

/**
 * Find and load an index from the global registry, by name
 * (Indexes_LoadIndexSpecUnsafe) or by IndexLoadOptions (Indexes_LoadIndexSpecUnsafeEx).
 * Performs the specDict_g lookup (with alias fallback unless INDEXSPEC_LOAD_NOALIAS)
 * then runs the per-spec post-load bookkeeping (IndexSpec_OnAcquire). The call
 * does not increase the spec's strong reference counter.
 * @return a borrowed strong reference to the spec, or NULL if it does not exist.
 *
 * The lookup is scoped to the logical DB of `ctx` (RedisModule_GetSelectedDb):
 * an index is only visible from the DB it was created on. Pass NULL to scope to
 * DB 0 (internal/registry callers that only ever deal with DB 0).
 */
StrongRef Indexes_LoadIndexSpecUnsafe(RedisModuleCtx *ctx, const char *name);
StrongRef Indexes_LoadIndexSpecUnsafeEx(IndexLoadOptions *options);

// Register the IndexSpecType module type (wires the registry-wide RDB aux
// callbacks together with the per-spec callbacks defined in spec.c).
int Indexes_RegisterType(RedisModuleCtx *ctx);

void Indexes_Init(RedisModuleCtx *ctx);
/*
 * Free all indexes.
 * @param deleteDiskData - delete the disk data
*/
void Indexes_Free(RedisModuleCtx *ctx, dict *d, bool deleteDiskData);
// Free only the indexes bound to logical DB `dbnum` (used by FLUSHDB on a
// single DB). Indexes on other DBs are left intact.
void Indexes_FreeByDb(RedisModuleCtx *ctx, int dbnum, bool deleteDiskData);
// Re-bind indexes after SWAPDB swapped logical DBs db_a and db_b: every index on
// db_a moves to db_b and vice versa (metadata-only; the indexed data is
// DB-independent). Any in-flight initial scan on an affected index is cancelled
// and restarted against the new DB. Must be called with the GIL held.
void Indexes_SwapDb(RedisModuleCtx *ctx, int db_a, int db_b);
size_t Indexes_Count();
void Indexes_Propagate(RedisModuleCtx *ctx);

void Indexes_SetTempSpecsTimers(TimerOp op);

void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields);
// Refresh the per-field TTL entries on every spec that indexes `key`: reads
// the hash's current per-field expiration timestamps and writes them onto
// the matching specs' TTL tables, without re-tokenizing the document or
// rebuilding inverted indexes. In-memory flow only; callers must use
// Indexes_UpdateMatchingWithSchemaRules for disk-backed indexes.
void Indexes_UpdateMatchingHashFieldExpiration(RedisModuleCtx *ctx, RedisModuleString *key,
                                               DocumentType type);
// Fast path for keyspace events that only change the document-level TTL
// (EXPIRE/PERSIST): re-reads the key's absolute expiration and writes it
// directly onto the matching DMDs, without re-running schema-rule filters or
// re-indexing the document. In-memory flow only; callers must fall back to
// Indexes_UpdateMatchingWithSchemaRules for disk-backed indexes.
void Indexes_UpdateMatchingDocExpiration(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type);
void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           DocumentType type,
                                           RedisModuleString **hashFields);
void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key);
void Indexes_List(RedisModule_Reply* reply, int dbid, bool obfuscate);

// Collect the specs whose schema rules match `key` (of document `type`) into a
// freshly allocated SpecOpIndexingCtx. `runFilters` controls whether FILTER
// expressions are evaluated; `keyToReadData` overrides the key the document
// data is read from (defaults to `key`).
SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   DocumentType type, bool runFilters,
                                                   RedisModuleString *keyToReadData);
void Indexes_SpecOpsIndexingCtxFree(SpecOpIndexingCtx *specs);

//---------------------------------------------------------------------------------------------

int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when);

// Finalize a spec just loaded from RDB by publishing it into the global registry
// (specDict_g/specIdDict_g) and starting its GC, or discarding it if a spec with
// the same name already exists. Consumes the spec's reference on the duplicate
// path. Accepts NULL (returns REDISMODULE_ERR). Callers obtain the spec from the
// IndexSpec core (e.g. IndexSpec_RdbLoad / IndexSpec_Deserialize) and pass it here.
int Indexes_StoreSpecAfterRdbLoad(IndexSpec *sp);

// This function is called in case the server starts RDB loading.
void Indexes_StartRDBLoadingEvent(RedisModuleCtx *ctx);

// This function is called in case the server ends RDB loading.
void Indexes_EndRDBLoadingEvent(RedisModuleCtx *ctx);

// This function is to be called when loading finishes (failed or not)
void Indexes_EndLoading();

// Replica-side SST replication completion.
//
// Upon SST and RDB replication ending, complete the binding
void Indexes_FinishSSTReplication(RedisModuleCtx *ctx);

// Replica-side SST replication abort.
//
// Tear down everything staged for SST replication. Frees any pending disk RDB
// state, closes any opened-but-unregistered disk specs, and unregisters +
// closes any specs that had already been registered. Removes the affected
// specs from specDict_g. Called from the REDISMODULE_SUBEVENT_SST_REPL_ABORT
// handler.
void Indexes_AbortSSTReplicationLoading(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif

#endif  // __INDEXES_H__
