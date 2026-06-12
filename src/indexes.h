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
// These functions operate over the global spec dictionaries (specDict_g /
// specIdDict_g, defined in indexes.c). Their extern declarations and the shared
// SpecOp* / TimerOp types live in spec.h, which this header includes.

#include "spec.h"

#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------------------------------------------------------------------

// Create a new index from FT.CREATE arguments, register it in the global
// registry, and (unless SKIPINITIALSCAN was given) schedule its initial
// background scan. This is the registry-level entry point that composes the
// IndexSpec core (IndexSpec_CreateNew) with the scanner (IndexSpec_ScanAndReindex);
// callers should use it rather than IndexSpec_CreateNew directly so the initial
// scan is not silently skipped.
// Returns the new spec, or NULL on error (with `status` set).
IndexSpec *Indexes_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
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
void Indexes_RemoveFromGlobals(StrongRef spec_ref, bool removeActive);

void Indexes_Init(RedisModuleCtx *ctx);
/*
 * Free all indexes.
 * @param deleteDiskData - delete the disk data
*/
void Indexes_Free(RedisModuleCtx *ctx, dict *d, bool deleteDiskData);
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
void Indexes_List(RedisModule_Reply* reply, bool obfuscate);

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
void Indexes_RdbSave(RedisModuleIO *rdb, int when);
void Indexes_RdbSave2(RedisModuleIO *rdb, int when);

// Finalize a spec just loaded from RDB by publishing it into the global registry
// (specDict_g/specIdDict_g) and starting its GC, or discarding it if a spec with
// the same name already exists. Consumes the spec's reference on the duplicate
// path. Accepts NULL (returns REDISMODULE_ERR). Callers obtain the spec from the
// IndexSpec core (e.g. IndexSpec_RdbLoad / IndexSpec_Deserialize) and pass it here.
int Indexes_StoreAfterRdbLoad(IndexSpec *sp);

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
