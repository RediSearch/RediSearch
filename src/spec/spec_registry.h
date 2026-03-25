/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
// Global index registry — management of specDict_g, index creation/removal,
// schema-rule matching dispatch, temporary index timers, and cleanup pool.
//
// This header is included from spec.h after all required types are defined.
// Do not include it directly; include spec.h instead.

#ifndef SPEC_REGISTRY_H
#define SPEC_REGISTRY_H

//---------------------------------------------------------------------------------------------
// Global index dictionary management
//---------------------------------------------------------------------------------------------

void Indexes_Init(RedisModuleCtx *ctx);
void Indexes_Free(RedisModuleCtx *ctx, dict *d, bool deleteDiskData);
size_t Indexes_Count();
void Indexes_List(RedisModule_Reply *reply, bool obfuscate);

//---------------------------------------------------------------------------------------------
// Index load/lookup
//---------------------------------------------------------------------------------------------

StrongRef IndexSpec_LoadUnsafe(const char *name);
StrongRef IndexSpec_LoadUnsafeEx(IndexLoadOptions *options);

//---------------------------------------------------------------------------------------------
// Index creation
//---------------------------------------------------------------------------------------------

IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status);

//---------------------------------------------------------------------------------------------
// Index removal
//---------------------------------------------------------------------------------------------

void IndexSpec_RemoveFromGlobals(StrongRef spec_ref, bool removeActive);

//---------------------------------------------------------------------------------------------
// Schema-rule matching dispatch
//---------------------------------------------------------------------------------------------

SpecOpIndexingCtx *Indexes_FindMatchingSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                                   DocumentType type, bool runFilters,
                                                   RedisModuleString *keyToReadData);
void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           DocumentType type, RedisModuleString **hashFields);
void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           DocumentType type, RedisModuleString **hashFields);
void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key);
void Indexes_SpecOpsIndexingCtxFree(SpecOpIndexingCtx *specs);

//---------------------------------------------------------------------------------------------
// Temporary index timers
//---------------------------------------------------------------------------------------------

void Indexes_SetTempSpecsTimers(TimerOp op);

//---------------------------------------------------------------------------------------------
// Clean pool & pending drops
//---------------------------------------------------------------------------------------------

void CleanPool_ThreadPoolStart();
void CleanPool_ThreadPoolDestroy();
size_t CleanInProgressOrPending();
void addPendingIndexDrop();
void removePendingIndexDrop();

// Submit work to the clean pool (used by IndexSpec_Free in spec.c)
void CleanPool_AddWork(void (*proc)(void *), void *arg);

#endif  // SPEC_REGISTRY_H
