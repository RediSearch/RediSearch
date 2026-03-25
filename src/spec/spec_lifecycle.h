/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_LIFECYCLE_H
#define SPEC_LIFECYCLE_H

#include "redismodule.h"
#include "util/references.h"
#include "obfuscation/hidden.h"
#include "gc.h"
#include "field_spec.h"
#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;
struct RefManager;

struct IndexSpec *NewIndexSpec(const HiddenString *name);
void IndexSpec_Free(struct IndexSpec *spec);

void initializeIndexSpec(struct IndexSpec *sp, const HiddenString *name, IndexFlags flags,
                         int16_t numFields);
void IndexSpec_InitLock(struct IndexSpec *sp);

FieldSpec *IndexSpec_CreateField(struct IndexSpec *sp, const char *name, const char *path);
int IndexSpec_CreateTextId(struct IndexSpec *sp, t_fieldIndex index);

void IndexSpec_MakeKeyless(struct IndexSpec *sp);

void IndexSpec_StartGC(StrongRef spec_ref, struct IndexSpec *sp, GCPolicy gcPolicy);
void IndexSpec_StartGCFromSpec(StrongRef spec_ref, struct IndexSpec *sp, uint32_t gcPolicy);

int IndexSpec_UpdateDoc(struct IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type);
int IndexSpec_DeleteDoc(struct IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);
void IndexSpec_DeleteDoc_Unsafe(struct IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);

void IndexSpec_AddTerm(struct IndexSpec *sp, const char *term, size_t len);
void Spec_AddToDict(struct RefManager *rm);

void IndexSpec_InitializeSynonym(struct IndexSpec *sp);

// Compaction FFI
void IndexSpec_AcquireWriteLock(struct IndexSpec *sp);
void IndexSpec_ReleaseWriteLock(struct IndexSpec *sp);
bool IndexSpec_DecrementTrieTermCount(struct IndexSpec *sp, const char *term, size_t term_len,
                                      size_t doc_count_decrement);
void IndexSpec_DecrementNumTerms(struct IndexSpec *sp, uint64_t num_terms_removed);

#ifdef __cplusplus
}
#endif

#endif  // SPEC_LIFECYCLE_H
