/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_STRUCT_H
#define SPEC_STRUCT_H

#include "redismodule.h"
#include "query_error.h"
#include "field_spec.h"
#include "util/references.h"
#include "obfuscation/hidden.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;
struct Version;

// Global variables
extern const char *(*IndexAlias_GetUserTableName)(RedisModuleCtx *, const char *);
extern RedisModuleType *IndexSpecType;
extern struct Version redisVersion;
extern struct Version rlecVersion;
extern bool isEnterprise;
extern bool isCrdt;
extern bool isTrimming;
extern bool isFlex;
extern size_t memoryLimit;
extern size_t used_memory;

void Cursors_initSpec(struct IndexSpec *spec);

const FieldSpec *IndexSpec_GetField(const struct IndexSpec *spec, const HiddenString *name);
const FieldSpec *IndexSpec_GetFieldWithLength(const struct IndexSpec *spec, const char *name, size_t len);
t_fieldMask IndexSpec_GetFieldBit(struct IndexSpec *spec, const char *name, size_t len);
const char *IndexSpec_GetFieldNameByBit(const struct IndexSpec *sp, t_fieldMask id);
const FieldSpec *IndexSpec_GetFieldByBit(const struct IndexSpec *sp, t_fieldMask id);
arrayof(FieldSpec *) IndexSpec_GetFieldsByMask(const struct IndexSpec *sp, t_fieldMask mask);
const FieldSpec *IndexSpec_GetFieldBySortingIndex(const struct IndexSpec *sp, uint16_t idx);
arrayof(FieldSpec *) getFieldsByType(struct IndexSpec *spec, FieldType type);

int IndexSpec_CheckPhoneticEnabled(const struct IndexSpec *sp, t_fieldMask fm);
int IndexSpec_CheckAllowSlopAndInorder(const struct IndexSpec *sp, t_fieldMask fm, QueryError *status);

const char *IndexSpec_FormatName(const struct IndexSpec *sp, bool obfuscate);
char *IndexSpec_FormatObfuscatedName(const HiddenString *specName);

bool IndexSpec_IsCoherent(struct IndexSpec *spec, sds *prefixes, size_t n_prefixes);

StrongRef IndexSpec_GetStrongRefUnsafe(const struct IndexSpec *spec);
StrongRef IndexSpecRef_Promote(WeakRef ref);
void IndexSpecRef_Release(StrongRef ref);

int isRdbLoading(RedisModuleCtx *ctx);
int CompareVersions(struct Version v1, struct Version v2);

#ifdef __cplusplus
}
#endif

#endif  // SPEC_STRUCT_H
