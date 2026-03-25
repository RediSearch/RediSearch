/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_RDB_H
#define SPEC_RDB_H

#include "redismodule.h"
#include "query_error.h"

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;

// RDB save/load for individual index specs
void IndexSpec_RdbSave(RedisModuleIO *rdb, struct IndexSpec *sp, int contextFlags);
struct IndexSpec *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status);

// Serialize/Deserialize index specs (used for slot migration)
RedisModuleString *IndexSpec_Serialize(struct IndexSpec *sp);
int IndexSpec_Deserialize(const RedisModuleString *serialized, int encver);

// Register the IndexSpecType with Redis
int IndexSpec_RegisterType(RedisModuleCtx *ctx);

// Auxiliary RDB callbacks
int Indexes_RdbLoad(RedisModuleIO *rdb, int encver, int when);
void Indexes_RdbSave(RedisModuleIO *rdb, int when);
void Indexes_RdbSave2(RedisModuleIO *rdb, int when);

// RDB load logic callback (handles both legacy and new formats)
void *IndexSpec_RdbLoad_Logic(RedisModuleIO *rdb, int encver);

// Legacy RDB callbacks
void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver);
void IndexSpec_LegacyRdbSave(RedisModuleIO *rdb, void *value);
void IndexSpec_LegacyFree(void *spec);

// Propagate indexes during slot migration
void Indexes_Propagate(RedisModuleCtx *ctx);

// Legacy index upgrade
void IndexSpec_DropLegacyIndexFromKeySpace(struct IndexSpec *sp);
void Indexes_UpgradeLegacyIndexes(void);

// Check if SST persistence is enabled
bool CheckRdbSstPersistence(RedisModuleCtx *ctx, const char *prefix);

#ifdef __cplusplus
}
#endif

#endif  // SPEC_RDB_H
