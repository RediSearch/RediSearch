/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef SPEC_PARSE_H
#define SPEC_PARSE_H

#include "redismodule.h"
#include "query_error.h"
#include "util/references.h"
#include "obfuscation/hidden.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;

StrongRef IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, const HiddenString *name,
                                   RedisModuleString **argv, int argc, QueryError *status);
StrongRef IndexSpec_Parse(RedisModuleCtx *ctx, const HiddenString *name, const char **argv, int argc, QueryError *status);
StrongRef IndexSpec_ParseC(RedisModuleCtx *ctx, const char *name, const char **argv, int argc, QueryError *status);

#ifdef __cplusplus
}
#endif

#endif  // SPEC_PARSE_H
