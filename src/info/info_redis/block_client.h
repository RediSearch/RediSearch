/*
* Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */
#pragma once
#include "redismodule.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

struct IndexSpec;
struct AREQ;
struct Cursor;
RedisModuleBlockedClient* BlockQueryClient(RedisModuleCtx *ctx, StrongRef spec, struct AREQ* req, int timeoutMS);
RedisModuleBlockedClient* BlockCursorClient(RedisModuleCtx *ctx, Cursor* cursor, size_t count, int timeoutMS);

#ifdef __cplusplus
}
#endif
