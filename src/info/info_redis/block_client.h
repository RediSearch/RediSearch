/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
