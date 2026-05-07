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

typedef RedisModuleCmdFunc BlockedClientTimeoutCB;
typedef RedisModuleCmdFunc BlockedClientReplyCB;
typedef void (*BlockedClientFreePrivDataCB) (void *privdata);

/**
 * Context for blocking client
 */
typedef struct BlockClientCtx{
  void *privdata;
  BlockedClientReplyCB replyCallback;
  BlockedClientTimeoutCB timeoutCallback;
  BlockedClientFreePrivDataCB freePrivData;
  rs_wall_clock_ms_t timeoutMS;
  QueryAST *ast;
} BlockClientCtx;

RedisModuleBlockedClient* BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec, BlockClientCtx *blockClientCtx);
RedisModuleBlockedClient* BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor* cursor, size_t count, BlockClientCtx *blockClientCtx);

#ifdef __cplusplus
}
#endif
