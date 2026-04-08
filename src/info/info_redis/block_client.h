/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once
#include <stddef.h>           // for size_t

#include "redismodule.h"      // for RedisModuleBlockedClient, ...
#include "util/references.h"  // for StrongRef
#include "cursor.h"           // for Cursor
#include "query.h"            // for QueryAST
#include "rs_wall_clock.h"    // for rs_wall_clock_ms_t

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
RedisModuleBlockedClient* BlockCursorClient(RedisModuleCtx *ctx, Cursor* cursor, size_t count, int timeoutMS);

#ifdef __cplusplus
}
#endif
