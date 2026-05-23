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
struct RequestSyncCtx;

typedef RedisModuleCmdFunc BlockedClientTimeoutCB;
typedef RedisModuleCmdFunc BlockedClientReplyCB;
RedisModuleBlockedClient *BlockQueryClientWithTimeout(RedisModuleCtx *ctx, StrongRef spec,
                                                      struct RequestSyncCtx *rsc, QueryAST *ast,
                                                      BlockedClientReplyCB replyCallback,
                                                      BlockedClientTimeoutCB timeoutCallback,
                                                      rs_wall_clock_ms_t timeoutMS);
RedisModuleBlockedClient *BlockCursorClientWithTimeout(RedisModuleCtx *ctx, Cursor *cursor, size_t count,
                                                       struct RequestSyncCtx *rsc,
                                                       BlockedClientReplyCB replyCallback,
                                                       BlockedClientTimeoutCB timeoutCallback,
                                                       rs_wall_clock_ms_t timeoutMS);

#ifdef __cplusplus
}
#endif
