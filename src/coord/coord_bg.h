/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "aggregate/aggregate.h"
#include "redismodule.h"
#include "rs_wall_clock.h"
#include "util/references.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct CoordBgJob CoordBgJob;
typedef void (*CoordBgJobHandler)(RedisModuleCtx *, RedisModuleString **, int, CoordBgJob *);

CoordBgJob *CoordBgJob_New(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                           CoordBgJobHandler handler, RequestSyncCtx *rsc,
                           RedisModuleCmdFunc replyCallback,
                           RedisModuleCmdFunc timeoutCallback, rs_wall_clock_ms_t timeoutMS,
                           WeakRef specRef, rs_wall_clock_ns_t coordStartTime,
                           size_t numShards);
void CoordBgJob_Run(void *arg);
void CoordBgJob_KeepRedisCtx(CoordBgJob *job);
WeakRef CoordBgJob_GetWeakRef(CoordBgJob *job);
rs_wall_clock_ns_t CoordBgJob_GetCoordStartTime(CoordBgJob *job);
size_t CoordBgJob_GetNumShards(const CoordBgJob *job);
RedisModuleBlockedClient *CoordBgJob_GetBlockedClient(CoordBgJob *job);
RequestSyncCtx *CoordBgJob_GetRequestSyncCtx(CoordBgJob *job);

#ifdef __cplusplus
}
#endif
