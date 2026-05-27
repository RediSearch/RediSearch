/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "coord_bg.h"
#include "info/info_redis/threads/main_thread.h"
#include "module.h"
#include "rmalloc.h"

#define COORD_BG_JOB_KEEP_RCTX 0x01

struct CoordBgJob {
  RedisModuleBlockedClient *bc;
  RedisModuleCtx *ctx;
  CoordBgJobHandler handler;
  RedisModuleString **argv;
  int argc;
  int options;
  RequestSyncCtx *rsc;
  WeakRef specRef;
  rs_wall_clock_ns_t coordStartTime;
  size_t numShards;
};

CoordBgJob *CoordBgJob_New(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                           CoordBgJobHandler handler, RequestSyncCtx *rsc,
                           RedisModuleCmdFunc replyCallback,
                           RedisModuleCmdFunc timeoutCallback, rs_wall_clock_ms_t timeoutMS,
                           WeakRef specRef, rs_wall_clock_ns_t coordStartTime,
                           size_t numShards) {
  RS_ASSERT(timeoutMS == 0 || (timeoutCallback != NULL && replyCallback != NULL));

  CoordBgJob *job = rm_calloc(1, sizeof(*job));
  job->bc = RedisModule_BlockClient(ctx, replyCallback, timeoutCallback, RequestSyncCtx_OnFree,
                                    timeoutMS);
  RedisModule_BlockClientSetPrivateData(job->bc, rsc);
  RSC_BeginCycle(rsc, job->bc, replyCallback, REQUEST_CYCLE_QUERY, 0, 0);
  BlockedQueries *blockedQueries = MainThread_GetBlockedQueries();
  if (blockedQueries) {
    BlockedQueries_LinkQuery(blockedQueries, rsc);
  }

  job->ctx = RedisModule_GetThreadSafeContext(job->bc);
  RS_AutoMemory(job->ctx);
  job->handler = handler;
  job->argc = argc;
  job->rsc = rsc;
  job->specRef = specRef;
  job->coordStartTime = coordStartTime;
  job->numShards = numShards;

  job->argv = rm_calloc(argc, sizeof(RedisModuleString *));
  for (int i = 0; i < argc; i++) {
    job->argv[i] = RedisModule_CreateStringFromString(job->ctx, argv[i]);
  }

  RedisModule_BlockedClientMeasureTimeStart(job->bc);
  return job;
}

void CoordBgJob_Run(void *arg) {
  CoordBgJob *job = arg;

  job->handler(job->ctx, job->argv, job->argc, job);

  if (!(job->options & COORD_BG_JOB_KEEP_RCTX)) {
    RedisModule_FreeThreadSafeContext(job->ctx);
  }

  RedisModule_BlockedClientMeasureTimeEnd(job->bc);
  RedisModule_UnblockClient(job->bc, job->rsc);
  rm_free(job->argv);
  rm_free(job);
}

void CoordBgJob_KeepRedisCtx(CoordBgJob *job) {
  job->options |= COORD_BG_JOB_KEEP_RCTX;
}

WeakRef CoordBgJob_GetWeakRef(CoordBgJob *job) {
  return job->specRef;
}

rs_wall_clock_ns_t CoordBgJob_GetCoordStartTime(CoordBgJob *job) {
  return job->coordStartTime;
}

size_t CoordBgJob_GetNumShards(const CoordBgJob *job) {
  return job->numShards;
}

RedisModuleBlockedClient *CoordBgJob_GetBlockedClient(CoordBgJob *job) {
  return job->bc;
}

RequestSyncCtx *CoordBgJob_GetRequestSyncCtx(CoordBgJob *job) {
  return job->rsc;
}
