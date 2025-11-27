/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_CONCERRNT_CTX_
#define RS_CONCERRNT_CTX_

#include "redisearch.h"
#include "redismodule.h"
#include "thpool/thpool.h"
#include "util/references.h"

/** Concurrent Search Execution Context.
 */

typedef void (*ConcurrentReopenCallback)(void *ctx);

/* Destroys all thread pools created with `ConcurrentSearch_CreatePool` */
void ConcurrentSearch_ThreadPoolDestroy(void);

/* Create a new thread pool, and return its identifying id */
int ConcurrentSearch_CreatePool(int numThreads);

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type);

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount();

bool ConcurrentSearchPool_IsCreated();

struct ConcurrentCmdCtx;
typedef void (*ConcurrentCmdHandler)(RedisModuleCtx *, RedisModuleString **, int,
                                     struct ConcurrentCmdCtx *);

#define CMDCTX_KEEP_RCTX 0x01

/**
 * Take ownership of the underlying Redis command context. Once ownership is
 * claimed, the context needs to be freed (at some point in the future) via
 * RM_FreeThreadSafeContext()
 *
 * TODO/FIXME:
 * The context is tied to a BlockedCLient, but it shouldn't actually utilize it.
 * Need to add an API to Redis to better manage a thread safe context, or to
 * otherwise 'detach' it from the Client so that trying to perform I/O on it
 * would result in an error rather than simply using a dangling pointer.
 */
void ConcurrentCmdCtx_KeepRedisCtx(struct ConcurrentCmdCtx *ctx);

// Returns the WeakRef held in the context.
WeakRef ConcurrentCmdCtx_GetWeakRef(struct ConcurrentCmdCtx *cctx);

/* Same as handleRedis command, but set flags for the concurrent context */
int ConcurrentSearch_HandleRedisCommandEx(int poolType, ConcurrentCmdHandler handler,
                                          RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                          WeakRef spec_ref);

#endif
