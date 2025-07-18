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
#include "config.h"
#include "thpool/thpool.h"
#include "util/references.h"

/** Concurrent Search Execution Context.
 */

typedef void (*ConcurrentReopenCallback)(void *ctx);

/* ConcurrentKeyCtx is a reference to a key that's being held open during concurrent execution and
 * needs to be reopened after yielding and gaining back execution. See ConcurrentSearch_AddKey for
 * more details */
typedef struct {
  void *privdata;
  ConcurrentReopenCallback cb;
  // A custom callback to free privdata. If NULL we don't do anything
  void (*freePrivData)(void *);
} ConcurrentKeyCtx;

/* The concurrent execution context struct itself. See above for details */
typedef struct {
  RedisModuleCtx *ctx;
  ConcurrentKeyCtx *openKeys;
  uint32_t numOpenKeys;
} ConcurrentSearchCtx;

/** The maximal size of the concurrent query thread pool. Since only one thread is operational at a
 * time, it's not a problem besides memory consumption, to have much more threads than CPU cores.
 * By default the pool starts with just one thread, and scales up as needed  */

/**
 * The maximum number of threads performing indexing on documents.
 * It's good to set this to approximately the number of CPUs running.
 *
 * NOTE: This is merely the *fallback* value if for some reason the number of
 * CPUs cannot be automatically determined. If you want to force the number
 * of tokenizer threads, make sure you also disable the CPU detection in the
 * source file
 */

/** The number of execution "ticks" per elapsed time check. This is intended to reduce the number of
 * calls to clock_gettime() */
#define CONCURRENT_TICK_CHECK 50

/** The timeout after which we try to switch to another query thread - in Nanoseconds */
#define CONCURRENT_TIMEOUT_NS 100000

/* Add a "monitored" key to the context. When keys are open during concurrent execution, they need
 * to be closed before we yield execution and release the GIL, and reopened when we get back the
 * execution context.
 * To simplify this, each place in the program that holds a reference to a redis key
 * based data, registers itself and the key to be automatically reopened.
 *
 * After reopening, a callback
 * is being called to notify the key holder that it has been reopened, and handle the consequences.
 * This is used by index iterators to avoid holding reference to deleted keys or changed data.
 *
 * We register the key, the flags to reopen it, a string holding its name for reopening, a callback
 * for notification, and private callback data. if freePrivDataCallback is provided, we will call it
 * when the context is freed to release the private data. If NULL is passed, we do nothing */
void ConcurrentSearch_AddKey(ConcurrentSearchCtx *ctx, ConcurrentReopenCallback cb,
                             void *privdata, void (*freePrivDataCallback)(void *));

/**
 * Replace the key at a given position. The context must not be locked. It
 * is assumed that the callback for the key remains the same
 * - redisCtx is the redis module context which owns this key
 * - keyName is the name of the new key
 * - pos is the position at which the key resides (usually 0)
 * - arg is the new arg to be passed to the callback
 */
static inline void ConcurrentSearch_SetKey(ConcurrentSearchCtx *ctx, RedisModuleString *keyName,
                                           void *privdata) {
  ctx->openKeys[0].privdata = privdata;
}

/* Destroys all thread pools created with `ConcurrentSearch_CreatePool` */
void ConcurrentSearch_ThreadPoolDestroy(void);

/* Create a new thread pool, and return its identifying id */
int ConcurrentSearch_CreatePool(int numThreads);

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type);

/** Initialize and reset a concurrent search ctx */
void ConcurrentSearchCtx_Init(RedisModuleCtx *rctx, ConcurrentSearchCtx *ctx);

/**
 * Initialize a concurrent context to contain a single key. This key can be swapped
 * out via SetKey()
 */
void ConcurrentSearchCtx_InitSingle(ConcurrentSearchCtx *ctx, RedisModuleCtx *rctx, ConcurrentReopenCallback cb);

/* Free the execution context's dynamically allocated resources */
void ConcurrentSearchCtx_Free(ConcurrentSearchCtx *ctx);

void ConcurrentSearchCtx_ReopenKeys(ConcurrentSearchCtx *ctx);

struct ConcurrentCmdCtx;
typedef void (*ConcurrentCmdHandler)(RedisModuleCtx *, RedisModuleString **, int,
                                     struct ConcurrentCmdCtx *);

#define CMDCTX_KEEP_RCTX 0x01
#define CMDCTX_NO_GIL 0x02

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

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* Same as handleRedis command, but set flags for the concurrent context */
int ConcurrentSearch_HandleRedisCommandEx(int poolType, int options, ConcurrentCmdHandler handler,
                                          RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                                          WeakRef spec_ref);

#endif
