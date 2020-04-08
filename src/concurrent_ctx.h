#ifndef RS_CONCERRNT_CTX_
#define RS_CONCERRNT_CTX_

#include "redisearch.h"
#include "redismodule.h"
#include "config.h"
#include <time.h>
#include <dep/thpool/thpool.h>

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

/** Start the concurrent search thread pool. Should be called when initializing the module */
void ConcurrentSearch_ThreadPoolStart();
void ConcurrentSearch_ThreadPoolDestroy(void);

/* Create a new thread pool, and return its identifying id */
int ConcurrentSearch_CreatePool(int numThreads);

extern int CONCURRENT_POOL_INDEX;
extern int CONCURRENT_POOL_SEARCH;

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type);

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

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* Same as handleRedis command, but set flags for the concurrent context */
int ConcurrentSearch_HandleRedisCommandEx(int poolType, int options, ConcurrentCmdHandler handler,
                                          RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Check if the current request can be executed in a threadb
static inline int CheckConcurrentSupport(RedisModuleCtx *ctx) {
  // See if this client should be concurrent
  if (!RSGlobalConfig.concurrentMode) {
    return 0;
  }

  // Redis cannot use blocked contexts in lua and/or multi commands. Concurrent
  // search relies on blocking a client. In such cases, force non-concurrent
  // search mode.
  if (RedisModule_GetContextFlags && (RedisModule_GetContextFlags(ctx) &
                                      (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI))) {
    return 0;
  }
  return 1;
}

#endif
