#ifndef RS_CONCERRNT_CTX_
#define RS_CONCERRNT_CTX_

#include "redisearch.h"
#include "redismodule.h"
#include <time.h>
#include <dep/thpool/thpool.h>

/** Concurrent Search Exection Context.
 *
 * We allow queries to run concurrently, each running on its own thread, locking the redis GIL
 * for a bit, releasing it, and letting others run as well.
 *
 * The queries do not really run in parallel, but one at a time, competing over the global lock.
 * This does not speed processing - in fact it can actually slow it down. But it prevents a
 * common situation, where very slow queries block the entire redis instance for a long time.
 *
 * We intend to switch this model to a single thread running multiple "coroutines", but for now
 * this naive implementation is good enough and will fix the search concurrency issue.
 *
 * The ConcurrentSearchCtx is part of a query, and the query calls the CONCURRENT_CTX_TICK macro
 * for every "cycle" - meaning a processed search result. The concurrency engine will switch
 * execution to another query when the current thread has spent enough time working.
 *
 * The current switch threshold is 200 microseconds. Since measuring time is slow in itself (~50ns)
 * we sample the elapsed time every 20 "cycles" of the query processor.
 *
 */

typedef void (*ConcurrentReopenCallback)(RedisModuleKey *k, void *ctx);

typedef enum {
  // no shared resources - the default mode
  ConcurrentKey_SharedNothing = 0x0,

  // the key name string is shared and should not be deleted
  ConcurrentKey_SharedKeyString = 0x01,

  // the key itself is shared and should not be deleted.
  // this may be rewritten on reopening
  ConcurrentKey_SharedKey = 0x02,
} ConcurrentKeyOptions;
/* ConcurrentKeyCtx is a reference to a key that's being held open during concurrent execution and
 * needs to be reopened after yielding and gaining back execution. See ConcurrentSearch_AddKey for
 * more details */
typedef struct {
  RedisModuleKey *key;
  RedisModuleString *keyName;
  int sharedKey;
  void *privdata;
  ConcurrentReopenCallback cb;
  // redis key open flags
  int keyFlags;
  // context specific flags
  ConcurrentKeyOptions opts;
  // A custom callback to free privdata. If NULL we don't do anything
  void (*freePrivData)(void *);
} ConcurrentKeyCtx;

/* The concurrent execution context struct itself. See above for details */
typedef struct {
  long long ticker;
  struct timespec lastTime;
  RedisModuleCtx *ctx;
  ConcurrentKeyCtx *openKeys;
  uint32_t numOpenKeys;
  uint32_t isLocked;
} ConcurrentSearchCtx;

/** The maximal size of the concurrent query thread pool. Since only one thread is operational at a
 * time, it's not a problem besides memory consumption, to have much more threads than CPU cores.
 * By default the pool starts with just one thread, and scales up as needed  */
#define CONCURRENT_SEARCH_POOL_SIZE 8

/**
 * The maximum number of threads performing indexing on documents.
 * It's good to set this to approximately the number of CPUs running.
 *
 * NOTE: This is merely the *fallback* value if for some reason the number of
 * CPUs cannot be automatically determined. If you want to force the number
 * of tokenizer threads, make sure you also disable the CPU detection in the
 * source file
 */
#define CONCURRENT_INDEX_POOL_SIZE 8

/** The number of execution "ticks" per elapsed time check. This is intended to reduce the number of
 * calls to clock_gettime() */
#define CONCURRENT_TICK_CHECK 25

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
void ConcurrentSearch_AddKey(ConcurrentSearchCtx *ctx, RedisModuleKey *key, int openFlags,
                             RedisModuleString *keyName, ConcurrentReopenCallback cb,
                             void *privdata, void (*freePrivDataCallback)(void *),
                             ConcurrentKeyOptions opts);

/**
 * Replace the key at a given position. The context must not be locked. It
 * is assumed that the callback for the key remains the same
 * - redisCtx is the redis module context which owns this key
 * - keyName is the name of the new key
 * - pos is the position at which the key resides (usually 0)
 * - arg is the new arg to be passed to the callback
 */
static inline void ConcurrentSearch_SetKey(ConcurrentSearchCtx *ctx, RedisModuleCtx *redisCtx,
                                           RedisModuleString *keyName, void *privdata) {
  ctx->openKeys[0].keyName = keyName;
  ctx->openKeys[0].privdata = privdata;
  ctx->ctx = redisCtx;
}

/** Start the concurrent search thread pool. Should be called when initializing the module */
void ConcurrentSearch_ThreadPoolStart();

#define CONCURRENT_POOL_INDEX 1
#define CONCURRENT_POOL_SEARCH 2

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type);

/** Check the elapsed timer, and release the lock if enough time has passed.
 * Return 1 if switching took place
 */
int ConcurrentSearch_CheckTimer(ConcurrentSearchCtx *ctx);

/** Initialize and reset a concurrent search ctx */
void ConcurrentSearchCtx_Init(RedisModuleCtx *rctx, ConcurrentSearchCtx *ctx);

/**
 * Initialize a concurrent context to contain a single key. This key can be swapped
 * out via SetKey()
 */
void ConcurrentSearchCtx_InitEx(ConcurrentSearchCtx *ctx, int mode, ConcurrentReopenCallback cb);

/** Reset the clock variables in the concurrent search context */
void ConcurrentSearchCtx_ResetClock(ConcurrentSearchCtx *ctx);

/* Free the execution context's dynamically allocated resources */
void ConcurrentSearchCtx_Free(ConcurrentSearchCtx *ctx);

void ConcurrentSearchCtx_Lock(ConcurrentSearchCtx *ctx);

void ConcurrentSearchCtx_Unlock(ConcurrentSearchCtx *ctx);

/** This macro is called by concurrent executors (currently the query only).
 * It checks if enough time has passed and releases the global lock if that is the case.
 */
#define CONCURRENT_CTX_TICK(x)                               \
  ({                                                         \
    int conctx__didSwitch = 0;                               \
    if ((x) && ++(x)->ticker % CONCURRENT_TICK_CHECK == 0) { \
      if (ConcurrentSearch_CheckTimer((x))) {                \
        conctx__didSwitch = 1;                               \
      }                                                      \
    }                                                        \
    conctx__didSwitch;                                       \
  })

#endif
