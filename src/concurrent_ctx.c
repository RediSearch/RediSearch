
#include "concurrent_ctx.h"

#include "dep/thpool/thpool.h"
#include "util/arr.h"
#include "rmutil/rm_assert.h"

#include <unistd.h>

///////////////////////////////////////////////////////////////////////////////////////////////

static threadpool *threadpools_g = NULL;

int CONCURRENT_POOL_INDEX = -1;
int CONCURRENT_POOL_SEARCH = -1;

//---------------------------------------------------------------------------------------------

int ConcurrentSearch_CreatePool(int numThreads) {
  if (!threadpools_g) {
    threadpools_g = array_new(threadpool, 4);
  }
  int poolId = array_len(threadpools_g);
  threadpools_g = array_append(threadpools_g, thpool_init(numThreads));
  return poolId;
}

//---------------------------------------------------------------------------------------------

// Start the concurrent search thread pool. Should be called when initializing the module
void ConcurrentSearch_ThreadPoolStart() {

  if (CONCURRENT_POOL_SEARCH == -1) {
    CONCURRENT_POOL_SEARCH = ConcurrentSearch_CreatePool(RSGlobalConfig.searchPoolSize);
    long numProcs = 0;

    if (!RSGlobalConfig.poolSizeNoAuto) {
      numProcs = sysconf(_SC_NPROCESSORS_ONLN);
    }

    if (numProcs < 1) {
      numProcs = RSGlobalConfig.indexPoolSize;
    }
    CONCURRENT_POOL_INDEX = ConcurrentSearch_CreatePool(numProcs);
  }
}

//---------------------------------------------------------------------------------------------

// Stop all the concurrent threads
void ConcurrentSearch_ThreadPoolDestroy() {
  if (!threadpools_g) {
    return;
  }
  for (size_t ii = 0; ii < array_len(threadpools_g); ++ii) {
    thpool_destroy(threadpools_g[ii]);
  }
  array_free(threadpools_g);
  threadpools_g = NULL;
}

//---------------------------------------------------------------------------------------------

// Run a function on the concurrent thread pool

void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type) {
  threadpool p = threadpools_g[type];
  thpool_add_work(p, func, arg);
}

///////////////////////////////////////////////////////////////////////////////////////////////

static void threadHandleCommand(void *p) {
  ConcurrentCmd *ctx = p;
  // Lock GIL if needed
  if (!(ctx->options & CMDCTX_NO_GIL)) {
    RedisModule_ThreadSafeContextLock(ctx->ctx);
  }

  ctx->handler(ctx->ctx, ctx->argv, ctx->argc, ctx);

  // Unlock GIL if needed
  if (!(ctx->options & CMDCTX_NO_GIL)) {
    RedisModule_ThreadSafeContextUnlock(ctx->ctx);
  }

  if (!(ctx->options & CMDCTX_KEEP_RCTX)) {
    RedisModule_FreeThreadSafeContext(ctx->ctx);
  }

  RedisModule_UnblockClient(ctx->bc, NULL);
  rm_free(ctx->argv);
  rm_free(p);
}

//---------------------------------------------------------------------------------------------

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

void ConcurrentCmd::KeepRedisCtx() {
  options |= CMDCTX_KEEP_RCTX;
}

//---------------------------------------------------------------------------------------------

ConcurrentCmd::ConcurrentCmd(int options_, ConcurrentCmdHandler handler_,
                             RedisModuleCtx *ctx_, RedisModuleString **argv_, int argc_) {
  bc = RedisModule_BlockClient(ctx_, NULL, NULL, NULL, 0);
  ctx = RedisModule_GetThreadSafeContext(bc);
  RedisModule_AutoMemory(ctx);
  handler = handler_;
  options = options_;

  argc = argc_;
  // Copy command arguments so they can be released by the calling thread
  argv = rm_calloc(argc, sizeof(RedisModuleString *));
  for (int i = 0; i < argc; i++) {
    argv[i] = RedisModule_CreateStringFromString(ctx, argv_[i]);
  }
}

//---------------------------------------------------------------------------------------------

// Used by RSCordinator
int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  auto cmd = new ConcurrentCmd(0, handler, ctx, argv, argc);
  ConcurrentSearch_ThreadPoolRun(threadHandleCommand, cmd, poolType);
  return REDISMODULE_OK
}

///////////////////////////////////////////////////////////////////////////////////////////////

void ConcurrentSearchCtx::CloseKeys() {
  size_t sz = numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKeyCtx *kx = openKeys[i];
    if (kx->key) {
      RedisModule_CloseKey(kx->key);
      kx->key = NULL;
    }
  }
}

//---------------------------------------------------------------------------------------------

void ConcurrentSearchCtx::ReopenKeys() {
  size_t sz = numOpenKeys;
  for (size_t i = 0; i < sz; i++) {
    ConcurrentKeyCtx *kx = openKeys[i];
    kx->key = RedisModule_OpenKey(ctx, kx->keyName, kx->keyFlags);
    // if the key is marked as shared, make sure it isn't now
    kx->Reopen(kx->key);
  }
}

//---------------------------------------------------------------------------------------------

// Check the elapsed timer, and release the lock if enough time has passed.
// Return 1 if switching took place

int ConcurrentSearchCtx::CheckTimer() {
  static struct timespec now;
  clock_gettime(CLOCK_MONOTONIC_RAW, &now);

  long long durationNS = (long long)1000000000 * (now.tv_sec - ctx->lastTime.tv_sec) +
                         (now.tv_nsec - ctx->lastTime.tv_nsec);

  // Timeout - release the thread safe context lock and let other threads run as well
  if (durationNS > CONCURRENT_TIMEOUT_NS) {
    Unlock();

    // Right after releasing, we try to acquire the lock again.
    // If other threads are waiting on it, the kernel will decide which one
    // will get the chance to run again. Calling sched_yield is not necessary here.
    // See http://blog.firetree.net/2005/06/22/thread-yield-after-mutex-unlock/
    Lock();

    // Right after re-acquiring the lock, we sample the current time.
    // This will be used to calculate the elapsed running time
    ResetClock();
    return 1;
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

// Reset the clock variables in the concurrent search context

void ConcurrentSearchCtx::ResetClock() {
  clock_gettime(CLOCK_MONOTONIC_RAW, &lastTime);
  ticker = 0;
}

//---------------------------------------------------------------------------------------------

// Initialize a concurrent context

ConcurrentSearchCtx::ConcurrentSearchCtx(RedisModuleCtx *rctx) {
  ctx = rctx;
  isLocked = 0;
  numOpenKeys = 0;
  openKeys = NULL;
  ResetClock();
}

//---------------------------------------------------------------------------------------------

// Initialize a concurrent context to contain a single key.
// This key can be swapped out via SetKey().

ConcurrentSearchCtx::ConcurrentSearchCtx(RedisModuleCtx *rctx, ConcurrentKey *concKey) {
  ctx = rctx;
  isLocked = 0;
  numOpenKeys = 1;
  openKeys = rm_calloc(1, sizeof(ConcurrentKey*));
  openKeys[0] = concKey;
}

//---------------------------------------------------------------------------------------------

ConcurrentSearchCtx::~ConcurrentSearchCtx() {
  // Release the monitored open keys
  for (size_t i = 0; i < numOpenKeys; i++) {
    ConcurrentKey *concKey = &openKeys[i];

    RedisModule_FreeString(ctx, concKey->keyName);

    if (concKey->key) {
      RedisModule_CloseKey(concKey->key);
      concKey->key = NULL;
    }

    delete concKey;
  }

  rm_free(ctx->openKeys);
  numOpenKeys = 0;
}

//---------------------------------------------------------------------------------------------

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

/*
void ConcurrentSearchCtx::AddKey(RedisModuleKey *key, int openFlags,
                                 RedisModuleString *keyName, ConcurrentReopenCallback cb,
                                 void *privdata, void (*freePrivDataCallback)(void *)) {
*/
void ConcurrentSearchCtx::AddKey(ConcurrentKey *concKey) {
  openKeys = rm_realloc(openKeys, ++numOpenKeys * sizeof(ConcurrentKey*));
  openKeys[numOpenKeys - 1] = concKey;
}

//---------------------------------------------------------------------------------------------

/**
 * Replace the key at a given position. The context must not be locked. It
 * is assumed that the callback for the key remains the same
 * - redisCtx is the redis module context which owns this key
 * - keyName is the name of the new key
 * - pos is the position at which the key resides (usually 0)
 * - arg is the new arg to be passed to the callback
 */

void ConcurrentSearchCtx::SetKey(RedisModuleString *keyName, void *privdata) {
  openKeys[0].keyName = keyName;
  openKeys[0].privdata = privdata;
}

//---------------------------------------------------------------------------------------------

void ConcurrentSearchCtx::Lock() {
  RS_LOG_ASSERT(!ctx->isLocked, "Redis GIL shouldn't be locked");
  RedisModule_ThreadSafeContextLock(ctx);
  isLocked = 1;
  ReopenKeys();
}

//---------------------------------------------------------------------------------------------

void ConcurrentSearchCtx::Unlock() {
  CloseKeys();
  RedisModule_ThreadSafeContextUnlock(ctx);
  isLocked = 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

ConcurrentKey::ConcurrentKey(RedisModuleKey *key, RedisModuleString *keyName, int openFlags) :
    key(key), keyName(keyName), keyFlags(openFlags) {
  RedisModule_RetainString(ctx, keyName);
}

///////////////////////////////////////////////////////////////////////////////////////////////
