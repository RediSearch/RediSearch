
#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "config.h"

#include "dep/thpool/thpool.h"

#include <time.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#if defined(__FreeBSD__)
#define CLOCK_MONOTONIC_RAW CLOCK_MONOTONIC
#endif

//---------------------------------------------------------------------------------------------

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

// ConcurrentKey is a reference to a key that's being held open during concurrent execution and
// needs to be reopened after yielding and gaining back execution.
// See ConcurrentSearch_AddKey for more details.

struct ConcurrentKey {
  ConcurrentKey(RedisModuleKey *key, RedisModuleString *keyName, int openFlags = REDISMODULE_READ);
  virtual ~ConcurrentKey() {}

  RedisModuleKey *key;
  RedisModuleString *keyName;
  // redis key open flags
  int keyFlags;

#if 0
  //@@ int sharedKey;
  ConcurrentReopenCallback cb;
  void *privdata;
  // A custom callback to free privdata. If NULL we don't do anything
  void (*freePrivData)(void *);
#endif // 0

  virtual void Reopen(RedisModuleKey *k);
};

//---------------------------------------------------------------------------------------------

struct ConcurrentSearchCtx {
  long long ticker;
  struct timespec lastTime;
  RedisModuleCtx *ctx;
  ConcurrentKey **openKeys;
  uint32_t numOpenKeys;
  uint32_t isLocked;

  ConcurrentSearchCtx(RedisModuleCtx *rctx);
  ConcurrentSearchCtx(RedisModuleCtx *rctx, ConcurrentKey *concKey);
  ~ConcurrentSearchCtx();

  void AddKey(ConcurrentKey *concKey);
  void SetKey(RedisModuleString *keyName, void *privdata);

  void InitSingle(RedisModuleCtx *rctx, int mode, ConcurrentReopenCallback cb);

  int CheckTimer();
  void ResetClock();

  void Lock();
  void Unlock();

  void ReopenKeys();
};

//---------------------------------------------------------------------------------------------

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

// The number of execution "ticks" per elapsed time check. This is intended to reduce the number of
// calls to clock_gettime()
#define CONCURRENT_TICK_CHECK 50

// The timeout after which we try to switch to another query thread - in Nanoseconds
#define CONCURRENT_TIMEOUT_NS 100000

//---------------------------------------------------------------------------------------------

extern int CONCURRENT_POOL_INDEX;
extern int CONCURRENT_POOL_SEARCH;

// Start the concurrent search thread pool. Should be called when initializing the module
void ConcurrentSearch_ThreadPoolStart();
void ConcurrentSearch_ThreadPoolDestroy();

// Create a new thread pool, and return its identifying id 
int ConcurrentSearch_CreatePool(int numThreads);

// Run a function on the concurrent thread pool
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type);

//---------------------------------------------------------------------------------------------

typedef void (*ConcurrentCmdHandler)(RedisModuleCtx *, RedisModuleString **, int, struct ConcurrentCmd *);

struct ConcurrentCmd : public Object {
  RedisModuleBlockedClient *bc;
  RedisModuleCtx *ctx;
  ConcurrentCmdHandler handler;
  RedisModuleString **argv;
  int argc;
  int options;

  ConcurrentCmd(int options, ConcurrentCmdHandler handler, RedisModuleCtx *ctx,
                RedisModuleString **argv, int argc);

  void KeepRedisCtx();
};

#define CMDCTX_KEEP_RCTX 0x01
#define CMDCTX_NO_GIL 0x02

//---------------------------------------------------------------------------------------------

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// This macro is called by concurrent executors (currently the query only).
// It checks if enough time has passed and releases the global lock if that is the case.

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

//---------------------------------------------------------------------------------------------

// Check if the current request can be executed in a thread

inline int CheckConcurrentSupport(RedisModuleCtx *ctx) {
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

///////////////////////////////////////////////////////////////////////////////////////////////

class ThreadSafeCtx {
public:
  ThreadSafeCtx(RedisModuleBlockedClient *bc = NULL) {
    ctx = RedisModule_GetThreadSafeContext(bc);
  }

  ThreadSafeCtx(const ThreadSafeCtx &) = delete;

  ~ThreadSafeCtx() {
    RedisModule_FreeThreadSafeContext(ctx);
  }

  operator RedisModuleCtx *() { return ctx; }

  RedisModuleCtx *ctx;
};

///////////////////////////////////////////////////////////////////////////////////////////////
