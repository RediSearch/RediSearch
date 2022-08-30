
#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "config.h"

#include "thpool/thpool.h"
#include "rmutil/vector.h"
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
 * The ConcurrentSearch is part of a query, and the query calls the Tick function
 * for every "cycle" - meaning a processed search result. The concurrency engine will switch
 * execution to another query when the current thread has spent enough time working.
 *
 * The current switch threshold is 200 microseconds. Since measuring time is slow in itself (~50ns)
 * we sample the elapsed time every 20 "cycles" of the query processor.
 *
 */

// ConcurrentKey is a reference to a key that's being held open during concurrent execution and
// needs to be reopened after yielding and gaining back execution.
// See ConcurrentSearch_AddKey for more details.

struct ConcurrentKey {
  ConcurrentKey(RedisModuleKey *key, RedisModuleString *keyName, int openFlags = REDISMODULE_READ) :
    key(key), keyName(keyName), keyFlags(openFlags) {}
  //ConcurrentKey(ConcurrentKey &&k) : key(k.key), keyName(k.keyName), keyFlags(k.openFlags) {}

  RedisModuleKey *key;
  RedisModuleString *keyName;
  int keyFlags; // redis key open flags

  virtual void Reopen(); // @@@ TODO restore = 0;
};

//---------------------------------------------------------------------------------------------

struct ConcurrentSearch {
  long long ticker;
  struct timespec lastTime;
  RedisModuleCtx *ctx;
  Vector<ConcurrentKey> concKeys; //@@@ TODO Vector<std::unique_ptr<ConcurrentKey>>
  bool isLocked;

  ConcurrentSearch(RedisModuleCtx *rctx);
  ~ConcurrentSearch();

  template <class ConcurrentKey1>
  void AddKey(ConcurrentKey1 &&concKeys);

  bool CheckTimer();
  void ResetClock();

  void Lock();
  void Unlock();

  void ReopenKeys();
  void CloseKeys();

  bool Tick();

  // Start the concurrent search thread pool. Should be called when initializing the module
  static void ThreadPoolStart();
  static void ThreadPoolDestroy();
};

#include "concurrent_ctx.hxx"

//---------------------------------------------------------------------------------------------

extern int CONCURRENT_POOL_INDEX;
extern int CONCURRENT_POOL_SEARCH;

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
};

#define CMDCTX_KEEP_RCTX 0x01
#define CMDCTX_NO_GIL 0x02

//---------------------------------------------------------------------------------------------

int ConcurrentSearch_HandleRedisCommand(int poolType, ConcurrentCmdHandler handler,
                                        RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

//---------------------------------------------------------------------------------------------

// Check if the current request can be executed in a thread
inline bool CheckConcurrentSupport(RedisModuleCtx *ctx) {
  // See if this client should be concurrent
  if (!RSGlobalConfig.concurrentMode) {
    return false;
  }

  // Redis cannot use blocked contexts in lua and/or multi commands. Concurrent
  // search relies on blocking a client. In such cases, force non-concurrent
  // search mode.
  if (RedisModule_GetContextFlags && (RedisModule_GetContextFlags(ctx) &
                                     (REDISMODULE_CTX_FLAGS_LUA | REDISMODULE_CTX_FLAGS_MULTI))) {
    return false;
  }
  return true;
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
