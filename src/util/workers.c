/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "workers_pool.h"
#include "workers.h"
#include "redismodule.h"
#include "config.h"

#include <pthread.h>

#ifdef POWER_TO_THE_WORKERS

//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

redisearch_threadpool _workers_thpool = NULL;
size_t yield_counter = 0;

static void yieldCallback(void *yieldCtx) {
  yield_counter++;
  if (yield_counter % 10 == 0 || yield_counter == 1) {
    RedisModule_Log(RSDummyContext, "notice", "Yield every 100 ms to allow redis server run while"
                    " waiting for workers to finish: call number %zu", yield_counter);
  }
  RedisModuleCtx *ctx = yieldCtx;
  RedisModule_Yield(ctx, REDISMODULE_YIELD_FLAG_CLIENTS, NULL);
}

// set up workers' thread pool
int workersThreadPool_CreatePool(size_t worker_count) {
  assert(worker_count);
  assert(_workers_thpool == NULL);

  _workers_thpool = redisearch_thpool_create(worker_count);
  if (_workers_thpool == NULL) return REDISMODULE_ERR;

  return REDISMODULE_OK;
}

void workersThreadPool_InitPool() {
  assert(_workers_thpool != NULL);

  redisearch_thpool_init(_workers_thpool);
}

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void) {
  if (!_workers_thpool) {
    return 0;
  }

  return redisearch_thpool_num_threads_working(_workers_thpool);
}

// add task for worker thread
// DvirDu: I think we should add a priority parameter to this function
int workersThreadPool_AddWork(redisearch_thpool_proc function_p, void *arg_p) {
  assert(_workers_thpool != NULL);

  return redisearch_thpool_add_work(_workers_thpool, function_p, arg_p, THPOOL_PRIORITY_HIGH);
}

// Wait until all jobs have finished
void workersThreadPool_Wait(RedisModuleCtx *ctx) {
  if (!_workers_thpool) {
    return;
  }
  if (RedisModule_Yield) {
    // Wait until all the threads in the pool finish the remaining jobs. Periodically return and
    // call RedisModule_Yield even if threads are not done yet, so redis can answer PINGs
    // (and other stuff) so that the node-watch dog won't kill redis, for example.
    redisearch_thpool_timedwait(_workers_thpool, 100, yieldCallback, ctx);
    yield_counter = 0;  // reset
  } else {
    // In Redis versions < 7, RedisModule_Yield doesn't exist. Just wait for without yield.
    redisearch_thpool_wait(_workers_thpool);
  }
}

void workersThreadPool_Terminate(void) {
  redisearch_thpool_terminate_threads(_workers_thpool);
}

void workersThreadPool_Destroy(void) {
  redisearch_thpool_destroy(_workers_thpool);
}

void workersThreadPool_InitIfRequired() {
  if(USE_BURST_THREADS()) {
    // Initialize the thread pool temporarily for fast RDB loading of vector index (if needed).
    VecSim_SetWriteMode(VecSim_WriteAsync);
    workersThreadPool_InitPool();
    RedisModule_Log(RSDummyContext, "notice", "Created workers threadpool of size %lu for loading",
                    RSGlobalConfig.numWorkerThreads);
  }
}

void workersThreadPool_waitAndTerminate(RedisModuleCtx *ctx) {
    // Wait until all the threads are finished the jobs currently in the queue. Note that we call
    // RM_Yield periodically while we wait, so we won't block redis for too long
    // (for answering PING etc.)
    if (RSGlobalConfig.numWorkerThreads == 0) return;
    workersThreadPool_Wait(ctx);
    RedisModule_Log(RSDummyContext, "notice",
                    "Done running pending background workers jobs");
    if (USE_BURST_THREADS()) {
      VecSim_SetWriteMode(VecSim_WriteInPlace);
      workersThreadPool_Terminate();
      RedisModule_Log(RSDummyContext, "notice",
                      "Terminated workers threadpool of size %lu for loading",
                      RSGlobalConfig.numWorkerThreads);
  }
}

void workersThreadPool_pauseBeforeDump() {
  redisearch_thpool_pause_before_dump(_workers_thpool);
}


void workersThreadPool_ShutdownLog(RedisModuleInfoCtx *ctx) {
  redisearch_thpool_ShutdownLog(_workers_thpool, ctx, "=== WORKERS THREADS LOG: ===");
}

#endif // POWER_TO_THE_WORKERS
