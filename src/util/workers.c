/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "workers_pool.h"
#include "workers.h"
#include "redismodule.h"
#include "config.h"
#include "logging.h"

#include <pthread.h>

#ifdef MT_BUILD
//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

redisearch_thpool_t *_workers_thpool = NULL;
size_t yield_counter = 0;

static void yieldCallback(void *yieldCtx) {
  yield_counter++;
  if (yield_counter % 10 == 0 || yield_counter == 1) {
    RedisModule_Log(RSDummyContext, "verbose", "Yield every 100 ms to allow redis server run while"
                    " waiting for workers to finish: call number %zu", yield_counter);
  }
  RedisModuleCtx *ctx = yieldCtx;
  RedisModule_Yield(ctx, REDISMODULE_YIELD_FLAG_CLIENTS, NULL);
}

// set up workers' thread pool
int workersThreadPool_CreatePool(size_t worker_count) {
  assert(_workers_thpool == NULL);

  _workers_thpool = redisearch_thpool_create(worker_count, RSGlobalConfig.highPriorityBiasNum, LogCallback, "workers");
  if (_workers_thpool == NULL) return REDISMODULE_ERR;

  return REDISMODULE_OK;
}

size_t workersThreadPool_SetNumWorkers(size_t worker_count) {
  if(_workers_thpool == NULL) return worker_count;

  size_t curr_workers = redisearch_thpool_get_num_threads(_workers_thpool);
  size_t new_num_threads = worker_count;

  if(worker_count == 0) {
    RedisModule_Log(RSDummyContext, "notice", "Terminating workers threadpool. Executing pending jobs and terminating threads");
    redisearch_thpool_terminate_when_empty(_workers_thpool);
    new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers);
  } else if (worker_count > curr_workers) {
    new_num_threads = redisearch_thpool_add_threads(_workers_thpool, worker_count - curr_workers);
  } else if (worker_count < curr_workers) {
    new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers - worker_count);
  }

  return new_num_threads;
}

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void) {
  assert(_workers_thpool != NULL);

  return redisearch_thpool_num_jobs_in_progress(_workers_thpool);
}

// return n_threads value.
size_t workersThreadPool_NumThreads(void) {
  assert(_workers_thpool);
  return redisearch_thpool_get_num_threads(_workers_thpool);
}

// add task for worker thread
// DvirDu: I think we should add a priority parameter to this function
int workersThreadPool_AddWork(redisearch_thpool_proc function_p, void *arg_p) {
  assert(_workers_thpool != NULL);

  return redisearch_thpool_add_work(_workers_thpool, function_p, arg_p, THPOOL_PRIORITY_HIGH);
}

// Wait until job queue contains no more than <threshold> pending jobs.
void workersThreadPool_Drain(RedisModuleCtx *ctx, size_t threshold) {
  if (!_workers_thpool || redisearch_thpool_paused(_workers_thpool)) {
    return;
  }
  if (RedisModule_Yield) {
    // Wait until all the threads in the pool run the jobs until there are no more than <threshold>
    // jobs in the queue. Periodically return and call RedisModule_Yield, so redis can answer PINGs
    // (and other stuff) so that the node-watch dog won't kill redis, for example.
    redisearch_thpool_drain(_workers_thpool, 100, yieldCallback, ctx, threshold);
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

void workersThreadPool_OnEventStart() {
  if (RSGlobalConfig.minOperationWorkers > RSGlobalConfig.numWorkerThreads) {
    workersThreadPool_SetNumWorkers(RSGlobalConfig.minOperationWorkers);
  }
  /* Configure here anything that needs to know it can use the thread pool */
  if (!RSGlobalConfig.numWorkerThreads && RSGlobalConfig.minOperationWorkers) {
    // Change VecSim write mode temporarily for fast RDB loading of vector index (if needed).
    VecSim_SetWriteMode(VecSim_WriteAsync);

    // Finally, log that we've enabled the thread pool.
    RedisModule_Log(RSDummyContext, "notice", "Enabled workers threadpool of size %lu",
                    RSGlobalConfig.minOperationWorkers);
  }
}

void workersThreadPool_OnEventEnd(bool wait) {
  if (RSGlobalConfig.minOperationWorkers > RSGlobalConfig.numWorkerThreads) {
    workersThreadPool_SetNumWorkers(RSGlobalConfig.numWorkerThreads);
  }
  if (wait) {
    // Wait until all the threads are finished the jobs currently in the queue. Note that we call
    // block main thread while we wait, so we have to make sure that number of jobs isn't too large.
    // no-op if numWorkerThreads == minOperationWorkers == 0
    redisearch_thpool_wait(_workers_thpool);
  }
  /* Configure here anything that needs to know it cannot use the thread pool anymore */
  if (!RSGlobalConfig.numWorkerThreads && RSGlobalConfig.minOperationWorkers) {
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    RedisModule_Log(RSDummyContext, "notice", "Disabled workers threadpool of size %lu",
                    RSGlobalConfig.minOperationWorkers);
  }
}

/********************************************* for debugging **********************************/

int workerThreadPool_isPaused() {
  return redisearch_thpool_paused(_workers_thpool);
}

int workersThreadPool_pause() {
  if (!_workers_thpool || RSGlobalConfig.numWorkerThreads == 0 || workerThreadPool_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_pause_threads(_workers_thpool);
  return REDISMODULE_OK;
}

int workersThreadPool_resume() {
  if (!_workers_thpool || RSGlobalConfig.numWorkerThreads == 0 || !workerThreadPool_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_resume_threads(_workers_thpool);
  return REDISMODULE_OK;
}

thpool_stats workersThreadPool_getStats() {
  thpool_stats stats = {0};
  if (!_workers_thpool) {
    return stats;
  }
  return redisearch_thpool_get_stats(_workers_thpool);
}

void workersThreadPool_wait() {
  if (!_workers_thpool || workerThreadPool_isPaused()) {
    return;
  }
  redisearch_thpool_wait(_workers_thpool);
}

#endif // MT_BUILD
