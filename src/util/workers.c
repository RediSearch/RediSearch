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
#include "rmutil/rm_assert.h"
#include "VecSim/vec_sim.h"

#include <pthread.h>

#ifdef MT_BUILD
//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

redisearch_thpool_t *_workers_thpool = NULL;
size_t yield_counter = 0;
bool in_event = false;

static void yieldCallback(void *yieldCtx) {
  yield_counter++;
  if (yield_counter % 10 == 0 || yield_counter == 1) {
    RedisModule_Log(RSDummyContext, "verbose", "Yield every 100 ms to allow redis server run while"
                    " waiting for workers to finish: call number %zu", yield_counter);
  }
  RedisModuleCtx *ctx = yieldCtx;
  RedisModule_Yield(ctx, REDISMODULE_YIELD_FLAG_CLIENTS, NULL);
}

/* Configure here anything that needs to know it can use the thread pool */
static void workersThreadPool_OnActivation(size_t new_num) {
  // Log that we've enabled the thread pool.
  RedisModule_Log(RSDummyContext, "notice", "Enabled workers threadpool of size %lu", new_num);
  // Change VecSim write mode temporarily for fast RDB loading of vector index (if needed).
  VecSim_SetWriteMode(VecSim_WriteAsync);
}

/* Configure here anything that needs to know it cannot use the thread pool anymore */
static void workersThreadPool_OnDeactivation(size_t old_num) {
  RedisModule_Log(RSDummyContext, "notice", "Disabled workers threadpool of size %lu", old_num);
  VecSim_SetWriteMode(VecSim_WriteInPlace);
}

// set up workers' thread pool
int workersThreadPool_CreatePool(size_t worker_count) {
  RS_ASSERT(_workers_thpool == NULL);

  _workers_thpool = redisearch_thpool_create(worker_count, RSGlobalConfig.highPriorityBiasNum, LogCallback, "workers");
  if (_workers_thpool == NULL) return REDISMODULE_ERR;
  if (worker_count > 0) {
    workersThreadPool_OnActivation(worker_count);
  } else {
    workersThreadPool_OnDeactivation(worker_count);
  }
  return REDISMODULE_OK;
}

/**
 * Set the number of workers according to the configuration.
 * Global input:
 * @param numWorkerThreads (from RSGlobalConfig),
 * @param minOperationWorkers (from RSGlobalConfig).
 * @param in_event (global flag in this file).
 * New workers number should be `in_event ? MAX(numWorkerThreads, minOperationWorkers) : numWorkerThreads`.
 * This function also handles the cases where the thread pool is turned on/off.
 * If new worker count is 0, the current living workers will continue to execute pending jobs and then terminate.
 * No new jobs should be added after setting the number of workers to 0.
 */
void workersThreadPool_SetNumWorkers() {
  if (_workers_thpool == NULL) return;

  size_t worker_count = RSGlobalConfig.numWorkerThreads;
  if (in_event && RSGlobalConfig.minOperationWorkers > worker_count) {
    worker_count = RSGlobalConfig.minOperationWorkers;
  }
  size_t curr_workers = redisearch_thpool_get_num_threads(_workers_thpool);
  size_t new_num_threads = worker_count;

  if (worker_count == 0 && curr_workers > 0) {
    redisearch_thpool_terminate_when_empty(_workers_thpool);
    new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers);
    workersThreadPool_OnDeactivation(curr_workers);
  } else if (worker_count > curr_workers) {
    new_num_threads = redisearch_thpool_add_threads(_workers_thpool, worker_count - curr_workers);
    if (!curr_workers) workersThreadPool_OnActivation(worker_count);
  } else if (worker_count < curr_workers) {
    new_num_threads = redisearch_thpool_remove_threads(_workers_thpool, curr_workers - worker_count);
  }

  RS_LOG_ASSERT_FMT(new_num_threads == worker_count,
    "Attempt to change the workers thpool size to %lu "
    "resulted unexpectedly in %lu threads.", worker_count, new_num_threads);
}

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void) {
  RS_ASSERT(_workers_thpool != NULL);

  return redisearch_thpool_num_jobs_in_progress(_workers_thpool);
}

// return n_threads value.
size_t workersThreadPool_NumThreads(void) {
  RS_ASSERT(_workers_thpool);
  return redisearch_thpool_get_num_threads(_workers_thpool);
}

// add task for worker thread
// DvirDu: I think we should add a priority parameter to this function
int workersThreadPool_AddWork(redisearch_thpool_proc function_p, void *arg_p) {
  RS_ASSERT(_workers_thpool != NULL);

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
  in_event = true;
  workersThreadPool_SetNumWorkers();
}

void workersThreadPool_OnEventEnd(bool wait) {
  in_event = false;
  workersThreadPool_SetNumWorkers();
  // Wait until all the threads are finished the jobs currently in the queue. Note that we call
  // block main thread while we wait, so we have to make sure that number of jobs isn't too large.
  // no-op if numWorkerThreads == minOperationWorkers == 0
  if (wait) {
    redisearch_thpool_wait(_workers_thpool);
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
