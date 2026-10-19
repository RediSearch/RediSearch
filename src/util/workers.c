/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "workers.h"
#include "redismodule.h"
#include "config.h"
#include "logging.h"
#include "rmutil/rm_assert.h"
#include "VecSim/vec_sim.h"

#include <sys/param.h>

//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

redisearch_thpool_t *_workers_thpool = NULL;
size_t yield_counter = 0;
size_t in_event = 0; // event counter, >0 means we should be in event mode (some events can start before others end)

#define DEFERRED_SHRINK_POLL_MS 100
static bool shrinkDeferred = false;
// Completed-jobs count at which a deferred shrink is applied (see resizePool).
static size_t shrinkJobsDoneTarget = 0;
static bool shrinkTimerArmed = false;
static RedisModuleTimerID shrinkTimer;

static void resizePool(bool newRequest);

static void deferredShrinkCallback(RedisModuleCtx *ctx, void *data) {
  REDISMODULE_NOT_USED(ctx);
  REDISMODULE_NOT_USED(data);
  shrinkTimerArmed = false;
  resizePool(false);
}

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
}

/* Configure here anything that needs to know it cannot use the thread pool anymore */
static void workersThreadPool_OnDeactivation(size_t old_num) {
  RedisModule_Log(RSDummyContext, "notice", "Disabled workers threadpool of size %lu", old_num);
}

// Only `numWorkerThreads` routes queries to the pool (see `RunInThread`); the other floors keep
// background index jobs off the main thread, since VecSim writes in place whenever the pool is
// empty.
static size_t targetNumWorkers(void) {
  size_t worker_count = MAX(RSGlobalConfig.numWorkerThreads, RSGlobalConfig.minMaintenanceWorkers);
  if (in_event) {
    worker_count = MAX(worker_count, RSGlobalConfig.minOperationWorkers);
  }
  return worker_count;
}

// set up workers' thread pool
int workersThreadPool_CreatePool(void) {
  RS_ASSERT(_workers_thpool == NULL);
  size_t worker_count = targetNumWorkers();

  _workers_thpool = redisearch_thpool_create(worker_count, RSGlobalConfig.highPriorityBiasNum, LogCallback, "workers");
  if (_workers_thpool == NULL) return REDISMODULE_ERR;
  if (worker_count > 0) {
    workersThreadPool_OnActivation(worker_count);
  } else {
    workersThreadPool_OnDeactivation(worker_count);
  }
  // Set the shared SVS thread pool size to match the worker pool.
  VecSim_UpdateThreadPoolSize(worker_count);
  return REDISMODULE_OK;
}

/**
 * Resize the pool to `targetNumWorkers()`.
 * If new worker count is 0, the current living workers will continue to execute pending jobs and
 * then terminate. No new jobs should be added after setting the number of workers to 0.
 */
void workersThreadPool_SetNumWorkers() {
  resizePool(true);
}

// A new request re-snapshots a deferred shrink's target, so each request drains the work pending
// at its own time (e.g. an event ending mid-deferral); the poll timer keeps the existing target.
static void resizePool(bool newRequest) {
  if (_workers_thpool == NULL) return;

  size_t worker_count = targetNumWorkers();
  size_t curr_workers = redisearch_thpool_get_num_threads(_workers_thpool);

  // Shrink to the floor only once the work pending or running at request time is done, so the
  // leaving threads drain it without blocking the main thread; later jobs do not postpone it. The
  // running count includes admin jobs, which never count as done, so an empty queue also ends the
  // wait: every backlog job has then started, and leaving threads finish their current job.
  bool shrinkToFloor = worker_count > 0 && worker_count < curr_workers &&
                       !RSGlobalConfig.numWorkerThreads && RedisModule_CreateTimer;
  thpool_stats stats = {0};
  if (shrinkToFloor) {
    stats = redisearch_thpool_get_stats(_workers_thpool);
    if (newRequest || !shrinkDeferred) {
      size_t queued = stats.low_priority_pending_jobs + stats.high_priority_pending_jobs;
      if (queued) {
        shrinkDeferred = true;
        shrinkJobsDoneTarget = stats.total_jobs_done + queued + stats.num_jobs_in_progress;
        RedisModule_Log(RSDummyContext, "notice",
                        "Deferring the workers threadpool shrink from %zu to %zu threads until %zu "
                        "jobs are done in total (%zu queued and %zu running now)",
                        curr_workers, worker_count, shrinkJobsDoneTarget, queued,
                        stats.num_jobs_in_progress);
      }
    }
  }

  // The pool can only drop threads while running; workersThreadPool_resume applies the shrink.
  if (worker_count < curr_workers && redisearch_thpool_paused(_workers_thpool)) {
    RedisModule_Log(RSDummyContext, "notice",
                    "Workers threadpool is paused, deferring its shrink from %zu to %zu threads",
                    curr_workers, worker_count);
    return;
  }

  if (shrinkToFloor) {
    size_t queued = stats.low_priority_pending_jobs + stats.high_priority_pending_jobs;
    if (shrinkDeferred && stats.total_jobs_done < shrinkJobsDoneTarget && queued) {
      if (!shrinkTimerArmed) {
        shrinkTimer = RedisModule_CreateTimer(RSDummyContext, DEFERRED_SHRINK_POLL_MS,
                                              deferredShrinkCallback, NULL);
        shrinkTimerArmed = true;
      }
      return;
    }
  }
  shrinkDeferred = false;

  if (worker_count != curr_workers) {
    RedisModule_Log(RSDummyContext, "notice", "Changing workers threadpool size from %zu to %zu", curr_workers, worker_count);
  }

  if (worker_count == 0 && curr_workers > 0) {
    // Schedule in the thpool in the config_worker_reducer_job -> a pointer to
    RedisModule_Log(RSDummyContext, "notice", "Scheduling config_reduce_threads_job to remove all %zu threads when empty", curr_workers);
    redisearch_thpool_schedule_config_reduce_threads_job(_workers_thpool, curr_workers, true);
    workersThreadPool_OnDeactivation(curr_workers);
  } else if (worker_count > curr_workers) {
    size_t new_num_threads = redisearch_thpool_add_threads(_workers_thpool, worker_count - curr_workers);
    if (!curr_workers) workersThreadPool_OnActivation(worker_count);
    RS_LOG_ASSERT_FMT(new_num_threads == worker_count,
      "Attempt to change the workers thpool size to %lu "
      "resulted unexpectedly in %lu threads.", worker_count, new_num_threads);
  } else if (worker_count < curr_workers) {
    RedisModule_Log(RSDummyContext, "notice", "Scheduling config_reduce_threads_job to remove %zu threads ASAP", curr_workers - worker_count);
    redisearch_thpool_schedule_config_reduce_threads_job(_workers_thpool, curr_workers - worker_count, false);
  }

  // Notify VecSim of the (possibly new) pool size. VecSim_UpdateThreadPoolSize handles all
  // transitions: 0 sets in-place mode, >0 sets async mode and resizes the shared SVS thread pool.
  VecSim_UpdateThreadPoolSize(worker_count);
}

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void) {
  RS_ASSERT(_workers_thpool != NULL);

  return redisearch_thpool_num_jobs_in_progress(_workers_thpool);
}

size_t workersThreadPool_LowPriorityPendingJobsCount(void) {
  RS_ASSERT(_workers_thpool != NULL);

  return redisearch_thpool_low_priority_pending_jobs(_workers_thpool);
}

size_t workersThreadPool_HighPriorityPendingJobsCount(void) {
  RS_ASSERT(_workers_thpool != NULL);

  return redisearch_thpool_high_priority_pending_jobs(_workers_thpool);
}

size_t workersThreadPool_AdminPriorityPendingJobsCount(void) {
  RS_ASSERT(_workers_thpool != NULL);

  return redisearch_thpool_admin_priority_pending_jobs(_workers_thpool);
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
  RedisModule_Log(RSDummyContext, "notice", "Draining workers thread pool with threshold %zu", threshold);
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
  if (shrinkTimerArmed) {
    RedisModule_StopTimer(RSDummyContext, shrinkTimer, NULL);
    shrinkTimerArmed = false;
  }
  redisearch_thpool_destroy(_workers_thpool);
  _workers_thpool = NULL;
}

void workersThreadPool_OnEventStart() {
  in_event++;
  workersThreadPool_SetNumWorkers();
}

int workersThreadPool_OnEventEnd(bool wait) {
  in_event--;
  if (_workers_thpool == NULL) return REDISMODULE_OK;
  if (wait && in_event) {
    workersThreadPool_SetNumWorkers();
    return REDISMODULE_ERR;  // cannot wait while another event is in progress
  }
  // Wait until the jobs currently in the queue are done, blocking the main thread, so the number
  // of jobs must not be too large. Wait before shrinking to the steady-state size, so that all
  // of the event's workers drain the backlog. A paused pool would never drain.
  bool drain = wait && !redisearch_thpool_paused(_workers_thpool);
  if (drain) {
    RedisModule_Log(
        RSDummyContext, "notice",
        "Waiting for %zu queued jobs on %zu workers before resizing the workers threadpool",
        redisearch_thpool_low_priority_pending_jobs(_workers_thpool) +
            redisearch_thpool_high_priority_pending_jobs(_workers_thpool),
        redisearch_thpool_get_num_threads(_workers_thpool));
    redisearch_thpool_wait(_workers_thpool);
  }
  workersThreadPool_SetNumWorkers();
  // The shrink queues admin jobs that remove threads; wait for them too, so the queue is empty
  // when the event ends.
  if (drain) redisearch_thpool_wait(_workers_thpool);
  return REDISMODULE_OK;
}

/********************************************* for debugging **********************************/

int workerThreadPool_isPaused() {
  return _workers_thpool && redisearch_thpool_paused(_workers_thpool);
}

int workersThreadPool_pause() {
  if (!_workers_thpool || workersThreadPool_NumThreads() == 0 || workerThreadPool_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_pause_threads(_workers_thpool);
  return REDISMODULE_OK;
}

int workersThreadPool_resume() {
  if (!_workers_thpool || !workerThreadPool_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_resume_threads(_workers_thpool);
  // Apply a shrink deferred while paused.
  resizePool(false);
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
