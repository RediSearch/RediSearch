/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "concurrent_ctx.h"
#include "thpool/thpool.h"
#include <util/arr.h>
#include "rmutil/rm_assert.h"
#include "module.h"
#include "util/logging.h"
#include "coord/config.h"
#include "aggregate/aggregate.h"
#include "info/info_redis/threads/main_thread.h"

static arrayof(redisearch_thpool_t *) threadpools_g = NULL;

int ConcurrentSearch_CreatePool(int numThreads) {
  if (!threadpools_g) {
    threadpools_g = array_new(redisearch_thpool_t *, 1); // Only used by the coordinator, so 1 is enough
  }
  int poolId = array_len(threadpools_g);
  array_append(threadpools_g, redisearch_thpool_create(numThreads, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD,
                                                                       LogCallback, "coord"));

  return poolId;
}

/** Stop all the concurrent threads */
void ConcurrentSearch_ThreadPoolDestroy(void) {
  if (!threadpools_g) {
    return;
  }
  for (size_t ii = 0; ii < array_len(threadpools_g); ++ii) {
    redisearch_thpool_destroy(threadpools_g[ii]);
  }
  array_free(threadpools_g);
  threadpools_g = NULL;
}

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg, int type) {
  redisearch_thpool_t *p = threadpools_g[type];
  redisearch_thpool_add_work(p, func, arg, THPOOL_PRIORITY_HIGH);
}

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount() {
  RS_ASSERT(threadpools_g);
  // Assert we only have 1 pool
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");
  return redisearch_thpool_num_jobs_in_progress(threadpools_g[0]);
}

size_t ConcurrentSearchPool_HighPriorityPendingJobsCount() {
  RS_ASSERT(threadpools_g);
  // Assert we only have 1 pool
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");
  return redisearch_thpool_high_priority_pending_jobs(threadpools_g[0]);
}

/********************************************* for debugging **********************************/

int ConcurrentSearch_isPaused() {
  RS_ASSERT(threadpools_g);
  // Assert we only have 1 pool
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");
  return redisearch_thpool_paused(threadpools_g[0]);
}

int ConcurrentSearch_pause() {
  RS_ASSERT(threadpools_g);
  // Assert we only have 1 pool
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");

  if (clusterConfig.coordinatorPoolSize == 0 || ConcurrentSearch_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_pause_threads(threadpools_g[0]);
  return REDISMODULE_OK;
}

int ConcurrentSearch_resume() {
  RS_ASSERT(threadpools_g);
  // Assert we only have 1 pool
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");
  if (clusterConfig.coordinatorPoolSize == 0 || !ConcurrentSearch_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_resume_threads(threadpools_g[0]);
  return REDISMODULE_OK;
}

thpool_stats ConcurrentSearch_getStats() {
  thpool_stats stats = {0};
  if (!threadpools_g) {
    return stats;
  }
  RS_LOG_ASSERT(array_len(threadpools_g) == 1, "assuming 1 ConcurrentSearch pool");
  return redisearch_thpool_get_stats(threadpools_g[0]);
}
