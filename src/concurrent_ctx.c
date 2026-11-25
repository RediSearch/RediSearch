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
#include "rmutil/rm_assert.h"
#include "module.h"
#include "util/logging.h"
#include "coord/config.h"

static redisearch_thpool_t *coordinatorPool = NULL;

void ConcurrentSearch_CreatePool(int numThreads) {
  RS_ASSERT(!coordinatorPool);
  coordinatorPool = redisearch_thpool_create(numThreads, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD,
                                             LogCallback, "coord");
}

void ConcurrentSearch_ThreadPoolDestroy(void) {
  if (!coordinatorPool) {
    return;
  }
  redisearch_thpool_destroy(coordinatorPool);
  coordinatorPool = NULL;
}

void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg) {
  redisearch_thpool_add_work(ConcurrentSearch_GetPool(), func, arg, THPOOL_PRIORITY_HIGH);
}

redisearch_thpool_t *ConcurrentSearch_GetPool(void) {
  RS_ASSERT(coordinatorPool);
  return coordinatorPool;
}

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount() {
  RS_ASSERT(coordinatorPool);
  return redisearch_thpool_num_jobs_in_progress(coordinatorPool);
}

size_t ConcurrentSearchPool_HighPriorityPendingJobsCount() {
  RS_ASSERT(coordinatorPool);
  return redisearch_thpool_high_priority_pending_jobs(coordinatorPool);
}

/********************************************* for debugging **********************************/

int ConcurrentSearch_isPaused() {
  RS_ASSERT(coordinatorPool);
  return redisearch_thpool_paused(coordinatorPool);
}

int ConcurrentSearch_pause() {
  RS_ASSERT(coordinatorPool);

  if (clusterConfig.coordinatorPoolSize == 0 || ConcurrentSearch_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_pause_threads(coordinatorPool);
  return REDISMODULE_OK;
}

int ConcurrentSearch_resume() {
  RS_ASSERT(coordinatorPool);
  if (clusterConfig.coordinatorPoolSize == 0 || !ConcurrentSearch_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_resume_threads(coordinatorPool);
  return REDISMODULE_OK;
}

thpool_stats ConcurrentSearch_getStats() {
  thpool_stats stats = {0};
  if (!coordinatorPool) {
    return stats;
  }
  return redisearch_thpool_get_stats(coordinatorPool);
}
