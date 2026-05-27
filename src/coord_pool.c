/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "coord_pool.h"
#include "thpool/thpool.h"
#include "rmutil/rm_assert.h"
#include "module.h"
#include "util/logging.h"
#include "coord/config.h"
#include "aggregate/aggregate.h"
#include "info/info_redis/threads/main_thread.h"

static redisearch_thpool_t *coordPool_g = NULL;

void CoordPool_CreatePool(int numThreads) {
  if (coordPool_g) {
    return;
  }

  coordPool_g = redisearch_thpool_create(numThreads, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD,
                                         LogCallback, "coord");
}

/** Stop all coordinator threads */
void CoordPool_ThreadPoolDestroy(void) {
  if (!coordPool_g) {
    return;
  }
  redisearch_thpool_destroy(coordPool_g);
  coordPool_g = NULL;
}

/* Run a function on the coordinator thread pool */
void CoordPool_ThreadPoolRun(void (*func)(void *), void *arg) {
  RS_ASSERT(coordPool_g);
  redisearch_thpool_add_work(coordPool_g, func, arg, THPOOL_PRIORITY_HIGH);
}

/* return number of currently working threads */
size_t CoordPool_WorkingThreadCount() {
  RS_ASSERT(coordPool_g);
  return redisearch_thpool_num_jobs_in_progress(coordPool_g);
}

size_t CoordPool_HighPriorityPendingJobsCount() {
  RS_ASSERT(coordPool_g);
  return redisearch_thpool_high_priority_pending_jobs(coordPool_g);
}

/********************************************* for debugging **********************************/

int CoordPool_isPaused() {
  RS_ASSERT(coordPool_g);
  return redisearch_thpool_paused(coordPool_g);
}

int CoordPool_pause() {
  RS_ASSERT(coordPool_g);

  if (clusterConfig.coordinatorPoolSize == 0 || CoordPool_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_pause_threads(coordPool_g);
  return REDISMODULE_OK;
}

int CoordPool_resume() {
  RS_ASSERT(coordPool_g);
  if (clusterConfig.coordinatorPoolSize == 0 || !CoordPool_isPaused()) {
    return REDISMODULE_ERR;
  }
  redisearch_thpool_resume_threads(coordPool_g);
  return REDISMODULE_OK;
}

thpool_stats CoordPool_getStats() {
  thpool_stats stats = {0};
  if (!coordPool_g) {
    return stats;
  }
  return redisearch_thpool_get_stats(coordPool_g);
}
