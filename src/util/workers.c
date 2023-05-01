/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include <pthread.h>
#include "workers.h"
#include "redismodule.h"

//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

static redisearch_threadpool _workers_thpool = NULL;

// set up workers' thread pool
int workersThreadPool_CreatePool(size_t worker_count) {
  assert(worker_count);
  assert(_workers_thpool == NULL);

  _workers_thpool = redisearch_thpool_init(worker_count);
  if (_workers_thpool == NULL) return REDISMODULE_ERR;

  return REDISMODULE_OK;
}

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void) {
  assert(_workers_thpool != NULL);

  return redisearch_thpool_num_threads_working(_workers_thpool);
}

// add task for worker thread
// DvirDu: I think we should add a priority parameter to this function
int workersThreadPool_AddWork(thpool_proc function_p, void *arg_p) {
  assert(_workers_thpool != NULL);

  return redisearch_thpool_add_work(_workers_thpool, function_p, arg_p, THPOOL_PRIORITY_HIGH);
}

// Wait until all jobs have finished
void workersThreadPool_Wait(void) {
  assert(_workers_thpool != NULL);

  redisearch_thpool_wait(_workers_thpool);
}

void workersThreadPool_Destroy(void) {
  redisearch_thpool_destroy(_workers_thpool);
}
