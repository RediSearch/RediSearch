/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include <pthread.h>
#include "workers.h"

//------------------------------------------------------------------------------
// Thread pool
//------------------------------------------------------------------------------

static threadpool _workers_thpool = NULL;

// set up workers' thread pool
// returns 0 if thread pool initialized, 1 otherwise
int ThreadPool_CreatePool(int worker_count) {
  assert(_workers_thpool == NULL);

  _workers_thpool = thpool_init(worker_count);
  if (_workers_thpool == NULL) return 1;

  return 0;
}

// return number of threads in the workers' pool
int ThreadPool_ThreadCount(void) {
  assert(_workers_thpool != NULL);

  return thpool_num_threads(_workers_thpool);
}

// add task for worker thread
int ThreadPool_AddWork(thpool_proc function_p, void *arg_p) {
  assert(_workers_thpool != NULL);

  return thpool_add_work(_workers_thpool, function_p, arg_p);
}

// Wait until all jobs have finished
void ThreadPool_Wait(void) {
  assert(_workers_thpool != NULL);

  thpool_wait(_workers_thpool);
}

void ThreadPool_Destroy(void) {
  assert(_workers_thpool != NULL);

  thpool_destroy(_workers_thpool);
}