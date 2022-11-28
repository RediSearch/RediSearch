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
int ThreadPool_CreatePool(uint worker_count) {
  assert(_workers_thpool == NULL);

  _workers_thpool = thpool_init(worker_count);
  if (_workers_thpool == NULL) return 1;

  return 0;
}

// return number of threads in the workers' pool
uint ThreadPool_ThreadCount(void) {
  assert(_workers_thpool != NULL);

  return thpool_num_threads(_workers_thpool);
}

// retrieve current thread id
// 0         redis-main
// 1..N + 1  workers
// int ThreadPool_GetThreadID
// (
// 	void
// ) {
// 	assert(_workers_thpool != NULL);

// 	// thpool_get_thread_id returns -1 if pthread_self isn't in the thread pool
// 	// most likely Redis main thread
// 	int thread_id;
// 	pthread_t pthread = pthread_self();

// 	// search in workers pool
// 	thread_id = thpool_get_thread_id(_workers_thpool, pthread);
// 	// compensate for Redis main thread
// 	if(thread_id != -1) return thread_id + 1;

// 	return 0; // assuming Redis main thread
// }

// // pause all thread pools
// void ThreadPool_Pause
// (
// 	void
// ) {
// 	assert(_workers_thpool != NULL);

// 	thpool_pause(_workers_thpool);
// }

// void ThreadPool_Resume
// (
// 	void
// ) {

// 	assert(_workers_thpool != NULL);

// 	thpool_resume(_workers_thpool);
// }

// add task for worker thread
int ThreadPool_AddWork(void (*function_p)(void *), void *arg_p) {
  assert(_workers_thpool != NULL);

  return thpool_add_work(_workers_thpool, function_p, arg_p);
}

void ThreadPool_Destroy(void) {
  assert(_workers_thpool != NULL);

  thpool_destroy(_workers_thpool);
}
