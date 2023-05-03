/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#ifdef POWER_TO_THE_WORKERS

#include "thpool/thpool.h"
#include <assert.h>

// create workers thread pool
// returns REDISMODULE_OK if thread pool initialized, REDISMODULE_ERR otherwise
int workersThreadPool_CreatePool(size_t worker_count);

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void);

// adds a task
int workersThreadPool_AddWork(redisearch_thpool_proc, void *arg_p);

// Wait until all jobs have finished
void workersThreadPool_Wait(void);

// destroys thread pool, allows threads to exit gracefully
// Can be called on uninitialized threadpool.
void workersThreadPool_Destroy(void);

#endif // POWER_TO_THE_WORKERS
