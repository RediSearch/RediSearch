/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#ifdef POWER_TO_THE_WORKERS

#include "redismodule.h"
#include "thpool/thpool.h"
#include <assert.h>

// create workers thread pool
// returns REDISMODULE_OK if thread pool created, REDISMODULE_ERR otherwise
int workersThreadPool_CreatePool(size_t worker_count);

// Initialize an existing worker thread pool.
void workersThreadPool_InitPool(size_t worker_count);

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void);

// adds a task
int workersThreadPool_AddWork(redisearch_thpool_proc, void *arg_p);

// Wait until all jobs have finished
void workersThreadPool_Wait(RedisModuleCtx *ctx);

// Terminate threads, allows threads to exit gracefully (without deallocating).
void workersThreadPool_Terminate(void);

// Destroys thread pool, can be called on uninitialized threadpool.
void workersThreadPool_Destroy(void);

#endif // POWER_TO_THE_WORKERS
