/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#ifdef MT_BUILD

#include "redismodule.h"
#include "thpool/thpool.h"
#include "config.h"
#include <assert.h>

#define USE_BURST_THREADS() (RSGlobalConfig.numWorkerThreads && RSGlobalConfig.mt_mode == MT_MODE_RCE)

// create workers thread pool
// returns REDISMODULE_OK if thread pool created, REDISMODULE_ERR otherwise
int workersThreadPool_CreatePool(size_t worker_count);

// Initialize an existing worker thread pool.
void workersThreadPool_InitPool();

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

// Initialize the worker thread pool based on the model configuration.
void workersThreadPool_InitIfRequired(void);

// Terminates the running workers pool after all pending jobs are done.
void workersThreadPool_waitAndTerminate(RedisModuleCtx *ctx);

#endif // MT_BUILD
