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

#define USE_BURST_THREADS() (RSGlobalConfig.numWorkerThreads && RSGlobalConfig.mt_mode == MT_MODE_ONLY_ON_OPERATIONS)

// create workers thread pool
// returns REDISMODULE_OK if thread pool created, REDISMODULE_ERR otherwise
int workersThreadPool_CreatePool(size_t worker_count);

// Initialize an existing worker thread pool.
void workersThreadPool_InitPool();

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void);

// adds a task
int workersThreadPool_AddWork(redisearch_thpool_proc, void *arg_p);

// Wait until the workers job queue contains no more than <threshold> jobs.
void workersThreadPool_Drain(RedisModuleCtx *ctx, size_t threshold);

// Terminate threads, allows threads to exit gracefully (without deallocating).
void workersThreadPool_Terminate(void);

// Destroys thread pool, can be called on uninitialized threadpool.
void workersThreadPool_Destroy(void);

// Initialize the worker thread pool based on the model configuration.
void workersThreadPool_InitIfRequired(void);

// Actively wait and terminates the running workers pool after all pending jobs are done.
void workersThreadPool_waitAndTerminate(RedisModuleCtx *ctx);

// Set a signal for the running threads to terminate once all pending jobs are done.
void workersThreadPool_SetTerminationWhenEmpty();

/********************************************* for debugging **********************************/

int workerThreadPool_running();

int workersThreadPool_pause();

int workersThreadPool_resume();

thpool_stats workersThreadPool_getStats();

#endif // MT_BUILD
