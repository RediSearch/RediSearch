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

// create workers thread pool
// returns REDISMODULE_OK if thread pool created, REDISMODULE_ERR otherwise
int workersThreadPool_CreatePool(size_t worker_count);

/** Set the number of workers.
 * If @param worker_count is 0, the current living workers will continue to execute pending jobs and then terminate.
 * No new jobs should be added after setting the number of workers to 0.
 * @returns the number of workers ready to accept new jobs after the change.
*/
size_t workersThreadPool_SetNumWorkers(size_t worker_count);

// return number of currently working threads
size_t workersThreadPool_WorkingThreadCount(void);

// return n_threads value.
size_t workersThreadPool_NumThreads(void);

// adds a task
int workersThreadPool_AddWork(redisearch_thpool_proc, void *arg_p);

// Wait until the workers job queue contains no more than <threshold> jobs.
void workersThreadPool_Drain(RedisModuleCtx *ctx, size_t threshold);

// Terminate threads, allows threads to exit gracefully (without deallocating).
void workersThreadPool_Terminate(void);

// Destroys thread pool, can be called on uninitialized threadpool.
void workersThreadPool_Destroy(void);

// Configure the thread pool for operation start according to module configuration.
void workersThreadPool_OnEventStart(void);

/** Configure the thread pool for operation end according to module configuration.
 * @param wait - if true, the function will wait for all pending jobs to finish. */
void workersThreadPool_OnEventEnd(bool wait);

/********************************************* for debugging **********************************/

int workerThreadPool_isPaused();

int workersThreadPool_pause();

int workersThreadPool_resume();

thpool_stats workersThreadPool_getStats();

void workersThreadPool_wait();

#endif // MT_BUILD
