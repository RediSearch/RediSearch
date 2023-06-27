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
#include "aggregate/aggregate.h"

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

// Wait until all jobs have finished
void workersThreadPool_Wait(RedisModuleCtx *ctx);

// Terminate threads, allows threads to exit gracefully (without deallocating).
void workersThreadPool_Terminate(void);

// Destroys thread pool, can be called on uninitialized threadpool.
void workersThreadPool_Destroy(void);

// Initialize the worker thread pool based on the model configuration.
void workersThreadPool_InitIfRequired(void);

// Actively wait and terminates the running workers pool after all pending jobs are done.
void workersThreadPool_waitAndTerminate(RedisModuleCtx *ctx);

// Pause the workers before we start collecting crash info.
void workersThreadPool_PauseBeforeDump();

// Return threads to the original execution point where pause was called.
void workersThreadPool_Resume();

// Collect and print crash info.
void workersThreadPool_log_state_to_info(RedisModuleInfoCtx *ctx);

// Print the current backtrace of the workers threads.
void workersThreadPool_log_state_to_reply(RedisModule_Reply *reply);

// Set a signal for the running threads to terminate once all pending jobs are done.
void workersThreadPool_SetTerminationWhenEmpty();

// Save the request currently running in the calling thread to the thread:running_request dictionary.
void workersThreadPool_TrackReq(AREQ *req);

// Remove the request associated with the calling thread from the thread:running_request dictionary.
void workersThreadPool_UnTrackReq();

#endif // MT_BUILD
