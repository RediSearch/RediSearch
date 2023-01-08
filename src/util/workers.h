/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#include "deps/thpool/thpool.h"
#include <assert.h>

// create workers thread pool
int ThreadPool_CreatePool(size_t worker_count);

// return number of currently working threads
int ThreadPool_WorkingThreadCount(void);

// adds a task
int ThreadPool_AddWork(thpool_proc, void *arg_p);

// Wait until all jobs have finished
void ThreadPool_Wait(void);

// destroys thread pool, allows threads to exit gracefully
// Can be called on uninitialized threadpool.
void ThreadPool_Destroy(void);