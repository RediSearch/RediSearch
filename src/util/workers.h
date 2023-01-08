/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#pragma once

#include "deps/thpool/thpool.h"
#include <stdbool.h>
#include <sys/types.h>
#include <stdint.h>
#include <assert.h>

// create workers thread pool
int ThreadPool_CreatePool(int worker_count);

// return number of threads in the pool
int ThreadPool_ThreadCount(void);

// adds a task
int ThreadPool_AddWork(thpool_proc, void *arg_p);

// Wait until all jobs have finished
void ThreadPool_Wait(void);

// destroys thread pool, allows threads to exit gracefully
void ThreadPool_Destroy(void);