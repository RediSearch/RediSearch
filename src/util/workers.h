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
int ThreadPool_CreatePool(uint worker_count);

// return number of threads in the pool
uint ThreadPool_ThreadCount(void);

// retrieve current thread id
// 0         redis-main
// 1..N + 1  workers
// int ThreadPool_GetThreadID
// (
// 	void
// );

// // pause all thread pools
// void ThreadPool_Pause
// (
// 	void
// );

// // resume all threads
// void ThreadPool_Resume
// (
// 	void
// );

// adds a task
int ThreadPool_AddWork(void (*function_p)(void *), void *arg_p);

// destroys thread pool, allows threads to exit gracefully
void ThreadPool_Destroy(void);
