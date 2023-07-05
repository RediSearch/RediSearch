/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdlib.h>
#include "thpool/thpool.h"
#include "reply.h"

/**
 * This is an API to collect state data from redis search threadpools' threads
 *  * NOTE: These functions are not **threadpool** safe.
 * General dump flow synchronization:
 * Handling thread: signal threads in the thread pool - wait until all the threads are paused - init buffer and mark buffer as ready- wait until the threads are done writing to the buffer----- print log - resume threads-
 * thread:          mark itself as paused and wait for the buffer initialization ---------------------------------------------------- write current state info to the buffer and wait for resume ---------------------------- resume
 *
 * Please check if it is safe to start a data collecting process by calling thpool_dump_test_and_start()
 */

/* =================================== STRUCTURES ======================================= */

/* On crash backtrace report */
typedef enum {
  FINE,
  CRASHED,
} statusOnCrash;

/* =================================== GENERAL ======================================= */

/**
 * This function marks that the process started collecting data from the threadpools.
 * It returns true if it is safe to start collecting information from the threadpols.
 * NOTE: The flag is turned off by void ThpoolDump_finish().
*/
bool ThpoolDump_test_and_start();

void ThpoolDump_finish();

/**
 * Dump all the state of all the threads known to the process to the reply.
 * @return Always returns REDISMODULE_OK.
*/
int ThpoolDump_all_to_reply(RedisModule_Reply *reply);

void ThpoolDump_all_to_info(RedisModuleInfoCtx *ctx);

/**
 * Check if it's ok to give the threads "green light" to start writing their dump data.
*/
bool ThpoolDump_all_ready();

/**
 * Check if it's we are in collecting data from all the process threads mode
*/
bool ThpoolDump_collect_all_mode();
/* =================================== THPOOL ======================================= */

/**
 *
 * @brief   Collect and reply the current state data of all the threads in the thread pool.
 *
 * @param reply            A reply context to print the data to.
 * @param threadpool      the threadpool of threads to collect dump data from.
 *
 * @return REDISMODULE_ERR if the threadpool doesn't exist.
 */
int ThpoolDump_collect_and_log_to_reply(redisearch_threadpool,
                                       RedisModule_Reply *reply);

/* =================================== THREADS API ======================================= */

/**
 * Write the backtrace of the calling thread.
*/
void ThpoolDump_log_backtrace(statusOnCrash status_on_crash, size_t thread_id);

#ifdef __cplusplus
}
#endif
