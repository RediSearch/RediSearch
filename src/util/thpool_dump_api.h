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

/* On crash backtrace report */
typedef enum {
  FINE,
  CRASHED,
} statusOnCrash;

/**
 * This function marks that the process started collecting data from the threadpools.
 * It returns true if it is safe to start collecting information from the threadpols.
 * NOTE: The flag is turned off by ThpoolDump_done().
*/
bool ThpoolDump_test_and_start();



// Write the backtrace of the calling thread.
void ThpoolDump_log_backtrace(statusOnCrash status_on_crash, size_t thread_id);

/**
 * @brief Prepare the threadpool for dump report and pause it.
 *
 * @param threadpool     the threadpool of interest.
 * @return nothing
 */
void ThpoolDump_pause(redisearch_threadpool);

/* ====== EXAMPLE OUTPUT ON CRASH ======

        # search_=== GC THREADS LOG: ===

        # search_thread #0 backtrace:

        search_0:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b7698) [0xffffaf827698]
        search_1:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b72b4) [0xffffaf8272b4]
        search_2:linux-vdso.so.1(__kernel_rt_sigreturn+0) [0xffffb0ec2790]
        search_3:/lib/aarch64-linux-gnu/libc.so.6(+0x79dfc) [0xffffb0cb9dfc]
        search_4:/lib/aarch64-linux-gnu/libc.so.6(pthread_cond_wait+0x208) [0xffffb0cbc8fc]
        search_5:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b7cfc) [0xffffaf827cfc]
        search_6:/workspaces/Code/RediSearch/bin/linux-arm64v8-debug/search/redisearch.so(+0x2b7414) [0xffffaf827414]
        search_7:/lib/aarch64-linux-gnu/libc.so.6(+0x7d5c8) [0xffffb0cbd5c8]
        search_8:/lib/aarch64-linux-gnu/libc.so.6(+0xe5d1c) [0xffffb0d25d1c]

====== END OF EXAMPLE ====== **/
/**
 * @brief Collect and print data from all the threads in the thread pool to the crash log.
 *
 * @param threadpool            the threadpool of threads to print dump data from.
 * @param ctx                   the info ctx to print the data to.
 *
 */
void ThpoolDump_log_to_info(redisearch_threadpool,
                                   RedisModuleInfoCtx *ctx);


/**
 *
 * @brief General Collect and reply the current state data of all the threads in the thread pool.
 *
 * @param reply            A reply context to print the data to.
 * @param threadpool      the threadpool of threads to collect dump data from.
 */
void ThpoolDump_log_to_reply(redisearch_threadpool,
                                       RedisModule_Reply *reply);
/**
 *
 * @brief General cleanups after all the threadpools are done dumping their state data.
 */
void ThpoolDump_done();

/* ============================ REDISEARCH THPOOLS ============================== */

// Call ThpoolDump_pause() for all the threadpool of redisearch
void RS_ThreadpoolsPauseBeforeDump();

// Resume all the threadpools of redisearch.
void RS_ThreadpoolsResume();

#ifdef __cplusplus
}
#endif
