/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef RS_CONCERRNT_CTX_
#define RS_CONCERRNT_CTX_

#include "thpool/thpool.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Coordinator thread-pool management. */

/* Destroy the coordinator thread pool. */
void ConcurrentSearch_ThreadPoolDestroy(void);

/* Create the coordinator thread pool. */
void ConcurrentSearch_CreatePool(int numThreads);

/* Run a function on the concurrent thread pool */
void ConcurrentSearch_ThreadPoolRun(void (*func)(void *), void *arg);

/* Return the underlying thread pool for direct submission. */
redisearch_thpool_t *ConcurrentSearch_GetPool(void);

/* return number of currently working threads */
size_t ConcurrentSearchPool_WorkingThreadCount();

/* return number of pending high priority jobs */
size_t ConcurrentSearchPool_HighPriorityPendingJobsCount();

/********************************************* for debugging **********************************/

int ConcurrentSearch_isPaused();

int ConcurrentSearch_pause();

int ConcurrentSearch_resume();

thpool_stats ConcurrentSearch_getStats();

#ifdef __cplusplus
}
#endif
#endif  // RS_CONCERRNT_CTX_
