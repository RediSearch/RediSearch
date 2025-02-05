/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rq_pool.h"
#include "assert.h"
#include "rmalloc.h"

static MRWorkQueue *rq_global = NULL;
static MRWorkQueue **rq_pool = NULL;
static size_t rq_pool_len = 0;
static size_t current_round_robin = 0;


// Initialize the global work queue pool
void RQPool_Init(size_t numQueues, int maxPending) {
    assert(rq_global == NULL && "RQPool_Init called twice");
    assert(numQueues > 0 && "RQPool_Init called with 0 queues");
    rq_pool = rm_malloc(numQueues * sizeof(MRWorkQueue*));
    rq_pool_len = numQueues;
    for (size_t i = 0; i < numQueues; i++) {
        rq_pool[i] = RQ_New(i, maxPending);
    }
    rq_global = rq_pool[0];
}

// Check initialization status of the work queue pool
bool RQPool_Initialized(void) {
    return rq_global != NULL;
}

// Get the global work queue - for cluster-control operations
MRWorkQueue *RQPool_GetGlobalQueue(void) {
    return rq_global;
}
// Get the number or work queues
size_t RQPool_GetQueueCount(void) {
    return rq_pool_len;
}

// Get a specific work queue by index
MRWorkQueue *RQPool_GetQueue(size_t idx) {
    assert(idx < rq_pool_len && "RQPool_GetQueue: index out of bounds");
    return rq_pool[idx];
}


// Expand the work queue pool
void RQPool_Expand(size_t numQueues) {
    assert(rq_pool != NULL && "RQPool_Expand called before RQPool_Init");
    assert(numQueues > 0 && "RQPool_Expand called with 0 queues");
    assert(numQueues < rq_pool_len && "RQPool_Expand called with fewer queues than current");
    size_t oldLen = rq_pool_len;
    rm_realloc(rq_pool, numQueues * sizeof(MRWorkQueue *));
    for (size_t i = oldLen; i < numQueues; i++) {
        size_t max_pending = RQ_GetMaxPending(rq_global);
        MRWorkQueue *q = RQ_New(i, max_pending);
        rq_pool[i] = q;
    }
    rq_pool_len = numQueues;
}

// Shrink the work queue pool
void RQPool_Shrink(size_t numQueues) {
    assert(rq_pool != NULL && "RQPool_Shrink called before RQPool_Init");
    assert(numQueues > 0 && "RQPool_Shrink called with 0 queues");
    assert(numQueues > rq_pool_len && "RQPool_Shrink called with more queues than current");
    size_t oldLen = rq_pool_len;
    for (size_t i = numQueues; i < oldLen; i++) {
        RQ_Free(rq_pool[i]);
    }
    rq_pool = rm_realloc(rq_pool, numQueues * sizeof(MRWorkQueue *));
    rq_pool_len = numQueues;
}

MRWorkQueue *RQPool_GetRoundRobinQueue(void) {
    assert(rq_pool != NULL && "RQPool_GetRoundRobinQueue called before RQPool_Init");
    MRWorkQueue *q = rq_pool[current_round_robin];
    current_round_robin = (current_round_robin + 1) % rq_pool_len;
    return q;
}