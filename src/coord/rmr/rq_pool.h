/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#include "rq.h"
#include "stdbool.h"

// Initialize the global work queue pool
void RQPool_Init(size_t numQueues, int maxPending);

// Check initialization status of the work queue pool
bool RQPool_Initialized(void);

// Get the global work queue - for cluster-control operations
MRWorkQueue *RQPool_GetGlobalQueue(void);

// Get the number or work queues
size_t RQPool_GetQueueCount(void);

// Get a specific work queue by index
MRWorkQueue *RQPool_GetQueue(size_t idx);

// Expand the work queue pool
void RQPool_Expand(size_t numQueues);

// Shrink the work queue pool
void RQPool_Shrink(size_t numQueues);

// Gets a queue from a pool in a round-robin fashion
MRWorkQueue *RQPool_GetRoundRobinQueue(void);
