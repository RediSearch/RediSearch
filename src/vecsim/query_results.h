/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <stdlib.h>
#include <stdbool.h>

#include "vec_sim_common.h"

#ifdef __cplusplus
extern "C" {
#endif

// The possible ordering for results that return from a query
typedef enum { BY_SCORE, BY_ID, BY_SCORE_THEN_ID } VecSimQueryReply_Order;

typedef enum {
    VecSim_QueryReply_OK = VecSim_OK,
    VecSim_QueryReply_TimedOut,
} VecSimQueryReply_Code;

////////////////////////////////////// VecSimQueryResult API //////////////////////////////////////

/**
 * @brief A single query result. This is an opaque object from which a user can get the result
 * vector id and score (comparing to the query vector).
 */
typedef struct VecSimQueryResult VecSimQueryResult;

/**
 * @brief Get the id of the result vector. If item is nullptr, return INVALID_ID (defined as the
 * -1).
 */
int64_t VecSimQueryResult_GetId(const VecSimQueryResult *item);

/**
 * @brief Get the score of the result vector. If item is nullptr, return INVALID_SCORE (defined as
 * the special value of NaN).
 */
double VecSimQueryResult_GetScore(const VecSimQueryResult *item);

////////////////////////////////////// VecSimQueryReply API ///////////////////////////////////////

/**
 * @brief An opaque object from which results can be obtained via iterator.
 */
typedef struct VecSimQueryReply VecSimQueryReply;

/**
 * @brief Get the length of the result list that returned from a query.
 */
size_t VecSimQueryReply_Len(VecSimQueryReply *results);

/**
 * @brief Get the return code of a query.
 */
VecSimQueryReply_Code VecSimQueryReply_GetCode(VecSimQueryReply *results);

/**
 * @brief Release the entire query results list.
 */
void VecSimQueryReply_Free(VecSimQueryReply *results);

////////////////////////////////// VecSimQueryReply_Iterator API //////////////////////////////////

/**
 * @brief Iterator for going over the list of results that had returned form a query
 */
typedef struct VecSimQueryReply_Iterator VecSimQueryReply_Iterator;

/**
 * @brief Create an iterator for going over the list of results. The iterator needs to be free
 * with VecSimQueryReply_IteratorFree.
 */
VecSimQueryReply_Iterator *VecSimQueryReply_GetIterator(VecSimQueryReply *results);

/**
 * @brief Advance the iterator, so it will point to the next item, and return the value.
 * The first call will return the first result. This will return NULL once the iterator is depleted.
 */
VecSimQueryResult *VecSimQueryReply_IteratorNext(VecSimQueryReply_Iterator *iterator);

/**
 * @brief Return true while the iterator points to some result, false if it is depleted.
 */
bool VecSimQueryReply_IteratorHasNext(VecSimQueryReply_Iterator *iterator);

/**
 * @brief Rewind the iterator to the beginning of the result list
 */
void VecSimQueryReply_IteratorReset(VecSimQueryReply_Iterator *iterator);

/**
 * @brief Release the iterator
 */
void VecSimQueryReply_IteratorFree(VecSimQueryReply_Iterator *iterator);

///////////////////////////////////// VecSimBatchIterator API /////////////////////////////////////

/**
 * @brief Iterator for running the same query over an index, getting the in each iteration
 * the best results that hasn't returned in the previous iterations.
 */
typedef struct VecSimBatchIterator VecSimBatchIterator;

/**
 * @brief Run TopKQuery over the underling index of the given iterator using BatchIterator_Next
 * method, and return n_results new results.
 * @param iterator the iterator that olds the current state of this "batched search".
 * @param n_results number of new results to return.
 * @param order enum - determine the returned results order (by id or by score).
 * @return List of (at most) new n_results vectors which are the "nearest neighbours" to the
 * underline query vector in the iterator.
 */
VecSimQueryReply *VecSimBatchIterator_Next(VecSimBatchIterator *iterator, size_t n_results,
                                           VecSimQueryReply_Order order);

/**
 * @brief Return true while the iterator has new results to return, false if it is depleted
 * (using BatchIterator_HasNext method) .
 */
bool VecSimBatchIterator_HasNext(VecSimBatchIterator *iterator);

/**
 * @brief Release the iterator using BatchIterator_Free method
 */
void VecSimBatchIterator_Free(VecSimBatchIterator *iterator);

/**
 * @brief Reset the iterator - back to the initial state using BatchIterator_Reset method.
 */
void VecSimBatchIterator_Reset(VecSimBatchIterator *iterator);

#ifdef __cplusplus
}
#endif
