/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef RS_INDEX_RESULT_ASYNC_READ_H_
#define RS_INDEX_RESULT_ASYNC_READ_H_

#include "redisearch.h"
#include "search_disk_api.h"
#include "util/dllist.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * IndexResultNode - Node wrapper for IndexResults in async disk I/O pipeline
 * 
 * This structure wraps an RSIndexResult in a doubly-linked list node, allowing
 * it to be tracked through the async disk I/O pipeline stages.
 */
typedef struct IndexResultNode {
  DLLIST node;           // DLLIST node for linking
  RSIndexResult *result; // Deep-copied IndexResult from iterator
} IndexResultNode;

/**
 * IndexResultAsyncReadState - State management for async disk reads of index results
 *
 * This structure manages a three-level buffering pipeline for async disk I/O:
 * 1. iteratorResults: Buffered IndexResults from iterator (not yet submitted)
 * 2. pendingResults: IndexResults with in-flight async disk reads
 * 3. readyResults: Completed disk reads ready for consumption
 *
 * The pipeline maintains FIFO ordering to ensure results are returned in the
 * same order as the iterator produces them.
 */
typedef struct IndexResultAsyncReadState {
  // Async pool handle
  RedisSearchDiskAsyncReadPool asyncPool;  // Async read pool (NULL if not using async disk)

  // Configuration
  uint16_t poolSize;                       // Maximum number of concurrent async reads

  // Level 1: Iterator buffer (not yet submitted to async pool)
  DLLIST iteratorResults;                  // Deep-copied IndexResults from iterator
  uint16_t iteratorResultCount;            // Number of nodes in iteratorResults list

  // Level 2: Pending async reads (in-flight disk I/O)
  DLLIST pendingResults;                   // IndexResults with submitted async reads

  // Level 3: Ready results (completed disk reads)
  arrayof(AsyncReadResult) readyResults;   // Completed async reads (DMD + user_data pairs)
  uint16_t readyResultsIndex;              // Next index to consume from readyResults

  // Failed reads tracking
  arrayof(uint64_t) failedUserData;        // user_data from failed async reads

  // Memory management
  RSIndexResult *lastReturnedIndexResult;  // Last returned result (freed on next call)
} IndexResultAsyncReadState;

/**
 * Initialize async read state structure
 *
 * @param state Async read state structure to initialize
 * @param poolSize Maximum number of concurrent async reads
 */
void IndexResultAsyncRead_Init(IndexResultAsyncReadState *state, uint16_t poolSize);

/**
 * Setup async pool for disk I/O
 *
 * @param state Async read state structure
 * @param asyncPool Pre-created async read pool handle
 */
void IndexResultAsyncRead_SetupAsyncPool(IndexResultAsyncReadState *state,
                                         RedisSearchDiskAsyncReadPool asyncPool);

/**
 * Clean up and free async read state
 *
 * @param state Async read state structure to clean up
 */
void IndexResultAsyncRead_Free(IndexResultAsyncReadState *state);

/**
 * Refill the async pool from the iterator buffer
 *
 * Moves IndexResults from iteratorResults to pendingResults by submitting
 * them to the async read pool. Maintains FIFO ordering. Stops when the pool
 * is full or no more buffered results are available.
 *
 * @param state Async read state structure
 */
void IndexResultAsyncRead_RefillPool(IndexResultAsyncReadState *state);

/**
 * Poll for completed async reads
 *
 * Polls the async pool for completed reads and updates the ready results.
 * Resets the readyResultsIndex to start consuming from the beginning.
 * Cleans up any failed reads (not found/error).
 *
 * @param state Async read state structure
 * @param timeout_ms Timeout in milliseconds for the poll operation
 * @return Number of pending async reads still in progress
 */
size_t IndexResultAsyncRead_Poll(IndexResultAsyncReadState *state, uint32_t timeout_ms);

/**
 * Pop a ready result from the completed async reads
 *
 * Returns an IndexResult with its DMD field populated from a completed
 * async disk read. The caller takes ownership of the IndexResult.
 *
 * Ownership model:
 * - The IndexResult is passed to the parent result processor via SearchResult
 * - The parent processor will eventually free it via IndexResult_Free
 * - Store the pointer in state->lastReturnedIndexResult for cleanup tracking
 * - On the next call to PopReadyResult, the previous IndexResult will be freed
 *   (it has been consumed by the parent processor by then)
 *
 * @param state Async read state structure
 * @return IndexResult with DMD populated, or NULL if no ready results
 */
RSIndexResult* IndexResultAsyncRead_PopReadyResult(IndexResultAsyncReadState *state);

/**
 * Check if async iteration is complete
 *
 * Returns true if the iterator is at EOF and all async operations are complete
 * (no buffered results, no pending reads, no ready results).
 *
 * @param state Async read state structure
 * @param iteratorAtEOF Whether the iterator has reached EOF
 * @param pendingCount Number of pending async reads (from poll)
 * @return true if iteration is complete, false otherwise
 */
bool IndexResultAsyncRead_IsIterationComplete(const IndexResultAsyncReadState *state,
                                     bool iteratorAtEOF, 
                                     size_t pendingCount);

#ifdef __cplusplus
}
#endif

#endif  // RS_INDEX_RESULT_ASYNC_READ_H_

