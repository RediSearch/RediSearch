/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "index_result_async_read.h"
#include "types_ffi.h"
#include "search_disk.h"
#include "rmalloc.h"
#include "util/dllist.h"

void IndexResultAsyncRead_Init(IndexResultAsyncReadState *state, uint16_t poolSize) {
  // Initialize all fields to safe defaults
  dllist_init(&state->iteratorResults);
  dllist_init(&state->pendingResults);
  state->poolSize = poolSize;
  state->iteratorResultCount = 0;
  state->readyResults = NULL;
  state->failedUserData = NULL;
  state->asyncPool = NULL;
  state->readyResultsIndex = 0;
  state->lastReturnedIndexResult = NULL;
}

void IndexResultAsyncRead_SetupAsyncPool(IndexResultAsyncReadState *state,
                                         RedisSearchDiskAsyncReadPool asyncPool) {
  RS_ASSERT(asyncPool);

  state->asyncPool = asyncPool;

  // Allocate async I/O buffers with capacity for poll results (len=0 initially)
  state->readyResults = array_new(AsyncReadResult, state->poolSize);
  state->failedUserData = array_new(uint64_t, state->poolSize);
}

// Timeout (ms) for each blocking poll while draining in-flight reads at teardown.
// Async reads always complete (their callback fires exactly once), so the drain
// loop terminates; the timeout only bounds how long a single poll waits per turn.
#define ASYNC_DRAIN_POLL_TIMEOUT_MS 1000

// Wait for every in-flight async read to complete and discard its result.
//
// The disk-side pool does NOT cancel or wait for pending reads when freed (see
// `IndexDiskAPI.createAsyncReadPool`): each in-flight read borrows the query
// snapshot and dereferences it on I/O completion, so the snapshot must outlive
// them. Releasing the pool (and, shortly after, the snapshot in SearchCtx_CleanUp)
// while reads are still pending is a use-after-free. Draining to `pending == 0`
// here upholds that contract; on the normal EOF path `pending` is already 0 so
// this returns immediately.
static void IndexResultAsyncRead_Drain(IndexResultAsyncReadState *state) {
  // Value is irrelevant: drained results are discarded regardless of expiration.
  const t_expirationTimePoint expiration = {0};
  size_t pending;
  do {
    pending = IndexResultAsyncRead_Poll(state, ASYNC_DRAIN_POLL_TIMEOUT_MS, &expiration);
    RSIndexResult *r;
    while ((r = IndexResultAsyncRead_PopReadyResult(state)) != NULL) {
      if (r->dmd) {
        DMD_Return(r->dmd);
      }
      IndexResult_Free(r);
    }
  } while (pending > 0);
}

void IndexResultAsyncRead_Free(IndexResultAsyncReadState *state) {
  if (!state) return;

  // Drain in-flight reads before freeing the pool: they borrow the query
  // snapshot and deref it on completion, and the pool does not wait on free.
  if (state->asyncPool) {
    IndexResultAsyncRead_Drain(state);
    SearchDisk_FreeAsyncReadPool(state->asyncPool);
    state->asyncPool = NULL;
  }

  // Free nodes in iteratorResults list
  DLLIST_node *dlnode;
  while ((dlnode = dllist_pop_tail(&state->iteratorResults)) != NULL) {
    IndexResultNode *node = DLLIST_ITEM(dlnode, IndexResultNode, node);
    if (node->result) {
      IndexResult_Free(node->result);
    }
    rm_free(node);
  }

  // Free nodes in pendingResults list (includes pending async reads)
  while ((dlnode = dllist_pop_tail(&state->pendingResults)) != NULL) {
    IndexResultNode *node = DLLIST_ITEM(dlnode, IndexResultNode, node);
    if (node->result) {
      IndexResult_Free(node->result);
    }
    rm_free(node);
  }

  if (state->readyResults) {
    // Free any remaining DMD data that wasn't consumed
    array_free_ex(state->readyResults, {
      AsyncReadResult *result = (AsyncReadResult*)ptr;
      if (result->dmd) {
        DMD_Return(result->dmd);
      }
    });
    state->readyResults = NULL;
  }

  if (state->failedUserData) {
    array_free(state->failedUserData);
    state->failedUserData = NULL;
  }

  // Free the last returned deep-copied IndexResult if any
  if (state->lastReturnedIndexResult) {
    IndexResult_Free(state->lastReturnedIndexResult);
    state->lastReturnedIndexResult = NULL;
  }
}

void IndexResultAsyncRead_RefillPool(IndexResultAsyncReadState *state) {
  uint16_t added = 0;

  // Move nodes from iteratorResults to pendingResults
  while (added < state->poolSize && !DLLIST_IS_EMPTY(&state->iteratorResults)) {
    // Peek at the head of iteratorResults to maintain FIFO order
    DLLIST_node *dlnode = state->iteratorResults.next;
    IndexResultNode *node = DLLIST_ITEM(dlnode, IndexResultNode, node);

    RSIndexResult *indexResult = node->result;
    t_docId docId = indexResult->docId;

    // Try to add to async pool, using the node pointer as user_data
    if (!SearchDisk_AddAsyncRead(state->asyncPool, docId, (uint64_t)node)) {
      // Pool is full - stop without removing the node
      break;
    }

    // Successfully added to async pool - now remove from iteratorResults and add to pendingResults list
    dllist_delete(dlnode);
    dllist_append(&state->pendingResults, dlnode);
    state->iteratorResultCount--;
    added++;
  }
}

static void IndexResultAsyncRead_CleanupFailedReads(IndexResultAsyncReadState *state) {
  RS_ASSERT(array_len(state->failedUserData) <= state->poolSize);
  for (uint16_t i = 0; i < array_len(state->failedUserData); i++) {
    IndexResultNode *node = (IndexResultNode *)state->failedUserData[i];

    // Remove node from pendingResults list
    dllist_delete(&node->node);

    // Free the deep-copied IndexResult
    if (node->result) {
      IndexResult_Free(node->result);
    }

    // Free the node itself
    rm_free(node);
  }
}

size_t IndexResultAsyncRead_Poll(IndexResultAsyncReadState *state, uint32_t timeout_ms, const t_expirationTimePoint *expiration_point) {
  // Poll writes directly to the arrays (capacity is poolSize)
  const size_t pendingCount = SearchDisk_PollAsyncReads(
      state->asyncPool, timeout_ms,
      state->readyResults, state->failedUserData, expiration_point);

  // Reset index to start consuming from the beginning of readyResults
  state->readyResultsIndex = 0;

  // Clean up nodes for failed reads (not found/error)
  IndexResultAsyncRead_CleanupFailedReads(state);

  return pendingCount;
}

RSIndexResult* IndexResultAsyncRead_PopReadyResult(IndexResultAsyncReadState *state) {
  if (state->readyResultsIndex >= array_len(state->readyResults)) {
    return NULL;  // No more ready results
  }

  AsyncReadResult *result = &state->readyResults[state->readyResultsIndex++];

  // Take ownership of the DMD pointer from the result
  RSDocumentMetadata *dmd = result->dmd;
  result->dmd = NULL;  // Clear to prevent double-free

  // Retrieve the node pointer from user_data
  IndexResultNode *node = (IndexResultNode *)result->user_data;
  RSIndexResult *indexResult = node->result;

  // Populate the DMD field in the IndexResult
  indexResult->dmd = dmd;

  // Remove node from pendingResults list and free it
  dllist_delete(&node->node);
  rm_free(node);

  return indexResult;
}

bool IndexResultAsyncRead_IsIterationComplete(const IndexResultAsyncReadState *state,
                                     bool iteratorAtEOF,
                                     size_t pendingCount) {
  // We're done only if: iterator is at EOF, no ready results, no in-flight async reads,
  // and no buffered results waiting to be submitted
  return iteratorAtEOF &&
         array_len(state->readyResults) == 0 &&
         pendingCount == 0 &&
         DLLIST_IS_EMPTY(&state->iteratorResults);
}

