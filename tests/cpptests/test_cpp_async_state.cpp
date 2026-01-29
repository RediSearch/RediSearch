/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "result_processor.h"
#include "redisearch_rs/headers/types_rs.h"
#include "util/arr.h"
#include "util/dllist.h"
#include "search_disk.h"
#include "redismock/redismock.h"
#include "index_result_async_read.h"

// Test pool size constant
#define TEST_ASYNC_POOL_SIZE 16

class AsyncStateTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Use the proper Init function - no async pool needed for state machine tests
    IndexResultAsyncRead_Init(&state, TEST_ASYNC_POOL_SIZE);

    // Manually allocate arrays for testing (normally done by SetupAsyncPool)
    state.readyResults = array_new(AsyncReadResult, TEST_ASYNC_POOL_SIZE);
    state.failedUserData = array_new(uint64_t, TEST_ASYNC_POOL_SIZE);
  }

  void TearDown() override {
    // Use the proper Free function
    IndexResultAsyncRead_Free(&state);
  }

  // Helper: Create a mock IndexResult
  RSIndexResult* createMockIndexResult(t_docId docId) {
    RSIndexResult *result = NewVirtualResult(1.0, 0xFF);
    result->docId = docId;
    return result;
  }

  // Helper: Add a node to iteratorResults
  void addToIteratorResults(t_docId docId) {
    RSIndexResult *result = createMockIndexResult(docId);
    IndexResultNode *node = (IndexResultNode *)rm_calloc(1, sizeof(IndexResultNode));
    node->result = result;
    dllist_append(&state.iteratorResults, &node->node);
    state.iteratorResultCount++;
  }

  // Helper: Move a node from iteratorResults to pendingResults
  void moveIteratorToPending() {
    ASSERT_FALSE(DLLIST_IS_EMPTY(&state.iteratorResults));
    DLLIST_node *dlnode = dllist_pop_head(&state.iteratorResults);
    IndexResultNode *node = (IndexResultNode *)dlnode;
    dllist_append(&state.pendingResults, &node->node);
    state.iteratorResultCount--;
  }

  // Helper: Verify state consistency
  void assertStateConsistent() {
    uint16_t actualCount = countNodes(&state.iteratorResults);
    ASSERT_EQ(actualCount, state.iteratorResultCount)
      << "iteratorResultCount mismatch: expected " << actualCount
      << " but got " << state.iteratorResultCount;
  }

  // Helper: Count nodes in a DLLIST
  uint16_t countNodes(DLLIST *list) {
    uint16_t count = 0;
    DLLIST_FOREACH(dlnode, list) {
      count++;
    }
    return count;
  }

  IndexResultAsyncReadState state;
};

// Test: Initial state is empty
TEST_F(AsyncStateTest, testInitialState) {
  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.pendingResults));
  ASSERT_EQ(array_len(state.readyResults), 0);
  ASSERT_EQ(array_len(state.failedUserData), 0);
  ASSERT_EQ(state.readyResultsIndex, 0);
  ASSERT_EQ(state.lastReturnedIndexResult, nullptr);
  assertStateConsistent();
}

// Test: State 1 → State 2: Empty → Buffered
TEST_F(AsyncStateTest, testEmptyToBuffered) {
  // Start in empty state
  ASSERT_EQ(state.iteratorResultCount, 0);

  // Add 10 results to buffer
  for (t_docId i = 1; i <= 10; i++) {
    addToIteratorResults(i);
  }

  // Verify buffered state
  ASSERT_EQ(state.iteratorResultCount, 10);
  ASSERT_EQ(countNodes(&state.iteratorResults), 10);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.pendingResults));
  assertStateConsistent();

  // Verify FIFO ordering
  t_docId expectedDocId = 1;
  DLLIST_FOREACH(dlnode, &state.iteratorResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, expectedDocId);
    expectedDocId++;
  }
}

// Test: State 2 → State 3: Buffered → Pending
TEST_F(AsyncStateTest, testBufferedToPending) {
  // Setup: Add 10 results to buffer
  for (t_docId i = 1; i <= 10; i++) {
    addToIteratorResults(i);
  }
  ASSERT_EQ(state.iteratorResultCount, 10);

  // Move all to pending (simulating refillAsyncPool)
  for (int i = 0; i < 10; i++) {
    moveIteratorToPending();
  }

  // Verify pending state
  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));
  ASSERT_EQ(countNodes(&state.pendingResults), 10);
  assertStateConsistent();

  // Verify FIFO ordering is maintained
  t_docId expectedDocId = 1;
  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, expectedDocId);
    expectedDocId++;
  }
}

// Test: State 3 → State 4: Pending → Ready (simulated poll)
TEST_F(AsyncStateTest, testPendingToReady) {
  // Setup: Add 5 results and move to pending
  for (t_docId i = 1; i <= 5; i++) {
    addToIteratorResults(i);
  }
  for (int i = 0; i < 5; i++) {
    moveIteratorToPending();
  }

  // Simulate poll completing: populate readyResults
  // In real code, SearchDisk_PollAsyncReads would do this
  array_set_len(state.readyResults, 5);
  t_docId docId = 1;
  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    AsyncReadResult *result = &state.readyResults[docId - 1];

    // Create mock DMD
    RSDocumentMetadata *dmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
    dmd->ref_count = 1;
    dmd->id = docId;
    dmd->keyPtr = sdsnewlen("key", 3);

    result->dmd = dmd;
    result->user_data = (uint64_t)node;
    docId++;
  }

  // Verify ready state
  ASSERT_EQ(array_len(state.readyResults), 5);
  ASSERT_EQ(state.readyResultsIndex, 0);
  ASSERT_EQ(countNodes(&state.pendingResults), 5); // Still in pending until consumed

  // Verify results are in order
  for (int i = 0; i < 5; i++) {
    ASSERT_NE(state.readyResults[i].dmd, nullptr);
    ASSERT_EQ(state.readyResults[i].dmd->id, i + 1);
  }
}

// Test: State 4 → State 5: Ready → Consumed (simulated popReadyResult)
TEST_F(AsyncStateTest, testReadyToConsumed) {
  // Setup: Create ready results
  for (t_docId i = 1; i <= 3; i++) {
    addToIteratorResults(i);
    moveIteratorToPending();
  }

  // Populate readyResults
  array_set_len(state.readyResults, 3);
  t_docId docId = 1;
  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    AsyncReadResult *result = &state.readyResults[docId - 1];

    RSDocumentMetadata *dmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
    dmd->ref_count = 1;
    dmd->id = docId;
    dmd->keyPtr = sdsnewlen("key", 3);

    result->dmd = dmd;
    result->user_data = (uint64_t)node;
    docId++;
  }

  // Consume results one by one (simulating popReadyResult)
  for (int i = 0; i < 3; i++) {
    ASSERT_LT(state.readyResultsIndex, array_len(state.readyResults));

    AsyncReadResult *result = &state.readyResults[state.readyResultsIndex];
    IndexResultNode *node = (IndexResultNode *)result->user_data;

    // Populate DMD in IndexResult (what popReadyResult does)
    node->result->dmd = result->dmd;
    result->dmd = NULL;

    // Verify the result
    ASSERT_EQ(node->result->docId, i + 1);
    ASSERT_NE(node->result->dmd, nullptr);
    ASSERT_EQ(node->result->dmd->id, i + 1);

    // Remove from pending (what popReadyResult does)
    dllist_delete(&node->node);

    // Clean up (in real code, this happens later)
    DMD_Return(node->result->dmd);
    IndexResult_Free(node->result);
    rm_free(node);

    state.readyResultsIndex++;
  }

  // Verify consumed state
  ASSERT_EQ(state.readyResultsIndex, 3);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.pendingResults));
}

// Test: Full lifecycle - Empty → Buffered → Pending → Ready → Consumed → Empty
TEST_F(AsyncStateTest, testFullLifecycle) {
  // State 1: Empty
  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.pendingResults));
  ASSERT_EQ(array_len(state.readyResults), 0);
  assertStateConsistent();

  // State 2: Buffered - Add 5 results
  for (t_docId i = 100; i <= 104; i++) {
    addToIteratorResults(i);
  }
  ASSERT_EQ(state.iteratorResultCount, 5);
  ASSERT_EQ(countNodes(&state.iteratorResults), 5);
  assertStateConsistent();

  // State 3: Pending - Move to pending
  for (int i = 0; i < 5; i++) {
    moveIteratorToPending();
  }
  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));
  ASSERT_EQ(countNodes(&state.pendingResults), 5);
  assertStateConsistent();

  // State 4: Ready - Simulate poll
  array_set_len(state.readyResults, 5);
  t_docId docId = 100;
  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    AsyncReadResult *result = &state.readyResults[docId - 100];

    RSDocumentMetadata *dmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
    dmd->ref_count = 1;
    dmd->id = docId;
    dmd->keyPtr = sdsnewlen("key", 3);

    result->dmd = dmd;
    result->user_data = (uint64_t)node;
    docId++;
  }
  ASSERT_EQ(array_len(state.readyResults), 5);

  // State 5: Consumed - Pop all results
  for (int i = 0; i < 5; i++) {
    AsyncReadResult *result = &state.readyResults[state.readyResultsIndex];
    IndexResultNode *node = (IndexResultNode *)result->user_data;

    node->result->dmd = result->dmd;
    result->dmd = NULL;

    ASSERT_EQ(node->result->docId, 100 + i);

    dllist_delete(&node->node);
    DMD_Return(node->result->dmd);
    IndexResult_Free(node->result);
    rm_free(node);

    state.readyResultsIndex++;
  }

  // State 6: Back to Empty
  array_set_len(state.readyResults, 0);
  state.readyResultsIndex = 0;

  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.pendingResults));
  ASSERT_EQ(array_len(state.readyResults), 0);
  ASSERT_EQ(state.readyResultsIndex, 0);
  assertStateConsistent();
}

// Test: FIFO ordering is maintained through all transitions
TEST_F(AsyncStateTest, testFIFOOrdering) {
  // Add results in specific order
  std::vector<t_docId> docIds = {42, 17, 99, 3, 88};

  for (t_docId docId : docIds) {
    addToIteratorResults(docId);
  }

  // Verify order in iteratorResults
  size_t idx = 0;
  DLLIST_FOREACH(dlnode, &state.iteratorResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, docIds[idx]);
    idx++;
  }

  // Move to pending and verify order
  for (size_t i = 0; i < docIds.size(); i++) {
    moveIteratorToPending();
  }

  idx = 0;
  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, docIds[idx]);
    idx++;
  }
}

// Test: Pool size limit (max TEST_ASYNC_POOL_SIZE)
TEST_F(AsyncStateTest, testPoolSizeLimit) {
  // Add more than pool size to buffer
  for (t_docId i = 1; i <= TEST_ASYNC_POOL_SIZE + 5; i++) {
    addToIteratorResults(i);
  }
  ASSERT_EQ(state.iteratorResultCount, TEST_ASYNC_POOL_SIZE + 5);

  // Move only TEST_ASYNC_POOL_SIZE to pending (simulating pool full)
  for (int i = 0; i < TEST_ASYNC_POOL_SIZE; i++) {
    moveIteratorToPending();
  }

  // Verify pool is full
  ASSERT_EQ(countNodes(&state.pendingResults), TEST_ASYNC_POOL_SIZE);

  // Verify remaining in buffer
  ASSERT_EQ(state.iteratorResultCount, 5);
  ASSERT_EQ(countNodes(&state.iteratorResults), 5);

  // Verify the remaining items are the later ones (FIFO)
  t_docId expectedDocId = TEST_ASYNC_POOL_SIZE + 1;
  DLLIST_FOREACH(dlnode, &state.iteratorResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, expectedDocId);
    expectedDocId++;
  }
}

// Test: Failed reads handling
TEST_F(AsyncStateTest, testFailedReads) {
  // Setup: Add 5 results to pending
  for (t_docId i = 1; i <= 5; i++) {
    addToIteratorResults(i);
    moveIteratorToPending();
  }

  // Simulate poll with 3 successes and 2 failures
  array_set_len(state.readyResults, 3);
  array_set_len(state.failedUserData, 2);

  // First 3 succeed
  t_docId docId = 1;
  int resultIdx = 0;
  int failedIdx = 0;

  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;

    if (docId <= 3) {
      // Success
      AsyncReadResult *result = &state.readyResults[resultIdx++];
      RSDocumentMetadata *dmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
      dmd->ref_count = 1;
      dmd->id = docId;
      dmd->keyPtr = sdsnewlen("key", 3);
      result->dmd = dmd;
      result->user_data = (uint64_t)node;
    } else {
      // Failure
      state.failedUserData[failedIdx++] = (uint64_t)node;
    }
    docId++;
  }

  // Verify results
  ASSERT_EQ(array_len(state.readyResults), 3);
  ASSERT_EQ(array_len(state.failedUserData), 2);

  // Clean up failed reads (simulating cleanupFailedReads)
  for (uint16_t i = 0; i < array_len(state.failedUserData); i++) {
    IndexResultNode *node = (IndexResultNode *)state.failedUserData[i];
    dllist_delete(&node->node);
    IndexResult_Free(node->result);
    rm_free(node);
  }

  // Verify only successful nodes remain
  ASSERT_EQ(countNodes(&state.pendingResults), 3);
}

// Test: Empty buffer operations
TEST_F(AsyncStateTest, testEmptyBufferOperations) {
  // Try to move from empty buffer
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));

  // Verify we can't pop from empty list
  DLLIST_node *node = dllist_pop_head(&state.iteratorResults);
  ASSERT_EQ(node, nullptr);

  // Verify state remains consistent
  ASSERT_EQ(state.iteratorResultCount, 0);
  assertStateConsistent();
}

// Test: Single result lifecycle
TEST_F(AsyncStateTest, testSingleResultLifecycle) {
  // Add single result
  addToIteratorResults(42);
  ASSERT_EQ(state.iteratorResultCount, 1);

  // Move to pending
  moveIteratorToPending();
  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_EQ(countNodes(&state.pendingResults), 1);

  // Simulate poll
  array_set_len(state.readyResults, 1);
  IndexResultNode *node = (IndexResultNode *)state.pendingResults.next;

  RSDocumentMetadata *dmd = (RSDocumentMetadata *)rm_calloc(1, sizeof(RSDocumentMetadata));
  dmd->ref_count = 1;
  dmd->id = 42;
  dmd->keyPtr = sdsnewlen("key42", 5);

  state.readyResults[0].dmd = dmd;
  state.readyResults[0].user_data = (uint64_t)node;

  // Consume
  AsyncReadResult *result = &state.readyResults[0];
  node->result->dmd = result->dmd;
  result->dmd = NULL;

  ASSERT_EQ(node->result->docId, 42);
  ASSERT_NE(node->result->dmd, nullptr);

  // Cleanup
  dllist_delete(&node->node);
  DMD_Return(node->result->dmd);
  IndexResult_Free(node->result);
  rm_free(node);

  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.pendingResults));
}

// Test: State invariants are maintained
TEST_F(AsyncStateTest, testStateInvariants) {
  // Invariant 1: iteratorResultCount always matches actual list size
  for (int i = 0; i < 10; i++) {
    addToIteratorResults(i);
    assertStateConsistent();
  }

  // Invariant 2: Moving to pending decrements count correctly
  for (int i = 0; i < 5; i++) {
    moveIteratorToPending();
    assertStateConsistent();
  }

  ASSERT_EQ(state.iteratorResultCount, 5);
  ASSERT_EQ(countNodes(&state.iteratorResults), 5);
  ASSERT_EQ(countNodes(&state.pendingResults), 5);
}

// Test: Interleaved operations (refill while consuming)
TEST_F(AsyncStateTest, testInterleavedOperations) {
  // Add initial batch
  for (t_docId i = 1; i <= 5; i++) {
    addToIteratorResults(i);
  }

  // Move to pending
  for (int i = 0; i < 5; i++) {
    moveIteratorToPending();
  }

  // While pending, add more to buffer (simulating continuous iteration)
  for (t_docId i = 6; i <= 10; i++) {
    addToIteratorResults(i);
  }

  // Verify both lists have data
  ASSERT_EQ(state.iteratorResultCount, 5);
  ASSERT_EQ(countNodes(&state.iteratorResults), 5);
  ASSERT_EQ(countNodes(&state.pendingResults), 5);

  // Verify ordering in both lists
  t_docId expectedDocId = 6;
  DLLIST_FOREACH(dlnode, &state.iteratorResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, expectedDocId);
    expectedDocId++;
  }

  expectedDocId = 1;
  DLLIST_FOREACH(dlnode, &state.pendingResults) {
    IndexResultNode *node = (IndexResultNode *)dlnode;
    ASSERT_EQ(node->result->docId, expectedDocId);
    expectedDocId++;
  }
}

// Test: Maximum capacity handling
TEST_F(AsyncStateTest, testMaximumCapacity) {
  // Fill to maximum buffer size
  for (t_docId i = 1; i <= TEST_ASYNC_POOL_SIZE; i++) {
    addToIteratorResults(i);
  }

  ASSERT_EQ(state.iteratorResultCount, TEST_ASYNC_POOL_SIZE);
  ASSERT_EQ(countNodes(&state.iteratorResults), TEST_ASYNC_POOL_SIZE);

  // Move all to pending (should fit exactly in pool)
  for (int i = 0; i < TEST_ASYNC_POOL_SIZE; i++) {
    moveIteratorToPending();
  }

  ASSERT_EQ(state.iteratorResultCount, 0);
  ASSERT_EQ(countNodes(&state.pendingResults), TEST_ASYNC_POOL_SIZE);
  ASSERT_TRUE(DLLIST_IS_EMPTY(&state.iteratorResults));
}

