/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "aggregate/aggregate.h"
#include "hybrid/hybrid_request.h"
#include "redismock/util.h"

class HybridRequestBasicTest : public ::testing::Test {};

// Tests that don't require full Redis Module integration

// Test basic HybridRequest creation and initialization with multiple AREQ requests
TEST_F(HybridRequestBasicTest, testHybridRequestCreationBasic) {
  // Test basic HybridRequest creation without Redis dependencies
  AREQ **requests = array_new(AREQ*, 2);
  // Initialize the AREQ structures
  AREQ *req1 = AREQ_New();
  AREQ *req2 = AREQ_New();

  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);

  HybridRequest *hybridReq = HybridRequest_New(NULL, requests, 2);
  ASSERT_TRUE(hybridReq != nullptr);
  ASSERT_EQ(hybridReq->nrequests, 2);
  ASSERT_TRUE(hybridReq->requests != nullptr);

  // Verify the merge pipeline is initialized
  ASSERT_TRUE(hybridReq->tailPipeline->ap.steps.next != nullptr);
  // Clean up
  HybridRequest_DecrRef(hybridReq);
}

//------------------------------------------------------------------------------
// Tests for the dual reference counting mechanism in HybridRequest.
//
// HybridRequest uses TWO separate reference counts for different ownership purposes:
//
// 1. StrongRef (RefManager.strong_refcount):
//    - Created via StrongRef_New(hybridRequest, &FreeHybridRequest)
//    - Manages the "external view" lifetime of the HybridRequest
//    - Used by blockedClientHybridCtx and cursors to keep the HybridRequest alive
//    - When strong_refcount reaches 0, calls FreeHybridRequest callback
//
// 2. syncCtx.refcount:
//    - Initialized to 1 via RequestSyncCtx_Init()
//    - Manages the "internal" shared ownership within timeout handling
//    - Used for coordination between timeout callback (main thread) and background thread
//    - When this refcount reaches 0, calls HybridRequest_Free()
//
// The interaction between these two ref counts:
// - StrongRef_Release decrements strong_refcount, and when it hits 0, calls FreeHybridRequest
// - FreeHybridRequest calls HybridRequest_DecrRef, which decrements syncCtx.refcount
// - HybridRequest_Free is only called when syncCtx.refcount reaches 0
//
// This dual-layer design allows:
// - External components (cursors, blocked client contexts) to hold StrongRefs
// - Internal timeout coordination to manage shared ownership via syncCtx.refcount
// - Thread-safe handoff between main thread and background thread
//------------------------------------------------------------------------------

// Helper function to create a HybridRequest for testing
static HybridRequest* createTestHybridRequest() {
  AREQ **requests = array_new(AREQ*, 2);
  AREQ *req1 = AREQ_New();
  AREQ *req2 = AREQ_New();
  requests = array_ensure_append_1(requests, req1);
  requests = array_ensure_append_1(requests, req2);
  return HybridRequest_New(NULL, requests, 2);
}

// Test that syncCtx.refcount is initialized to 1
TEST_F(HybridRequestBasicTest, testSyncCtxRefcountInitialization) {
  HybridRequest *hybridReq = createTestHybridRequest();
  ASSERT_TRUE(hybridReq != nullptr);

  // syncCtx.refcount should be initialized to 1 by RequestSyncCtx_Init
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  HybridRequest_DecrRef(hybridReq);
}

// Test HybridRequest_IncrRef and HybridRequest_DecrRef for syncCtx.refcount
TEST_F(HybridRequestBasicTest, testSyncCtxRefcountIncrDecr) {
  HybridRequest *hybridReq = createTestHybridRequest();
  ASSERT_TRUE(hybridReq != nullptr);

  // Initial refcount should be 1
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Increment refcount
  HybridRequest *returned = HybridRequest_IncrRef(hybridReq);
  EXPECT_EQ(returned, hybridReq) << "IncrRef should return the same pointer";
  EXPECT_EQ(hybridReq->syncCtx.refcount, 2);

  // Increment again
  HybridRequest_IncrRef(hybridReq);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 3);

  // Decrement refcount (should not free, refcount becomes 2)
  HybridRequest_DecrRef(hybridReq);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 2);

  // Decrement again (should not free, refcount becomes 1)
  HybridRequest_DecrRef(hybridReq);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Final decrement - this should free the request
  HybridRequest_DecrRef(hybridReq);
  // hybridReq is now invalid - cannot access it
}

// FreeHybridRequest callback - calls HybridRequest_DecrRef on the HybridRequest
static void FreeHybridRequest(void *ptr) {
  HybridRequest_DecrRef((HybridRequest *)ptr);
}

// Test StrongRef creation and basic operations with HybridRequest
TEST_F(HybridRequestBasicTest, testStrongRefBasicOperations) {
  HybridRequest *hybridReq = createTestHybridRequest();
  ASSERT_TRUE(hybridReq != nullptr);

  // Create a StrongRef to the HybridRequest
  StrongRef hybrid_ref = StrongRef_New(hybridReq, &FreeHybridRequest);

  // StrongRef_Get should return the same pointer
  EXPECT_EQ(StrongRef_Get(hybrid_ref), hybridReq);

  // syncCtx.refcount should still be 1 (StrongRef doesn't affect it)
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Release the StrongRef - this should:
  // 1. Decrement RefManager.strong_refcount to 0
  // 2. Call FreeHybridRequest callback
  // 3. FreeHybridRequest calls HybridRequest_DecrRef
  // 4. HybridRequest_DecrRef decrements syncCtx.refcount to 0
  // 5. HybridRequest_Free is called
  StrongRef_Release(hybrid_ref);
  // hybridReq is now invalid
}

// Test cloning StrongRef
TEST_F(HybridRequestBasicTest, testStrongRefClone) {
  HybridRequest *hybridReq = createTestHybridRequest();
  ASSERT_TRUE(hybridReq != nullptr);

  StrongRef ref1 = StrongRef_New(hybridReq, &FreeHybridRequest);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Clone the StrongRef
  StrongRef ref2 = StrongRef_Clone(ref1);
  EXPECT_EQ(StrongRef_Get(ref2), hybridReq);

  // syncCtx.refcount should still be 1 (StrongRef cloning doesn't affect it)
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Release ref2 - should not free yet (ref1 still holds)
  StrongRef_Release(ref2);
  EXPECT_EQ(StrongRef_Get(ref1), hybridReq) << "Request should still be valid";
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Release ref1 - now it should free
  StrongRef_Release(ref1);
  // hybridReq is now invalid
}

// Test the dual ref count interaction:
// - StrongRef manages external lifetime
// - syncCtx.refcount manages internal shared ownership
TEST_F(HybridRequestBasicTest, testDualRefCountInteraction) {
  HybridRequest *hybridReq = createTestHybridRequest();
  ASSERT_TRUE(hybridReq != nullptr);

  // Create StrongRef for external ownership
  StrongRef hybrid_ref = StrongRef_New(hybridReq, &FreeHybridRequest);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Simulate timeout callback adding internal ref for coordination
  HybridRequest_IncrRef(hybridReq);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 2);

  // Clone StrongRef for cursor or blockedClientHybridCtx
  StrongRef cursor_ref = StrongRef_Clone(hybrid_ref);

  // Release original StrongRef (e.g., main execution path done)
  StrongRef_Release(hybrid_ref);

  // Request should still be valid (cursor_ref holds it, and syncCtx.refcount is 2)
  EXPECT_EQ(StrongRef_Get(cursor_ref), hybridReq);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 2);

  // Background thread releases its internal ref
  HybridRequest_DecrRef(hybridReq);
  EXPECT_EQ(hybridReq->syncCtx.refcount, 1);

  // Release cursor's StrongRef - this should now free everything
  StrongRef_Release(cursor_ref);
  // hybridReq is now invalid
}

// Test StrongRef invalidation
TEST_F(HybridRequestBasicTest, testStrongRefInvalidation) {
  HybridRequest *hybridReq = createTestHybridRequest();
  ASSERT_TRUE(hybridReq != nullptr);

  StrongRef ref1 = StrongRef_New(hybridReq, &FreeHybridRequest);
  StrongRef ref2 = StrongRef_Clone(ref1);

  // Invalidate - prevents new StrongRefs from being cloned
  StrongRef_Invalidate(ref1);

  // Existing refs should still work
  EXPECT_EQ(StrongRef_Get(ref1), hybridReq);
  EXPECT_EQ(StrongRef_Get(ref2), hybridReq);

  // But cloning an invalidated ref should return invalid ref
  StrongRef ref3 = StrongRef_Clone(ref1);
  EXPECT_EQ(StrongRef_Get(ref3), nullptr) << "Clone of invalidated ref should be invalid";

  // No need to release ref3 since it's invalid (NULL rm)
  StrongRef_Release(ref2);
  StrongRef_Release(ref1);
}
