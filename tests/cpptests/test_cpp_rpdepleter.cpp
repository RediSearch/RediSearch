/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "result_processor.h"
#include "gtest/gtest.h"
#include "spec.h"
#include "search_ctx.h"
#include "rmalloc.h"
#include "common.h"
#include <thread>
#include <chrono>
#include "redismock/redismock.h"
#include "search_result.h"

#include <thread>
#include <chrono>
#include <atomic>

#define NumberOfContexts 3

// Base test class for parameterized tests
class RPDepleterTest : public ::testing::Test, public ::testing::WithParamInterface<bool> {
protected:
  // Reusable mock upstream processor
  struct MockUpstream : public ResultProcessor {
    int count = 0;
    int max_docs;
    int final_result;
    int sleep_ms;
    int doc_id_offset;

    MockUpstream(int max_docs = 3, int final_result = RS_RESULT_EOF, int sleep_ms = 0, int doc_id_offset = 0) {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
      this->max_docs = max_docs;
      this->final_result = final_result;
      this->sleep_ms = sleep_ms;
      this->doc_id_offset = doc_id_offset;
    }

    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream *self = (MockUpstream *)rp;
      if (self->count >= self->max_docs) return self->final_result;

      // Sleep if specified (for timing tests)
      if (self->sleep_ms > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(self->sleep_ms));
      }

      SearchResult_SetDocId(res, ++self->count + self->doc_id_offset);
      return RS_RESULT_OK;
    }
  };
  
  void SetUp() override {
    // Initialize Redis contexts for all test variants (WithoutIndexLock and WithIndexLock)
    for (size_t i = 0; i < NumberOfContexts; ++i) {
      redisContexts[i] = RedisModule_GetThreadSafeContext(NULL);
    }

    // Create a real index for testing index locking
    if (GetParam()) {  // Only create spec when testing with index locking
      // Generate a unique index name for each test to avoid conflicts
      const ::testing::TestInfo* const test_info =
        ::testing::UnitTest::GetInstance()->current_test_info();
      std::string index_name = std::string("test_index_") + test_info->test_case_name() + "_" + test_info->name();

      QueryError err = QueryError_Default();
      RedisModuleCtx *ctx = redisContexts[0];
      RMCK::ArgvList argv(ctx, "FT.CREATE", index_name.c_str(), "SKIPINITIALSCAN", "SCHEMA", "field1", "TEXT");
      mockSpec = IndexSpec_CreateNew(ctx, argv, argv.size(), &err);
      if (!mockSpec) {
        printf("Failed to create index spec. Error code: %d, Error message: %s\n",
               QueryError_GetCode(&err), QueryError_GetUserError(&err));
      }
      ASSERT_NE(mockSpec, nullptr) << "Failed to create index spec. Error: " << QueryError_GetUserError(&err);
    }

    // Initialize search contexts for all tests (with or without real spec)
    for (size_t i = 0; i < NumberOfContexts; ++i) {
      searchContexts[i] = SEARCH_CTX_STATIC(redisContexts[i], mockSpec);
    }

    // Set proper timeout on all search contexts to avoid immediate timeout
    // Since RS_IsMock prevents SearchCtx_UpdateTime from working, set timeout directly
    struct timespec future_timeout;
    clock_gettime(CLOCK_MONOTONIC_RAW, &future_timeout);
    future_timeout.tv_sec += 10; // 10 seconds from now
    for (size_t i = 0; i < NumberOfContexts; ++i) {
      searchContexts[i].time.timeout = future_timeout;
    }
  }

  void TearDown() override {
    // Free Redis contexts for all test variants (WithoutIndexLock and WithIndexLock)
    for (auto ctx : redisContexts) {
      RedisModule_FreeThreadSafeContext(ctx);
    }
  }

  std::array<RedisModuleCtx*, NumberOfContexts> redisContexts;
  std::array<RedisSearchCtx, NumberOfContexts> searchContexts;
  IndexSpec* mockSpec = nullptr;
};

TEST_P(RPDepleterTest, RPDepleter_Basic) {
  // Tests basic RPDepleter functionality: background thread depletes upstream results,
  // main thread waits on condition variable, then yields results in order.

  bool take_index_lock = GetParam();

  // Mock upstream processor: yields 3 results, then EOF
  const size_t n_docs = 3;
  QueryProcessingCtx qitr = {0};

  MockUpstream mockUpstream(n_docs, RS_RESULT_EOF);

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New(1, take_index_lock), &searchContexts[0], &searchContexts[1]);

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;
  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
  }
  ASSERT_GT(depletingCount, 0); // Should have at least one depleting state

  // Now, results should be available
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(SearchResult_GetDocId(&res), ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  // We expect to have received all results from the upstream processor.
  ASSERT_EQ(resultCount, n_docs);
  // The last return code should be RS_RESULT_EOF, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_EOF);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

TEST_P(RPDepleterTest, RPDepleter_Timeout) {
  // Tests RPDepleter handling of upstream timeout: background thread gets timeout,
  // main thread waits on condition variable, then yields results and timeout.

  bool take_index_lock = GetParam();

  // Mock upstream processor: yields 3 results, then timeout.
  const size_t n_docs = 3;
  QueryProcessingCtx qitr = {0};

  MockUpstream mockUpstream(n_docs, RS_RESULT_TIMEDOUT);

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New(1, take_index_lock), &searchContexts[0], &searchContexts[1]);

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;

  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
  }
  ASSERT_GT(depletingCount, 0);

  // Now, results should be available
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(SearchResult_GetDocId(&res), ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  ASSERT_EQ(resultCount, n_docs);
  // The last return code should be RS_RESULT_TIMEDOUT, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_TIMEDOUT);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

TEST_P(RPDepleterTest, RPDepleter_CrossWakeup) {
  // Tests cross-depleter condition variable signaling: when one depleter finishes,
  // it signals the shared condition variable, waking up other depleters that return
  // `RS_RESULT_DEPLETING` (allowing downstream to try other depleters for results).
  // Test that one depleter can wake up another depleter waiting on the same condition variable.
  // This tests the core mechanism where depleters share sync objects and signal each other.
  // High sleep times are used in order to avoid flakiness.

  bool take_index_lock = GetParam();

  const size_t n_docs = 2;
  QueryProcessingCtx qitr1 = {0}, qitr2 = {0};

  // Mock upstream that finishes quickly (500ms sleep per result)
  MockUpstream fastUpstream(n_docs, RS_RESULT_EOF, 500, 0);

  // Mock upstream that takes much longer (1000ms sleep per result, different doc IDs)
  MockUpstream slowUpstream(n_docs, RS_RESULT_EOF, 1000, 100);

  // Create shared sync reference and two depleters sharing it
  StrongRef sync_ref = DepleterSync_New(2, take_index_lock);
  ResultProcessor *fastDepleter = RPDepleter_New(StrongRef_Clone(sync_ref), &searchContexts[0], &searchContexts[2]);
  ResultProcessor *slowDepleter = RPDepleter_New(StrongRef_Clone(sync_ref), &searchContexts[1], &searchContexts[2]);
  StrongRef_Release(sync_ref);  // Release our reference

  // Set up pipelines
  QITR_PushRP(&qitr1, &fastUpstream);
  QITR_PushRP(&qitr1, fastDepleter);
  QITR_PushRP(&qitr2, &slowUpstream);
  QITR_PushRP(&qitr2, slowDepleter);

  SearchResult res = {0};

  // Start both depleters - they should both return DEPLETING initially
  int rc2 = slowDepleter->Next(slowDepleter, &res);
  int rc1 = fastDepleter->Next(fastDepleter, &res);
  ASSERT_EQ(rc1, RS_RESULT_DEPLETING);
  ASSERT_EQ(rc2, RS_RESULT_DEPLETING);

  // Call Next on the slow depleter, and get `RS_RESULT_DEPLETING`, indicating
  // that the fast depleter-thread has finished and woke it up.
  rc2 = slowDepleter->Next(slowDepleter, &res);
  ASSERT_EQ(rc2, RS_RESULT_DEPLETING);

  if (take_index_lock) {
    // Wait for the locks to be taken
    while ((rc1 = fastDepleter->Next(fastDepleter, &res)) == RS_RESULT_DEPLETING) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Deplete the fast depleter - each result should be available immediately,
  // until we reach the end.
  int resultCount = 0;
  do {
    if (rc1 == RS_RESULT_OK) {
      ASSERT_EQ(SearchResult_GetDocId(&res), ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc1 = fastDepleter->Next(fastDepleter, &res)) == RS_RESULT_OK);
  ASSERT_EQ(rc1, RS_RESULT_EOF);
  ASSERT_EQ(resultCount, n_docs);

  // Deplete the slow depleter. There is no other thread to wake it up, so we
  // need to wait for the thread to finish, getting all the results until we
  // reach the end.
  resultCount = 0;
  do {
    if (rc2 == RS_RESULT_OK) {
      ASSERT_EQ(SearchResult_GetDocId(&res), ++resultCount + 100);
      SearchResult_Clear(&res);
    }
  } while ((rc2 = slowDepleter->Next(slowDepleter, &res)) == RS_RESULT_OK);
  ASSERT_EQ(rc2, RS_RESULT_EOF);
  ASSERT_EQ(resultCount, n_docs);

  // Clean up
  SearchResult_Destroy(&res);
  fastDepleter->Free(fastDepleter);
  slowDepleter->Free(slowDepleter);
}

TEST_P(RPDepleterTest, RPDepleter_Error) {
  // Tests RPDepleter handling of upstream error: background thread gets error,
  // main thread waits on condition variable, then propagates the error.
  // Mock upstream processor sends an error on the first call.

  bool take_index_lock = GetParam();

  QueryProcessingCtx qitr = {0};

  MockUpstream mockUpstream(0, RS_RESULT_ERROR);

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New(1, take_index_lock), &searchContexts[0], &searchContexts[1]);

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;

  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
  }
  // We now expect to have more than one call to the Next function while the
  // depleter is running in the background.
  ASSERT_GT(depletingCount, 0);

  // Now, results should be available (no results will be reached here)
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(SearchResult_GetDocId(&res), ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  // The last return code should be RS_RESULT_EOF, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_EOF);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

// Note: This test verifies that RPDepleter_ClearResults compiles and links correctly.
// A more comprehensive test would require access to RPDepleter's internal fields,
// which are intentionally opaque. The function is tested indirectly through
// the destructor and other integration tests.
TEST_P(RPDepleterTest, TestClearResults) {
  bool take_index_lock = GetParam();

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New(1, take_index_lock), &searchContexts[0], &searchContexts[1]);

  // Cast to RPDepleter to call the clear function
  RPDepleter *rp_depleter = (RPDepleter *)depleter;

  // Call the clear function - this should not crash
  // The function clears internal results and resets the index
  RPDepleter_ClearResults(rp_depleter);

  // Clean up
  depleter->Free(depleter);

  // If we got here without crashing, the test passed
  SUCCEED();
}

// Instantiate the parameterized test with both true and false values
INSTANTIATE_TEST_SUITE_P(
    LockingVariants,
    RPDepleterTest,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "WithIndexLock" : "WithoutIndexLock";
    }
);

// Test thread pool failure simulation
TEST_P(RPDepleterTest, TestThreadPoolFailure) {
  bool take_index_lock = GetParam();

  // Mock upstream processor: yields 3 results, then EOF
  const size_t n_docs = 3;
  QueryProcessingCtx qitr = {0};

  MockUpstream mockUpstream(n_docs, RS_RESULT_EOF);

  // Test 1: First test the successful case (no thread pool failure)
  Test_SetThreadPoolFailure(false);

  // If testing with index locking, the main thread must acquire the lock first
  if (take_index_lock) {
    RedisSearchCtx_LockSpecRead(&searchContexts[1]);
  }

  // Create depleter processor - should work normally
  ResultProcessor *depleter1 = RPDepleter_New(DepleterSync_New(1, take_index_lock), &searchContexts[0], &searchContexts[1]);

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter1);

  SearchResult res = {0};
  int rc;

  int depletingCount = 0;
  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter1->Next(depleter1, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
  }
  ASSERT_GT(depletingCount, 0) << "Should have at least one depleting state when thread pool works";

  // Now, results should be available
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter1->Next(depleter1, &res)) == RS_RESULT_OK);

  // Should have processed all documents
  ASSERT_EQ(resultCount, n_docs) << "Should have processed all documents when thread pool works";
  ASSERT_EQ(rc, RS_RESULT_EOF) << "Should end with RS_RESULT_EOF when thread pool works";

  // Clean up first depleter
  depleter1->Free(depleter1);

  // Unlock the index lock if we acquired it in test 1
  if (take_index_lock) {
    RedisSearchCtx_UnlockSpec(&searchContexts[1]);
  }

  // Test 2: Enable thread pool failure - should return RS_RESULT_ERROR immediately
  Test_SetThreadPoolFailure(true);

  // Reset query processing context for second test
  QueryProcessingCtx qitr2 = {0};

  // If testing with index locking, the main thread must acquire the lock first
  if (take_index_lock) {
    RedisSearchCtx_LockSpecRead(&searchContexts[1]);
  }

  // Create new depleter processor
  ResultProcessor *depleter2 = RPDepleter_New(DepleterSync_New(1, take_index_lock), &searchContexts[0], &searchContexts[1]);

  QITR_PushRP(&qitr2, &mockUpstream);
  QITR_PushRP(&qitr2, depleter2);

  // Reset mock upstream counter for second test
  mockUpstream.count = 0;

  // The first call should return RS_RESULT_ERROR due to thread pool failure
  rc = depleter2->Next(depleter2, &res);
  ASSERT_EQ(rc, RS_RESULT_ERROR) << "Expected RS_RESULT_ERROR when thread pool failure is enabled";

  SearchResult_Destroy(&res);
  depleter2->Free(depleter2);

  // Unlock the index lock if we acquired it in test 2
  if (take_index_lock) {
    RedisSearchCtx_UnlockSpec(&searchContexts[1]);
  }

  // Reset thread pool failure state for other tests
  Test_SetThreadPoolFailure(false);
}
