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
#include "search_result_ffi.h"
#include "spec.h"
#include "search_ctx.h"
#include "query_request.h"
#include "rmalloc.h"
#include "common.h"
#include "module.h"
#include <thread>
#include <chrono>
#include "redismock/redismock.h"
#include "search_result.h"
#include "thpool/thpool.h"
#include "value_ffi.h"

#include <thread>
#include <chrono>
#include <atomic>

#define NumberOfContexts 3

// Base test class for parameterized tests
class RPSafeDepleterTest : public ::testing::Test, public ::testing::WithParamInterface<bool> {
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
      mockSpec = Indexes_CreateNewSpec(ctx, argv, argv.size(), &err);
      if (!mockSpec) {
        printf("Failed to create index spec. Error code: %d, Error message: %s\n",
               QueryError_GetCode(&err), QueryError_GetUserError(&err));
      }
      ASSERT_NE(mockSpec, nullptr) << "Failed to create index spec. Error: " << QueryError_GetUserError(&err);
    }

    // Initialize search contexts for all tests (with or without real spec)
    for (size_t i = 0; i < NumberOfContexts; ++i) {
      searchContexts[i] = SEARCH_CTX_STATIC(redisContexts[i], mockSpec);
      timeouts[i] = static_cast<QueryRequestTimeout *>(rm_calloc(1, sizeof(QueryRequestTimeout)));
      QueryRequestTimeout_Init(timeouts[i], TimeoutPolicy_Return, 10000);
      QueryRequestTimeout_BeginCycle(timeouts[i], QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE);
      searchContexts[i].timeout = timeouts[i];
    }

    // Set a stable request-owned deadline for tests that temporarily disable mock behavior.
    struct timespec future_timeout;
    clock_gettime(CLOCK_MONOTONIC_RAW, &future_timeout);
    future_timeout.tv_sec += 10; // 10 seconds from now
    for (size_t i = 0; i < NumberOfContexts; ++i) {
      *QueryRequestTimeout_GetClockDeadlineForUpdate(searchContexts[i].timeout) = future_timeout;
    }
  }

  void TearDown() override {
    // Free Redis contexts for all test variants (WithoutIndexLock and WithIndexLock)
    for (auto ctx : redisContexts) {
      RedisModule_FreeThreadSafeContext(ctx);
    }
    for (auto timeout : timeouts) {
      rm_free(timeout);
    }
  }

  // Build a single-depleter pipeline over `upstream`, start depletion, drain
  // the DEPLETING phase, then yield every buffered result, expecting doc ids
  // sequential from 1. Returns the last (non-OK) return code.
  int runDepleterToCompletion(ResultProcessor *upstream, int expectedResults) {
    QueryProcessingCtx qitr = {0};
    ResultProcessor *depleter =
        RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
    QITR_PushRP(&qitr, upstream);
    QITR_PushRP(&qitr, depleter);

    RPSafeDepleter_StartDepletion(depleter);

    SearchResult res = SearchResult_New();
    int rc;
    while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
      // Next blocks on the shared cv; nothing to do between wakeups.
    }

    int resultCount = 0;
    do {
      if (rc == RS_RESULT_OK) {
        EXPECT_EQ(SearchResult_GetDocId(&res), ++resultCount);
        SearchResult_Clear(&res);
      }
    } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);
    EXPECT_EQ(resultCount, expectedResults);

    SearchResult_Destroy(&res);
    depleter->Free(depleter);
    return rc;
  }

  std::array<RedisModuleCtx*, NumberOfContexts> redisContexts;
  std::array<RedisSearchCtx, NumberOfContexts> searchContexts;
  std::array<QueryRequestTimeout *, NumberOfContexts> timeouts = {nullptr};
  IndexSpec* mockSpec = nullptr;
};

TEST_P(RPSafeDepleterTest, RPSafeDepleter_Basic) {
  // Tests basic RPSafeDepleter functionality: background thread depletes upstream results,
  // main thread waits on condition variable, then yields results in order.

  // Mock upstream processor: yields 3 results, then EOF
  const int n_docs = 3;
  MockUpstream mockUpstream(n_docs, RS_RESULT_EOF);

  // The last return code should be RS_RESULT_EOF, as the upstream last returned.
  ASSERT_EQ(runDepleterToCompletion(&mockUpstream, n_docs), RS_RESULT_EOF);
}

TEST_P(RPSafeDepleterTest, RPSafeDepleter_Timeout) {
  // Tests RPSafeDepleter handling of upstream timeout: background thread gets timeout,
  // main thread waits on condition variable, then yields results and timeout.

  // Mock upstream processor: yields 3 results, then timeout.
  const int n_docs = 3;
  MockUpstream mockUpstream(n_docs, RS_RESULT_TIMEDOUT);

  // The last return code should be RS_RESULT_TIMEDOUT, as the upstream last returned.
  ASSERT_EQ(runDepleterToCompletion(&mockUpstream, n_docs), RS_RESULT_TIMEDOUT);
}

TEST_P(RPSafeDepleterTest, RPSafeDepleter_CrossWakeup) {
  // Tests cross-safe-depleter condition variable signaling: when one safe depleter finishes,
  // it signals the shared condition variable, waking up other safe depleters that return
  // `RS_RESULT_DEPLETING` (allowing downstream to try other safe depleters for results).
  // Test that one safe depleter can wake up another safe depleter waiting on the same condition variable.
  // This tests the core mechanism where safe depleters share sync objects and signal each other.
  // High sleep times are used in order to avoid flakiness.

  bool take_index_lock = GetParam();

  const size_t n_docs = 2;
  QueryProcessingCtx qitr1 = {0}, qitr2 = {0};

  // Mock upstream that finishes quickly (500ms sleep per result)
  MockUpstream fastUpstream(n_docs, RS_RESULT_EOF, 500, 0);

  // Mock upstream that takes much longer (1000ms sleep per result, different doc IDs)
  MockUpstream slowUpstream(n_docs, RS_RESULT_EOF, 1000, 100);

  // Create shared sync reference and two safe depleters sharing it
  StrongRef sync_ref = DepleterSync_New(2, take_index_lock);
  ResultProcessor *fastDepleter = RPSafeDepleter_New(StrongRef_Clone(sync_ref), &searchContexts[0], depleterPool);
  ResultProcessor *slowDepleter = RPSafeDepleter_New(StrongRef_Clone(sync_ref), &searchContexts[1], depleterPool);
  StrongRef_Release(sync_ref);  // Release our reference

  // Set up pipelines
  QITR_PushRP(&qitr1, &fastUpstream);
  QITR_PushRP(&qitr1, fastDepleter);
  QITR_PushRP(&qitr2, &slowUpstream);
  QITR_PushRP(&qitr2, slowDepleter);

  RPSafeDepleter_StartDepletion(slowDepleter);
  RPSafeDepleter_StartDepletion(fastDepleter);

  SearchResult res = SearchResult_New();

  // Call Next on the slow depleter, and get `RS_RESULT_DEPLETING`, indicating
  // that the fast depleter-thread has finished and woke it up.
  int rc2 = slowDepleter->Next(slowDepleter, &res);
  ASSERT_EQ(rc2, RS_RESULT_DEPLETING);

  // Drain any further cross-wakeups until the fast depleter itself completes.
  int rc1;
  while ((rc1 = fastDepleter->Next(fastDepleter, &res)) == RS_RESULT_DEPLETING) {
    // Next blocks on the shared cv; nothing to do between wakeups.
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

TEST_P(RPSafeDepleterTest, RPSafeDepleter_Error) {
  // Tests RPSafeDepleter handling of upstream error: background thread gets error,
  // main thread waits on condition variable, then propagates the error.
  // Mock upstream processor sends an error on the first call; no results reach
  // the yield phase.

  MockUpstream mockUpstream(0, RS_RESULT_ERROR);

  // The last return code should be RS_RESULT_EOF, as the upstream last returned.
  ASSERT_EQ(runDepleterToCompletion(&mockUpstream, 0), RS_RESULT_EOF);
}

// Drive RPSafeDepleter_WaitForCompletion on a separate thread and assert it
// returns within `timeout_ms`. If it blocks longer, fail the test rather than
// deadlock the whole binary.
static void AssertWaitForCompletionDoesNotBlock(ResultProcessor *depleter, int timeout_ms = 5000) {
  std::atomic done{false};
  std::thread waiter([&] {
    RPSafeDepleter_WaitForCompletion(depleter);
    done.store(true);
  });
  for (int i = 0; i < timeout_ms / 10 && !done.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_TRUE(done.load())
      << "WaitForCompletion blocked despite no BG depletion in flight (deadlock)";
  waiter.join();
}

// Regression for the deadlock found in the split-pipeline path: a depleter the
// launcher resolved as timed out has no BG job in flight, so WaitForCompletion
// must return immediately rather than block on `done_depleting`, which will
// never be signaled — and Next must yield the timeout.
TEST_P(RPSafeDepleterTest, RPSafeDepleter_MarkTimedOut) {
  bool take_index_lock = GetParam();
  QueryProcessingCtx qitr = {nullptr};

  MockUpstream mockUpstream(3, RS_RESULT_EOF);

  ResultProcessor *depleter = RPSafeDepleter_New(
      DepleterSync_New(1, take_index_lock), &searchContexts[0], depleterPool);

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  RPSafeDepleter_MarkTimedOut(depleter);

  AssertWaitForCompletionDoesNotBlock(depleter);

  SearchResult res = SearchResult_New();
  int rc = depleter->Next(depleter, &res);
  ASSERT_EQ(rc, RS_RESULT_TIMEDOUT);

  // Still a no-op after yielding on the marked depleter.
  AssertWaitForCompletionDoesNotBlock(depleter);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, DrainBeforeQueuedJobStartsNeverWaitsOrPullsUpstream) {
  auto *pool = redisearch_thpool_create(1, 0, nullptr, "drain");
  ASSERT_NE(nullptr, pool);
  redisearch_thpool_pause_threads(pool);
  QueryProcessingCtx qctx = {};
  qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  MockUpstream source;
  auto *depleter = RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], pool);
  depleter->parent = &qctx;
  depleter->upstream = &source;
  SearchResult result = SearchResult_New();
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &result));
  RPSafeDepleter_StartDepletion(depleter);
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &result));
  EXPECT_EQ(RS_RESULT_TIMEDOUT, depleter->Next(depleter, &result));
  redisearch_thpool_resume_threads(pool);
  RPSafeDepleter_WaitForCompletion(depleter);
  EXPECT_EQ(0, source.count);
  SearchResult_Destroy(&result);
  depleter->Free(depleter);
  redisearch_thpool_destroy(pool);
}

TEST_P(RPSafeDepleterTest, DrainCommittedRowsWhileWorkerAndNextArePending) {
  struct Source : MockUpstream {
    std::atomic<bool> entered{false}, release{false};
    Source() {
      Next = [](ResultProcessor *base, SearchResult *row) -> int {
        auto *self = static_cast<Source *>(base);
        if (self->count == 2) {
          self->entered.store(true, std::memory_order_release);
          while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
        }
        return MockUpstream::NextFn(base, row);
      };
    }
  } source;
  QueryProcessingCtx qctx = {};
  qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  depleter->parent = &qctx;
  depleter->upstream = &source;
  RPSafeDepleter_StartDepletion(depleter);
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  SearchResult next = SearchResult_New(), drained = SearchResult_New();
  int nextStatus = RS_RESULT_MAX;
  std::atomic<bool> nextStarted{false};
  std::thread reader([&] {
    nextStarted.store(true, std::memory_order_release);
    nextStatus = depleter->Next(depleter, &next);
  });
  bool started = RS::WaitForCondition([&] { return nextStarted.load(); }, 5);
  if (entered && started) {
    for (t_docId id : {1, 2}) {
      EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &drained));
      EXPECT_EQ(id, SearchResult_GetDocId(&drained));
      SearchResult_Clear(&drained);
    }
    EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &drained));
  }
  source.release.store(true, std::memory_order_release);
  reader.join();
  RPSafeDepleter_WaitForCompletion(depleter);
  EXPECT_TRUE(entered);
  EXPECT_TRUE(started);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, nextStatus);
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &drained));
  SearchResult_Destroy(&next);
  SearchResult_Destroy(&drained);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, DrainCompletedBufferAfterNextPrefix) {
  QueryProcessingCtx qctx = {};
  qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  MockUpstream source;
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  depleter->parent = &qctx;
  depleter->upstream = &source;
  RPSafeDepleter_StartDepletion(depleter);
  RPSafeDepleter_WaitForCompletion(depleter);
  SearchResult result = SearchResult_New();
  EXPECT_EQ(RS_RESULT_OK, depleter->Next(depleter, &result));
  EXPECT_EQ(1, SearchResult_GetDocId(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &result));
  EXPECT_EQ(2, SearchResult_GetDocId(&result));
  depleter->Free(depleter);
  EXPECT_EQ(2, SearchResult_GetDocId(&result));
  SearchResult_Destroy(&result);
}

TEST_P(RPSafeDepleterTest, DrainDoesNotWaitForWorkerBufferGrowth) {
  static thread_local bool pauseAllocation = false;
  struct Source : MockUpstream {
    Source() {
      Next = [](ResultProcessor *base, SearchResult *row) -> int {
        auto *self = static_cast<Source *>(base);
        pauseAllocation = self->count == 2;
        return MockUpstream::NextFn(base, row);
      };
    }
  } source;
  QueryProcessingCtx qctx = {};
  qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  depleter->parent = &qctx;
  depleter->upstream = &source;
  static std::atomic<bool> entered{false}, release{false};
  static void *(*originalAlloc)(size_t);
  entered.store(false);
  release.store(false);
  originalAlloc = RedisModule_Alloc;
  RedisModule_Alloc = [](size_t size) -> void * {
    if (pauseAllocation && size == sizeof(array_hdr_t) + 4 * sizeof(SearchResult *)) {
      pauseAllocation = false;
      entered.store(true, std::memory_order_release);
      while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
    }
    return originalAlloc(size);
  };
  RPSafeDepleter_StartDepletion(depleter);
  bool paused = RS::WaitForCondition([&] { return entered.load(); }, 5);
  SearchResult result = SearchResult_New();
  if (paused) {
    for (t_docId id : {1, 2}) {
      EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &result));
      EXPECT_EQ(id, SearchResult_GetDocId(&result));
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &result));
  }
  release.store(true, std::memory_order_release);
  RPSafeDepleter_WaitForCompletion(depleter);
  redisearch_thpool_wait(depleterPool);
  RedisModule_Alloc = originalAlloc;
  EXPECT_TRUE(paused);
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &result));
  SearchResult_Destroy(&result);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, DrainPayloadOwnershipSurvivesCleanupAndRejectsLateRowExactlyOnce) {
  struct Source : MockUpstream {
    RLookup lookup = RLookup_New();
    const RLookupKey *key = RLookup_GetKey_Write(&lookup, "payload", RLOOKUP_F_NOFLAGS);
    std::array<RSValue *, 4> values;
    std::atomic<bool> entered{false}, release{false};
    Source() : MockUpstream(4) {
      RLookup_Seal(&lookup);
      for (size_t i = 0; i < values.size(); ++i) values[i] = RSValue_NewNumber(i + 1);
      Next = [](ResultProcessor *base, SearchResult *row) -> int {
        auto *self = static_cast<Source *>(base);
        if (self->count == 3) {
          self->entered.store(true, std::memory_order_release);
          while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
        }
        int rc = MockUpstream::NextFn(base, row);
        if (rc == RS_RESULT_OK) {
          RSValue *value = self->values[self->count - 1];
          RSValue_IncrRef(value);
          RLookup_WriteOwnKey(self->key, SearchResult_GetRowDataMut(row), value);
        }
        return rc;
      };
    }
    ~Source() {
      for (auto *value : values) RSValue_DecrRef(value);
      RLookup_Cleanup(&lookup);
    }
  } source;
  QueryProcessingCtx qctx = {};
  qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  depleter->parent = &qctx;
  depleter->upstream = &source;
  RPSafeDepleter_StartDepletion(depleter);
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  SearchResult result = SearchResult_New();
  if (entered) EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &result));
  source.release.store(true, std::memory_order_release);
  RPSafeDepleter_WaitForCompletion(depleter);
  EXPECT_TRUE(entered);
  EXPECT_EQ(2, RSValue_Refcount(source.values[0]));
  EXPECT_EQ(2, RSValue_Refcount(source.values[1]));
  EXPECT_EQ(2, RSValue_Refcount(source.values[2]));
  EXPECT_EQ(1, RSValue_Refcount(source.values[3]));
  depleter->Free(depleter);
  EXPECT_EQ(2, RSValue_Refcount(source.values[0]));
  for (size_t i = 1; i < source.values.size(); ++i) {
    EXPECT_EQ(1, RSValue_Refcount(source.values[i]));
  }
  const RSValue *transferred = RLookupRow_Get(source.key, SearchResult_GetRowData(&result));
  EXPECT_EQ(source.values[0], transferred);
  if (transferred) EXPECT_DOUBLE_EQ(1, RSValue_Number_Get(transferred));
  SearchResult_Destroy(&result);
  EXPECT_EQ(1, RSValue_Refcount(source.values[0]));
}

// Instantiate the parameterized test with both true and false values
INSTANTIATE_TEST_SUITE_P(LockingVariants, RPSafeDepleterTest, ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool> &info) {
                           return info.param ? "WithIndexLock" : "WithoutIndexLock";
                         });
