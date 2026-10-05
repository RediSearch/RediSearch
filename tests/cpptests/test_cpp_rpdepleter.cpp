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
#include "pipeline_execution.h"
#include "rmalloc.h"
#include "common.h"
#include "module.h"
#include <thread>
#include <chrono>
#include "redismock/redismock.h"
#include "search_result.h"
#include "hybrid/hybrid_scoring.h"

#include <thread>
#include <chrono>
#include <atomic>
#include <future>

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
      this->Drain = RPDrain_EOF;
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

  TimeoutConfig timeoutConfig = {};

  void SetUp() override {
    timeoutConfig.timeoutPolicy = TimeoutPolicy_Return;
    timeoutConfig.queryTimeoutMS = 10000;
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
      QueryRequestTimeout_Init(timeouts[i], &timeoutConfig);
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
    if (rc == RS_RESULT_TIMEDOUT) {
      EXPECT_EQ(0, resultCount);
      while (depleter->Drain(depleter, &res) == RP_DRAIN_OK) {
        EXPECT_EQ(SearchResult_GetDocId(&res), ++resultCount);
        SearchResult_Clear(&res);
      }
    }
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
  // Next folds immediately; Drain yields the buffered partial results.

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

TEST_P(RPSafeDepleterTest, DrainDoesNotStartUnscheduledProducer) {
  QueryProcessingCtx context = {};
  MockUpstream upstream;
  auto *depleter = RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0],
                                    depleterPool);
  QITR_PushRP(&context, &upstream);
  QITR_PushRP(&context, depleter);
  auto row = SearchResult_New();
  EXPECT_FALSE(RPSafeDepleter_HasPublishedOutput(depleter));
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &row));
  EXPECT_EQ(0, upstream.count);
  SearchResult_Destroy(&row);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, ReturnRecoversUpstreamBeforePublishingButFailDoesNotDrain) {
  for (auto policy : {TimeoutPolicy_Return, TimeoutPolicy_Fail}) {
    QueryProcessingCtx context = {};
    context.timeoutPolicy = policy;
    MockUpstream upstream(0, RS_RESULT_TIMEDOUT);
    upstream.Drain = [](ResultProcessor *base, SearchResult *row) {
      auto *self = static_cast<MockUpstream *>(base);
      if (self->count == 1) return RP_DRAIN_EOF;
      SearchResult_SetDocId(row, ++self->count);
      return RP_DRAIN_OK;
    };
    auto *depleter =
        RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
    QITR_PushRP(&context, &upstream);
    QITR_PushRP(&context, depleter);
    RPSafeDepleter_StartDepletion(depleter);
    RPSafeDepleter_WaitForCompletion(depleter);
    EXPECT_TRUE(RPSafeDepleter_HasPublishedOutput(depleter));
    EXPECT_EQ(policy == TimeoutPolicy_Return ? 1 : 0, upstream.count);
    auto row = SearchResult_New();
    if (policy == TimeoutPolicy_Return) {
      EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &row));
      EXPECT_EQ(1, SearchResult_GetDocId(&row));
      SearchResult_Clear(&row);
    }
    EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &row));
    SearchResult_Destroy(&row);
    depleter->Free(depleter);
  }
}

TEST_P(RPSafeDepleterTest, MergerDrainPreservesPublishedProducerTimeout) {
  QueryProcessingCtx producer = {}, consumer = {};
  producer.timeoutPolicy = TimeoutPolicy_Return;
  MockUpstream upstream(0, RS_RESULT_TIMEDOUT);
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  QITR_PushRP(&producer, &upstream);
  QITR_PushRP(&producer, depleter);
  RPSafeDepleter_StartDepletion(depleter);
  RPSafeDepleter_WaitForCompletion(depleter);
  auto **inputs = array_new(ResultProcessor *, 1);
  array_append(inputs, depleter);
  RPStatus status[] = {RS_RESULT_OK};
  auto *merger = RPHybridMerger_New(&searchContexts[0],
      HybridScoringContext_NewRRF(60, 3, false), inputs, 1, nullptr, nullptr,
      status, nullptr, nullptr);
  merger->parent = &consumer;
  auto row = SearchResult_New();
  EXPECT_EQ(RP_DRAIN_EOF, merger->Drain(merger, &row));
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status[0]);
  SearchResult_Destroy(&row);
  merger->Free(merger);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, ReturnPublishesUpstreamDrainErrorAfterRecoveredRow) {
  QueryProcessingCtx context = {};
  context.timeoutPolicy = TimeoutPolicy_Return;
  MockUpstream upstream(0, RS_RESULT_TIMEDOUT);
  upstream.Drain = [](ResultProcessor *base, SearchResult *row) {
    auto *self = static_cast<MockUpstream *>(base);
    if (self->count++) return RP_DRAIN_ERROR;
    SearchResult_SetDocId(row, 1);
    return RP_DRAIN_OK;
  };
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  QITR_PushRP(&context, &upstream);
  QITR_PushRP(&context, depleter);
  RPSafeDepleter_StartDepletion(depleter);
  RPSafeDepleter_WaitForCompletion(depleter);
  EXPECT_TRUE(RPSafeDepleter_HasPublishedOutput(depleter));
  EXPECT_EQ(2, upstream.count);
  auto row = SearchResult_New();
  EXPECT_EQ(RP_DRAIN_ERROR, depleter->Drain(depleter, &row));
  EXPECT_EQ(RP_DRAIN_ERROR, depleter->Drain(depleter, &row));
  EXPECT_EQ(2, upstream.count);
  SearchResult_Destroy(&row);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, DrainUsesCompletedPublicationAndPreservesError) {
  for (int terminal : {RS_RESULT_EOF, RS_RESULT_TIMEDOUT, RS_RESULT_ERROR}) {
    SCOPED_TRACE(terminal);
    QueryProcessingCtx context = {};
    context.timeoutPolicy = TimeoutPolicy_Return;
    MockUpstream upstream(3, terminal);
    upstream.Drain = [](ResultProcessor *, SearchResult *) {
      ADD_FAILURE() << "A nonempty producer must not replenish during recovery";
      return RP_DRAIN_ERROR;
    };
    auto *depleter =
        RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
    QITR_PushRP(&context, &upstream);
    QITR_PushRP(&context, depleter);
    RPSafeDepleter_StartDepletion(depleter);
    RPSafeDepleter_WaitForCompletion(depleter);
    const auto next = depleter->Next;
    EXPECT_TRUE(RPSafeDepleter_HasPublishedOutput(depleter));
    EXPECT_EQ(next, depleter->Next);
    auto row = SearchResult_New();
    if (terminal == RS_RESULT_ERROR) {
      EXPECT_EQ(RP_DRAIN_ERROR, depleter->Drain(depleter, &row));
    } else {
      for (unsigned id = 1; id <= 3; ++id) {
        EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &row));
        EXPECT_EQ(id, SearchResult_GetDocId(&row));
        SearchResult_Clear(&row);
      }
      EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &row));
    }
    EXPECT_EQ(3, upstream.count);
    SearchResult_Destroy(&row);
    depleter->Free(depleter);
  }
}

TEST_P(RPSafeDepleterTest, DrainReturnsBeforeParkedProducerCompletes) {
  struct ParkedSource : ResultProcessor {
    std::promise<void> entered;
    std::shared_future<void> resume;
    explicit ParkedSource(std::shared_future<void> resume) : ResultProcessor{}, resume(resume) {
      Next = [](ResultProcessor *base, SearchResult *) -> int {
        auto *self = static_cast<ParkedSource *>(base);
        self->entered.set_value();
        self->resume.wait();
        return RS_RESULT_EOF;
      };
    }
  };
  std::promise<void> resume;
  ParkedSource upstream(resume.get_future().share());
  auto parked = upstream.entered.get_future();
  QueryProcessingCtx context = {};
  auto *depleter =
      RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  QITR_PushRP(&context, &upstream);
  QITR_PushRP(&context, depleter);
  RPSafeDepleter_StartDepletion(depleter);
  parked.wait();
  auto recovery = std::async(std::launch::async, [&] {
    auto row = SearchResult_New();
    EXPECT_FALSE(RPSafeDepleter_HasPublishedOutput(depleter));
    const auto status = depleter->Drain(depleter, &row);
    SearchResult_Destroy(&row);
    return status;
  });
  const auto ready = recovery.wait_for(std::chrono::seconds(1));
  resume.set_value();
  EXPECT_EQ(std::future_status::ready, ready);
  EXPECT_EQ(RP_DRAIN_EOF, recovery.get());
  RPSafeDepleter_WaitForCompletion(depleter);
  depleter->Free(depleter);
}

TEST_P(RPSafeDepleterTest, OwnedRecoveryPublishesRowsWithoutCompletingParkedJob) {
  struct Source : ResultProcessor {
    unsigned calls = 0;
    bool drained = false;
    std::promise<void> parked;
    std::promise<void> resume;
    Source() : ResultProcessor{} {
      Next = [](ResultProcessor *base, SearchResult *row) -> int {
        auto *self = static_cast<Source *>(base);
        if (++self->calls == 1) {
          SearchResult_SetDocId(row, 1);
          return RS_RESULT_OK;
        }
        auto *access = static_cast<PipelineAccess *>(base->parent->executionAccess);
        auto resume = self->resume.get_future();
        auto *parked = &self->parked;
        PipelineAccess_ReleaseForWait(access);
        parked->set_value();
        resume.wait();
        return PipelineAccess_ResumeAfterWait(access) ? RS_RESULT_EOF : RS_RESULT_TIMEDOUT;
      };
      Drain = [](ResultProcessor *base, SearchResult *row) {
        auto *self = static_cast<Source *>(base);
        if (self->drained) return RP_DRAIN_EOF;
        self->drained = true;
        SearchResult_SetDocId(row, 2);
        return RP_DRAIN_OK;
      };
    }
  } upstream;
  auto *timeout = searchContexts[0].timeout;
  const TimeoutConfig timeoutConfig = {.queryTimeoutMS = 1000, .timeoutPolicy = TimeoutPolicy_ReturnStrict};
  QueryRequestTimeout_Init(timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  auto *execution = PipelineExecution_New(timeout);
  QueryProcessingCtx producer = {}, consumer = {};
  auto *depleter = RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  QITR_PushRP(&producer, &upstream);
  QITR_PushRP(&producer, depleter);
  depleter->parent = &consumer;
  RPSafeDepleter_SetExecution(depleter, execution);
  auto parked = upstream.parked.get_future();
  RPSafeDepleter_StartDepletion(depleter);
  parked.wait();
  QueryRequestTimeout_MarkTimedOut(timeout);
  auto recovery = std::async(std::launch::async, [&] {
    RPSafeDepleter_Recover(depleter);
    EXPECT_TRUE(RPSafeDepleter_HasPublishedOutput(depleter));
    auto row = SearchResult_New();
    EXPECT_EQ(RP_DRAIN_OK, depleter->Drain(depleter, &row));
    EXPECT_EQ(1, SearchResult_GetDocId(&row));
    SearchResult_Clear(&row);
    EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &row));
    EXPECT_FALSE(upstream.drained);
    SearchResult_Destroy(&row);
  });
  const auto ready = recovery.wait_for(std::chrono::seconds(1));
  std::promise<void> joining;
  auto joinStarted = joining.get_future();
  auto joined = std::async(std::launch::async, [&] {
    joining.set_value();
    RPSafeDepleter_WaitForCompletion(depleter);
  });
  joinStarted.wait();
  EXPECT_EQ(std::future_status::timeout, joined.wait_for(std::chrono::milliseconds(50)));
  upstream.resume.set_value();
  recovery.get();
  joined.get();
  EXPECT_EQ(std::future_status::ready, ready);
  EXPECT_EQ(2, upstream.calls);
  auto row = SearchResult_New();
  EXPECT_EQ(RS_RESULT_TIMEDOUT, depleter->Next(depleter, &row));
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &row));
  SearchResult_Destroy(&row);
  depleter->Free(depleter);
  PipelineExecution_Free(execution);
}

TEST_P(RPSafeDepleterTest, OwnedRecoveryBeforeProducerStartsRejectsLateExecution) {
  QueryProcessingCtx producer = {}, consumer = {};
  MockUpstream upstream;
  upstream.Drain = RPDrain_EOF;
  auto *timeout = searchContexts[0].timeout;
  const TimeoutConfig timeoutConfig = {.queryTimeoutMS = 1000, .timeoutPolicy = TimeoutPolicy_ReturnStrict};
  QueryRequestTimeout_Init(timeout, &timeoutConfig);
  QueryRequestTimeout_BeginCycle(timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  auto *execution = PipelineExecution_New(timeout);
  auto *depleter = RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
  QITR_PushRP(&producer, &upstream);
  QITR_PushRP(&producer, depleter);
  depleter->parent = &consumer;
  RPSafeDepleter_SetExecution(depleter, execution);
  QueryRequestTimeout_MarkTimedOut(timeout);
  RPSafeDepleter_Recover(depleter);
  EXPECT_TRUE(RPSafeDepleter_HasPublishedOutput(depleter));
  RPSafeDepleter_StartDepletion(depleter);
  RPSafeDepleter_WaitForCompletion(depleter);
  EXPECT_EQ(0, upstream.count);
  auto row = SearchResult_New();
  EXPECT_EQ(RS_RESULT_TIMEDOUT, depleter->Next(depleter, &row));
  EXPECT_EQ(RP_DRAIN_EOF, depleter->Drain(depleter, &row));
  SearchResult_Destroy(&row);
  depleter->Free(depleter);
  PipelineExecution_Free(execution);
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

TEST_P(RPSafeDepleterTest, ConsumerOwnershipWaitResumesLocallyOrFoldsAfterDrain) {
  for (bool cancel : {false, true}) {
    SCOPED_TRACE(cancel);
    struct Source : ResultProcessor {
      std::promise<void> entered;
      std::shared_future<void> resume;
      bool produced = false;
      explicit Source(std::shared_future<void> resume) : ResultProcessor{}, resume(resume) {
        Next = [](ResultProcessor *base, SearchResult *row) -> int {
          auto *self = static_cast<Source *>(base);
          if (self->produced) return RS_RESULT_EOF;
          self->entered.set_value();
          self->resume.wait();
          self->produced = true;
          SearchResult_SetDocId(row, 7);
          return RS_RESULT_OK;
        };
      }
    };
    std::promise<void> resume;
    Source upstream(resume.get_future().share());
    QueryProcessingCtx producer = {}, consumer = {};
    auto *depleter =
        RPSafeDepleter_New(DepleterSync_New(1, GetParam()), &searchContexts[0], depleterPool);
    QITR_PushRP(&producer, &upstream);
    QITR_PushRP(&producer, depleter);
    depleter->parent = &consumer;
    consumer.endProc = consumer.rootProc = RPProfile_New(depleter, &consumer);
    RPSafeDepleter_StartDepletion(depleter);
    upstream.entered.get_future().wait();
    QueryRequestTimeout timeout = {};
    const TimeoutConfig timeoutConfig = {.queryTimeoutMS = 1000, .timeoutPolicy = TimeoutPolicy_ReturnStrict};
    QueryRequestTimeout_Init(&timeout, &timeoutConfig);
    QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    auto *execution = PipelineExecution_New(&timeout);
    struct Work {
      QueryProcessingCtx *context;
      std::promise<void> entered;
      int rc = RS_RESULT_ERROR;
      unsigned calls = 0;
      SearchResult row = SearchResult_New();
    } work{&consumer};
    auto entered = work.entered.get_future();
    auto worker = std::async(std::launch::async, [&] {
      return PipelineExecution_RunNext(
          execution,
          [](PipelineAccess *access, void *data) {
            auto *work = static_cast<Work *>(data);
            ++work->calls;
            PipelineAccess_Publish(access, work->context);
            work->entered.set_value();
            work->rc = work->context->endProc->Next(work->context->endProc, &work->row);
          },
          &work);
    });
    entered.wait();
    if (cancel) {
      QueryRequestTimeout_MarkTimedOut(&timeout);
      auto recovery = std::async(std::launch::async, [&] {
        PipelineExecution_RunDrain(
            execution,
            [](PipelineAccess *access, void *) {
              auto *context = PipelineAccess_Context(access);
              auto row = SearchResult_New();
              EXPECT_EQ(RP_DRAIN_EOF, context->endProc->Drain(context->endProc, &row));
              SearchResult_Destroy(&row);
            },
            nullptr);
      });
      auto ready = recovery.wait_for(std::chrono::seconds(1));
      resume.set_value();
      recovery.get();
      EXPECT_EQ(std::future_status::ready, ready);
      EXPECT_FALSE(worker.get());
      EXPECT_EQ(RS_RESULT_TIMEDOUT, work.rc);
      EXPECT_EQ(1, RPProfile_GetCount(consumer.endProc));
    } else {
      // Prove the consumer released its gate before letting the producer finish;
      // otherwise this test could pass entirely through the already-done path.
      bool released = false;
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
      do {
        released = PipelineExecution_RunNext(execution, [](PipelineAccess *, void *) {}, nullptr);
        if (!released) std::this_thread::yield();
      } while (!released && std::chrono::steady_clock::now() < deadline);
      EXPECT_TRUE(released);
      resume.set_value();
      EXPECT_TRUE(worker.get());
      EXPECT_EQ(RS_RESULT_OK, work.rc);
      EXPECT_EQ(7, SearchResult_GetDocId(&work.row));
    }
    EXPECT_EQ(1, work.calls);
    RPSafeDepleter_WaitForCompletion(depleter);
    SearchResult_Destroy(&work.row);
    consumer.endProc->Free(consumer.endProc);
    depleter->Free(depleter);
    PipelineExecution_Free(execution);
  }
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

  EXPECT_TRUE(RPSafeDepleter_HasPublishedOutput(depleter));

  AssertWaitForCompletionDoesNotBlock(depleter);

  SearchResult res = SearchResult_New();
  int rc = depleter->Next(depleter, &res);
  ASSERT_EQ(rc, RS_RESULT_TIMEDOUT);

  // Still a no-op after yielding on the marked depleter.
  AssertWaitForCompletionDoesNotBlock(depleter);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

// Both hybrid depleters deliberately share one context. The gate keeps their
// read locks overlapping until the launcher's handoff has completed.
TEST_P(RPSafeDepleterTest, SharedContextOwnership) {
  if (!GetParam()) {
    GTEST_SKIP() << "Requires spec locking";
  }
  struct GatedUpstream : ResultProcessor {
    std::shared_future<void> gate;
    IndexSpec *spec;
    bool yielded = false;

    GatedUpstream(std::shared_future<void> gate, IndexSpec *spec)
        : ResultProcessor{}, gate(gate), spec(spec) {
      Next = [](ResultProcessor *base, SearchResult *result) -> int {
        auto *self = static_cast<GatedUpstream *>(base);
        EXPECT_TRUE(IndexSpec_IsReadLocked(self->spec));
        self->gate.wait();
        if (self->yielded) return RS_RESULT_EOF;
        self->yielded = true;
        SearchResult_SetDocId(result, 1);
        return RS_RESULT_OK;
      };
    }
  };
  std::promise<void> release;
  auto gate = release.get_future().share();
  GatedUpstream upstream1(gate, mockSpec), upstream2(gate, mockSpec);
  QueryProcessingCtx qctx1{}, qctx2{};
  QueryError error = QueryError_Default();
  qctx1.err = qctx2.err = &error;
  StrongRef sync = DepleterSync_New(2, true);
  ResultProcessor *first =
      RPSafeDepleter_New(StrongRef_Clone(sync), &searchContexts[0], depleterPool);
  ResultProcessor *second = RPSafeDepleter_New(sync, &searchContexts[0], depleterPool);
  QITR_PushRP(&qctx1, &upstream1);
  QITR_PushRP(&qctx1, first);
  QITR_PushRP(&qctx2, &upstream2);
  QITR_PushRP(&qctx2, second);
  auto depleters = array_new(ResultProcessor *, 2);
  array_append(depleters, first);
  array_append(depleters, second);

  IndexSpec_LockRead(mockSpec);
  EXPECT_EQ(RPSafeDepleter_StartAll(depleters, &searchContexts[0], &error), RS_RESULT_OK);
  EXPECT_FALSE(IndexSpec_IsLocked(mockSpec));
  EXPECT_EQ(mockSpec->keysDict->pauserehash, 2);
  EXPECT_NE(pthread_rwlock_trywrlock(&mockSpec->rwlock), 0);
  release.set_value();
  RPSafeDepleter_JoinAll(depleters);
  EXPECT_EQ(mockSpec->keysDict->pauserehash, 0);
  for (auto *depleter : {first, second}) {
    SearchResult result = SearchResult_New();
    EXPECT_EQ(depleter->Next(depleter, &result), RS_RESULT_OK);
    EXPECT_EQ(SearchResult_GetDocId(&result), 1);
    SearchResult_Clear(&result);
    EXPECT_EQ(depleter->Next(depleter, &result), RS_RESULT_EOF);
    SearchResult_Destroy(&result);
    depleter->Free(depleter);
  }
  array_free(depleters);
  QueryError_ClearError(&error);
}

// Instantiate the parameterized test with both true and false values
INSTANTIATE_TEST_SUITE_P(
    LockingVariants,
    RPSafeDepleterTest,
    ::testing::Values(false, true),
    [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "WithIndexLock" : "WithoutIndexLock";
    }
);
