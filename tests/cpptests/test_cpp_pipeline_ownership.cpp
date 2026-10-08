/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
extern "C" {
#include "util/dict.h"
}
#include "pipeline_execution.h"
#include "query_request.h"
#include "search_result_ffi.h"
#include "search_result.h"
#include "pipeline/pipeline.h"
extern "C" {
#include "aggregate/aggregate_exec_common.h"
}

#include <chrono>
#include <future>
#include <thread>

using namespace std::chrono_literals;

class PipelineOwnershipTest : public ::testing::Test {
 protected:
  QueryRequestTimeout timeout = {};
  PipelineExecution *execution = nullptr;
  QueryProcessingCtx context = {};

  void SetUp() override {
    const TimeoutConfig timeoutConfig = {.queryTimeoutMS = 1000, .timeoutPolicy = TimeoutPolicy_ReturnStrict};
    QueryRequestTimeout_Init(&timeout, &timeoutConfig);
    QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    execution = PipelineExecution_New(&timeout);
  }

  void TearDown() override {
    PipelineExecution_Free(execution);
  }
};

TEST_F(PipelineOwnershipTest, QueuedWorkerCannotEnterAfterTimeout) {
  QueryRequestTimeout_MarkTimedOut(&timeout);
  PipelineExecution_RunDrain(
      execution,
      [](PipelineAccess *access, void *) { EXPECT_EQ(nullptr, PipelineAccess_Context(access)); },
      nullptr);
  EXPECT_FALSE(PipelineExecution_RunNext(
      execution,
      [](PipelineAccess *, void *) {
        ADD_FAILURE() << "Timed-out queued work entered the pipeline";
      },
      nullptr));
}

TEST_F(PipelineOwnershipTest, ActiveExecutionPublishesItsFinalStateBeforeDrain) {
  std::promise<void> entered;
  auto started = entered.get_future();
  struct ActiveWork {
    QueryProcessingCtx *context;
    QueryRequestTimeout *timeout;
    std::promise<void> *entered;
  } work{&context, &timeout, &entered};
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(
        execution,
        [](PipelineAccess *access, void *data) {
          auto *work = static_cast<ActiveWork *>(data);
          PipelineAccess_Publish(access, work->context);
          work->entered->set_value();
          while (!QueryRequestTimeout_IsBlockedClientTimedOut(work->timeout)) {
            std::this_thread::yield();
          }
          work->context->totalResults = 29;
        },
        &work);
  });
  started.wait();
  QueryRequestTimeout_MarkTimedOut(&timeout);
  PipelineExecution_RunDrain(
      execution,
      [](PipelineAccess *access, void *) {
        EXPECT_EQ(29, PipelineAccess_Context(access)->totalResults);
      },
      nullptr);
  EXPECT_TRUE(worker.get());
}

// The same C callback remains on the stack across the wait. Only privately owned
// synchronization is touched until admission succeeds again.
struct ScopedOwnershipWork {
  QueryProcessingCtx *context;
  std::promise<void> parked;
  std::promise<void> wake;
  unsigned calls = 0;
  bool resumed = false;

  static void step(PipelineAccess *access, void *data) {
    auto *work = static_cast<ScopedOwnershipWork *>(data);
    ++work->calls;
    PipelineAccess_Publish(access, work->context);
    work->context->totalResults = 9;
    PipelineAccess_ReleaseForWait(access);
    work->parked.set_value();
    work->wake.get_future().wait();
    work->resumed = PipelineAccess_ResumeAfterWait(access);
    if (!work->resumed) return;
    EXPECT_EQ(access, PipelineAccess_Context(access)->executionAccess);
    PipelineAccess_Context(access)->totalResults += 4;
  }
};

TEST_F(PipelineOwnershipTest, ScopedWaitContinuesSameCallbackWithoutTimeout) {
  ScopedOwnershipWork work{.context = &context};
  work.wake.set_value();
  EXPECT_TRUE(PipelineExecution_RunNext(execution, ScopedOwnershipWork::step, &work));
  EXPECT_TRUE(work.resumed);
  EXPECT_EQ(1, work.calls);
  EXPECT_EQ(13, context.totalResults);
  EXPECT_EQ(nullptr, context.executionAccess);
}

TEST_F(PipelineOwnershipTest, ScopedWaitRejectsResumeAfterMainAlreadyDrained) {
  ScopedOwnershipWork work{.context = &context};
  auto parked = work.parked.get_future();
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, ScopedOwnershipWork::step, &work);
  });
  parked.wait();
  QueryRequestTimeout_MarkTimedOut(&timeout);
  auto drainer = std::async(std::launch::async, [&] {
    PipelineExecution_RunDrain(
        execution,
        [](PipelineAccess *access, void *) {
          EXPECT_EQ(9, PipelineAccess_Context(access)->totalResults);
          PipelineAccess_Context(access)->totalResults = 100;
        },
        nullptr);
  });
  const auto status = drainer.wait_for(1s);
  // On success recovery has finished before waking the worker, just as a GIL
  // acquisition can complete only after the timeout callback already replied.
  work.wake.set_value();
  drainer.get();
  EXPECT_FALSE(worker.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_FALSE(work.resumed);
  EXPECT_EQ(1, work.calls);
  EXPECT_EQ(100, context.totalResults);
  EXPECT_EQ(nullptr, context.executionAccess);
}

TEST_F(PipelineOwnershipTest, ScopedWaitRejectsTimeoutThatPrecedesRelease) {
  struct Work {
    QueryProcessingCtx *context;
    QueryRequestTimeout *timeout;
    bool cleaned = false;
  } work{&context, &timeout};
  EXPECT_FALSE(PipelineExecution_RunNext(
      execution,
      [](PipelineAccess *access, void *data) {
        auto *work = static_cast<Work *>(data);
        PipelineAccess_Publish(access, work->context);
        work->context->totalResults = 9;
        QueryRequestTimeout_MarkTimedOut(work->timeout);
        PipelineAccess_ReleaseForWait(access);
        EXPECT_FALSE(PipelineAccess_ResumeAfterWait(access));
        EXPECT_FALSE(PipelineAccess_IsOwned(access));
        work->cleaned = true;
      },
      &work));
  EXPECT_TRUE(work.cleaned);
  EXPECT_EQ(nullptr, context.executionAccess);
  PipelineExecution_RunDrain(
      execution,
      [](PipelineAccess *access, void *) {
        EXPECT_EQ(9, PipelineAccess_Context(access)->totalResults);
      },
      nullptr);
}

// Force growth beyond the collector's initial allocation before parking inside
// Next. Recovery must see the current allocation, not a stale local array pointer.
struct ScopedCollectorWork {
  ResultProcessor rp = {};
  ResultProcessor *tail = &rp;
  SearchResult **results = nullptr;
  std::promise<void> parked;
  std::promise<void> wake;
  unsigned produced = 0;
  int storedStatus = RS_RESULT_OK;

  explicit ScopedCollectorWork(QueryProcessingCtx *context) {
    rp.parent = context;
    rp.Drain = RPDrain_EOF;
    rp.Next = [](ResultProcessor *rp, SearchResult *row) -> int {
      auto *work = reinterpret_cast<ScopedCollectorWork *>(rp);
      if (work->produced == 20) {
        auto *access = static_cast<PipelineAccess *>(rp->parent->executionAccess);
        PipelineAccess_ReleaseForWait(access);
        work->parked.set_value();
        work->wake.get_future().wait();
        if (!PipelineAccess_ResumeAfterWait(access)) return RS_RESULT_TIMEDOUT;
      }
      if (work->produced == 25) return RS_RESULT_EOF;
      SearchResult_SetDocId(row, ++work->produced);
      return RS_RESULT_OK;
    };
  }

  static void step(PipelineAccess *access, void *data) {
    auto *work = static_cast<ScopedCollectorWork *>(data);
    PipelineAccess_Publish(access, work->rp.parent);
    int rc = RS_RESULT_EOF;
    AggregateResultsContinue(work->tail, nullptr, &rc, &work->results);
    if (rc == RS_RESULT_TIMEDOUT && !PipelineAccess_IsOwned(access)) return;
    work->storedStatus = rc;
  }
};

TEST_F(PipelineOwnershipTest, ScopedCollectorResumesWithoutDuplicatingPrefixOrBudget) {
  context.resultLimit = 23;
  ScopedCollectorWork work(&context);
  work.wake.set_value();
  EXPECT_TRUE(PipelineExecution_RunNext(execution, ScopedCollectorWork::step, &work));
  EXPECT_EQ(23, array_len(work.results));
  EXPECT_EQ(0, context.resultLimit);
  for (unsigned i = 0; i < array_len(work.results); ++i) {
    EXPECT_EQ(i + 1, SearchResult_GetDocId(work.results[i]));
  }
  destroyResults(work.results);
}

TEST_F(PipelineOwnershipTest, ScopedCollectorDoesNotOverwriteConsumedPrefixAfterTimeout) {
  context.resultLimit = 23;
  ScopedCollectorWork work(&context);
  auto parked = work.parked.get_future();
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, ScopedCollectorWork::step, &work);
  });
  parked.wait();
  QueryRequestTimeout_MarkTimedOut(&timeout);
  auto drainer = std::async(std::launch::async, [&] {
    PipelineExecution_RunDrain(
        execution,
        [](PipelineAccess *access, void *data) {
          auto *work = static_cast<ScopedCollectorWork *>(data);
          EXPECT_EQ(20, array_len(work->results));
          EXPECT_EQ(3, PipelineAccess_Context(access)->resultLimit);
          for (unsigned i = 0; i < array_len(work->results); ++i) {
            EXPECT_EQ(i + 1, SearchResult_GetDocId(work->results[i]));
          }
          destroyResults(work->results);
          work->results = nullptr;
          work->storedStatus = RS_RESULT_ERROR;
        },
        &work);
  });
  const auto status = drainer.wait_for(1s);
  work.wake.set_value();
  drainer.get();
  EXPECT_FALSE(worker.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(nullptr, work.results);
  EXPECT_EQ(RS_RESULT_ERROR, work.storedStatus);
  EXPECT_EQ(20, work.produced);
}

class ScopedAccumulatorTest : public PipelineOwnershipTest,
                              public ::testing::WithParamInterface<ResultProcessorType> {};

TEST_P(ScopedAccumulatorTest, LosingFramesPreserveRecoveredBudgetAndProfile) {
  context.resultLimit = 23;
  context.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  context.isProfile = true;
  ScopedCollectorWork work(&context);
  ResultProcessor *buffer = nullptr;
  switch (GetParam()) {
    case RP_SORTER:
      buffer = RPSorter_NewByScore(30, nullptr);
      break;
    case RP_DEPLETER:
      buffer = RPDepleter_New();
      break;
    case RP_MAX_SCORE_NORMALIZER:
      buffer = RPMaxScoreNormalizer_New(nullptr);
      break;
    case RP_PAGER_LIMITER:
      buffer = RPPager_New(25, 3);
      break;
    case RP_GROUP:
      buffer = Grouper_GetRP(Grouper_New(nullptr, nullptr, 0, GroupByLimits_Default(100)));
      break;
    default:
      FAIL() << "Unsupported test processor";
  }
  buffer->parent = &context;
  buffer->upstream = &work.rp;
  work.tail = RPProfile_New(buffer, &context);
  context.rootProc = &work.rp;
  context.endProc = work.tail;
  auto parked = work.parked.get_future();
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, ScopedCollectorWork::step, &work);
  });
  parked.wait();
  QueryRequestTimeout_MarkTimedOut(&timeout);
  unsigned drained = 0;
  rs_wall_clock_ns_t profileAfterReply = 0;
  auto drainer = std::async(std::launch::async, [&] {
    struct Recovery {
      ScopedCollectorWork *work;
      unsigned *drained;
      rs_wall_clock_ns_t *profile;
    } recovery{&work, &drained, &profileAfterReply};
    PipelineExecution_RunDrain(
        execution,
        [](PipelineAccess *access, void *data) {
          auto *recovery = static_cast<Recovery *>(data);
          SearchResult row = SearchResult_New();
          while (recovery->work->tail->Drain(recovery->work->tail, &row) == RP_DRAIN_OK) {
            ++*recovery->drained;
            SearchResult_Clear(&row);
          }
          SearchResult_Destroy(&row);
          PipelineAccess_Context(access)->resultLimit = 7;
          *recovery->profile = RPProfile_GetTime(recovery->work->tail);
        },
        &recovery);
  });
  const auto status = drainer.wait_for(1s);
  work.wake.set_value();
  drainer.get();
  EXPECT_FALSE(worker.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(GetParam() == RP_PAGER_LIMITER || GetParam() == RP_GROUP ? 0 : 20, drained);
  EXPECT_EQ(7, context.resultLimit);
  EXPECT_EQ(profileAfterReply, RPProfile_GetTime(work.tail));
  if (GetParam() == RP_DEPLETER) EXPECT_EQ(0, RPDepleter_GetDepletionTime(buffer));
  destroyResults(work.results);
  work.tail->Free(work.tail);
  buffer->Free(buffer);
}

INSTANTIATE_TEST_SUITE_P(ScopedWait, ScopedAccumulatorTest,
                         ::testing::Values(RP_SORTER, RP_DEPLETER, RP_MAX_SCORE_NORMALIZER,
                                           RP_PAGER_LIMITER, RP_GROUP));

class SpecOwnershipTest : public PipelineOwnershipTest {
 protected:
  IndexSpec spec = {};
  dictType dictionaryType = {};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(nullptr, &spec);
  ResultProcessor *source = nullptr;
  SearchResult row = SearchResult_New();
  unsigned revalidations = 0, reads = 0;
  ValidateStatus validation = VALIDATE_OK;
  std::promise<void> suspended;
  unsigned steps = 0;
  int result = RS_RESULT_ERROR;

  struct SourceIterator : QueryIterator {
    SpecOwnershipTest *test;

    explicit SourceIterator(SpecOwnershipTest *test) : QueryIterator{}, test(test) {
      Revalidate = [](QueryIterator *base, IndexSpec *) {
        auto *test = static_cast<SourceIterator *>(base)->test;
        ++test->revalidations;
        EXPECT_TRUE(IndexSpec_IsReadLocked(&test->spec));
        return test->validation;
      };
      Read = [](QueryIterator *base) {
        auto *test = static_cast<SourceIterator *>(base)->test;
        ++test->reads;
        base->atEOF = true;
        return ITERATOR_EOF;
      };
      Free = [](QueryIterator *base) { delete static_cast<SourceIterator *>(base); };
    }
  };

  void SetUp() override {
    PipelineOwnershipTest::SetUp();
    ASSERT_EQ(0, pthread_rwlock_init(&spec.rwlock, nullptr));
    spec.keysDict = dictCreate(&dictionaryType, nullptr);
    sctx.timeout = &timeout;
    source = RPQueryIterator_New(new SourceIterator(this), nullptr, 0, &sctx);
    source->parent = &context;
    context.endProc = context.rootProc = source;
  }

  void TearDown() override {
    source->Free(source);
    SearchResult_Destroy(&row);
    IndexSpec_AssertLockNotHeld();
    EXPECT_EQ(0, spec.keysDict->pauserehash);
    EXPECT_EQ(0, pthread_rwlock_destroy(&spec.rwlock));
    dictRelease(spec.keysDict);
    PipelineOwnershipTest::TearDown();
  }

  static void run(PipelineAccess *access, void *data) {
    auto *test = static_cast<SpecOwnershipTest *>(data);
    ++test->steps;
    PipelineAccess_Publish(access, &test->context);
    test->suspended.set_value();
    test->result = test->source->Next(test->source, &test->row);
  }
};

TEST_F(SpecOwnershipTest, TimeoutRecoveryDoesNotWaitForSpecWriter) {
  auto parked = suspended.get_future();
  IndexSpec_LockWrite(&spec);
  auto worker = std::async(std::launch::async,
                           [&] { return PipelineExecution_RunNext(execution, run, this); });
  parked.wait();
  QueryRequestTimeout_MarkTimedOut(&timeout);
  auto drainer = std::async(std::launch::async, [&] {
    PipelineExecution_RunDrain(
        execution,
        [](PipelineAccess *access, void *) {
          auto *root = PipelineAccess_Context(access)->rootProc;
          auto output = SearchResult_New();
          EXPECT_EQ(RP_DRAIN_EOF, root->Drain(root, &output));
          SearchResult_Destroy(&output);
        },
        nullptr);
  });
  const auto drained = drainer.wait_for(1s);
  IndexSpec_Unlock(&spec);
  drainer.get();
  EXPECT_FALSE(worker.get());
  EXPECT_EQ(std::future_status::ready, drained);
  EXPECT_EQ(1, steps);
  EXPECT_EQ(0, revalidations);
  EXPECT_EQ(0, reads);
}

TEST_F(SpecOwnershipTest, ResumedReaderRevalidatesBeforeReading) {
  auto parked = suspended.get_future();
  IndexSpec_LockWrite(&spec);
  auto worker = std::async(std::launch::async,
                           [&] { return PipelineExecution_RunNext(execution, run, this); });
  parked.wait();
  IndexSpec_Unlock(&spec);
  EXPECT_TRUE(worker.get());
  EXPECT_EQ(1, steps);
  EXPECT_EQ(1, revalidations);
  EXPECT_EQ(1, reads);
  EXPECT_EQ(RS_RESULT_EOF, result);
}

TEST_F(SpecOwnershipTest, UncontendedReaderPropagatesRevalidationTimeout) {
  validation = VALIDATE_TIMEOUT;
  EXPECT_TRUE(PipelineExecution_RunNext(execution, run, this));
  EXPECT_EQ(1, steps);
  EXPECT_EQ(1, revalidations);
  EXPECT_EQ(0, reads);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, result);
}
