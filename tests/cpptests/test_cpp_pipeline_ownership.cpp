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

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

using namespace std::chrono_literals;

class PipelineOwnershipTest : public ::testing::Test {
 protected:
  QueryRequestTimeout timeout = {};
  PipelineExecution *execution = nullptr;
  QueryProcessingCtx context = {};

  void SetUp() override {
    QueryRequestTimeout_Init(&timeout, TimeoutPolicy_ReturnStrict, 1000);
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

struct PendingOwnershipWork {
  QueryProcessingCtx *context;
  QueryRequestTimeout *timeout;
  std::promise<void> parked;
  std::mutex mutex;
  std::condition_variable condition;
  bool released = false;
  bool cancelBeforeWait = false;
  unsigned steps = 0, resumes = 0, destroys = 0;

  void release() {
    std::lock_guard lock(mutex);
    released = true;
    condition.notify_one();
  }

  static void step(PipelineAccess *access, void *data) {
    auto *work = static_cast<PendingOwnershipWork *>(data);
    ++work->steps;
    if (work->steps == 1) {
      PipelineAccess_Publish(access, work->context);
      EXPECT_EQ(access, work->context->executionAccess);
      work->context->totalResults = 9;
      if (work->cancelBeforeWait) QueryRequestTimeout_MarkTimedOut(work->timeout);
      PipelineAccess_Suspend(
          access,
          PipelinePending{
              .data = data,
              .wait =
                  [](void *data) {
                    auto *work = static_cast<PendingOwnershipWork *>(data);
                    work->parked.set_value();
                    std::unique_lock lock(work->mutex);
                    work->condition.wait(lock, [&] { return work->released; });
                  },
              .resume =
                  [](PipelineAccess *access, void *data) {
                    auto *work = static_cast<PendingOwnershipWork *>(data);
                    ++work->resumes;
                    EXPECT_EQ(access, PipelineAccess_Context(access)->executionAccess);
                    PipelineAccess_Context(access)->totalResults += 4;
                  },
              .destroy =
                  [](void *data) { ++static_cast<PendingOwnershipWork *>(data)->destroys; }});
    } else {
      EXPECT_EQ(access, PipelineAccess_Context(access)->executionAccess);
      ++PipelineAccess_Context(access)->totalResults;
    }
  }
};

TEST_F(PipelineOwnershipTest, DrainFinishesBeforeParkedWorkerIsReleased) {
  PendingOwnershipWork work{.context = &context, .timeout = &timeout};
  auto parked = work.parked.get_future();
  auto worker = std::async(std::launch::async, [&] {
    return PipelineExecution_RunNext(execution, PendingOwnershipWork::step, &work);
  });
  parked.wait();
  QueryRequestTimeout_MarkTimedOut(&timeout);
  auto drainer = std::async(std::launch::async, [&] {
    PipelineExecution_RunDrain(
        execution,
        [](PipelineAccess *access, void *data) {
          auto *ctx = PipelineAccess_Context(access);
          EXPECT_EQ(data, ctx);
          EXPECT_EQ(access, ctx->executionAccess);
          EXPECT_EQ(9, ctx->totalResults);
          ctx->totalResults = 100;
        },
        &context);
  });
  const auto status = drainer.wait_for(1s);
  // Always release the worker, including on assertion failure, to avoid a hung test.
  work.release();
  drainer.get();
  EXPECT_FALSE(worker.get());
  EXPECT_EQ(std::future_status::ready, status);
  EXPECT_EQ(100, context.totalResults);
  EXPECT_EQ(nullptr, context.executionAccess);
  EXPECT_EQ(1, work.steps);
  EXPECT_EQ(0, work.resumes);
  EXPECT_EQ(1, work.destroys);
}

TEST_F(PipelineOwnershipTest, NaturalWakeReacquiresBeforeCommittingAndRestartsSegment) {
  PendingOwnershipWork work{.context = &context, .timeout = &timeout};
  work.released = true;
  EXPECT_TRUE(PipelineExecution_RunNext(execution, PendingOwnershipWork::step, &work));
  EXPECT_EQ(2, work.steps);
  EXPECT_EQ(1, work.resumes);
  EXPECT_EQ(1, work.destroys);
  EXPECT_EQ(14, context.totalResults);
  EXPECT_EQ(nullptr, context.executionAccess);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  PipelineExecution_RunDrain(
      execution,
      [](PipelineAccess *access, void *) {
        EXPECT_EQ(14, PipelineAccess_Context(access)->totalResults);
      },
      nullptr);
}

TEST_F(PipelineOwnershipTest, CancellationBeforeWaitDisposesPrivateOperationWithoutStartingIt) {
  PendingOwnershipWork work{.context = &context, .timeout = &timeout};
  work.cancelBeforeWait = true;
  auto parked = work.parked.get_future();
  EXPECT_FALSE(PipelineExecution_RunNext(execution, PendingOwnershipWork::step, &work));
  EXPECT_EQ(std::future_status::timeout, parked.wait_for(0s));
  EXPECT_EQ(1, work.steps);
  EXPECT_EQ(0, work.resumes);
  EXPECT_EQ(1, work.destroys);
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
    test->result = test->source->Next(test->source, &test->row);
    if (test->result == RS_RESULT_SUSPENDED) test->suspended.set_value();
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
  EXPECT_EQ(2, steps);
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
