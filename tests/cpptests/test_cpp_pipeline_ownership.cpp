/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "pipeline_execution.h"
#include "query_request.h"

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
