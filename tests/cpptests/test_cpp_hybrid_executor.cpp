/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "hybrid/hybrid_executor.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <future>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

struct Execution {
  static constexpr size_t count = 8;
  std::array<std::atomic<int>, count> calls{};
  int finishes = 0;

  static void run(void *ctx, size_t ordinal) {
    auto &state = *static_cast<Execution *>(ctx);
    ASSERT_LT(ordinal, state.calls.size());
    ++state.calls[ordinal];
  }

  static void finish(void *ctx) {
    auto &state = *static_cast<Execution *>(ctx);
    for (const auto &calls : state.calls) EXPECT_EQ(calls.load(), 1);
    ++state.finishes;
  }
};

struct Queued {
  HybridTicket run;
  void *ticket;
};

int collect(void *pool, HybridTicket run, void *ticket) {
  static_cast<std::vector<Queued> *>(pool)->push_back({run, ticket});
  return 0;
}

TEST(HybridExecutor, CallerCanFinishBeforeAnyHelperStarts) {
  for (const size_t runners : {0u, 1u, 2u, 4u, 16u}) {
    std::vector<Queued> queued;
    {
      Execution execution;
      HybridExecutor_Run(Execution::count, runners, collect, &queued, Execution::run,
                         Execution::finish, &execution);
      EXPECT_EQ(execution.finishes, 1);
    }
    EXPECT_EQ(queued.size(), runners ? std::min(runners, Execution::count) - 1 : 0u);
    for (auto job : queued) job.run(job.ticket);
  }
}

TEST(HybridExecutor, RequiresAtLeastOneChild) {
  EXPECT_THROW(HybridExecutor_Run(0, 1, nullptr, nullptr, nullptr, nullptr, nullptr),
               std::runtime_error);
}

TEST(HybridExecutor, CallerKeepsRequestAliveUntilSubmissionEnds) {
  struct InlinePool : Execution {
    size_t submissions = 0;
  } execution;
  auto submit = [](void *ctx, HybridTicket run, void *ticket) {
    auto &pool = *static_cast<InlinePool *>(ctx);
    ++pool.submissions;
    EXPECT_EQ(0, pool.calls[0]);
    EXPECT_EQ(0, pool.finishes);
    if (pool.submissions != 1) return -1;
    run(ticket);
    return 0;
  };
  HybridExecutor_Run(Execution::count, 3, submit, &execution, Execution::run,
                     Execution::finish, &execution);
  EXPECT_EQ(2u, execution.submissions);
  EXPECT_EQ(1, execution.finishes);
}

TEST(HybridExecutor, RejectedHelpersLeaveChildrenAvailable) {
  size_t submissions = 0;
  auto reject = [](void *ctx, HybridTicket, void *) {
    ++*static_cast<size_t *>(ctx);
    return -1;
  };
  Execution execution;
  HybridExecutor_Run(Execution::count, 3, reject, &submissions, Execution::run, Execution::finish,
                     &execution);
  EXPECT_EQ(submissions, 2u);
  EXPECT_EQ(execution.finishes, 1);
}

TEST(HybridExecutor, LastHelperRunsContinuationAfterCallerReturns) {
  struct Parallel : Execution {
    std::promise<void> helperStarted;
    std::promise<void> releaseHelper;
    std::thread helper;
    std::thread::id finisher;
  } execution;
  auto submit = [](void *ctx, HybridTicket run, void *ticket) {
    auto *state = static_cast<Parallel *>(ctx);
    state->helper = std::thread(run, ticket);
    state->helperStarted.get_future().wait();
    return 0;
  };
  auto child = [](void *ctx, size_t ordinal) {
    auto *state = static_cast<Parallel *>(ctx);
    if (ordinal == 1) {
      state->helperStarted.set_value();
      state->releaseHelper.get_future().wait();
    }
    Execution::run(state, ordinal);
  };
  auto finish = [](void *ctx) {
    auto *state = static_cast<Parallel *>(ctx);
    state->finisher = std::this_thread::get_id();
    Execution::finish(state);
  };
  HybridExecutor_Run(Execution::count, 2, submit, &execution, child, finish, &execution);
  EXPECT_EQ(execution.finishes, 0);
  const auto helperId = execution.helper.get_id();
  execution.releaseHelper.set_value();
  execution.helper.join();
  EXPECT_EQ(execution.finishes, 1);
  EXPECT_EQ(execution.finisher, helperId);
}

}  // namespace
