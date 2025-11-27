/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "common.h"
#include "redismock/redismock.h"
#include <chrono>
#include <thread>
#include <atomic>

extern "C" {
#include "coord/rmr/rq.h"
#include "info/global_stats.h"
}

class ActiveIoThreadsTest : public ::testing::Test {
protected:
  MRWorkQueue *queue;

  void SetUp() override {
    // Create a work queue for testing
    queue = RQ_New(10); // maxPending = 10

  }

  void TearDown() override {
    // Cleanup is handled by the module
  }
};

TEST_F(ActiveIoThreadsTest, TestMetricUpdateDuringCallback) {
  struct CallbackFlags {
    std::atomic<bool> started{false};
    std::atomic<bool> should_finish{false};
  };

  CallbackFlags flags;

  auto slowCallback = [](void *privdata) {
    auto *flags = (CallbackFlags *)privdata;
    flags->started.store(true);

    // Wait until test tells us to finish
    while (!flags->should_finish.load()) {
      usleep(100); // 100us
    }
  };

  // Phase 1: Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(0, stats.active_io_threads)
    << "active_io_threads should start at 0";

  // Phase 2: Schedule callback and verify metric increases
  RQ_Push(queue, slowCallback, &flags);

  // Mark the IO runtime as ready to process callbacks (bypass topology validation timeout)
  RQ_Debug_SetLoopReady();

  // Wait for callback to start
  bool started = RS::WaitForCondition([&]() { return flags.started.load(); });
  ASSERT_TRUE(started) << "Timeout waiting for callback to start";
  // Verify metric increased
  stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(1, stats.active_io_threads)
    << "active_io_threads should be 1 while callback is executing";

  // Phase 3: Signal callback to finish and wait for metric to return to 0
  flags.should_finish.store(true);

  // Wait for metric to return to 0
  bool returned_to_zero = RS::WaitForCondition(
    [&]() { return GlobalStats_GetMultiThreadingStats().active_io_threads == 0; });
  ASSERT_TRUE(returned_to_zero) << "Timeout waiting for metric to return to 0";
}
