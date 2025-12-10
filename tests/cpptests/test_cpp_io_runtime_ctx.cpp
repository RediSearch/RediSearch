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
#include "coord/rmr/rq.h"
#include "concurrent_ctx.h"
#include "info/global_stats.h"

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

  // Create ConcurrentSearch required to call GlobalStats_GetMultiThreadingStats
  ConcurrentSearch_CreatePool(1);

  // Phase 1: Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(0, stats.uv_threads_running_queries)
    << "uv_threads_running_queries should start at 0";

  // Mark the IO runtime as ready to process callbacks (bypass topology validation timeout)
  RQ_Debug_SetLoopReady();

  // Phase 2: Schedule callback and verify metric increases
  RQ_Push(queue, slowCallback, &flags);

  // Wait for callback to start
  bool started = RS::WaitForCondition([&]() { return flags.started.load(); });
  ASSERT_TRUE(started) << "Timeout waiting for callback to start";
  // Verify metric increased
  stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(1, stats.uv_threads_running_queries)
    << "uv_threads_running_queries should be 1 while callback is executing";

  // Phase 3: Signal callback to finish and wait for metric to return to 0
  flags.should_finish.store(true);

  // Wait for metric to return to 0
  bool returned_to_zero = RS::WaitForCondition(
    [&]() { return GlobalStats_GetMultiThreadingStats().uv_threads_running_queries == 0; });
  ASSERT_TRUE(returned_to_zero) << "Timeout waiting for metric to return to 0";

  // Free ConcurrentSearch resources
  ConcurrentSearch_ThreadPoolDestroy();
}

TEST_F(IORuntimeCtxCommonTest, ActiveTopologyUpdateThreadsMetric) {
  // Test that uv_threads_running_topology_update metric is tracked correctly

  // Setup
  ConcurrentSearch_CreatePool(1);

  // Phase 1: Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_topology_update, 0);

  // Phase 2: Use static flags for communication with the topo callback
  static std::atomic<bool> topo_started{false};
  static std::atomic<bool> topo_should_finish{false};
  topo_started = false;
  topo_should_finish = false;

  // Slow topo callback - signals start, waits for finish signal
  auto slowTopoCallback = [](void *privdata) {
    auto *ctx = (struct UpdateTopologyCtx *)privdata;

    topo_started.store(true);

    // Wait until test tells us to finish
    while (!topo_should_finish.load()) {
      usleep(100);
    }

    // Must free ctx and its topology (callback owns privdata)
    if (ctx->new_topo) {
      MRClusterTopology_Free(ctx->new_topo);
    }
    rm_free(ctx);
  };

  // Start the IO runtime thread (required for uv loop to process async events)
  int dummy = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &dummy);

  // Schedule topology update - this calls uv_async_send which triggers topologyAsyncCB
  MRClusterTopology *newTopo = getDummyTopology(9999);
  IORuntimeCtx_Schedule_Topology(ctx, slowTopoCallback, newTopo, true);

  // Wait for topo callback to start
  bool success = RS::WaitForCondition([&]() { return topo_started.load(); });
  ASSERT_TRUE(success) << "Timeout waiting for topo callback to start";

  // Phase 3: Verify metric is 1 while callback is running
  stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_topology_update, 1);

  // Signal callback to finish
  topo_should_finish.store(true);

  // Phase 4: Wait for metric to return to 0
  success = RS::WaitForCondition([&]() {
    stats = GlobalStats_GetMultiThreadingStats();
    return stats.uv_threads_running_topology_update == 0;
  });
  ASSERT_TRUE(success) << "Timeout waiting for metric to return to 0";

  // Cleanup
  ConcurrentSearch_ThreadPoolDestroy();
}
