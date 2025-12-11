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
#include "coord/rmr/cluster.h"
#include "coord/rmr/rmr.h"
#include "concurrent_ctx.h"
#include "info/global_stats.h"

class ActiveIoThreadsTest : public ::testing::Test {
protected:
  MRWorkQueue *queue;
  MRCluster *cluster;

  void SetUp() override {
    // Create empty topology for cluster - prevents crash in topology validation timer
    // (MRCluster_CheckConnections iterates over topo->numShards, so empty topo is safe)
    MRClusterTopology *topo = (MRClusterTopology *)rm_malloc(sizeof(MRClusterTopology));
    topo->numShards = 0;
    topo->capShards = 0;
    topo->shards = nullptr;
    cluster = MR_NewCluster(topo, 2);
    MR_Init(cluster, 5000);

    queue = RQ_New(10);
  }

  void TearDown() override {
    MRClust_Free(cluster);
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

TEST_F(ActiveIoThreadsTest, ActiveTopologyUpdateThreadsMetric) {
  // Test that uv_threads_running_topology_update metric is tracked correctly

  ConcurrentSearch_CreatePool(1);

  // Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_topology_update, 0);

  // Start UV thread by scheduling a dummy callback (required for uv loop to process async events)
  // This calls verify_uv_thread() which initializes timers, async handles, and starts the thread
  static std::atomic<bool> dummy_done{false};
  dummy_done = false;
  auto dummyCallback = [](void *privdata) {
    auto *flag = (std::atomic<bool> *)privdata;
    flag->store(true);
  };
  RQ_Debug_SetLoopReady();  // Sets loop_th_ready = true so callback executes
  RQ_Push(queue, dummyCallback, &dummy_done);
  bool success = RS::WaitForCondition([&]() { return dummy_done.load(); });
  ASSERT_TRUE(success) << "Timeout waiting for UV thread to start";

  // Static flags for C callback (lambdas used as C function pointers can't capture)
  static std::atomic<bool> topo_started{false};
  static std::atomic<bool> topo_should_finish{false};
  topo_started = false;
  topo_should_finish = false;

  auto slowTopoCallback = [](void *privdata) {
    topo_started.store(true);
    while (!topo_should_finish.load()) {
      usleep(100);
    }
  };

  // Create minimal dummy topology
  MRClusterTopology dummyTopo = {0};

  // Push topology update - triggers topologyAsyncCB which wraps the callback with metric updates
  RQ_Push_Topology(slowTopoCallback, &dummyTopo);

  // Wait for callback to start
  success = RS::WaitForCondition([&]() { return topo_started.load(); });
  ASSERT_TRUE(success) << "Timeout waiting for topo callback to start";

  // Verify metric is 1 while callback is running
  stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_topology_update, 1);

  // Signal callback to finish
  topo_should_finish.store(true);

  // Wait for metric to return to 0
  success = RS::WaitForCondition([&]() {
    stats = GlobalStats_GetMultiThreadingStats();
    return stats.uv_threads_running_topology_update == 0;
  });
  ASSERT_TRUE(success) << "Timeout waiting for metric to return to 0";

  // Stop topology timers to prevent interference with other tests
  RQ_Debug_StopTopologyTimers();

  ConcurrentSearch_ThreadPoolDestroy();
}
