/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "io_runtime_ctx.h"
#include "cluster.h"
#include "rmutil/alloc.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"
#include "info/global_stats.h"
#include "concurrent_ctx.h"
#include "common.h"
#include <unistd.h>
#include <atomic>

// Test callback for queue operations
static void testCallback(void *privdata) {
  int *counter = (int *)privdata;
  (*counter)++;
}

// Test callback for topology updates
static void testTopoCallback(void *privdata) {
  struct UpdateTopologyCtx *ctx = (struct UpdateTopologyCtx *)privdata;
  IORuntimeCtx *ioRuntime = ctx->ioRuntime;
  //Simulate what the TopologyValidationTimer should do
  ioRuntime->uv_runtime.loop_th_ready = true;
  MRClusterTopology *old_topo = ioRuntime->topo;
  MRClusterTopology *new_topo = ctx->new_topo;
  ioRuntime->topo = new_topo;
  rm_free(ctx);
  if (old_topo) {
    MRClusterTopology_Free(old_topo);
  }
}

class IORuntimeCtxCommonTest : public ::testing::Test {
protected:
  IORuntimeCtx *ctx;
  static MRClusterTopology *getDummyTopology(uint32_t identifier) {
    MRClusterTopology *topo = static_cast<MRClusterTopology*>(rm_malloc(sizeof(*topo)));
    topo->numShards = 0;
    topo->capShards = identifier; // Just to have a different value for the test
    topo->shards = nullptr;
    return topo;
  }

  void SetUp() override {
    struct MRClusterTopology *topo = getDummyTopology(4096);
    ctx = IORuntimeCtx_Create(2, topo, 1, true);
  }

  void TearDown() override {
    // Clear any pending topology before shutdown
    IORuntimeCtx_FireShutdown(ctx);
    IORuntimeCtx_Free(ctx);
  }
};

TEST_F(IORuntimeCtxCommonTest, InitialState) {
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->queue, nullptr);
  ASSERT_EQ(ctx->pendingTopo, nullptr);
  ASSERT_FALSE(ctx->uv_runtime.loop_th_ready);
  ASSERT_FALSE(ctx->uv_runtime.io_runtime_started_or_starting);
  ASSERT_FALSE(ctx->pendingItems);
  ASSERT_FALSE(ctx->uv_runtime.loop_th_created);
  ASSERT_FALSE(ctx->uv_runtime.loop_th_creation_failed);
}

TEST_F(IORuntimeCtxCommonTest, Schedule) {
  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  // Give some time for thread to start
  usleep(100);
  ASSERT_TRUE(ctx->uv_runtime.io_runtime_started_or_starting);
  ASSERT_TRUE(ctx->uv_runtime.loop_th_created);
  ASSERT_FALSE(ctx->uv_runtime.loop_th_creation_failed);
  // Verify the callback has not been called yet, thread not ready because no Topology is called
  ASSERT_EQ(counter, 0);
  struct MRClusterTopology *topo = getDummyTopology(4091);
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, topo, false);
  MRClusterTopology_Free(topo);


  for (int i = 0; i < 10; i++) {
    IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  }

  while (counter < 11) {
    usleep(1); // 1us delay
  }
  // Now the Runtime processed the topology and the pending queue
  ASSERT_EQ(counter, 11);
}

TEST_F(IORuntimeCtxCommonTest, ScheduleTopology) {
  // Create a new topology
  MRClusterTopology *newTopo = getDummyTopology(4097);

  // Schedule the topology update
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, true);

  // Verify the topology was not yet updated (will be updated once a request is scheduled)
  ASSERT_EQ(ctx->topo->capShards, 4096);

  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);

  while (counter < 1) {
    usleep(1); // 1us delay
  }
  ASSERT_EQ(ctx->topo->capShards, 4097);

  // We don't need to free newTopo here as it's handled by testTopoCallback
}

TEST_F(IORuntimeCtxCommonTest, MultipleTopologyUpdates) {
  // Schedule one dummy request to start the thread and still have the flag io_runtime_started_or_starting set to true
  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  // Schedule multiple topology updates in quick succession
  for (int i = 3; i <= 5; i++) {
    MRClusterTopology *newTopo = getDummyTopology(4096 + i);
    IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, true);
  }

  // Give some time for the last topology to be applied
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  while (counter < 2) {
    usleep(1); // 1us delay
  }

  // Only the last topology should be applied
  ASSERT_EQ(ctx->topo->capShards, 4101);
}

TEST_F(IORuntimeCtxCommonTest, ClearPendingTopo) {
  // Create a new topology but don't start the runtime
  const char *new_hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409", "localhost:6419"};
  MRClusterTopology *newTopo = getDummyTopology(2048);

  // Schedule the topology update
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, false);

  MRClusterTopology_Free(newTopo);
  // Verify we have a pending topology
  ASSERT_NE(ctx->pendingTopo, nullptr);

  // Clear the pending topology
  IORuntimeCtx_Debug_ClearPendingTopo(ctx);
}

TEST_F(IORuntimeCtxCommonTest, ShutdownWithPendingRequests) {
  IORuntimeCtx *io_runtime_ctx = IORuntimeCtx_Create(2, NULL, 1, false);
  int counter = 0;

  MRClusterTopology *newTopo = getDummyTopology(4097);
  IORuntimeCtx_Schedule_Topology(io_runtime_ctx, testTopoCallback, newTopo, false);
  MRClusterTopology_Free(newTopo);

  // Create a delayed callback that takes 100ms to complete
  auto delayedCallback = [](void *privdata) {
    int *counter = (int *)privdata;
    usleep(1000); // 1ms delay
    (*counter)++;
  };

  IORuntimeCtx_Schedule(io_runtime_ctx, testCallback, &counter);
  // Send one request and make sure it runs to make the test better. Otherwise the async callback does not see the topology applied
  // and delays the callback call (and shutdown call may be called before all the callbacks are called)
  while (counter < 1) {
    usleep(1); // 1us delay
  }

  // Schedule 10 delayed requests
  for (int i = 0; i < 10; i++) {
    IORuntimeCtx_Schedule(io_runtime_ctx, delayedCallback, &counter);
  }

  // Fire shutdown and wait for completion, the shutdown is scheduled to run at the end of the event loop (is just another event)
  IORuntimeCtx_FireShutdown(io_runtime_ctx);
  IORuntimeCtx_Free(io_runtime_ctx);

  // Verify all requests were processed despite shutdown
  ASSERT_EQ(counter, 11);
}

TEST_F(IORuntimeCtxCommonTest, ActiveIoThreadsMetric) {
  // Test that the uv_threads_running_queries metric is tracked correctly

  // Create ConcurrentSearch required to call GlobalStats_GetMultiThreadingStats
  ConcurrentSearch_CreatePool(1);

  // Phase 1: Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_queries, 0) << "uv_threads_running_queries should start at 0";

  // Phase 2: Schedule a callback that sleeps, and verify metric increases
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

  // Mark the IO runtime as ready to process callbacks
  ctx->uv_runtime.loop_th_ready = true;

  // Schedule the slow callback - this will start the IO runtime automatically
  IORuntimeCtx_Schedule(ctx, slowCallback, &flags);

  // Wait for callback to start
  while (!flags.started.load()) {
    usleep(100); // 100us
  }

  // Now the callback is executing - check that uv_threads_running_queries > 0
  stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_queries, 1) << "uv_threads_running_queries should be > 0 while callback is executing";

  // Tell callback to finish
  flags.should_finish.store(true);

  // Phase 3: Wait for metric to return to 0 with timeout
  bool success = RS::WaitForCondition([&]() {
    stats = GlobalStats_GetMultiThreadingStats();
    return stats.uv_threads_running_queries == 0;
  });

  ASSERT_TRUE(success) << "Timeout waiting for uv_threads_running_queries to return to 0, current value: " << stats.uv_threads_running_queries;

  // Free ConcurrentSearch
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
