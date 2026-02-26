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
#include <array>
#include <initializer_list>
#include <span>

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

static RedisModuleSlotRangeArray *createEmptySlotRangeArray() {
  size_t total_size = sizeof(RedisModuleSlotRangeArray);
  auto array = (RedisModuleSlotRangeArray *)rm_malloc(total_size);
  array->num_ranges = 0;
  return array;
}

static MRClusterTopology *getTopology(std::span<const char *const> hosts) {
  size_t numNodes = hosts.size();
  auto topo = (MRClusterTopology *)rm_new(MRClusterTopology);
  topo->numShards = numNodes;
  topo->capShards = numNodes;
  topo->shards = (MRClusterShard *)rm_calloc(numNodes, sizeof(MRClusterShard));

  for (size_t i = 0; i < numNodes; i++) {
    MRClusterNode *node = &topo->shards[i].node;
    int rc = MREndpoint_Parse(hosts[i], &node->endpoint);
    assert(rc == REDIS_OK);
    node->id = rm_strdup(hosts[i]);
    topo->shards[i].slotRanges = createEmptySlotRangeArray();
  }

  return topo;
}

static void startAndShutdownRuntime(IORuntimeCtx *io) {
  int counter = 0;
  // Start runtime through schedule path so io_runtime_started_or_starting is set.
  IORuntimeCtx_Schedule(io, testCallback, &counter);
  bool started = RS::WaitForCondition([&]() {
    return io->uv_runtime.loop_th_created;
  });
  ASSERT_TRUE(started) << "Timeout waiting for IO runtime thread creation";
  IORuntimeCtx_FireShutdown(io);
}

static void replaceTopologyAndUpdateNodes(IORuntimeCtx *io, MRClusterTopology *new_topo) {
  MRClusterTopology *old_topo = io->topo;
  io->topo = new_topo;
  IORuntimeCtx_UpdateNodes(io);
  MRClusterTopology_Free(old_topo);
}

static void assertConnMapContains(IORuntimeCtx *io, std::initializer_list<const char *> present,
                                  std::initializer_list<const char *> absent = {}) {
  ASSERT_NE(io, nullptr);
  ASSERT_NE(io->conn_mgr.map, nullptr);

  for (const char *node_id : present) {
    ASSERT_NE(dictFind(io->conn_mgr.map, node_id), nullptr) << "Expected node in conn map: " << node_id;
  }
  for (const char *node_id : absent) {
    ASSERT_EQ(dictFind(io->conn_mgr.map, node_id), nullptr) << "Unexpected node in conn map: " << node_id;
  }
}

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
  bool success = RS::WaitForCondition([&]() {
    return flags.started.load();
  });
  ASSERT_TRUE(success) << "Timeout waiting for callback to start";

  // Now the callback is executing - check that uv_threads_running_queries > 0
  stats = GlobalStats_GetMultiThreadingStats();
  ASSERT_EQ(stats.uv_threads_running_queries, 1) << "uv_threads_running_queries should be > 0 while callback is executing";

  // Tell callback to finish
  flags.should_finish.store(true);

  // Phase 3: Wait for metric to return to 0 with timeout
  success = RS::WaitForCondition([&]() {
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

TEST_F(IORuntimeCtxCommonTest, UpdateNodesAddRemove) {
  std::array<const char *, 3> hosts_v1 = {"localhost:6379", "localhost:6389", "localhost:6399"};
  MRClusterTopology *topo_v1 = getTopology(hosts_v1);
  ASSERT_NE(topo_v1, nullptr);

  IORuntimeCtx *io = IORuntimeCtx_Create(2, topo_v1, 11, true);
  IORuntimeCtx_UpdateNodes(io);

  ASSERT_EQ(dictSize(io->conn_mgr.map), 3);
  assertConnMapContains(io, {"localhost:6379", "localhost:6389", "localhost:6399"});

  std::array<const char *, 3> hosts_v2 = {"localhost:6379", "localhost:6399", "localhost:6409"};
  MRClusterTopology *topo_v2 = getTopology(hosts_v2);
  ASSERT_NE(topo_v2, nullptr);
  replaceTopologyAndUpdateNodes(io, topo_v2);

  ASSERT_EQ(dictSize(io->conn_mgr.map), 3);
  assertConnMapContains(io, {"localhost:6379", "localhost:6399", "localhost:6409"},
                         {"localhost:6389"});

  startAndShutdownRuntime(io);
  IORuntimeCtx_Free(io);
}

TEST_F(IORuntimeCtxCommonTest, UpdateNodesResizesConnectionMap) {
  std::array<const char *, 3> hosts_v1 = {"localhost:6379", "localhost:6389", "localhost:6399"};
  MRClusterTopology *topo_v1 = getTopology(hosts_v1);
  ASSERT_NE(topo_v1, nullptr);

  IORuntimeCtx *io = IORuntimeCtx_Create(1, topo_v1, 12, true);
  IORuntimeCtx_UpdateNodes(io);
  ASSERT_EQ(dictSize(io->conn_mgr.map), 3);
  assertConnMapContains(io, {"localhost:6379", "localhost:6389", "localhost:6399"});

  std::array<const char *, 2> hosts_v2 = {"localhost:6379", "localhost:6399"};
  MRClusterTopology *topo_v2 = getTopology(hosts_v2);
  ASSERT_NE(topo_v2, nullptr);
  replaceTopologyAndUpdateNodes(io, topo_v2);
  ASSERT_EQ(dictSize(io->conn_mgr.map), 2);
  assertConnMapContains(io, {"localhost:6379", "localhost:6399"},
                         {"localhost:6389"});

  std::array<const char *, 4> hosts_v3 = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  MRClusterTopology *topo_v3 = getTopology(hosts_v3);
  ASSERT_NE(topo_v3, nullptr);
  replaceTopologyAndUpdateNodes(io, topo_v3);
  ASSERT_EQ(dictSize(io->conn_mgr.map), 4);
  assertConnMapContains(io, {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"});

  startAndShutdownRuntime(io);
  IORuntimeCtx_Free(io);
}
