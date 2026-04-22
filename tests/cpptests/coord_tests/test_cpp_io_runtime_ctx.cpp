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

extern "C" {
  // Test callback for queue operations
  static void testCallback(void *privdata) {
    int *counter = (int *)privdata;
    (*counter)++;
  }
} // extern "C"

// Test callback for topology updates - signals completion to test thread
// by storing the capShards value in an atomic, avoiding race conditions
// where the test thread might read a freed topology pointer.
static std::atomic<uint32_t> lastAppliedCapShards{0};

extern "C" {
  // Returns `false` so `topologyAsyncCB` does not clear `loop_th_ready` or
  // arm the validation timers: these tests have no real server to handshake
  // with, so we simulate a successful handshake inline by keeping the loop
  // ready.
  static bool testTopoCallback(struct UpdateTopologyCtx *updateCtx) {
    IORuntimeCtx *ioRuntime = updateCtx->ioRuntime;
    //Simulate what the TopologyValidationTimer should do
    ioRuntime->uv_runtime.loop_th_ready = true;
    MRClusterTopology *old_topo = ioRuntime->topo;
    MRClusterTopology *new_topo = updateCtx->new_topo;
    // Store the capShards value BEFORE updating the pointer, so test can safely check it
    uint32_t newCapShards = new_topo->capShards;
    ioRuntime->topo = new_topo;
    // Signal to the test thread that this topology was applied
    lastAppliedCapShards.store(newCapShards, std::memory_order_release);
    rm_free(updateCtx);
    if (old_topo) {
      MRClusterTopology_Free(old_topo);
    }
    return false;
  }
} // extern "C"

// Counts how many topology updates actually change connectivity. Used to
// verify that connectivity-equal topology updates are short-circuited (the
// callback returns `false` and `topologyAsyncCB` skips the handshake work).
static std::atomic<int> topoCallbackInvocations{0};

extern "C" {
  // Always returns `false` so `topologyAsyncCB` does not clear
  // `loop_th_ready` or arm the validation timers: without a real server the
  // timer loop would never observe connected nodes and would wedge the test
  // until the failsafe fires. The counter is only bumped on actual
  // connectivity changes, which is what the short-circuit test asserts.
  static bool countingTopoCallback(struct UpdateTopologyCtx *updateCtx) {
    IORuntimeCtx *ioRuntime = updateCtx->ioRuntime;
    MRClusterTopology *old_topo = ioRuntime->topo;
    MRClusterTopology *new_topo = updateCtx->new_topo;
    bool connectivityChanged = !MRClusterTopology_ConnectivityEqual(old_topo, new_topo);
    ioRuntime->uv_runtime.loop_th_ready = true;
    ioRuntime->topo = new_topo;
    if (connectivityChanged) {
      topoCallbackInvocations.fetch_add(1, std::memory_order_release);
    }
    rm_free(updateCtx);
    if (old_topo) {
      MRClusterTopology_Free(old_topo);
    }
    return false;
  }
} // extern "C"

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
  // Reset the signal before starting
  lastAppliedCapShards.store(0, std::memory_order_relaxed);

  // Create a new topology
  MRClusterTopology *newTopo = getDummyTopology(4097);

  // Schedule the topology update
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, true);

  // Verify the topology was not yet updated (will be updated once a request is scheduled)
  ASSERT_EQ(ctx->topo->capShards, 4096);

  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);

  // Wait for topology to be applied by checking the atomic signal set by the callback.
  // This avoids the race condition of reading a potentially-freed topology pointer.
  bool success = RS::WaitForCondition([&]() {
    return lastAppliedCapShards.load(std::memory_order_acquire) == 4097;
  });
  ASSERT_TRUE(success) << "Timeout waiting for topology to be applied, lastAppliedCapShards=" << lastAppliedCapShards.load();

  // Wait for the testCallback to complete before `counter` goes out of scope.
  // Otherwise the event loop thread may write to a dangling stack address,
  // corrupting the stack canary and triggering "stack smashing detected".
  success = RS::WaitForCondition([&]() { return counter >= 1; });
  ASSERT_TRUE(success) << "Timeout waiting for scheduled callback to complete";
}

TEST_F(IORuntimeCtxCommonTest, MultipleTopologyUpdates) {
  // Reset the signal before starting
  lastAppliedCapShards.store(0, std::memory_order_relaxed);

  // Schedule one dummy request to start the thread and still have the flag io_runtime_started_or_starting set to true
  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  // Schedule multiple topology updates in quick succession
  for (int i = 3; i <= 5; i++) {
    MRClusterTopology *newTopo = getDummyTopology(4096 + i);
    IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, true);
  }
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  // Wait for the last topology (4101) to be applied by checking the atomic signal.
  // This avoids the race condition of reading a potentially-freed topology pointer.
  bool success = RS::WaitForCondition([&]() {
    return lastAppliedCapShards.load(std::memory_order_acquire) == 4101;
  });
  ASSERT_TRUE(success) << "Timeout waiting for topology to be applied, lastAppliedCapShards=" << lastAppliedCapShards.load();

  // Wait for the testCallbacks to complete before `counter` goes out of scope.
  // Otherwise the event loop thread may write to a dangling stack address,
  // corrupting the stack canary and triggering "stack smashing detected".
  success = RS::WaitForCondition([&]() { return counter >= 2; });
  ASSERT_TRUE(success) << "Timeout waiting for scheduled callbacks to complete";
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
  int dummy_counter = 0;
  topo_started = false;
  topo_should_finish = false;

  // Slow topo callback - signals start, waits for finish signal. Returns
  // `true` so `topologyAsyncCB` arms the validation timer, which (with an
  // empty shard list) immediately marks `loop_th_ready` and drains any
  // pending requests scheduled before the topology update.
  auto slowTopoCallback = [](struct UpdateTopologyCtx *ctx) -> bool {
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
    return true;
  };

  // Start the IO runtime thread (required for uv loop to process async events)
  IORuntimeCtx_Schedule(ctx, testCallback, &dummy_counter);

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

  // Phase 5: Wait for testCallback to complete before returning
  // (it runs asynchronously after topology validation timer fires)
  success = RS::WaitForCondition([&]() { return dummy_counter >= 1; });
  ASSERT_TRUE(success) << "Timeout waiting for testCallback to complete";

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

// Regression test for issue #9225: when a topology update does not change
// cluster connectivity (node ids, hosts, ports), the topology callback must
// signal that by returning `false`, so topologyAsyncCB skips the validation
// handshake and does not rearm the validation/failure timers. The topology
// pointer is still refreshed inline so the fanout path sees current slot
// ranges, but the handshake work is bypassed. The counter below is bumped
// only on actual connectivity changes, mirroring what the production
// uvUpdateTopologyRequest signals to topologyAsyncCB.
TEST_F(IORuntimeCtxCommonTest, IdenticalTopologyUpdateIsSkipped) {
  topoCallbackInvocations.store(0, std::memory_order_relaxed);

  // Step 1: apply an initial, non-empty topology so that a subsequent
  // connectivity-equal topology can be compared against it.
  std::array<const char *, 2> hosts = {"localhost:6379", "localhost:6389"};
  MRClusterTopology *topo1 = getTopology(hosts);
  IORuntimeCtx_Schedule_Topology(ctx, countingTopoCallback, topo1, true);

  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);

  bool ok = RS::WaitForCondition([&]() {
    return topoCallbackInvocations.load(std::memory_order_acquire) >= 1;
  });
  ASSERT_TRUE(ok) << "Initial topology was not applied";
  ASSERT_EQ(topoCallbackInvocations.load(), 1);

  // Step 2: schedule a connectivity-equal topology. The callback must
  // still swap ctx->topo inline (so slot ranges stay fresh) but must
  // return `false` so topologyAsyncCB skips the handshake work. We assert
  // this indirectly: the counter only increments on real connectivity
  // changes, and a subsequent regular callback drains (meaning
  // `loop_th_ready` was not cleared by the short-circuit path).
  MRClusterTopology *topo1_dup = getTopology(hosts);
  IORuntimeCtx_Schedule_Topology(ctx, countingTopoCallback, topo1_dup, true);

  // Drain the event loop: a regular callback is only processed after
  // loop_th_ready is true. Since the short-circuit path does not clear
  // loop_th_ready, this callback firing proves the runtime has iterated past
  // the topology async.
  int counter2 = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter2);
  ok = RS::WaitForCondition([&]() { return counter2 >= 1; });
  ASSERT_TRUE(ok) << "Runtime did not drain regular callback after identical topology update";
  // Additional quiesce window to be safe against async-handle ordering.
  usleep(20 * 1000);

  ASSERT_EQ(topoCallbackInvocations.load(std::memory_order_acquire), 1)
      << "Connectivity-equal topology update must not count as a real change";
  ASSERT_EQ(ctx->topo, topo1_dup)
      << "Connectivity-equal topology update must still refresh ctx->topo inline";

  // Step 3: schedule a structurally different topology to confirm that genuine
  // updates are still applied (i.e. the short-circuit is not over-eager).
  std::array<const char *, 3> hosts_v2 = {"localhost:6379", "localhost:6389", "localhost:6399"};
  MRClusterTopology *topo2 = getTopology(hosts_v2);
  IORuntimeCtx_Schedule_Topology(ctx, countingTopoCallback, topo2, true);

  ok = RS::WaitForCondition([&]() {
    return topoCallbackInvocations.load(std::memory_order_acquire) >= 2;
  });
  ASSERT_TRUE(ok) << "Different topology was unexpectedly skipped";

  // Drain any outstanding regular callbacks so counter stack variables outlive
  // their uses on the event loop thread.
  int counter3 = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter3);
  ok = RS::WaitForCondition([&]() { return counter3 >= 1; });
  ASSERT_TRUE(ok);
}
