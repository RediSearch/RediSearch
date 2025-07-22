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
#include <unistd.h>

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
  static MRClusterTopology *getDummyTopology(size_t numSlots) {
    MRClusterTopology *topo = static_cast<MRClusterTopology*>(rm_malloc(sizeof(*topo)));
    topo->hashFunc = MRHashFunc_CRC16;
    topo->numShards = 0;
    topo->numSlots = numSlots;
    topo->shards = nullptr;
    return topo;
  }

  void SetUp() override {
    struct MRClusterTopology *topo = getDummyTopology(4096);
    ctx = IORuntimeCtx_Create(2, topo, 1, false);
    MRClusterTopology_Free(topo);
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

  usleep(1000);

  for (int i = 0; i < 10; i++) {
    IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  }
  // Give some time for thread to start
  usleep(1000);
  // Now the Runtime processed the topology and the pending queue
  ASSERT_EQ(counter, 11);
}

TEST_F(IORuntimeCtxCommonTest, ScheduleTopology) {
  // Create a new topology
  MRClusterTopology *newTopo = getDummyTopology(4097);

  // Schedule the topology update
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, false);
  MRClusterTopology_Free(newTopo);

  // Give some time to consider that if it had to be applied it would have time
  usleep(2000);

  // Verify the topology was not yet updated (will be updated once a request is scheduled)
  ASSERT_EQ(ctx->topo->numSlots, 4096);

  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);

  usleep(2000);
  ASSERT_EQ(ctx->topo->numSlots, 4097);

  // We don't need to free newTopo here as it's handled by testTopoCallback
}

TEST_F(IORuntimeCtxCommonTest, MultipleTopologyUpdates) {
  // Schedule one dummy request to start the thread and still have the flag io_runtime_started_or_starting set to true
  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  // Schedule multiple topology updates in quick succession
  for (int i = 3; i <= 5; i++) {
    MRClusterTopology *newTopo = getDummyTopology(4096 + i);
    IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, false);
    MRClusterTopology_Free(newTopo);
  }

  // Give some time for the last topology to be applied
  usleep(3000);

  // Only the last topology should be applied
  ASSERT_EQ(ctx->topo->numSlots, 4101);
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
    usleep(100000); // 100ms delay
    (*counter)++;
  };

  IORuntimeCtx_Schedule(io_runtime_ctx, testCallback, &counter);
  // Send one request and make sure it runs to make the test better. Otherwise the async callback does not see the topology applied
  // and delays the callback call (and shutdown call may be called before all the callbacks are called)
  usleep(20000);

  // Schedule 10 delayed requests
  for (int i = 0; i < 10; i++) {
    IORuntimeCtx_Schedule(io_runtime_ctx, delayedCallback, &counter);
  }
  //usleep(100); // 100ms delay
  ASSERT_LT(counter, 11);

  // Fire shutdown and wait for completion, the shutdown is scheduled to run at the end of the event loop (is just another event)
  IORuntimeCtx_FireShutdown(io_runtime_ctx);
  IORuntimeCtx_Free(io_runtime_ctx);

  // Verify all requests were processed despite shutdown
  ASSERT_EQ(counter, 11);
}
