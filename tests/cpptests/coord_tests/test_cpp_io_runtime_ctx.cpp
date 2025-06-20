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
  ioRuntime->loop_th_ready = true;
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
  static MRClusterTopology *getTopology(size_t numSlots, size_t numNodes, const char **hosts) {
    MRClusterTopology *topo = static_cast<MRClusterTopology*>(rm_malloc(sizeof(*topo)));
    topo->hashFunc = MRHashFunc_CRC16;
    topo->numShards = numNodes;
    topo->numSlots = numSlots;
    topo->shards = static_cast<MRClusterShard*>(rm_calloc(numNodes, sizeof(MRClusterShard)));
    size_t slotRange = numSlots / numNodes;

    MRClusterNode nodes[numNodes];
    for (int i = 0; i < numNodes; i++) {
      if (REDIS_OK != MREndpoint_Parse(hosts[i], &nodes[i].endpoint)) {
        return NULL;
      }
      nodes[i].flags = MRNode_Master;
      nodes[i].id = rm_strdup(hosts[i]);
    }

    int i = 0;
    for (size_t slot = 0; slot < topo->numSlots; slot += slotRange) {
      topo->shards[i] = (MRClusterShard){
        .startSlot = static_cast<mr_slot_t>(slot),
        .endSlot = static_cast<mr_slot_t>(slot + slotRange - 1),
        .numNodes = 1,
      };
      topo->shards[i].nodes = static_cast<MRClusterNode*>(rm_calloc(1, sizeof(MRClusterNode)));
      topo->shards[i].nodes[0] = nodes[i];

      i++;
    }

    return topo;
  }

  void SetUp() override {
    const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
    struct MRClusterTopology *topo = getTopology(4096, 4, hosts);
    ctx = IORuntimeCtx_Create(2, topo, 1, true);
  }

  void TearDown() override {
    IORuntimeCtx_FireShutdown(ctx);
    IORuntimeCtx_Free(ctx);
  }
};

TEST_F(IORuntimeCtxCommonTest, InitialState) {
  ASSERT_NE(ctx, nullptr);
  ASSERT_NE(ctx->conn_mgr, nullptr);
  ASSERT_NE(ctx->queue, nullptr);
  ASSERT_EQ(ctx->pendingTopo, nullptr);
  ASSERT_FALSE(ctx->loop_th_ready);
  ASSERT_FALSE(ctx->io_runtime_started_or_starting);
  ASSERT_EQ(ctx->pendingQueues, nullptr);
  ASSERT_FALSE(ctx->loop_th_created);
  ASSERT_FALSE(ctx->loop_th_creation_failed);
}

TEST_F(IORuntimeCtxCommonTest, Schedule) {
  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  // Give some time for thread to start
  usleep(100);
  ASSERT_TRUE(ctx->io_runtime_started_or_starting);
  ASSERT_TRUE(ctx->loop_th_created);
  ASSERT_FALSE(ctx->loop_th_creation_failed);
  // Verify the callback has not been called yet, thread not ready because no Topology is called
  ASSERT_EQ(counter, 0);
  const char *hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409"};
  struct MRClusterTopology *topo = getTopology(4096, 4, hosts);
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, topo, true);

  usleep(100);
  ASSERT_EQ(counter, 0); //Trigger pending queue will not fired because the validation timer will not see successfull connections and triggerPendingQueues. This will not be fired until next request arrives.

  for (int i = 0; i < 10; i++) {
    IORuntimeCtx_Schedule(ctx, testCallback, &counter);
  }
  // Give some time for thread to start
  usleep(100);
  // Now the Runtime processed the topology and the pending queue
  ASSERT_EQ(counter, 11);
}

TEST_F(IORuntimeCtxCommonTest, ScheduleTopology) {
  // Create a new topology
  const char *new_hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409", "localhost:6419"};
  MRClusterTopology *newTopo = getTopology(4096, 5, new_hosts);

  // Schedule the topology update
  struct UpdateTopologyCtx updateCtx = {ctx, newTopo};
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, true);

  // Give some time to consider that if it had to be applied it would have time
  usleep(2000);

  // Verify the topology was not yet updated (will be updated once a request is scheduled)
  ASSERT_EQ(ctx->topo->numShards, 4);

  int counter = 0;
  IORuntimeCtx_Schedule(ctx, testCallback, &counter);

  usleep(200);
  ASSERT_EQ(ctx->topo->numShards, 5);
}

TEST_F(IORuntimeCtxCommonTest, MultipleTopologyUpdates) {
  // Start the runtime
  IORuntimeCtx_Start(ctx);
  // Schedule multiple topology updates in quick succession
  for (int i = 3; i <= 5; i++) {
    const char *new_hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409", "localhost:6419"};
    MRClusterTopology *newTopo = getTopology(4096, i, new_hosts);
    struct UpdateTopologyCtx updateCtx = {ctx, newTopo};
    IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, true);
  }

  // Give some time for the last topology to be applied
  usleep(300);

  // Only the last topology should be applied
  ASSERT_EQ(ctx->topo->numShards, 5);
}

TEST_F(IORuntimeCtxCommonTest, ClearPendingTopo) {
  // Create a new topology but don't start the runtime
  const char *new_hosts[] = {"localhost:6379", "localhost:6389", "localhost:6399", "localhost:6409", "localhost:6419"};
  MRClusterTopology *newTopo = getTopology(2048, 5, new_hosts);

  // Schedule the topology update
  struct UpdateTopologyCtx updateCtx = {ctx, newTopo};
  IORuntimeCtx_Schedule_Topology(ctx, testTopoCallback, newTopo, false);

  // Verify we have a pending topology
  ASSERT_NE(ctx->pendingTopo, nullptr);

  // Clear the pending topology
  IORuntimeCtx_Debug_ClearPendingTopo(ctx);
}
