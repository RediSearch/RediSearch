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

// Helper function to create a test topology
// Callback for regular tasks
static void callback(void *privdata) {
  usleep(100000); // 10ms delay
  int *counter = static_cast<int*>(privdata);
  (*counter)++;
}

// Callback for topology updates
static void topoCallback(void *privdata) {
  struct UpdateTopologyCtx *ctx = static_cast<struct UpdateTopologyCtx*>(privdata);
  IORuntimeCtx *ioRuntime = ctx->ioRuntime;
  // Update the topology
  if (ioRuntime->topo) {
    MRClusterTopology_Free(ioRuntime->topo);
  }
  ioRuntime->topo = ctx->new_topo;

  // Set loop_th_ready to true to allow processing requests
  ioRuntime->loop_th_ready = true;
  rm_free(ctx);
}

// Test fixture for cluster IO threads tests
class ClusterIOThreadsTest : public ::testing::Test {
protected:
  static MRClusterTopology *getDummyTopology(size_t numSlots) {
    MRClusterTopology *topo = static_cast<MRClusterTopology*>(rm_malloc(sizeof(*topo)));
    topo->hashFunc = MRHashFunc_CRC16;
    topo->numShards = 0;
    topo->numSlots = numSlots;
    topo->shards = nullptr;
    return topo;
  }
};

TEST_F(ClusterIOThreadsTest, TestIOThreadsResize) {
  // Create a cluster with 3 IO threads initially
  MRCluster *cluster = MR_NewCluster(nullptr, 2, 3);
  ASSERT_EQ(cluster->num_io_threads, 3);

  size_t first_num_io_threads = cluster->num_io_threads;

  // Create counters to track callback execution
  int counters[5] = {0};
  MRClusterTopology *topo = getDummyTopology(4096);

  // Schedule callbacks on each IO runtime
  for (int i = 0; i < cluster->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cluster, i);
    IORuntimeCtx_Schedule_Topology(ioRuntime, topoCallback, topo, false);
    // Schedule multiple callbacks on each runtime
    for (int j = 0; j < 10; j++) {
      IORuntimeCtx_Schedule(ioRuntime, callback, &counters[i]);
    }
  }

  usleep(1000); // 100ms
  // make sure topology is applied, it either is put before the async, or the timer will triggerPendingQueues

  // Change number of IO threads (increase)
  MRCluster_UpdateNumIOThreads(cluster, 5);
  ASSERT_EQ(cluster->num_io_threads, 5);

  // Schedule more callbacks on the new threads
  for (int i = first_num_io_threads; i < cluster->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cluster, i);
    for (int j = 0; j < 10; j++) {
      IORuntimeCtx_Schedule(ioRuntime, callback, &counters[i]);
    }
  }

  // Change number of IO threads (decrease)
  MRCluster_UpdateNumIOThreads(cluster, 1);
  ASSERT_EQ(cluster->num_io_threads, 1);
  // Schedule more callbacks on the new threads
  for (int i = 0; i < cluster->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cluster, i);
    for (int j = 0; j < 10; j++) {
      IORuntimeCtx_Schedule(ioRuntime, callback, &counters[i]);
    }
  }

  for (int i = 0; i < cluster->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cluster, i);
    IORuntimeCtx_FireShutdown(ioRuntime);
  }

  // Free the topology before freeing the cluster
  rm_free(topo);
  MRClust_Free(cluster);
  ASSERT_EQ(counters[0], 20);
  ASSERT_EQ(counters[1], 10);
  // Thread that was removed should still have executed its callbacks
  ASSERT_EQ(counters[2], 10);
  // New threads that were added and then removed should have executed their callbacks
  ASSERT_EQ(counters[3], 10);
  ASSERT_EQ(counters[4], 10);
}
