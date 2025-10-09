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
#include "coord/config.h"

// Helper function to create a test topology
// Callback for regular tasks
static void callback(void *privdata) {
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
  ioRuntime->uv_runtime.loop_th_ready = true;
  rm_free(ctx);
}

// Test fixture for cluster IO threads tests
class ClusterIOThreadsTest : public ::testing::Test {
protected:
  static MRClusterTopology *getDummyTopology(size_t numSlots) {
    MRClusterTopology *topo = static_cast<MRClusterTopology*>(rm_malloc(sizeof(*topo)));
    topo->numShards = 0;
    topo->numSlots = numSlots;
    topo->shards = nullptr;
    return topo;
  }
};

static void UpdateNumIOThreads(MRCluster *cl, size_t num_io_threads) {
  RS_ASSERT(num_io_threads > 0);

  if (num_io_threads == cl->num_io_threads) return;

  if (num_io_threads < cl->num_io_threads) {
    // Then free the runtime contexts
    for (size_t i = num_io_threads; i < cl->num_io_threads; i++) {
      IORuntimeCtx_FireShutdown(cl->io_runtimes_pool[i]);
    }
    for (size_t i = num_io_threads; i < cl->num_io_threads; i++) {
      IORuntimeCtx_Free(cl->io_runtimes_pool[i]);
    }
    // Resize the pool
    cl->io_runtimes_pool = (IORuntimeCtx**)rm_realloc(cl->io_runtimes_pool, sizeof(IORuntimeCtx*) * num_io_threads);
  } else {
    // Need to increase the number of IO threads
    // Resize the pool
    cl->io_runtimes_pool = (IORuntimeCtx**)rm_realloc(cl->io_runtimes_pool, sizeof(IORuntimeCtx*) * num_io_threads);

    // Create new runtime contexts
    for (size_t i = cl->num_io_threads; i < num_io_threads; i++) {
      cl->io_runtimes_pool[i] = IORuntimeCtx_Create(
          cl->io_runtimes_pool[0]->conn_mgr.nodeConns,
          NULL,
          i + 1,
          false);
      if (cl->io_runtimes_pool[0]->topo) {
        //TODO(Joan): We should make sure this is the last topology from user, so the UpdateTopology request should wait to return
        cl->io_runtimes_pool[i]->topo = MRClusterTopology_Clone(cl->io_runtimes_pool[0]->topo);
        cl->io_runtimes_pool[i]->uv_runtime.loop_th_ready = true;
      }
    }
  }
  cl->num_io_threads = num_io_threads;
}

TEST_F(ClusterIOThreadsTest, TestIOThreadsResize) {
  // Create a cluster with 3 IO threads initially
  MRCluster *cluster = MR_NewCluster(nullptr, 2, 3);
  ASSERT_EQ(cluster->num_io_threads, 3);

  size_t first_num_io_threads = cluster->num_io_threads;

  // Create counters to track callback execution
  int target = 10;
  int counters[5] = {0};
  MRClusterTopology *topo = getDummyTopology(4096);

  // Schedule callbacks on each IO runtime
  for (int i = 0; i < cluster->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cluster, i);
    IORuntimeCtx_Schedule_Topology(ioRuntime, topoCallback, topo, false);
    // Schedule multiple callbacks on each runtime
    for (int j = 0; j < target; j++) {
      IORuntimeCtx_Schedule(ioRuntime, callback, &counters[i]);
    }
  }

  // make sure topology is applied, it either is put before the async, or the Topology timer will triggerPendingQueues.
  // Since the order of the callbacks is not guaranteed, we can't assert on the counters (even if 2 async_t are sent in an specific order,
  // the order of processing is not guaranteed in the uvloop)
  // Wait up to 30 seconds for callbacks to complete
  int attempt = 0;
  for (; attempt < 30'000'000; attempt++) {
    bool all_done = true;
    for (int i = 0; i < cluster->num_io_threads; i++) {
      if (counters[i] < target) {
        all_done = false;
      }
    }
    if (all_done) break;
    usleep(1); // Sleep 1us
  }
  ASSERT_LT(attempt, 30'000'000) << "Timeout waiting for callbacks to complete";

  // Change number of IO threads (increase)
  UpdateNumIOThreads(cluster, 5);
  ASSERT_EQ(cluster->num_io_threads, 5);

  // Schedule more callbacks on the new threads
  for (int i = first_num_io_threads; i < cluster->num_io_threads; i++) {
    IORuntimeCtx *ioRuntime = MRCluster_GetIORuntimeCtx(cluster, i);
    for (int j = 0; j < 10; j++) {
      IORuntimeCtx_Schedule(ioRuntime, callback, &counters[i]);
    }
  }

  // Change number of IO threads (decrease)
  UpdateNumIOThreads(cluster, 1);
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
  MRCluster_Free(cluster);
  ASSERT_EQ(counters[0], 20);
  ASSERT_EQ(counters[1], 10);
  // Thread that was removed should still have executed its callbacks
  ASSERT_EQ(counters[2], 10);
  // New threads that were added and then removed should have executed their callbacks
  ASSERT_EQ(counters[3], 10);
  ASSERT_EQ(counters[4], 10);
}
