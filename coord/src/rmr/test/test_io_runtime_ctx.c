/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "coord/tests/utils/minunit.h"
#include "rq.h"
#include "cluster.h"
#include "rmr.h"
#include "info/global_stats.h"
#include "util/workers.h"
#include "rmutil/alloc.h"
#include <unistd.h>
#include <stdatomic.h>
#include "concurrent_ctx.h"

// Test flags to track callback execution using C11 atomics
typedef struct {
  atomic_bool started;
  atomic_bool should_finish;
} CallbackFlags;

// Callback function that blocks until signaled to finish
// This simulates a long-running IO operation
static void slowCallback(void *arg) {
  CallbackFlags *flags = (CallbackFlags *)arg;

  // Signal that we've started
  atomic_store(&flags->started, true);

  // Wait until test tells us to finish
  while (!atomic_load(&flags->should_finish)) {
    usleep(100); // 100us
  }
}

// Static flags for topology callback (needed because RQ_Push_Topology uses void* privdata for topology)
static atomic_bool topo_callback_started;
static atomic_bool topo_callback_should_finish;

// Slow topology callback that blocks until signaled to finish
static void slowTopologyCallback(void *arg) {
  // Signal that we've started
  atomic_store(&topo_callback_started, true);

  // Wait until test tells us to finish
  while (!atomic_load(&topo_callback_should_finish)) {
    usleep(100); // 100us
  }
}

// Helper function to wait for an atomic bool condition with timeout
static int wait_for_atomic_bool(atomic_bool *condition, int timeout_ms) {
  int elapsed = 0;
  while (!atomic_load(condition) && elapsed < timeout_ms) {
    usleep(1000); // 1ms
    elapsed++;
  }
  return atomic_load(condition);
}

// Helper function to wait for metric to reach a specific value with timeout
static int wait_for_metric_value(size_t expected_value, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
    if (stats.uv_threads_running_queries == expected_value) {
      return 1; // Success
    }
    usleep(1000); // 1ms
    elapsed++;
  }
  return 0; // Timeout
}

// Helper function to wait for topology update metric to reach a specific value with timeout
static int wait_for_topo_metric_value(size_t expected_value, int timeout_ms) {
  int elapsed = 0;
  while (elapsed < timeout_ms) {
    MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
    if (stats.uv_threads_running_topology_update == expected_value) {
      return 1; // Success
    }
    usleep(1000); // 1ms
    elapsed++;
  }
  return 0; // Timeout
}

void testMetricUpdateDuringCallback() {

  CallbackFlags flags;
  atomic_init(&flags.started, false);
  atomic_init(&flags.should_finish, false);

  // Create a work queue
  MRWorkQueue *q = RQ_New(10);
  mu_check(q != NULL);

  // Phase 1: Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  mu_assert_int_eq(0, stats.uv_threads_running_queries);

  // Mark the IO runtime as ready to process callbacks (bypass topology validation timeout)
  RQ_Debug_SetLoopReady();

  // Phase 2: Schedule callback and verify metric increases
  RQ_Push(q, slowCallback, &flags);

  // Wait for callback to start
  int started = wait_for_atomic_bool(&flags.started, 30000); // 30s timeout
  mu_check(started);

  // Verify metric increased to 1 while callback is executing
  stats = GlobalStats_GetMultiThreadingStats();
  mu_assert_int_eq(1, stats.uv_threads_running_queries);

  // Phase 3: Signal callback to finish and wait for metric to return to 0
  atomic_store(&flags.should_finish, true);

  // Wait for metric to return to 0
  int returned_to_zero = wait_for_metric_value(0, 30000); // 30s timeout
  mu_check(returned_to_zero);

  // Clean up
  RQ_Done(q);
}

void testActiveTopologyUpdateThreadsMetric() {

  // Create an empty cluster with empty topology to prevent crashes in topology validation timer
  // (MR_CheckTopologyConnections accesses cl->topo->numShards)
  MRClusterTopology *emptyTopo = MR_NewTopology(0, 0, MRHashFunc_None);
  MRCluster *cluster = MR_NewCluster(emptyTopo, 1);
  MR_Init(cluster, 5000);

  // Reset static flags for this test run
  atomic_store(&topo_callback_started, false);
  atomic_store(&topo_callback_should_finish, false);

  // Phase 1: Verify metric starts at 0
  MultiThreadingStats stats = GlobalStats_GetMultiThreadingStats();
  mu_assert_int_eq(0, stats.uv_threads_running_topology_update);

  // Mark the IO runtime as ready to process callbacks (bypass topology validation timeout)
  RQ_Debug_SetLoopReady();

  // Phase 2: Schedule topology callback and verify metric increases
  // Create an empty topology (will be freed by RQ_Push_Topology if replaced)
  MRClusterTopology *dummyTopo = MR_NewTopology(0, 0, MRHashFunc_None);
  RQ_Push_Topology(slowTopologyCallback, dummyTopo);

  // Wait for callback to start
  int started = wait_for_atomic_bool(&topo_callback_started, 30000); // 30s timeout
  mu_check(started);

  // Verify metric increased to 1 while callback is executing
  stats = GlobalStats_GetMultiThreadingStats();
  mu_assert_int_eq(1, stats.uv_threads_running_topology_update);

  // Phase 3: Signal callback to finish and wait for metric to return to 0
  atomic_store(&topo_callback_should_finish, true);

  // Wait for metric to return to 0
  int returned_to_zero = wait_for_topo_metric_value(0, 30000); // 30s timeout
  mu_check(returned_to_zero);

  // Clean up
  RQ_Debug_StopTopologyTimers();
  MRClust_Free(cluster);
}

static void dummyLog(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  RedisModule_Log = dummyLog;
  // Init workers thpool and ConcurrentSearch required to call GlobalStats_GetMultiThreadingStats
#ifdef MT_BUILD
  workersThreadPool_CreatePool(1);
#endif
  ConcurrentSearch_CreatePool(1);
  MU_RUN_TEST(testMetricUpdateDuringCallback);
  MU_RUN_TEST(testActiveTopologyUpdateThreadsMetric);
  MU_REPORT();
#ifdef MT_BUILD
  workersThreadPool_Destroy();
#endif
  ConcurrentSearch_ThreadPoolDestroy();
  return minunit_status;
}
