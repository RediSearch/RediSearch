/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "coord/tests/utils/minunit.h"
#include "coord/src/rmr/rq.h"
#include "info/global_stats.h"
#include "rmutil/alloc.h"
#include <unistd.h>
#include <stdatomic.h>

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
    if (stats.active_io_threads == expected_value) {
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
  mu_assert_int_eq(0, stats.active_io_threads);

  // Mark the IO runtime as ready to process callbacks (bypass topology validation timeout)
  RQ_Debug_SetLoopReady();

  // Phase 2: Schedule callback and verify metric increases
  RQ_Push(q, slowCallback, &flags);

  // Wait for callback to start
  int started = wait_for_atomic_bool(&flags.started, 30000); // 30s timeout
  mu_check(started);

  // Verify metric increased to 1 while callback is executing
  stats = GlobalStats_GetMultiThreadingStats();
  mu_assert_int_eq(1, stats.active_io_threads);

  // Phase 3: Signal callback to finish and wait for metric to return to 0
  atomic_store(&flags.should_finish, true);

  // Wait for metric to return to 0
  int returned_to_zero = wait_for_metric_value(0, 30000); // 30s timeout
  mu_check(returned_to_zero);

  // Clean up
  RQ_Done(q);
}

static void dummyLog(RedisModuleCtx *ctx, const char *level, const char *fmt, ...) {}

int main(int argc, char **argv) {
  RMUTil_InitAlloc();
  RedisModule_Log = dummyLog;
  MU_RUN_TEST(testMetricUpdateDuringCallback);
  MU_REPORT();

  return minunit_status;
}
