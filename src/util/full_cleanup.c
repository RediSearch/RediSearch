/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "full_cleanup.h"

#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>

#include "module.h"
#include "redismodule.h"

// How long a pool may take to go idle, and how often it is checked.
#define FULL_CLEANUP_IDLE_WAIT_MS 5000
#define FULL_CLEANUP_IDLE_POLL_MS 100

static bool fullCleanupInProgress_g = false;
static bool fullCleanupHoldsGIL_g = false;
// Set from the moment a SHUTDOWN command is filtered until the event loop runs again.
static bool inShutdownCommand_g = false;

static void shutdownCommandReturned(void *unused) {
  inShutdownCommand_g = false;
}

static void shutdownCommandFilter(RedisModuleCommandFilterCtx *fctx) {
  size_t len;
  const char *cmd = RedisModule_StringPtrLen(RedisModule_CommandFilterArgGet(fctx, 0), &len);
  if (len != strlen("shutdown") || strncasecmp(cmd, "shutdown", len)) {
    return;
  }
  inShutdownCommand_g = true;
  // Runs only once the command has returned: it failed, or it waits for replicas and the
  // shutdown event comes from the cron, where releasing the GIL is allowed.
  if (RedisModule_EventLoopAddOneShot) {
    RedisModule_EventLoopAddOneShot(shutdownCommandReturned, NULL);
  }
}

void FullCleanup_WatchShutdownCommand(RedisModuleCtx *ctx) {
  RedisModule_RegisterCommandFilter(ctx, shutdownCommandFilter, 0);
}

void FullCleanup_Begin(void) {
  fullCleanupInProgress_g = true;
  // Latched: a pool drain processes events, which may run shutdownCommandReturned.
  fullCleanupHoldsGIL_g = inShutdownCommand_g;
}

bool FullCleanup_InProgress(void) {
  return fullCleanupInProgress_g;
}

bool FullCleanup_HoldsGIL(void) {
  return fullCleanupHoldsGIL_g;
}

bool FullCleanup_WaitUntilIdle(bool (*idle)(void *arg), void *arg, const char *what) {
  struct timespec start;
  clock_gettime(CLOCK_MONOTONIC, &start);
  while (!idle(arg)) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    long elapsedMs = (now.tv_sec - start.tv_sec) * 1000 + (now.tv_nsec - start.tv_nsec) / 1000000;
    if (elapsedMs >= FULL_CLEANUP_IDLE_WAIT_MS) {
      RedisModule_Log(RSDummyContext, "warning",
                      "Full cleanup: %s still busy after %d ms; leaving it running", what,
                      FULL_CLEANUP_IDLE_WAIT_MS);
      return false;
    }
    usleep(FULL_CLEANUP_IDLE_POLL_MS * 1000);
  }
  return true;
}

static bool poolIdle(void *arg) {
  redisearch_thpool_t *pool = arg;
  // Pending before in-progress: a thread moves a job from one count to the other under the queue
  // lock, so this order cannot miss a job in flight between them.
  return redisearch_thpool_high_priority_pending_jobs(pool) == 0 &&
         redisearch_thpool_low_priority_pending_jobs(pool) == 0 &&
         redisearch_thpool_admin_priority_pending_jobs(pool) == 0 &&
         redisearch_thpool_num_jobs_in_progress(pool) == 0;
}

bool FullCleanup_WaitForPool(redisearch_thpool_t *pool, const char *what) {
  return FullCleanup_WaitUntilIdle(poolIdle, pool, what);
}

bool FullCleanup_DestroyPool(redisearch_thpool_t *pool, const char *what) {
  if (FullCleanup_HoldsGIL()) {
    if (!FullCleanup_WaitForPool(pool, what)) {
      return false;
    }
    redisearch_thpool_destroy(pool);
    return true;
  }
  RedisModule_ThreadSafeContextUnlock(RSDummyContext);
  redisearch_thpool_destroy(pool);
  RedisModule_ThreadSafeContextLock(RSDummyContext);
  return true;
}
