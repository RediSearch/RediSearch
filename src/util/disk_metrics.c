/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "disk_metrics.h"
#include "thpool/thpool.h"
#include "rmutil/rm_assert.h"
#include <pthread.h>
#include <stdatomic.h>

static pthread_mutex_t gate = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t nativeGate = PTHREAD_MUTEX_INITIALIZER;
static redisearch_thpool_t* pool;
static bool (*collectBatch)(void*);
static void* collectionContext;
static RedisModuleTimerID timer;
static bool timerActive;
static bool submitted;
static bool wakeRequested;
static _Atomic bool stopping;
static _Atomic bool forking;
static _Atomic bool inChild;
static bool forkHooksInstalled;

static void collectJob(void* unused);

/* Called under gate. A successor replaces the current job; it does not accumulate. */
static bool submit(void) {
  if (!pool || atomic_load(&stopping) || atomic_load(&forking) ||
      atomic_load_explicit(&inChild, memory_order_relaxed))
    return false;
  if (submitted) return true;
  submitted = redisearch_thpool_add_work(pool, collectJob, NULL, THPOOL_PRIORITY_LOW) == 0;
  return submitted;
}

static void collectJob(void* unused) {
  (void)unused;
  pthread_mutex_lock(&nativeGate);
  pthread_mutex_lock(&gate);
  wakeRequested = false;
  bool run = pool && !atomic_load(&stopping) && !atomic_load(&forking);
  pthread_mutex_unlock(&gate);
  bool more = run && collectBatch(collectionContext);
  pthread_mutex_lock(&gate);
  submitted = false;
  if (more || wakeRequested) submit();
  pthread_mutex_unlock(&gate);
  pthread_mutex_unlock(&nativeGate);
}

static void tick(RedisModuleCtx* ctx, void* unused) {
  (void)unused;
  timerActive = false;
  if (atomic_load_explicit(&inChild, memory_order_relaxed)) return;
  pthread_mutex_lock(&gate);
  submit();
  pthread_mutex_unlock(&gate);
  timer = RedisModule_CreateTimer(ctx, 50, tick, NULL);
  timerActive = true;
}

static void beforeFork(void) {
  atomic_store(&forking, true);
  pthread_mutex_lock(&nativeGate);
  pthread_mutex_lock(&gate);
}

static void afterForkParent(void) {
  atomic_store(&forking, false);
  pthread_mutex_unlock(&gate);
  pthread_mutex_unlock(&nativeGate);
}

static void afterForkChild(void) {
  atomic_store_explicit(&inChild, true, memory_order_relaxed);
  pthread_mutex_unlock(&gate);
  pthread_mutex_unlock(&nativeGate);
}

bool DiskMetrics_Start(RedisModuleCtx* ctx, bool (*collect)(void*), void* collector) {
  RS_ASSERT(!pool);
  if (!forkHooksInstalled) {
    if (pthread_atfork(beforeFork, afterForkParent, afterForkChild) != 0) return false;
    forkHooksInstalled = true;
  }
  pool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, NULL, "metrics");
  if (!pool) return false;
  collectionContext = collector;
  collectBatch = collect;
  stopping = false;
  submitted = false;
  timer = RedisModule_CreateTimer(ctx, 50, tick, NULL);
  timerActive = true;
  if (DiskMetrics_Wake()) return true;
  DiskMetrics_Stop(ctx);
  return false;
}

void DiskMetrics_Stop(RedisModuleCtx* ctx) {
  if (DiskMetrics_InForkChild() || !pool) return;
  if (timerActive) {
    RedisModule_StopTimer(ctx, timer, NULL);
    timerActive = false;
  }
  atomic_store(&stopping, true);
  redisearch_thpool_wait(pool);
  redisearch_thpool_destroy(pool);
  pthread_mutex_lock(&gate);
  pool = NULL;
  collectBatch = NULL;
  collectionContext = NULL;
  submitted = false;
  pthread_mutex_unlock(&gate);
}

bool DiskMetrics_Wake(void) {
  if (DiskMetrics_InForkChild()) return false;
  pthread_mutex_lock(&gate);
  wakeRequested = true;
  bool accepted = submit();
  pthread_mutex_unlock(&gate);
  return accepted;
}

bool DiskMetrics_InForkChild(void) {
  return atomic_load_explicit(&inChild, memory_order_relaxed);
}
