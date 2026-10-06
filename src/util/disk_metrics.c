/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "disk_metrics.h"
#include "rmutil/rm_assert.h"
#include "thpool/thpool.h"
#include <pthread.h>
#include <stdatomic.h>

static pthread_mutex_t gate = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t nativeGate = PTHREAD_MUTEX_INITIALIZER;
static redisearch_thpool_t* pool;
static bool (*collectBatch)(void*, bool);
static void* collectionContext;
static RedisModuleTimerID timer;
static bool timerActive;
static bool submitted;
static bool requested;
static bool periodicRequested;
static _Atomic bool stopping;
static _Atomic bool forking;
static _Atomic bool inChild;
static bool forkHooksInstalled;

static void collectJob(void* unused);

/* Called under gate. A successor replaces the current job; it does not accumulate. */
static bool submit(void) {
  if (!pool || atomic_load(&stopping) || atomic_load(&forking) || atomic_load_explicit(&inChild, memory_order_relaxed))
    return false;
  if (submitted) return true;
  submitted = redisearch_thpool_add_work(pool, collectJob, NULL, THPOOL_PRIORITY_LOW) == 0;
  return submitted;
}

static void collectJob(void* unused) {
  (void)unused;
  pthread_mutex_lock(&nativeGate);
  pthread_mutex_lock(&gate);
  bool run = pool && !atomic_load(&stopping) && !atomic_load(&forking);
  bool periodic = periodicRequested;
  if (run) {
    requested = false;
    periodicRequested = false;
  }
  pthread_mutex_unlock(&gate);
  bool more = run && collectBatch(collectionContext, periodic);
  pthread_mutex_lock(&gate);
  submitted = false;
  if (more || requested) submit();
  pthread_mutex_unlock(&gate);
  pthread_mutex_unlock(&nativeGate);
}

/* Periodic reconciliation is the backstop; storage events request immediate work. */
static void tick(RedisModuleCtx* ctx, void* unused) {
  (void)unused;
  timerActive = false;
  if (atomic_load_explicit(&inChild, memory_order_relaxed)) return;
  pthread_mutex_lock(&gate);
  requested = periodicRequested = true;
  submit();
  pthread_mutex_unlock(&gate);
  timer = RedisModule_CreateTimer(ctx, 1000, tick, NULL);
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

bool DiskMetrics_Start(RedisModuleCtx* ctx, bool (*collect)(void*, bool), void* collector) {
  RS_ASSERT(!pool);
  if (!forkHooksInstalled) {
    if (pthread_atfork(beforeFork, afterForkParent, afterForkChild) != 0) return false;
    forkHooksInstalled = true;
  }
  pthread_mutex_lock(&gate);
  pool = redisearch_thpool_create(1, DEFAULT_HIGH_PRIORITY_BIAS_THRESHOLD, NULL, "metrics");
  if (!pool) {
    pthread_mutex_unlock(&gate);
    return false;
  }
  collectionContext = collector;
  collectBatch = collect;
  stopping = false;
  submitted = false;
  requested = periodicRequested = true;
  timer = RedisModule_CreateTimer(ctx, 1000, tick, NULL);
  timerActive = true;
  bool started = submit();
  pthread_mutex_unlock(&gate);
  if (started) return true;
  DiskMetrics_Stop(ctx);
  return false;
}

void DiskMetrics_Stop(RedisModuleCtx* ctx) {
  if (DiskMetrics_InForkChild() || !pool) return;
  if (timerActive) {
    RedisModule_StopTimer(ctx, timer, NULL);
    timerActive = false;
  }
  pthread_mutex_lock(&gate);
  atomic_store(&stopping, true);
  pthread_mutex_unlock(&gate);
  redisearch_thpool_wait(pool);
  redisearch_thpool_destroy(pool);
  pthread_mutex_lock(&gate);
  pool = NULL;
  collectBatch = NULL;
  collectionContext = NULL;
  submitted = false;
  pthread_mutex_unlock(&gate);
}

/* Safe from native event threads: no Redis timer API, and no native work under gate. */
void DiskMetrics_Request(void) {
  if (DiskMetrics_InForkChild()) return;
  pthread_mutex_lock(&gate);
  if (!atomic_load(&stopping)) {
    requested = true;
    submit();
  }
  pthread_mutex_unlock(&gate);
}

bool DiskMetrics_InForkChild(void) {
  return atomic_load_explicit(&inChild, memory_order_relaxed);
}
