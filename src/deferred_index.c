/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "deferred_index.h"

#include "spec.h"
#include "indexes.h"
#include "config.h"
#include "util/arr.h"
#include "rs_wall_clock.h"
#include "module.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"

// A drain runs on the main thread, so this bounds how long one event-loop
// iteration can spend applying entries. Without it a large backlog would simply
// relocate the stall this exists to remove.
#define DRAIN_BUDGET_US 1000

// Retry period when the drain finds the lock still contended.
#define DRAIN_RETRY_MS 1

// How long the queue head may stay contended before the drain parks on the lock
// once to force it through. `pthread_rwlock_trywrlock` never registers as a
// *waiting* writer, so writer preference gives the drain no eventual-acquisition
// guarantee: with readers whose lifetimes continuously overlap, retrying could
// starve indefinitely and index lag would be unbounded. Escalating trades one
// bounded main-thread block for a bound on lag.
#define ESCALATE_AFTER_MS 50

// Above this depth the caller parks on the lock rather than growing the queue,
// so sustained overload degrades to the previous behaviour instead of to
// unbounded index lag and unbounded memory.
#define MAX_PENDING 4096

typedef struct {
  // Weak, not strong: a queued entry must not keep a dropped index alive. The
  // drain promotes and skips the entry if promotion fails.
  WeakRef ref;
  RedisModuleString *key;  // owned
  DocumentType type;       // the notification's type; re-applying as the wrong type corrupts
  DeferOp op;
  long long enqueuedMs;  // for the escalation deadline
} DeferredEntry;

// Ring-style queue: `head_g` advances on pop so a pop is O(1). Popping by
// memmove would be O(n) per entry and O(n^2) to drain a full queue.
static arrayof(DeferredEntry) pending_g = NULL;
static size_t head_g = 0;
static bool drainScheduled_g = false;

size_t DeferredIndex_PendingCount(void) {
  return pending_g ? array_len(pending_g) - head_g : 0;
}

bool DeferredIndex_ShouldBlock(void) {
  return DeferredIndex_PendingCount() >= MAX_PENDING;
}

static void drainCb(RedisModuleCtx *ctx, void *arg);

static void scheduleDrain(void) {
  if (drainScheduled_g) return;
  RedisModule_CreateTimer(RSDummyContext, DRAIN_RETRY_MS, drainCb, NULL);
  drainScheduled_g = true;
}

void DeferredIndex_Enqueue(IndexSpec *sp, RedisModuleString *key, DocumentType type, DeferOp op) {
  if (!pending_g) pending_g = array_new(DeferredEntry, 16);
  DeferredEntry e = {
      .ref = StrongRef_Demote(IndexSpec_GetStrongRefUnsafe(sp)),
      .key = RedisModule_HoldString(NULL, key),
      .type = type,
      .op = op,
      .enqueuedMs = (long long)RedisModule_Milliseconds(),
  };
  array_append(pending_g, e);
  scheduleDrain();
}

static void popHead(void) {
  DeferredEntry *e = &pending_g[head_g];
  RedisModule_FreeString(NULL, e->key);
  WeakRef_Release(e->ref);
  head_g++;
  // Reclaim once drained, so the array does not grow across the process's life.
  if (head_g == array_len(pending_g)) {
    array_clear(pending_g);
    head_g = 0;
  }
}

static void drainCb(RedisModuleCtx *ctx, void *arg) {
  REDISMODULE_NOT_USED(arg);
  drainScheduled_g = false;

  rs_wall_clock start;
  rs_wall_clock_init(&start);

  while (DeferredIndex_PendingCount() > 0 &&
         rs_wall_clock_elapsed_ns(&start) < (rs_wall_clock_ns_t)DRAIN_BUDGET_US * 1000) {
    DeferredEntry *e = &pending_g[head_g];
    StrongRef strong = IndexSpecRef_Promote(e->ref);
    IndexSpec *sp = StrongRef_Get(strong);
    if (!sp) {
      popHead();  // index was dropped while this entry was queued
      continue;
    }

    const bool escalate =
        (long long)RedisModule_Milliseconds() - e->enqueuedMs >= ESCALATE_AFTER_MS;
    const DeferMode mode = escalate ? DEFER_MODE_DRAIN_BLOCKING : DEFER_MODE_DRAIN;

    // Preflight: rebuilding the document only to fail the lock would repeat that
    // work on every retry, on the main thread, which is the cost this feature
    // exists to avoid. Skip straight to the retry unless the lock looks free.
    if (!escalate && !IndexSpec_WriteLockAvailable(sp)) {
      IndexSpecRef_Release(strong);
      break;
    }

    bool contended = false;
    int rc;
    if (e->op == DEFER_OP_ADD) {
      rc = IndexSpec_UpdateDocEx(sp, ctx, e->key, e->type, NULL, mode, &contended);
    } else {
      rc = IndexSpec_DeleteDocEx(sp, ctx, e->key, NULL, mode, &contended);
    }
    IndexSpecRef_Release(strong);

    if (rc == REDISMODULE_ERR && contended) {
      // Only genuine contention is retried. Any other error is terminal -- the
      // key is gone, or has changed type -- and its cleanup has already run, so
      // retrying it would keep it at the head forever and, since this queue is
      // global, stop every other index from draining too.
      break;
    }
    popHead();
  }

  if (DeferredIndex_PendingCount() > 0) scheduleDrain();
}
