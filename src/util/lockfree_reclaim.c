/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "lockfree_reclaim.h"
#include "rmalloc.h"

#include <pthread.h>

/* One pending retirement. */
typedef struct RetireNode {
  void *ptr;
  void (*dtor)(void *);
  struct RetireNode *next;
} RetireNode;

/* Number of threads currently inside a lock-free read section.
 * Accessed with sequentially-consistent ordering so it totally-orders against
 * the reader's pointer loads and the writer's publish/unlink stores (see the
 * correctness note in LFReclaim_TryReclaim). */
static long g_activeReaders = 0;

/* Pending retire list (LIFO). Touched only off the read hot path — by writers
 * on retire, and by the last reader to leave / a writer draining — so a plain
 * mutex is fine; it never guards a read. */
static RetireNode *g_retired = NULL;
static pthread_mutex_t g_retireLock = PTHREAD_MUTEX_INITIALIZER;

void LFReclaim_ReadBegin(void) {
  __atomic_fetch_add(&g_activeReaders, 1, __ATOMIC_SEQ_CST);
}

/* Detach and destroy the whole pending batch. Frees outside the lock so the
 * per-object dtor never runs under g_retireLock. */
static void reclaimDrain(void) {
  pthread_mutex_lock(&g_retireLock);
  RetireNode *batch = g_retired;
  g_retired = NULL;
  pthread_mutex_unlock(&g_retireLock);

  while (batch) {
    RetireNode *next = batch->next;
    batch->dtor(batch->ptr);
    rm_free(batch);
    batch = next;
  }
}

void LFReclaim_TryReclaim(void) {
  /* Safe to free the pending set iff no reader is active. Every retired object
   * was made unreachable to new readers before being retired
   * (RETIRE-AFTER-UNLINK), so the only threads that could still hold a pointer
   * to it are readers active at retire time. If the count is zero now, each of
   * those has executed its seq-cst decrement, which this seq-cst load observes,
   * so all their dereferences have completed. */
  if (__atomic_load_n(&g_activeReaders, __ATOMIC_SEQ_CST) != 0) {
    return;
  }
  reclaimDrain();
}

void LFReclaim_ReadEnd(void) {
  /* If we were the last reader, drain opportunistically so the pending set does
   * not accumulate between writer calls. */
  if (__atomic_fetch_sub(&g_activeReaders, 1, __ATOMIC_SEQ_CST) == 1) {
    LFReclaim_TryReclaim();
  }
}

void LFReclaim_Retire(void *ptr, void (*dtor)(void *)) {
  if (!ptr) {
    return;
  }
  /* Fast path: no lock-free reader in flight. The object is already
   * unreachable to new readers (RETIRE-AFTER-UNLINK), so destroy it now with no
   * queueing or locking. This is the steady state whenever reads run under the
   * spec read lock. */
  if (__atomic_load_n(&g_activeReaders, __ATOMIC_SEQ_CST) == 0) {
    dtor(ptr);
    return;
  }

  RetireNode *n = rm_malloc(sizeof(*n));
  n->ptr = ptr;
  n->dtor = dtor;
  pthread_mutex_lock(&g_retireLock);
  n->next = g_retired;
  g_retired = n;
  pthread_mutex_unlock(&g_retireLock);

  /* A reader may have quiesced between the fast-path check and enqueue; try to
   * drain so we do not strand the object until the next reader leaves. */
  LFReclaim_TryReclaim();
}
