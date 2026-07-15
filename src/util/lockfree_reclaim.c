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
#include <stddef.h>

/* One pending retirement. */
typedef struct RetireNode {
  void *ptr;
  void (*dtor)(void *);
  struct RetireNode *next;
} RetireNode;

/* Active-reader count, sharded across cache-line-isolated slots.
 *
 * The lock-free query path enters and leaves a read section once per *result*
 * (see rpQueryItNext), i.e. hundreds of thousands of times per scan and from
 * every worker thread at once. A single shared counter turns each of those into
 * a contended atomic RMW on one cache line, which serializes all workers and
 * dominates read latency under concurrency. Sharding gives each thread its own
 * line.
 *
 * A read section bumps its opening thread's fixed slot and returns that slot
 * index as its token; ReadEnd decrements the slot named by the token. Because
 * the token names the slot (rather than ReadEnd re-deriving it from the current
 * thread), a section may legitimately be closed from a *different* thread than
 * it was opened on -- e.g. a cursor scan whose batches resume on another worker
 * -- and every slot still stays >= 0. That non-negativity is what makes the
 * non-atomic sum in activeReaders() safe: a torn read of the slots can only
 * over-count a live reader (harmlessly deferring reclamation), never
 * under-count to a false zero. All slot ops use sequentially-consistent
 * ordering, preserving the total order the RETIRE-AFTER-UNLINK argument relies
 * on (see lockfree_reclaim.h and the note in LFReclaim_TryReclaim). */
#define LFR_NUM_SLOTS 64
#define LFR_CACHELINE 64

typedef struct {
  long v;
  char pad[LFR_CACHELINE - sizeof(long)];
} __attribute__((aligned(LFR_CACHELINE))) LFRSlot;

static LFRSlot g_readers[LFR_NUM_SLOTS];
static unsigned g_slotAlloc = 0;  /* hands out slot indices round-robin */
static __thread int t_slot = -1;  /* this thread's fixed slot, assigned lazily */

static inline int readerSlot(void) {
  if (t_slot < 0) {
    unsigned n = __atomic_fetch_add(&g_slotAlloc, 1u, __ATOMIC_RELAXED);
    t_slot = (int)(n % LFR_NUM_SLOTS);
  }
  return t_slot;
}

/* Total active readers = sum over all slots. Read off the hot path only (retire
 * / drain). Each load is seq-cst; slots are never negative, so the sum is a
 * safe upper bound even though the slots are not read atomically together. */
static long activeReaders(void) {
  long sum = 0;
  for (int i = 0; i < LFR_NUM_SLOTS; i++) {
    sum += __atomic_load_n(&g_readers[i].v, __ATOMIC_SEQ_CST);
  }
  return sum;
}

/* Pending retire list (LIFO). Touched only off the read hot path — by writers
 * on retire, and by a writer draining — so a plain mutex is fine; it never
 * guards a read. */
static RetireNode *g_retired = NULL;
static pthread_mutex_t g_retireLock = PTHREAD_MUTEX_INITIALIZER;

LFReadToken LFReclaim_ReadBegin(void) {
  int slot = readerSlot();
  __atomic_fetch_add(&g_readers[slot].v, 1, __ATOMIC_SEQ_CST);
  return slot;
}

void LFReclaim_ReadEnd(LFReadToken token) {
  /* Plain decrement of the slot this section opened (named by the token, which
   * may differ from the current thread's slot). No draining here: summing all
   * slots to detect "last reader" would defeat the point of sharding on this
   * per-result hot path, so reclamation is driven entirely by the writer side
   * (Retire / TryReclaim). */
  __atomic_fetch_sub(&g_readers[token].v, 1, __ATOMIC_SEQ_CST);
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
   * to it are readers active at retire time. A reader active at retire time
   * incremented its slot (seq-cst) before it could load the object pointer, and
   * that increment is ordered before this seq-cst sum; so this sum observes it
   * as long as the reader has not yet decremented. If the sum is zero, each
   * such reader has executed its seq-cst decrement, hence all their
   * dereferences have completed. Non-negative slots guarantee a live reader's
   * +1 cannot be cancelled by another slot, so a zero sum is never a false
   * negative. */
  if (activeReaders() != 0) {
    return;
  }
  reclaimDrain();
}

void LFReclaim_Retire(void *ptr, void (*dtor)(void *)) {
  if (!ptr) {
    return;
  }
  /* Fast path: no lock-free reader in flight. The object is already
   * unreachable to new readers (RETIRE-AFTER-UNLINK), so destroy it now with no
   * queueing or locking. This is the steady state whenever reads run under the
   * spec read lock. */
  if (activeReaders() == 0) {
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
