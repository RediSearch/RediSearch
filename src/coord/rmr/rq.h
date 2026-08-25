/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdlib.h>
#include <stdbool.h>
#include <stddef.h>
#include <uv.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*MRQueueCallback)(void *);

typedef struct queueItem {
  void *privdata;
  MRQueueCallback cb;
  struct queueItem *next;
} queueItem;

typedef struct MRWorkQueue {
  size_t id;
  queueItem *head;
  queueItem *tail;
  int pending;
  int maxPending;
  size_t sz;
  struct {
    queueItem *head;
    size_t warnSize;
  } pendingInfo;
  // Set (under `lock`) when the owning IO runtime starts shutting down; from
  // then on RQ_Push still enqueues but no longer signals the loop. See
  // RQ_Shutdown for the guarantee this provides.
  bool shuttingDown;
  uv_mutex_t lock;
} MRWorkQueue;

MRWorkQueue *RQ_New(int maxPending, size_t id);

void RQ_Free(MRWorkQueue *q);

void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending);

void RQ_Done(MRWorkQueue *q);

/* Enqueue an item and signal `async` to wake the loop thread. Once the queue
 * is shutting down the item is enqueued silently instead (returns false): the
 * loop is not woken and may already be gone, so the caller owns making sure
 * the queue still gets drained. Every enqueued item is executed exactly once
 * — pops are serialized by the queue lock — by the loop, its final shutdown
 * drain, or a post-shutdown drain. */
bool RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata, uv_async_t *async);

/* Stop RQ_Push from signaling the loop. The flag write shares the queue lock
 * with RQ_Push's check-and-signal, so once this returns no signal is in
 * flight and none will follow — the async handle may then be closed. Call
 * before firing the runtime's shutdown event. */
void RQ_Shutdown(MRWorkQueue *q);

queueItem *RQ_Pop(MRWorkQueue *q, uv_async_t* async);

/* Pop ignoring the maxPending backpressure. Shutdown-only: lets the
 * post-quiesce drains execute every item that was enqueued but never
 * processed by the loop. */
queueItem *RQ_PopUnbounded(MRWorkQueue *q);

#ifdef ENABLE_ASSERT
int RQ_Debug_GetPending(MRWorkQueue *q);
#endif

#ifdef __cplusplus
}
#endif
