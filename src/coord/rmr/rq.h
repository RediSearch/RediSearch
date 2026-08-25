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
 * is shutting down the item is rejected instead (returns false): it is never
 * enqueued, so it can never execute — the caller owns its resolution. An
 * enqueued item conversely executes exactly once, by the loop or its final
 * shutdown drain. */
bool RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata, uv_async_t *async);

/* Stop RQ_Push from signaling the loop. The flag write shares the queue lock
 * with RQ_Push's check-and-signal, so once this returns no signal is in
 * flight and none will follow — the async handle may then be closed. Call
 * before firing the runtime's shutdown event. */
void RQ_Shutdown(MRWorkQueue *q);

queueItem *RQ_Pop(MRWorkQueue *q, uv_async_t* async);

/* Pop ignoring the maxPending backpressure. Shutdown-only: lets the loop
 * thread's final drain execute every item that was enqueued but not yet
 * processed, so their commands reach the connections and get resolved by the
 * disconnect sweep. */
queueItem *RQ_PopUnbounded(MRWorkQueue *q);

#ifdef ENABLE_ASSERT
int RQ_Debug_GetPending(MRWorkQueue *q);
#endif

#ifdef __cplusplus
}
#endif
