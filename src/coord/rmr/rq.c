/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#define RQ_C__

#include <stdlib.h>
#include <uv.h>
#include "rq.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "rq.h"

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata) {
  queueItem *item = rm_new(struct queueItem);
  item->cb = cb;
  item->privdata = privdata;
  item->next = NULL;
  uv_mutex_lock(&q->lock);
  // append the request to the tail of the list
  if (q->tail) {
    // make it the next of the current tail
    q->tail->next = item;
    // set a new tail
    q->tail = item;
  } else {  // no tail means no head - empty queue
    q->head = q->tail = item;
  }
  q->sz++;

  uv_mutex_unlock(&q->lock);
}

// To be called from the event loop thread, need to protect the link list
queueItem *RQ_Pop(MRWorkQueue *q, uv_async_t* async) {
  uv_mutex_lock(&q->lock);

  if (q->head == NULL) {
    uv_mutex_unlock(&q->lock);
    return NULL;
  }
  if (q->pending >= q->maxPending) {
    uv_mutex_unlock(&q->lock);
    // If the queue is full we need to wake up the drain callback
    uv_async_send(async);
    // Handle pending info logging. Access only to a non-NULL head and pendingInfo,
    // So it's safe to do without the lock.
    if (q->head == q->pendingInfo.head && q->sz > q->pendingInfo.warnSize) {
      // If we hit the same head multiple times, we may have a problem. Log it once.
      RedisModule_Log(RSDummyContext, "warning", "Queue ID %zu: Work queue at max pending with the same head. Size: %zu", q->id, q->sz);
      q->pendingInfo.warnSize = q->sz + (1 << 10);
    } else {
      q->pendingInfo.head = q->head;
      q->pendingInfo.warnSize = q->sz + (1 << 10);
    }

    return NULL;
  } else {
    q->pendingInfo.head = NULL;
    q->pendingInfo.warnSize = 0;
  }

  queueItem *r = q->head;
  q->head = r->next;
  if (!q->head) q->tail = NULL;
  q->sz--;
  q->pending++;

  uv_mutex_unlock(&q->lock);
  return r;
}

// To be called from the event loop thread, after the request is done, no need to protect the pending
void RQ_Done(MRWorkQueue *q) {
  uv_mutex_lock(&q->lock);
  --q->pending;
  uv_mutex_unlock(&q->lock);
}

MRWorkQueue *RQ_New(int maxPending, size_t id) {
  MRWorkQueue *q = rm_calloc(1, sizeof(*q));
  q->sz = 0;
  q->head = NULL;
  q->tail = NULL;
  q->pending = 0;
  q->maxPending = maxPending;
  q->pendingInfo.head = NULL;
  q->pendingInfo.warnSize = 0;
  uv_mutex_init(&q->lock);
  q->id = id;
  return q;
}

void RQ_Free(MRWorkQueue *q) {
  uv_mutex_lock(&q->lock);
  // clear the queue
  queueItem *cur = q->head;
  while (cur) {
    queueItem *next = cur->next;
    rm_free(cur);
    cur = next;
  }
  uv_mutex_unlock(&q->lock);
  uv_mutex_destroy(&q->lock);
  rm_free(q);
}

// To be called from the event loop thread, no need to protect the maxPending
void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending) {
  q->maxPending = maxPending;
}
