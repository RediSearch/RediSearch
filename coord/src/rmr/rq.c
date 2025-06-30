/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#define RQ_C__

#include <stdlib.h>
#include <uv.h>
#include "rq.h"
#include "rmalloc.h"

extern RedisModuleCtx *RSDummyContext;

struct queueItem {
  void *privdata;
  MRQueueCallback cb;
  struct queueItem *next;
};

typedef struct MRWorkQueue {
  struct queueItem *head;
  struct queueItem *tail;
  int pending;
  int maxPending;
  size_t sz;
  struct {
    struct queueItem *head;
    size_t warnSize;
  } pendingInfo;
  uv_mutex_t lock;
  uv_async_t async;
} MRWorkQueue;

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata) {
  uv_mutex_lock(&q->lock);
  struct queueItem *item = rm_malloc(sizeof(*item));
  item->cb = cb;
  item->privdata = privdata;
  item->next = NULL;
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
  uv_async_send(&q->async);
}

static struct queueItem *rqPop(MRWorkQueue *q) {
  uv_mutex_lock(&q->lock);

  if (q->head == NULL) {
    uv_mutex_unlock(&q->lock);
    return NULL;
  }
  if (q->pending >= q->maxPending) {
    uv_mutex_unlock(&q->lock);
    // If the queue is full we need to wake up the drain callback
    uv_async_send(&q->async);

    // Handle pending info logging. Access only to a non-NULL head and pendingInfo,
    // So it's safe to do without the lock.
    if (q->head == q->pendingInfo.head && q->sz > q->pendingInfo.warnSize) {
      // If we hit the same head multiple times, we may have a problem. Log it once.
      RedisModule_Log(RSDummyContext, "warning", "Work queue at max pending with the same head. Size: %zu", q->sz);
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

  struct queueItem *r = q->head;
  q->head = r->next;
  if (!q->head) q->tail = NULL;
  q->sz--;
  q->pending++;

  uv_mutex_unlock(&q->lock);
  return r;
}

void RQ_Done(MRWorkQueue *q) {
  uv_mutex_lock(&q->lock);
  --q->pending;
  uv_mutex_unlock(&q->lock);
}

static void rqAsyncCb(uv_async_t *async) {
  MRWorkQueue *q = async->data;
  struct queueItem *req;
  while (NULL != (req = rqPop(q))) {
    req->cb(req->privdata);
    rm_free(req);
  }
}

MRWorkQueue *RQ_New(int maxPending) {

  MRWorkQueue *q = rm_calloc(1, sizeof(*q));
  q->sz = 0;
  q->head = NULL;
  q->tail = NULL;
  q->pending = 0;
  q->maxPending = maxPending;
  q->pendingInfo.head = NULL;
  q->pendingInfo.warnSize = 0;
  uv_mutex_init(&q->lock);
  uv_async_init(uv_default_loop(), &q->async, rqAsyncCb);
  q->async.data = q;
  return q;
}
