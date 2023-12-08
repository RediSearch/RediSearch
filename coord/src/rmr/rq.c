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

struct queueItem {
  void *privdata;
  MRQueueCallback cb;
  MRQueueCleanUpCallback free_cb;
  struct queueItem *next;
};

typedef struct MRWorkQueue {
  struct queueItem *head;
  struct queueItem *tail;
  int pending;
  int maxPending;
  size_t sz;
  uv_mutex_t lock;
  uv_async_t async;
} MRWorkQueue;

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata, MRQueueCleanUpCallback free_cb) {
  uv_mutex_lock(&q->lock);
  struct queueItem *item = rm_malloc(sizeof(*item));
  item->cb = cb;
  item->privdata = privdata;
  item->next = NULL;
  item->free_cb = free_cb;
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
  // fprintf(stderr, "%d %zd\n", concurrentRequests_g, q->sz);

  if (q->head == NULL) {
    uv_mutex_unlock(&q->lock);
    return NULL;
  }
  if (q->pending >= q->maxPending) {
    uv_mutex_unlock(&q->lock);
    // If the queue is full we need to wake up the drain callback
    uv_async_send(&q->async);

    return NULL;
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
  // fprintf(stderr, "Concurrent requests: %d/%d\n", q->pending, q->maxPending);
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

MRWorkQueue *RQ_New(size_t cap, int maxPending) {

  MRWorkQueue *q = rm_calloc(1, sizeof(*q));
  q->sz = 0;
  q->head = NULL;
  q->tail = NULL;
  q->pending = 0;
  q->maxPending = maxPending;
  uv_mutex_init(&q->lock);
  uv_async_init(uv_default_loop(), &q->async, rqAsyncCb);
  q->async.data = q;
  return q;
}

// For testing purposes
static void RQ_FreeInternal(uv_handle_t *handle) { rm_free(handle->data); }
void RQ_Free(MRWorkQueue *q) {
  uv_mutex_lock(&q->lock);

  size_t pending_req = 0;
  struct queueItem *next, *req = q->head;
  q->head = q->tail = NULL;
  for (; req; req = next) {
    next = req->next;
    if (req->free_cb) {
      req->free_cb(req->privdata);
    } else {
      ++pending_req;
    }
    rm_free(req);
    q->sz--;
  }

  if (pending_req) {
    RedisModule_Log(NULL, "warning",
                    "RQ_Free(): Note there were %zu pending requests without a free_cb in the rmr "
                    "queue during shutdown.",
                    pending_req);
  }

  uv_mutex_unlock(&q->lock);
  uv_mutex_destroy(&q->lock);
  // From the libuv docs [https://docs.libuv.org/en/v1.x/handle.html#c.uv_close]:
  // "close_cb will be called asynchronously after this call. This MUST be called on each handle before memory is released.
  //  Moreover, the memory can only be released in close_cb or after it has returned."
  uv_close((uv_handle_t *)&q->async, RQ_FreeInternal);
}
