/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#define RQ_C__

#include <stdlib.h>
#include <uv.h>
#include <stdatomic.h>
#include <stdbool.h>
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
  volatile atomic_bool isActive;
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
    RedisModule_Log(NULL, "warning","RQ_Push: queue is empty = %d\n", order_for_debug);

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
  RedisModule_Log(NULL, "warning","rqPop: popping from queue, current queue size is %d and there are %d pending requests\n", q->sz, q->pending);

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
  RedisModule_Log(NULL, "warning","rqAsyncCb: poped CB order of debug = %d\n", order_for_debug);
  //__atomic_exchange_n (&order_for_debug, 2, __ATOMIC_RELAXED);

    req->cb(req->privdata);
    rm_free(req);
  RedisModule_Log(NULL, "warning","rqAsyncCb: done callback order of debug = %d\n", order_for_debug);

  }
  RedisModule_Log(NULL, "warning","rqAsyncCb: exit function bye order of debug = %d\n", order_for_debug);

}

MRWorkQueue *RQ_New(int maxPending) {

  MRWorkQueue *q = rm_calloc(1, sizeof(*q));
  q->sz = 0;
  q->head = NULL;
  q->tail = NULL;
  q->pending = 0;
  q->maxPending = maxPending;
  uv_mutex_init(&q->lock);
  uv_async_init(uv_default_loop(), &q->async, rqAsyncCb);
  q->async.data = q;
  q->isActive = true;
  return q;
}

// For testing purposes
void RQ_Empty(MRWorkQueue *q) {
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
  RedisModule_Log(NULL, "warning","RQ_Empty: queue is empty");

}
volatile int active = 1;
static void RQ_Deactivate(uv_handle_t *handle) {
  RedisModule_Log(NULL, "warning","RQ_Deactivate: called");
  MRWorkQueue *q = handle->data;
  q->isActive = false;

}
void RQ_Close_uv(MRWorkQueue *q) {
  RedisModule_Log(NULL, "warning","RQ_Close_uv:close uv");
  uv_close((uv_handle_t *)&q->async, RQ_Deactivate);
}

void RQ_Free(MRWorkQueue *q) {
  while(q->isActive){
    //TODO: NEVER wait indefently
  }
  uv_mutex_destroy(&q->lock);
  rm_free(q);
  RedisModule_Log(NULL, "warning","RQ_FREE: done");
}
