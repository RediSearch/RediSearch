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
#include "rmr.h"

struct queueItem {
  void *privdata;
  MRQueueCallback cb;
  struct queueItem *next;
};

typedef struct MRWorkQueue {
  struct queueItem *head;
  struct queueItem *tail;
  struct queueItem *pendingTopo;
  int pending;
  int maxPending;
  size_t sz;
  uv_mutex_t lock;
  uv_async_t async;
} MRWorkQueue;

/* start the event loop side thread */
static void sideThread(void *arg) {
  REDISMODULE_NOT_USED(arg);
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);
}

static void rqPushItem(MRWorkQueue *q, struct queueItem *item) {
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
}

uv_thread_t loop_th;
static int loop_th_running = 0;
extern RedisModuleCtx *RSDummyContext;
static void verify_uv_thread(MRWorkQueue *q) {
  if (!loop_th_running) {
    // Verify that we are running on the event loop thread
    RedisModule_Assert(uv_thread_create(&loop_th, sideThread, NULL) == 0);
    RedisModule_Log(RSDummyContext, "verbose", "Created event loop thread");
    if (q->pendingTopo) {
      // If we have a pending topology, push it to the queue first
      rqPushItem(q, q->pendingTopo);
      q->pendingTopo = NULL;
    }
    loop_th_running = 1;
  }
}

void RQ_Push_Topology(MRWorkQueue *q, MRQueueCallback cb, MRClusterTopology *topo) {
  struct queueItem *item = rm_new(*item);
  item->cb = cb;
  item->privdata = topo;
  item->next = NULL;
  uv_mutex_lock(&q->lock);
  if (loop_th_running) {
    /* If the uv thread is already running, push request as usual */
    rqPushItem(q, item);
    uv_mutex_unlock(&q->lock);
    uv_async_send(&q->async);
    return;
  }
  /* If the uv thread is not running, set the topology as pending */
  if (q->pendingTopo) {
    MRClusterTopology_Free(q->pendingTopo->privdata);
    rm_free(q->pendingTopo);
  }
  q->pendingTopo = item;
  uv_mutex_unlock(&q->lock);
}

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata) {
  struct queueItem *item = rm_new(*item);
  item->cb = cb;
  item->privdata = privdata;
  item->next = NULL;
  uv_mutex_lock(&q->lock);
  verify_uv_thread(q);
  rqPushItem(q, item);
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
  uv_mutex_unlock(&q->lock);
}

static void rqAsyncCb(uv_async_t *async) {
  MRWorkQueue *q = async->data;
  struct queueItem *req;
  while (NULL != (req = rqPop(q))) {
    if (req->cb(req->privdata) != REDIS_OK) {
      // If the callback returns non-zero, it means that we need to re-queue the request
      uv_mutex_lock(&q->lock);
      rqPushItem(q, req);
      uv_mutex_unlock(&q->lock);
      uv_async_send(async); // wake up again
      // We also break from the loop to allow other events to be processed
      break;
    }
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
  uv_mutex_init(&q->lock);
  uv_async_init(uv_default_loop(), &q->async, rqAsyncCb);
  q->async.data = q;
  return q;
}
