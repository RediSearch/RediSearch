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

uv_thread_t loop_th;
static char loop_th_running = 0;
static char loop_th_ready = 0;
extern RedisModuleCtx *RSDummyContext;
static void sideThread(void *arg);
static void verify_uv_thread(MRWorkQueue *q) {
  if (!loop_th_running) {
    // Verify that we are running on the event loop thread
    RedisModule_Assert(uv_thread_create(&loop_th, sideThread, q) == 0);
    RedisModule_Log(RSDummyContext, "verbose", "Created event loop thread");
    loop_th_running = 1;
  }
}

static void ensureConnections(RedisModuleCtx *ctx, void *data) {
  static size_t tryCount = 1;
  if (MR_CheckTopologyConnections(true) == REDIS_OK) {
    // We are connected to all master nodes. We can mark the event loop thread as ready
    uv_async_t *async = data;
    __atomic_store_n(&loop_th_ready, 1, __ATOMIC_SEQ_CST);
    uv_async_send(async);
    RedisModule_Log(ctx, "verbose", "All nodes connected in %zu tries", tryCount);
  } else {
    // Try again in 1ms
    // If we fail to connect to the cluster for 30 seconds, we give up
    RedisModule_Assert(tryCount++ < 30000); // 30 seconds
    RedisModule_Log(ctx, "verbose", "Waiting for all nodes to connect: %zu", tryCount);
    RedisModule_CreateTimer(ctx, 1, ensureConnections, data);
  }
}

/* start the event loop side thread */
static void sideThread(void *arg) {
  MRWorkQueue *q = arg;
  if (q->pendingTopo) {
    q->pendingTopo->cb(q->pendingTopo->privdata);
    rm_free(q->pendingTopo);
    q->pendingTopo = NULL;
    RedisModule_ThreadSafeContextLock(RSDummyContext);
    RedisModule_CreateTimer(RSDummyContext, 1, ensureConnections, &q->async);
    RedisModule_ThreadSafeContextUnlock(RSDummyContext);
  } else {
    RedisModule_Log(RSDummyContext, "warning", "Topology is unknown");
    __atomic_store_n(&loop_th_ready, 1, __ATOMIC_RELEASE);
  }
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
  if (!__atomic_load_n(&loop_th_ready, __ATOMIC_ACQUIRE)) return;
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
  uv_mutex_init(&q->lock);
  uv_async_init(uv_default_loop(), &q->async, rqAsyncCb);
  q->async.data = q;
  return q;
}
