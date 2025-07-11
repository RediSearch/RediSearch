/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#define RQ_C__

#if defined(__linux__)
#include <sys/prctl.h>
#endif
#include <pthread.h>

#include <stdlib.h>
#include <uv.h>
#include "rq.h"
#include "rmalloc.h"
#include "rmr.h"
#include "coord/config.h"
#include "rmutil/rm_assert.h"

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

uv_thread_t loop_th;
static char loop_th_started = false; // set to true when the event loop thread is started
static char loop_th_running = false; // set to true when the event loop thread is initialized
static char loop_th_ready = false;   /* set to true when the event loop thread is ready to process requests.
                                      * This is set to false when a new topology is applied, and set to true
                                      * when the topology check is done. */
uv_timer_t topologyValidationTimer, topologyFailureTimer;
uv_async_t topologyAsync;
struct queueItem *pendingTopo = NULL;
arrayof(uv_async_t *) pendingQueues = NULL;

// Atomically exchange the pending topology with a new topology.
// Returns the old pending topology (or NULL if there was no pending topology).
static inline struct queueItem *exchangePendingTopo(struct queueItem *newTopo) {
  return __atomic_exchange_n(&pendingTopo, newTopo, __ATOMIC_SEQ_CST);
}

// Atomically check if the event loop thread is uninitialized and mark it as initialized.
// Returns true if the event loop thread was uninitialized, and in this case the caller should
// start the event loop thread. Should normally return false.
static inline bool loopThreadUninitialized() {
  return __builtin_expect((__atomic_test_and_set(&loop_th_started, __ATOMIC_ACQUIRE) == false), false);
}

static void triggerPendingQueues() {
  array_foreach(pendingQueues, async, uv_async_send(async));
  array_free(pendingQueues);
  pendingQueues = NULL;
}

extern RedisModuleCtx *RSDummyContext;

static void topologyFailureCB(uv_timer_t *timer) {
  RedisModule_Log(RSDummyContext, "warning", "Topology validation failed: not all nodes connected");
  uv_timer_stop(&topologyValidationTimer); // stop the validation timer
  // Mark the event loop thread as ready. This will allow any pending requests to be processed
  // (and fail, but it will unblock clients)
  loop_th_ready = true;
  triggerPendingQueues();
}

static void topologyTimerCB(uv_timer_t *timer) {
  if (MR_CheckTopologyConnections(true) == REDIS_OK) {
    // We are connected to all master nodes. We can mark the event loop thread as ready
    loop_th_ready = true;
    RedisModule_Log(RSDummyContext, "verbose", "All nodes connected");
    uv_timer_stop(&topologyValidationTimer); // stop the timer repetition
    uv_timer_stop(&topologyFailureTimer);    // stop failure timer (as we are connected)
    triggerPendingQueues();
  } else {
    RedisModule_Log(RSDummyContext, "verbose", "Waiting for all nodes to connect");
  }
}

static void topologyAsyncCB(uv_async_t *async) {
  struct queueItem *topo = exchangePendingTopo(NULL); // take the topology
  if (topo) {
    // Apply new topology
    RedisModule_Log(RSDummyContext, "verbose", "Applying new topology");
    // Mark the event loop thread as not ready. This will ensure that the next event on the event loop
    // will be the topology check. If the topology hasn't changed, the topology check will quickly
    // mark the event loop thread as ready again.
    loop_th_ready = false;
    topo->cb(topo->privdata);
    rm_free(topo);
    // Finish this round of topology checks to give the topology connections a chance to connect.
    // Schedule connectivity check immediately with a 1ms repeat interval
    uv_timer_start(&topologyValidationTimer, topologyTimerCB, 0, 1);
    if (clusterConfig.topologyValidationTimeoutMS) {
      // Schedule a timer to fail the topology validation if we don't connect to all nodes in time
      uv_timer_start(&topologyFailureTimer, topologyFailureCB, clusterConfig.topologyValidationTimeoutMS, 0);
    }
  }
}

/* start the event loop side thread */
static void sideThread(void *arg) {
  REDISMODULE_NOT_USED(arg);
  /* Set thread name for profiling and debugging */
  char *thread_name = REDISEARCH_MODULE_NAME "-uv";

#if defined(__linux__)
  /* Use prctl instead to prevent using _GNU_SOURCE flag and implicit
   * declaration */
  prctl(PR_SET_NAME, thread_name);
#elif defined(__APPLE__) && defined(__MACH__)
  pthread_setname_np(thread_name);
#else
  RedisModule_Log(RSDummyContext, "verbose",
      "sideThread(): pthread_setname_np is not supported on this system");
#endif
  // Mark the event loop thread as running before triggering the topology check.
  loop_th_running = true;
  uv_async_send(&topologyAsync); // start the topology check
  uv_run(uv_default_loop(), UV_RUN_DEFAULT);
}

static void verify_uv_thread() {
  if (loopThreadUninitialized()) {
    uv_timer_init(uv_default_loop(), &topologyValidationTimer);
    uv_timer_init(uv_default_loop(), &topologyFailureTimer);
    uv_async_init(uv_default_loop(), &topologyAsync, topologyAsyncCB);
    // Verify that we are running on the event loop thread
    int uv_thread_create_status = uv_thread_create(&loop_th, sideThread, NULL);
    RS_ASSERT(uv_thread_create_status == 0);
    REDISMODULE_NOT_USED(uv_thread_create_status);
    RedisModule_Log(RSDummyContext, "verbose", "Created event loop thread");
  }
}

void RQ_Push_Topology(MRQueueCallback cb, MRClusterTopology *topo) {
  struct queueItem *oldTask, *newTask = rm_new(struct queueItem);
  newTask->cb = cb;
  newTask->privdata = topo;
  oldTask = exchangePendingTopo(newTask);
  if (loop_th_running) {
    uv_async_send(&topologyAsync); // trigger the topology check
  }
  if (oldTask) {
    MRClusterTopology_Free(oldTask->privdata);
    rm_free(oldTask);
  }
}

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata) {
  verify_uv_thread();
  struct queueItem *item = rm_new(*item);
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
  if (!loop_th_ready) {
    array_ensure_append_1(pendingQueues, async); // try again later
    return;
  }
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

void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending) {
  uv_mutex_lock(&q->lock);
  q->maxPending = maxPending;
  uv_mutex_unlock(&q->lock);
}

void RQ_Debug_ClearPendingTopo() {
  struct queueItem *topo = exchangePendingTopo(NULL);
  if (topo) {
    MRClusterTopology_Free(topo->privdata);
    rm_free(topo);
  }
}
