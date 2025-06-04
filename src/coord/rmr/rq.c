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
#include "rq_pool.h"

struct queueItem {
  void *privdata;
  MRQueueCallback cb;
  struct queueItem *next;
};

typedef struct MRWorkQueue {
  size_t id;
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
  uv_loop_t loop;
  uv_thread_t loop_th;
  bool loop_th_started; // set to true when the event loop thread is started
  bool loop_th_running; // set to true when the event loop thread is initialized
  bool loop_th_ready;   /* set to true when the event loop thread is ready to process requests.
                                        * This is set to false when a new topology is applied, and set to true
                                        * when the topology check is done. */
  uv_timer_t topologyValidationTimer, topologyFailureTimer;
  uv_async_t topologyAsync;
  struct queueItem *pendingTopo;
  arrayof(uv_async_t *) pendingQueues;

} MRWorkQueue;

// Atomically exchange the pending topology with a new topology.
// Returns the old pending topology (or NULL if there was no pending topology).
static inline struct queueItem *exchangePendingTopo(MRWorkQueue *q,  struct queueItem *newTopo) {
  return __atomic_exchange_n(&q->pendingTopo, newTopo, __ATOMIC_SEQ_CST);
}

// Atomically check if the event loop thread is uninitialized and mark it as initialized.
// Returns true if the event loop thread was uninitialized, and in this case the caller should
// start the event loop thread. Should normally return false.
static inline bool loopThreadUninitialized(MRWorkQueue *q) {
  return __builtin_expect((__atomic_test_and_set(&q->loop_th_started, __ATOMIC_ACQUIRE) == false), false);
}

static void triggerPendingQueues(MRWorkQueue *q) {
  array_foreach(q->pendingQueues, async, uv_async_send(async));
  array_free(q->pendingQueues);
  q->pendingQueues = NULL;
}

extern RedisModuleCtx *RSDummyContext;

static void topologyFailureCB(uv_timer_t *timer) {

  MRWorkQueue *q = (MRWorkQueue *)timer->data;
  RedisModule_Log(RSDummyContext, "warning", "Queue ID %u: Topology validation failed: not all nodes connected", q->id);
  uv_timer_stop(&q->topologyValidationTimer); // stop the validation timer
  // Mark the event loop thread as ready. This will allow any pending requests to be processed
  // (and fail, but it will unblock clients)
  q->loop_th_ready = true;
  triggerPendingQueues(q);
}

static void topologyTimerCB(uv_timer_t *timer) {
  MRWorkQueue *q = (MRWorkQueue *)timer->data;
  if (MR_CheckTopologyConnections(true) == REDIS_OK) {
    MRWorkQueue *q = (MRWorkQueue *)timer->data;
    // We are connected to all master nodes. We can mark the event loop thread as ready
    q->loop_th_ready = true;
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %u: All nodes connected", q->id);
    uv_timer_stop(&q->topologyValidationTimer); // stop the timer repetition
    uv_timer_stop(&q->topologyFailureTimer);    // stop failure timer (as we are connected)
    triggerPendingQueues(q);
  } else {
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %u: Waiting for all nodes to connect", q->id);
  }
}

static void topologyAsyncCB(uv_async_t *async) {
  MRWorkQueue *q = (MRWorkQueue *)async->data;
  struct queueItem *topo = exchangePendingTopo(q, NULL); // take the topology
  if (topo) {
    // Apply new topology
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %u: Applying new topology", q->id);
    // Mark the event loop thread as not ready. This will ensure that the next event on the event loop
    // will be the topology check. If the topology hasn't changed, the topology check will quickly
    // mark the event loop thread as ready again.
    q->loop_th_ready = false;
    topo->cb(topo->privdata);
    rm_free(topo);
    // Finish this round of topology checks to give the topology connections a chance to connect.
    // Schedule connectivity check immediately with a 1ms repeat interval
    uv_timer_start(&q->topologyValidationTimer, topologyTimerCB, 0, 1);
    if (clusterConfig.topologyValidationTimeoutMS) {
      // Schedule a timer to fail the topology validation if we don't connect to all nodes in time
      uv_timer_start(&q->topologyFailureTimer, topologyFailureCB, clusterConfig.topologyValidationTimeoutMS, 0);
    }
  }
}

/* start the event loop side thread */
static void sideThread(void *arg) {
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
  MRWorkQueue *q = arg;
  // Mark the event loop thread as running before triggering the topology check.
  q->loop_th_running = true;
  if(q->id == 0 ) {
    // Only the global queue needs to initialize the timers.
    uv_timer_init(&q->loop, &q->topologyValidationTimer);
    uv_timer_init(&q->loop, &q->topologyFailureTimer);
    uv_async_init(&q->loop, &q->topologyAsync, topologyAsyncCB);
    uv_async_send(&q->topologyAsync); // start the topology check
  }
  uv_run(&q->loop, UV_RUN_DEFAULT);
}

static void verify_uv_thread(MRWorkQueue *q) {
  if (loopThreadUninitialized(q)) {
    // Verify that we are running on the event loop thread
    int uv_thread_create_status = uv_thread_create(&q->loop_th, sideThread, q);
    RS_ASSERT(uv_thread_create_status == 0);
    REDISMODULE_NOT_USED(uv_thread_create_status);
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %u: Created event loop thread", q->id);
  }
}

void RQ_Push_Topology(MRWorkQueue *q, MRQueueCallback cb, MRClusterTopology *topo) {
  struct queueItem *oldTask, *newTask = rm_new(struct queueItem);
  newTask->cb = cb;
  newTask->privdata = topo;
  oldTask = exchangePendingTopo(q, newTask);
  if (q->loop_th_running) {
    uv_async_send(&q->topologyAsync); // trigger the topology check
  }
  if (oldTask) {
    MRClusterTopology_Free(oldTask->privdata);
    rm_free(oldTask);
  }
}

void RQ_Push(MRWorkQueue *q, MRQueueCallback cb, void *privdata) {
  verify_uv_thread(q);
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
  // fprintf(stderr, "%d %zd\n", concurrentRequests_g, q->sz);

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
      RedisModule_Log(RSDummyContext, "warning", "Queue ID %u: Work queue at max pending with the same head. Size: %zu", q->id, q->sz);
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
  if (!q->loop_th_ready) {
    array_ensure_append_1(q->pendingQueues, async); // try again later
    return;
  }
  struct queueItem *req;
  while (NULL != (req = rqPop(q))) {
    req->cb(req->privdata);
    rm_free(req);
    RQ_Done(q);
  }
}

MRWorkQueue *RQ_New(size_t id, int maxPending) {

  MRWorkQueue *q = rm_calloc(1, sizeof(*q));
  q->id = id;
  q->sz = 0;
  q->head = NULL;
  q->tail = NULL;
  q->pending = 0;
  q->maxPending = maxPending;
  q->pendingInfo.head = NULL;
  q->pendingInfo.warnSize = 0;
  uv_mutex_init(&q->lock);
  // init loop
  uv_loop_init(&q->loop);
  uv_async_init(&q->loop, &q->async, rqAsyncCb);
  q->async.data = q;
  q->topologyAsync.data = q;
  q->topologyFailureTimer.data = q;
  q->topologyValidationTimer.data = q;
  q->pendingTopo = NULL;
  return q;
}

void RQ_Free(MRWorkQueue *q) {
  uv_mutex_lock(&q->lock);
  // clear the queue
  struct queueItem *cur = q->head;
  while (cur) {
    struct queueItem *next = cur->next;
    rm_free(cur);
    cur = next;
  }
  uv_mutex_unlock(&q->lock);
  uv_close((uv_handle_t *)&q->async, NULL);
  uv_close((uv_handle_t *)&q->topologyAsync, NULL);
  uv_close((uv_handle_t *)&q->topologyValidationTimer, NULL);
  uv_close((uv_handle_t *)&q->topologyFailureTimer, NULL);
  uv_loop_close(&q->loop);
  uv_mutex_destroy(&q->lock);
  rm_free(q);
}

void RQ_UpdateMaxPending(MRWorkQueue *q, int maxPending) {
  uv_mutex_lock(&q->lock);
  q->maxPending = maxPending;
  uv_mutex_unlock(&q->lock);
}

void RQ_Debug_ClearPendingTopo(MRWorkQueue *q) {
  struct queueItem *topo = exchangePendingTopo(q, NULL);
  if (topo) {
    MRClusterTopology_Free(topo->privdata);
    rm_free(topo);
  }
}


const void* RQ_GetRuntime(const MRWorkQueue *q) {
  return &q->loop;
}

size_t RQ_GetMaxPending(const MRWorkQueue *q) {
  return q->maxPending;
}
