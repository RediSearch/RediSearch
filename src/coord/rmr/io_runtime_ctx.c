/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "io_runtime_ctx.h"
#include "rmalloc.h"
#include "conn.h"
#include "cluster.h"
#include <rmutil/rm_assert.h>  // Include the assertion header
//TODO(Joan): Check where to put the MRCluster_TOpology thingy
#include "rmr.h"
//TODO(Joan): Check
#include "../config.h"

// Atomically exchange the pending topology with a new topology.
// Returns the old pending topology (or NULL if there was no pending topology).
static inline queueItem *exchangePendingTopo(IORuntimeCtx *io_runtime_ctx, queueItem *newTopo) {
  return __atomic_exchange_n(&io_runtime_ctx->pendingTopo, newTopo, __ATOMIC_SEQ_CST);
}

// Atomically check if the event loop thread is uninitialized and mark it as initialized.
// Returns true if the event loop thread was uninitialized, and in this case the caller should
// start the event loop thread. Should normally return false.
static inline bool loopThreadUninitialized(IORuntimeCtx *io_runtime_ctx) {
  return __builtin_expect((__atomic_test_and_set(&io_runtime_ctx->loop_th_started, __ATOMIC_ACQUIRE) == false), false);
}

static void triggerPendingQueues(IORuntimeCtx *io_runtime_ctx) {
  array_foreach(io_runtime_ctx->pendingQueues, async, uv_async_send(async));
  array_free(io_runtime_ctx->pendingQueues);
  io_runtime_ctx->pendingQueues = NULL;
}

extern RedisModuleCtx *RSDummyContext;

static void topologyFailureCB(uv_timer_t *timer) {

  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)timer->data;
  RedisModule_Log(RSDummyContext, "warning", "Runtime ID %zu: Topology validation failed: not all nodes connected", io_runtime_ctx->queue->id);
  uv_timer_stop(&io_runtime_ctx->topologyValidationTimer); // stop the validation timer
  // Mark the event loop thread as ready. This will allow any pending requests to be processed
  // (and fail, but it will unblock clients)
  io_runtime_ctx->loop_th_ready = true;
  triggerPendingQueues(io_runtime_ctx);
}

static void topologyTimerCB(uv_timer_t *timer) {
  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)timer->data;
 if (MR_CheckTopologyConnections(true) == REDIS_OK) {
    // We are connected to all master nodes. We can mark the event loop thread as ready
    io_runtime_ctx->loop_th_ready = true;
    RedisModule_Log(RSDummyContext, "verbose", "Runtime ID %zu: All nodes connected", io_runtime_ctx->queue->id);
    uv_timer_stop(&io_runtime_ctx->topologyValidationTimer); // stop the timer repetition
    uv_timer_stop(&io_runtime_ctx->topologyFailureTimer);    // stop failure timer (as we are connected)
    triggerPendingQueues(io_runtime_ctx);
  } else {
    RedisModule_Log(RSDummyContext, "verbose", "Runtime ID %zu: Waiting for all nodes to connect", io_runtime_ctx->queue->id);
  }
}


static void topologyAsyncCB(uv_async_t *async) {
  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)async->data;
  queueItem *topo = exchangePendingTopo(io_runtime_ctx, NULL); // take the topology
  if (topo) {
    // Apply new topology
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %zu: Applying new topology", io_runtime_ctx->queue->id);
    // Mark the event loop thread as not ready. This will ensure that the next event on the event loop
    // will be the topology check. If the topology hasn't changed, the topology check will quickly
    // mark the event loop thread as ready again.
    io_runtime_ctx->loop_th_ready = false;
    topo->cb(topo->privdata);
    rm_free(topo);
    // Finish this round of topology checks to give the topology connections a chance to connect.
    // Schedule connectivity check immediately with a 1ms repeat interval
    uv_timer_start(&io_runtime_ctx->topologyValidationTimer, topologyTimerCB, 0, 1);
    if (clusterConfig.topologyValidationTimeoutMS) {
      // Schedule a timer to fail the topology validation if we don't connect to all nodes in time
      uv_timer_start(&io_runtime_ctx->topologyFailureTimer, topologyFailureCB, clusterConfig.topologyValidationTimeoutMS, 0);
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
  IORuntimeCtx *io_runtime_ctx = arg;
  // Mark the event loop thread as running before triggering the topology check.
  io_runtime_ctx->loop_th_running = true;
  if(io_runtime_ctx->queue->id == 0 ) {
    // Only the global queue needs to initialize the timers.
    uv_timer_init(&io_runtime_ctx->loop, &io_runtime_ctx->topologyValidationTimer);
    uv_timer_init(&io_runtime_ctx->loop, &io_runtime_ctx->topologyFailureTimer);
    uv_async_init(&io_runtime_ctx->loop, &io_runtime_ctx->topologyAsync, topologyAsyncCB);
    uv_async_send(&io_runtime_ctx->topologyAsync); // start the topology check
  }
  uv_run(&io_runtime_ctx->loop, UV_RUN_DEFAULT);
}

static void verify_uv_thread(IORuntimeCtx *io_runtime_ctx) {
  if (loopThreadUninitialized(io_runtime_ctx)) {
    // Verify that we are running on the event loop thread
    int uv_thread_create_status = uv_thread_create(&io_runtime_ctx->loop_th, sideThread, io_runtime_ctx);
    RS_ASSERT(uv_thread_create_status == 0);
    REDISMODULE_NOT_USED(uv_thread_create_status);
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %zu: Created event loop thread", io_runtime_ctx->queue->id);
  }
}

void IORuntimeCtx_Push_Topology(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, struct MRClusterTopology *topo) {
  queueItem *oldTask, *newTask = rm_new(queueItem);
  newTask->cb = cb;
  newTask->privdata = topo;
  oldTask = exchangePendingTopo(io_runtime_ctx, newTask);
  if (io_runtime_ctx->loop_th_running) {
    uv_async_send(&io_runtime_ctx->topologyAsync); // trigger the topology check
  }
  if (oldTask) {
    MRClusterTopology_Free(oldTask->privdata);
    rm_free(oldTask);
  }
}

void IORuntimeCtx__Debug_ClearPendingTopo(IORuntimeCtx *io_runtime_ctx) {
  queueItem *topo = exchangePendingTopo(io_runtime_ctx, NULL);
  if (topo) {
    MRClusterTopology_Free(topo->privdata);
    rm_free(topo);
  }
}

uv_loop_t* IORuntimeCtx_GetLoop(IORuntimeCtx *io_runtime_ctx) {
  return &io_runtime_ctx->loop;
}

static void rqAsyncCb(uv_async_t *async) {
  IORuntimeCtx *io_runtime_ctx = async->data;
  if (!io_runtime_ctx->loop_th_ready) {
    array_ensure_append_1(io_runtime_ctx->pendingQueues, async); // try again later
    return;
  }
  queueItem *req;
  while (NULL != (req = RQ_Pop(io_runtime_ctx->queue, &io_runtime_ctx->async))) {
    req->cb(req->privdata);
    rm_free(req);
    RQ_Done(io_runtime_ctx->queue);
  }
}

/* Initialize the connections to all shards */
int IORuntimeCtx_ConnectAll(IORuntimeCtx *ioRuntime) {
  return MRConnManager_ConnectAll(ioRuntime->conn_mgr, IORuntimeCtx_GetLoop(ioRuntime));
}

void IORuntimeCtx_UpdateNodes(IORuntimeCtx *ioRuntime, struct MRClusterTopology *topo) {
  /* Get all the current node ids from the connection manager.  We will remove all the nodes
   * that are in the new topology, and after the update, delete all the nodes that are in this map
   * and not in the new topology */
  dict *nodesToDisconnect = dictCreate(&dictTypeHeapStrings, NULL);

  dictIterator *it = dictGetIterator(ioRuntime->conn_mgr->map);
  dictEntry *de;
  while ((de = dictNext(it))) {
    dictAdd(nodesToDisconnect, dictGetKey(de), NULL);
  }
  dictReleaseIterator(it);

  /* Walk the topology and add all nodes in it to the connection manager */
  for (int sh = 0; sh < topo->numShards; sh++) {
    for (int n = 0; n < topo->shards[sh].numNodes; n++) {
      // Update all the conn Manager in each of the runtimes.
      MRClusterNode *node = &topo->shards[sh].nodes[n];
      MRConnManager_Add(ioRuntime->conn_mgr, node->id, &node->endpoint, 0);
      /* This node is still valid, remove it from the nodes to delete list */
      dictDelete(nodesToDisconnect, node->id);
    }
  }

  // if we didn't remove the node from the original nodes map copy, it means it's not in the new topology,
  // we need to disconnect the node's connections
  it = dictGetIterator(nodesToDisconnect);
  while ((de = dictNext(it))) {
    MRConnManager_Disconnect(ioRuntime->conn_mgr, dictGetKey(de));
  }
  dictReleaseIterator(it);
  dictRelease(nodesToDisconnect);
}

/* Update the topology by calling the topology provider explicitly with ctx. If ctx is NULL, the
 * provider's current context is used. Otherwise, we call its function with the given context */
//TOODO(Joan): Review this code, Should make sure about thread safety
int IORuntimeCtx_UpdateNodesAndConnectAll(IORuntimeCtx *ioRuntime, struct MRClusterTopology *topo) {
  IORuntimeCtx_UpdateNodes(ioRuntime, topo);
  IORuntimeCtx_ConnectAll(ioRuntime);
  return REDIS_OK;
}

IORuntimeCtx *IORuntimeCtx_Create(size_t num_connections_per_shard, int max_pending, size_t id) {
  IORuntimeCtx *io_runtime_ctx = rm_malloc(sizeof(IORuntimeCtx));
  io_runtime_ctx->queue = RQ_New(max_pending, id);
  io_runtime_ctx->conn_mgr = MRConnManager_New(num_connections_per_shard);
  uv_loop_init(&io_runtime_ctx->loop);
  uv_async_init(&io_runtime_ctx->loop, &io_runtime_ctx->async, rqAsyncCb);
  io_runtime_ctx->async.data = io_runtime_ctx;
  io_runtime_ctx->topologyAsync.data = io_runtime_ctx;
  io_runtime_ctx->topologyFailureTimer.data = io_runtime_ctx;
  io_runtime_ctx->topologyValidationTimer.data = io_runtime_ctx;
  io_runtime_ctx->pendingTopo = NULL;
  return io_runtime_ctx;
}

void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx) {
  RQ_Free(io_runtime_ctx->queue);
  MRConnManager_Free(io_runtime_ctx->conn_mgr);
  uv_close((uv_handle_t *)&io_runtime_ctx->async, NULL);
  uv_close((uv_handle_t *)&io_runtime_ctx->topologyAsync, NULL);
  uv_close((uv_handle_t *)&io_runtime_ctx->topologyValidationTimer, NULL);
  uv_close((uv_handle_t *)&io_runtime_ctx->topologyFailureTimer, NULL);
  uv_loop_close(&io_runtime_ctx->loop);
  rm_free(io_runtime_ctx);
}

void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata) {
  verify_uv_thread(io_runtime_ctx);
  RQ_Push(io_runtime_ctx->queue, cb, privdata);
  uv_async_send(&io_runtime_ctx->async);
}
