/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#if defined(__linux__)
#include <sys/prctl.h>
#endif
#include "io_runtime_ctx.h"
#include "rmalloc.h"
#include "conn.h"
#include "cluster.h"
#include <rmutil/rm_assert.h>  // Include the assertion header
#include "../config.h"
#include <unistd.h> /* For usleep() */


typedef struct SideLoopThreadData {
  IORuntimeCtx *io_runtime_ctx;
  const struct MRClusterTopology *topo;
} SideLoopThreadData, TopologyValidationTimerCBData;

// Atomically exchange the pending topology with a new topology.
// Returns the old pending topology (or NULL if there was no pending topology).
static inline queueItem *exchangePendingTopo(IORuntimeCtx *io_runtime_ctx, queueItem *newTopo) {
  return __atomic_exchange_n(&io_runtime_ctx->pendingTopo, newTopo, __ATOMIC_SEQ_CST);
}

static void triggerPendingQueues(IORuntimeCtx *io_runtime_ctx) {
  array_foreach(io_runtime_ctx->pendingQueues, async, uv_async_send(async));
  array_free(io_runtime_ctx->pendingQueues);
  io_runtime_ctx->pendingQueues = NULL;
}

static void rqAsyncCb(uv_async_t *async) {
  IORuntimeCtx *io_runtime_ctx = async->data;
  if (!io_runtime_ctx->loop_th_ready) {
    // Topology is scheduled to change, add to the list of pending queues after topology is properly applied
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

static int CheckTopologyConnections(const MRClusterTopology *topo,
                                    IORuntimeCtx *ioRuntime,
                                    bool mastersOnly) {
  for (size_t i = 0; i < topo->numShards; i++) {
    MRClusterShard *sh = &topo->shards[i];
    for (size_t j = 0; j < sh->numNodes; j++) {
      if (mastersOnly && !(sh->nodes[j].flags & MRNode_Master)) {
        continue;
      }
      if (!MRConn_Get(ioRuntime->conn_mgr, sh->nodes[j].id)) {
        return REDIS_ERR;
      }
    }
  }
  return REDIS_OK;
}

static void topologyTimerCB(uv_timer_t *timer) {
  TopologyValidationTimerCBData *cbData = (TopologyValidationTimerCBData *)timer->data;
  IORuntimeCtx *io_runtime_ctx = cbData->io_runtime_ctx;
  const MRClusterTopology *topo = cbData->topo;
 if (CheckTopologyConnections(topo, io_runtime_ctx, true) == REDIS_OK) {
    // We are connected to all master nodes. We can mark the event loop thread as ready
    io_runtime_ctx->loop_th_ready = true;
    RedisModule_Log(RSDummyContext, "verbose", "Runtime ID %zu: All nodes connected: IO thread is ready to handle requests", io_runtime_ctx->queue->id);
    uv_timer_stop(&io_runtime_ctx->topologyValidationTimer); // stop the timer repetition
    uv_timer_stop(&io_runtime_ctx->topologyFailureTimer);    // stop failure timer (as we are connected)
    triggerPendingQueues(io_runtime_ctx);
  } else {
    RedisModule_Log(RSDummyContext, "verbose", "Runtime ID %zu: Waiting for all nodes to connect", io_runtime_ctx->queue->id);
  }
}


static void topologyAsyncCB(uv_async_t *async) {
  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)async->data;
  queueItem *task = exchangePendingTopo(io_runtime_ctx, NULL); // take the topology
  if (task) {
    // Apply new topology
    RedisModule_Log(RSDummyContext, "verbose", "Queue ID %zu: Applying new topology", io_runtime_ctx->queue->id);
    // Mark the event loop thread as not ready. This will ensure that the next event on the event loop
    // will be the topology check. If the topology hasn't changed, the topology check will quickly
    // mark the event loop thread as ready again.
    io_runtime_ctx->loop_th_ready = false;
    task->cb(task->privdata);
    rm_free(task);
    // Finish this round of topology checks to give the topology connections a chance to connect.
    // Schedule connectivity check immediately with a 1ms repeat interval
    uv_timer_start(&io_runtime_ctx->topologyValidationTimer, topologyTimerCB, 0, 1);
    if (clusterConfig.topologyValidationTimeoutMS) {
      // Schedule a timer to fail the topology validation if we don't connect to all nodes in time
      uv_timer_start(&io_runtime_ctx->topologyFailureTimer, topologyFailureCB, clusterConfig.topologyValidationTimeoutMS, 0);
    }
  }
}

void shutdown_cb(uv_async_t* handle) {
  IORuntimeCtx* io_runtime_ctx = (IORuntimeCtx*)handle->data;

  // Stop timers
  uv_timer_stop(&io_runtime_ctx->topologyValidationTimer);
  uv_timer_stop(&io_runtime_ctx->topologyFailureTimer);

  // Close handles
  if (io_runtime_ctx->topologyValidationTimer.data) {
    rm_free(io_runtime_ctx->topologyValidationTimer.data);
  }
  uv_close((uv_handle_t*)&io_runtime_ctx->topologyValidationTimer, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->topologyFailureTimer, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->topologyAsync, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->shutdownAsync, NULL);

  // Stop the loop
  uv_stop(&io_runtime_ctx->loop);
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
  SideLoopThreadData *data = arg;
  IORuntimeCtx *io_runtime_ctx = data->io_runtime_ctx;
  const struct MRClusterTopology *topo = data->topo;

  // Initialize the loop and timers
  uv_loop_init(&io_runtime_ctx->loop);
  uv_timer_init(&io_runtime_ctx->loop, &io_runtime_ctx->topologyValidationTimer);
  uv_timer_init(&io_runtime_ctx->loop, &io_runtime_ctx->topologyFailureTimer);
  uv_async_init(&io_runtime_ctx->loop, &io_runtime_ctx->async, rqAsyncCb);
  uv_async_init(&io_runtime_ctx->loop, &io_runtime_ctx->topologyAsync, topologyAsyncCB);
  uv_async_init(&io_runtime_ctx->loop, &io_runtime_ctx->shutdownAsync, shutdown_cb);

  io_runtime_ctx->shutdownAsync.data = io_runtime_ctx;
  io_runtime_ctx->async.data = io_runtime_ctx;
  io_runtime_ctx->topologyAsync.data = io_runtime_ctx;
  io_runtime_ctx->topologyFailureTimer.data = io_runtime_ctx;
  io_runtime_ctx->topologyValidationTimer.data = (TopologyValidationTimerCBData *)data;
  uv_async_send(&io_runtime_ctx->topologyAsync); // start the topology check

  // loop is initialized and handles are ready
  //io_runtime_ctx->loop_th_ready = true; // Until topology is validated, no requests are allowed (will be accumulated in the pending queue)
  uv_mutex_lock(&io_runtime_ctx->loop_th_mutex);
  io_runtime_ctx->loop_th_running = true;
  uv_cond_signal(&io_runtime_ctx->loop_th_cond);
  uv_mutex_unlock(&io_runtime_ctx->loop_th_mutex);

  uv_async_send(&io_runtime_ctx->topologyAsync); // start the topology check

  // Run the event loop
  uv_run(&io_runtime_ctx->loop, UV_RUN_DEFAULT);
  uv_loop_close(&io_runtime_ctx->loop);
}

void IORuntimeCtx_Debug_ClearPendingTopo(IORuntimeCtx *io_runtime_ctx) {
  queueItem *task = exchangePendingTopo(io_runtime_ctx, NULL);
  if (task) {
    UpdateTopologyCtx *ctx = task->privdata;
    if (ctx && ctx->topo) {
      MRClusterTopology_Free(ctx->topo);
    }
    rm_free(ctx);
    rm_free(task);
  }
}

uv_loop_t* IORuntimeCtx_GetLoop(IORuntimeCtx *io_runtime_ctx) {
  return &io_runtime_ctx->loop;
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
      MRConnManager_Add(ioRuntime->conn_mgr, &ioRuntime->loop, node->id, &node->endpoint, 0);
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
  ((TopologyValidationTimerCBData *)ioRuntime->topologyValidationTimer.data)->topo = topo;
  return REDIS_OK;
}

IORuntimeCtx *IORuntimeCtx_Create(size_t num_connections_per_shard, struct MRClusterTopology* topo, size_t id) {
  IORuntimeCtx *io_runtime_ctx = rm_malloc(sizeof(IORuntimeCtx));
  io_runtime_ctx->conn_mgr = MRConnManager_New(num_connections_per_shard);
  io_runtime_ctx->queue = RQ_New(io_runtime_ctx->conn_mgr->nodeConns * PENDING_FACTOR, id);
  io_runtime_ctx->pendingTopo = NULL;
  io_runtime_ctx->loop_th_ready = false;
  io_runtime_ctx->loop_th_running = false;

  // Initialize synchronization primitives
  uv_mutex_init(&io_runtime_ctx->loop_th_mutex);
  uv_cond_init(&io_runtime_ctx->loop_th_cond);

  SideLoopThreadData *data = rm_malloc(sizeof(SideLoopThreadData));
  data->io_runtime_ctx = io_runtime_ctx;
  data->topo = topo;

  int uv_thread_create_status = uv_thread_create(&io_runtime_ctx->loop_th, sideThread, data);
  RS_ASSERT(uv_thread_create_status == 0);

  // Wait for the thread to be created and running using condition variable
  uv_mutex_lock(&io_runtime_ctx->loop_th_mutex);
  while (!io_runtime_ctx->loop_th_running) {
    uv_cond_wait(&io_runtime_ctx->loop_th_cond, &io_runtime_ctx->loop_th_mutex);
  }
  uv_mutex_unlock(&io_runtime_ctx->loop_th_mutex);

  return io_runtime_ctx;
}

void IORuntimeCtx_FireShutdown(IORuntimeCtx *io_runtime_ctx) {
  if (io_runtime_ctx->loop_th_running) {
    // There may be a delaty between the thread starting and the loop running, we need to account for it
    uv_async_send(&io_runtime_ctx->shutdownAsync);
  }
}

void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx) {
  if (io_runtime_ctx->loop_th_running) {
    uv_thread_join(&io_runtime_ctx->loop_th);
  }
  RQ_Free(io_runtime_ctx->queue);
  MRConnManager_Free(io_runtime_ctx->conn_mgr);
  rm_free(io_runtime_ctx);
}

void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata) {
  RS_ASSERT(io_runtime_ctx->loop_th_running); // the thread is not lazily initialized, it's created on creation
  RQ_Push(io_runtime_ctx->queue, cb, privdata);
  uv_async_send(&io_runtime_ctx->async);
}

void IORuntimeCtx_Schedule_Topology(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, struct MRClusterTopology *topo) {
  struct queueItem *newTask = rm_new(struct queueItem);
  struct queueItem *oldTask = NULL;
  UpdateTopologyCtx *ctx = rm_malloc(sizeof(UpdateTopologyCtx));
  ctx->ioRuntime = io_runtime_ctx;
  ctx->topo = topo;
  newTask->cb = cb;
  newTask->privdata = ctx;
  oldTask = exchangePendingTopo(io_runtime_ctx, newTask);

  if (io_runtime_ctx->loop_th_running) {
    uv_async_send(&io_runtime_ctx->topologyAsync); // trigger the topology check
  }
  if (oldTask) {
    rm_free(oldTask);
  }
}
