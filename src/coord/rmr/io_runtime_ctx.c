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
#include "info/global_stats.h"

// Atomically exchange the pending topology with a new topology.
// Returns the old pending topology (or NULL if there was no pending topology).
static inline queueItem *exchangePendingTopo(IORuntimeCtx *io_runtime_ctx, queueItem *newTopo) {
  return __atomic_exchange_n(&io_runtime_ctx->pendingTopo, newTopo, __ATOMIC_SEQ_CST);
}

static inline bool CheckAndSetIoRuntimeNotStarted(IORuntimeCtx *io_runtime_ctx) {
  return __builtin_expect((__atomic_test_and_set(&io_runtime_ctx->uv_runtime.io_runtime_started_or_starting, __ATOMIC_ACQUIRE) == false), false);
}

static inline bool CheckIoRuntimeStarted(IORuntimeCtx *io_runtime_ctx) {
  return io_runtime_ctx->uv_runtime.io_runtime_started_or_starting;
}

static void triggerPendingItems(IORuntimeCtx *io_runtime_ctx) {
  if (io_runtime_ctx->pendingItems) {
    uv_async_send(&io_runtime_ctx->uv_runtime.async);
  }
  io_runtime_ctx->pendingItems = false;
}

static void rqAsyncCb(uv_async_t *async) {
  IORuntimeCtx *io_runtime_ctx = async->data;
  // EDGE CASE: If loop_th_ready is false when a shutdown is fired, it could happen that the shutdown comes before the pendingItems that are here being
  // "reescheduled".
  if (!io_runtime_ctx->uv_runtime.loop_th_ready) {
    // Topology is scheduled to change, note that there are pending items to pop
    io_runtime_ctx->pendingItems = true;
    return;
  }
  queueItem *req;
  while (NULL != (req = RQ_Pop(io_runtime_ctx->queue, &io_runtime_ctx->uv_runtime.async))) {
    GlobalStats_UpdateUvRunningQueries(1);
    req->cb(req->privdata);
    GlobalStats_UpdateUvRunningQueries(-1);
    rm_free(req);
  }
}

extern RedisModuleCtx *RSDummyContext;

static void topologyFailureCB(uv_timer_t *timer) {
  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)timer->data;
  RedisModule_Log(RSDummyContext, "warning", "IORuntime ID %zu: Topology validation failed: not all nodes connected", io_runtime_ctx->queue->id);
  uv_timer_stop(&io_runtime_ctx->uv_runtime.topologyValidationTimer); // stop the validation timer
  // Mark the event loop thread as ready. This will allow any pending requests to be processed
  // (and fail, but it will unblock clients)
  io_runtime_ctx->uv_runtime.loop_th_ready = true;
  triggerPendingItems(io_runtime_ctx);
}

static int CheckTopologyConnections(const MRClusterTopology *topo, IORuntimeCtx *ioRuntime) {
  for (size_t i = 0; i < topo->numShards; i++) {
    if (!MRConn_Get(&ioRuntime->conn_mgr, topo->shards[i].node.id)) {
      return REDIS_ERR;
    }
  }
  return REDIS_OK;
}

static void topologyTimerCB(uv_timer_t *timer) {
  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)timer->data;
  const MRClusterTopology *topo = io_runtime_ctx->topo;
  // Can we lock the topology? here?
 if (CheckTopologyConnections(topo, io_runtime_ctx) == REDIS_OK) {
    // We are connected to all master nodes. We can mark the event loop thread as ready
    io_runtime_ctx->uv_runtime.loop_th_ready = true;
    RedisModule_Log(RSDummyContext, "verbose", "IORuntime ID %zu: All nodes connected: IO thread is ready to handle requests", io_runtime_ctx->queue->id);
    uv_timer_stop(&io_runtime_ctx->uv_runtime.topologyValidationTimer); // stop the timer repetition
    uv_timer_stop(&io_runtime_ctx->uv_runtime.topologyFailureTimer);    // stop failure timer (as we are connected)
    triggerPendingItems(io_runtime_ctx);
  } else {
    RedisModule_Log(RSDummyContext, "verbose", "IORuntime ID %zu: Waiting for all nodes to connect", io_runtime_ctx->queue->id);
  }
}

static void topologyAsyncCB(uv_async_t *async) {
  IORuntimeCtx *io_runtime_ctx = (IORuntimeCtx *)async->data;
  queueItem *task = exchangePendingTopo(io_runtime_ctx, NULL); // take the topology
  if (task) {
    // Apply new topology
    RedisModule_Log(RSDummyContext, "verbose", "IORuntime ID %zu: Applying new topology", io_runtime_ctx->queue->id);
    // Mark the event loop thread as not ready. This will ensure that the next event on the event loop
    // will be the topology check. If the topology hasn't changed, the topology check will quickly
    // mark the event loop thread as ready again.
    io_runtime_ctx->uv_runtime.loop_th_ready = false;
    GlobalStats_UpdateActiveTopologyUpdateThreads(1);
    task->cb(task->privdata);
    GlobalStats_UpdateActiveTopologyUpdateThreads(-1);
    rm_free(task);
    // Finish this round of topology checks to give the topology connections a chance to connect.
    // Schedule connectivity check immediately with a 1ms repeat interval
    uv_timer_start(&io_runtime_ctx->uv_runtime.topologyValidationTimer, topologyTimerCB, 0, 1);
    if (clusterConfig.topologyValidationTimeoutMS) {
      // Schedule a timer to fail the topology validation if we don't connect to all nodes in time
      uv_timer_start(&io_runtime_ctx->uv_runtime.topologyFailureTimer, topologyFailureCB, clusterConfig.topologyValidationTimeoutMS, 0);
    }
  }
}

void shutdown_cb(uv_async_t* handle) {
  IORuntimeCtx* io_runtime_ctx = (IORuntimeCtx*)handle->data;
  // Stop the event loop first
  RedisModule_Log(RSDummyContext, "verbose", "IORuntime ID %zu: Stopping event loop", io_runtime_ctx->queue->id);
  // Go through all the connections and stop the timers
  MRConnManager_Stop(&io_runtime_ctx->conn_mgr);
  uv_stop(&io_runtime_ctx->uv_runtime.loop);
}

// Add this new function to walk and close all handles
static void close_walk_cb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

#define THREAD_NAME_MAX_LEN 32
/* start the event loop side thread */
static void sideThread(void *arg) {
  IORuntimeCtx *io_runtime_ctx = arg;
  /* Set thread name for profiling and debugging */
  char thread_name[THREAD_NAME_MAX_LEN]; // Increased buffer size to accommodate ID
  snprintf(thread_name, sizeof(thread_name), "%s-uv-%zu", REDISEARCH_MODULE_NAME, io_runtime_ctx->queue->id);

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
  // loop is initialized and handles are ready
  //io_runtime_ctx->loop_th_ready = false; // Until topology is validated, no requests are allowed (will be accumulated in the pending queue)
  uv_async_send(&io_runtime_ctx->uv_runtime.topologyAsync); // start the topology check
  // Run the event loop
  RedisModule_Log(RSDummyContext, "verbose", "IORuntime ID %zu: Running event loop", io_runtime_ctx->queue->id);
  uv_run(&io_runtime_ctx->uv_runtime.loop, UV_RUN_DEFAULT);
  RedisModule_Log(RSDummyContext, "verbose", "IORuntime ID %zu: Event loop stopped", io_runtime_ctx->queue->id);
  // After the loop stops, close all handles https://github.com/libuv/libuv/issues/709
  uv_walk(&io_runtime_ctx->uv_runtime.loop, close_walk_cb, NULL);
  // Run the loop one more time to process close callbacks
  uv_run(&io_runtime_ctx->uv_runtime.loop, UV_RUN_ONCE);
  uv_loop_close(&io_runtime_ctx->uv_runtime.loop);
}

uv_loop_t* IORuntimeCtx_GetLoop(IORuntimeCtx *io_runtime_ctx) {
  return &io_runtime_ctx->uv_runtime.loop;
}

/* Initialize the connections to all shards */
int IORuntimeCtx_ConnectAll(IORuntimeCtx *ioRuntime) {
  return MRConnManager_ConnectAll(&ioRuntime->conn_mgr);
}

void IORuntimeCtx_UpdateNodes(IORuntimeCtx *ioRuntime) {
  /* Get all the current node ids from the connection manager.  We will remove all the nodes
   * that are in the new topology, and after the update, delete all the nodes that are in this map
   * and not in the new topology */
  const struct MRClusterTopology *topo = ioRuntime->topo;
  dict *nodesToDisconnect = dictCreate(&dictTypeHeapStrings, NULL);

  dictIterator *it = dictGetIterator(ioRuntime->conn_mgr.map);
  dictEntry *de;
  while ((de = dictNext(it))) {
    dictAdd(nodesToDisconnect, dictGetKey(de), NULL);
  }
  dictReleaseIterator(it);

  /* Walk the topology and add all nodes in it to the connection manager */
  for (uint32_t sh = 0; sh < topo->numShards; sh++) {
    // Update all the conn Manager in each of the runtimes.
    MRClusterNode *node = &topo->shards[sh].node;
    MRConnManager_Add(&ioRuntime->conn_mgr, &ioRuntime->uv_runtime.loop, node->id, &node->endpoint, 0);
    /* This node is still valid, remove it from the nodes to delete list */
    dictDelete(nodesToDisconnect, node->id);
  }

  // if we didn't remove the node from the original nodes map copy, it means it's not in the new topology,
  // we need to disconnect the node's connections
  it = dictGetIterator(nodesToDisconnect);
  while ((de = dictNext(it))) {
    MRConnManager_Disconnect(&ioRuntime->conn_mgr, dictGetKey(de));
  }
  dictReleaseIterator(it);
  dictRelease(nodesToDisconnect);
}

int IORuntimeCtx_UpdateNodesAndConnectAll(IORuntimeCtx *ioRuntime) {
  IORuntimeCtx_UpdateNodes(ioRuntime);
  IORuntimeCtx_ConnectAll(ioRuntime);
  return REDIS_OK;
}

static void UV_Init(IORuntimeCtx *io_runtime_ctx) {
  io_runtime_ctx->uv_runtime.loop_th_ready = false;
  io_runtime_ctx->uv_runtime.io_runtime_started_or_starting = false;
  io_runtime_ctx->uv_runtime.loop_th_created = false;
  io_runtime_ctx->uv_runtime.loop_th_creation_failed = false;
  uv_loop_init(&io_runtime_ctx->uv_runtime.loop);
  uv_mutex_init(&io_runtime_ctx->uv_runtime.loop_th_created_mutex);
  uv_cond_init(&io_runtime_ctx->uv_runtime.loop_th_created_cond);
  io_runtime_ctx->uv_runtime.shutdownAsync.data = io_runtime_ctx;
  io_runtime_ctx->uv_runtime.async.data = io_runtime_ctx;
  io_runtime_ctx->uv_runtime.topologyAsync.data = io_runtime_ctx;
  io_runtime_ctx->uv_runtime.topologyFailureTimer.data = io_runtime_ctx;
  io_runtime_ctx->uv_runtime.topologyValidationTimer.data = io_runtime_ctx;
  uv_timer_init(&io_runtime_ctx->uv_runtime.loop, &io_runtime_ctx->uv_runtime.topologyValidationTimer);
  uv_timer_init(&io_runtime_ctx->uv_runtime.loop, &io_runtime_ctx->uv_runtime.topologyFailureTimer);
  uv_async_init(&io_runtime_ctx->uv_runtime.loop, &io_runtime_ctx->uv_runtime.async, rqAsyncCb);
  uv_async_init(&io_runtime_ctx->uv_runtime.loop, &io_runtime_ctx->uv_runtime.shutdownAsync, shutdown_cb);
  uv_async_init(&io_runtime_ctx->uv_runtime.loop, &io_runtime_ctx->uv_runtime.topologyAsync, topologyAsyncCB);
}

static void UV_Close(IORuntimeCtx *io_runtime_ctx) {
  // Close all handles when thread wasn't initialized
  uv_close((uv_handle_t*)&io_runtime_ctx->uv_runtime.topologyValidationTimer, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->uv_runtime.topologyFailureTimer, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->uv_runtime.async, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->uv_runtime.shutdownAsync, NULL);
  uv_close((uv_handle_t*)&io_runtime_ctx->uv_runtime.topologyAsync, NULL);

  // Run the loop once to process the close callbacks
  uv_run(&io_runtime_ctx->uv_runtime.loop, UV_RUN_ONCE);

  uv_loop_close(&io_runtime_ctx->uv_runtime.loop);
}

IORuntimeCtx *IORuntimeCtx_Create(size_t conn_pool_size, struct MRClusterTopology *initialTopology, size_t id, bool take_topo_ownership) {
  IORuntimeCtx *io_runtime_ctx = rm_malloc(sizeof(IORuntimeCtx));
  MRConnManager_Init(&io_runtime_ctx->conn_mgr, conn_pool_size);
  io_runtime_ctx->queue = RQ_New(io_runtime_ctx->conn_mgr.nodeConns * PENDING_FACTOR, id);
  io_runtime_ctx->pendingTopo = NULL;
  io_runtime_ctx->pendingItems = false;

  if (take_topo_ownership) {
    io_runtime_ctx->topo = initialTopology;
  } else {
    io_runtime_ctx->topo = MRClusterTopology_Clone(initialTopology);
  }
  UV_Init(io_runtime_ctx);

  return io_runtime_ctx;
}

void IORuntimeCtx_FireShutdown(IORuntimeCtx *io_runtime_ctx) {
  if (CheckIoRuntimeStarted(io_runtime_ctx)) {
    // There may be a delay between the thread starting and the loop running, we need to account for it
    // Stop the timers of all the connections before shutting down the loop
    uv_async_send(&io_runtime_ctx->uv_runtime.shutdownAsync);
  }
}

void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx) {
  if (CheckIoRuntimeStarted(io_runtime_ctx)) {
    // Here we know that at least the thread will be created
    uv_mutex_lock(&io_runtime_ctx->uv_runtime.loop_th_created_mutex);
    while (!io_runtime_ctx->uv_runtime.loop_th_created && !io_runtime_ctx->uv_runtime.loop_th_creation_failed) {
      uv_cond_wait(&io_runtime_ctx->uv_runtime.loop_th_created_cond, &io_runtime_ctx->uv_runtime.loop_th_created_mutex);
    }
    uv_mutex_unlock(&io_runtime_ctx->uv_runtime.loop_th_created_mutex);
    if (!io_runtime_ctx->uv_runtime.loop_th_creation_failed) {
      // Make sure IORuntimeCtx Free is not holding the GIL
      uv_thread_join(&io_runtime_ctx->uv_runtime.loop_th);
    }
  } else {
    UV_Close(io_runtime_ctx);
  }
  RQ_Free(io_runtime_ctx->queue);
  MRConnManager_Free(&io_runtime_ctx->conn_mgr);
  queueItem *task = exchangePendingTopo(io_runtime_ctx, NULL);
  if (task) {
    struct UpdateTopologyCtx *ctx = task->privdata;
    if (ctx && ctx->new_topo) {
      MRClusterTopology_Free(ctx->new_topo);
    }
    rm_free(ctx);
    rm_free(task);
  }
  if (io_runtime_ctx->topo) {
    MRClusterTopology_Free(io_runtime_ctx->topo);
  }

  // Destroy synchronization primitives
  uv_mutex_destroy(&io_runtime_ctx->uv_runtime.loop_th_created_mutex);
  uv_cond_destroy(&io_runtime_ctx->uv_runtime.loop_th_created_cond);

  rm_free(io_runtime_ctx);
}

//TODO(Joan): Handle potential error from uv_thread_create, what if thread is not properly created (Not sure other thdpools handle it)
void IORuntimeCtx_Start(IORuntimeCtx *io_runtime_ctx) {
  // Initialize the loop and timers
  // Verify that we are running on the event loop thread
  uv_mutex_lock(&io_runtime_ctx->uv_runtime.loop_th_created_mutex);
  int uv_thread_create_status = uv_thread_create(&io_runtime_ctx->uv_runtime.loop_th, sideThread, io_runtime_ctx);
  io_runtime_ctx->uv_runtime.loop_th_created = true;
  io_runtime_ctx->uv_runtime.loop_th_creation_failed = uv_thread_create_status != 0;
  uv_cond_signal(&io_runtime_ctx->uv_runtime.loop_th_created_cond);
  uv_mutex_unlock(&io_runtime_ctx->uv_runtime.loop_th_created_mutex);
  RS_ASSERT(uv_thread_create_status == 0);
  REDISMODULE_NOT_USED(uv_thread_create_status);
  RedisModule_Log(RSDummyContext, "verbose", "Created event loop thread for IORuntime ID %zu", io_runtime_ctx->queue->id);
}

void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata) {
  if (CheckAndSetIoRuntimeNotStarted(io_runtime_ctx)) {
    //This guarantees only one worker thread will start the IORuntime because of the atomic check. If started but loop is not ready, still RQ will accumulate the request
    // and would still be processed when the thread uvloop starts
    IORuntimeCtx_Start(io_runtime_ctx);
  }
  RQ_Push(io_runtime_ctx->queue, cb, privdata);
  uv_async_send(&io_runtime_ctx->uv_runtime.async);
}

void IORuntimeCtx_RequestCompleted(IORuntimeCtx *io_runtime_ctx) {
  RQ_Done(io_runtime_ctx->queue);
}

void IORuntimeCtx_Schedule_Topology(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, struct MRClusterTopology *topo, bool take_topo_ownership) {
  struct queueItem *newTask = rm_new(struct queueItem);
  struct queueItem *oldTask = NULL;
  //Clone it so that this runtime can handle its own copy
  struct MRClusterTopology *new_topo;
  if (take_topo_ownership) {
    new_topo = topo;
  } else {
    new_topo = MRClusterTopology_Clone(topo);
  }
  struct UpdateTopologyCtx *ctx = rm_new(struct UpdateTopologyCtx);
  ctx->ioRuntime = io_runtime_ctx;
  ctx->new_topo = new_topo;
  newTask->cb = cb;
  newTask->privdata = ctx;
  oldTask = exchangePendingTopo(io_runtime_ctx, newTask);
  // I need to trigger regardless of the thread running or not, it would be eventually picked, the same way a regular Request is scheduled without checking
  // if the thread is running or not. Otherwise there may be a race condition where a topology is never scheduled.
  uv_async_send(&io_runtime_ctx->uv_runtime.topologyAsync); // trigger the topology check
  if (oldTask) {
    // If there was an old task
    struct UpdateTopologyCtx *oldCtx = oldTask->privdata;
    if (oldCtx->new_topo) {
      MRClusterTopology_Free(oldCtx->new_topo);
    }
    rm_free(oldCtx);
    rm_free(oldTask);
  }
}

void IORuntimeCtx_Debug_ClearPendingTopo(IORuntimeCtx *io_runtime_ctx) {
  queueItem *task = exchangePendingTopo(io_runtime_ctx, NULL);
  if (task) {
    struct UpdateTopologyCtx *ctx = task->privdata;
    if (ctx && ctx->new_topo) {
      MRClusterTopology_Free(ctx->new_topo);
    }
    rm_free(ctx);
    rm_free(task);
  }
}

void IORuntimeCtx_UpdateConnPoolSize(IORuntimeCtx *ioRuntime, size_t new_conn_pool_size) {
  RS_ASSERT(new_conn_pool_size > 0);
  size_t old_conn_pool_size = ioRuntime->conn_mgr.nodeConns;
  if (old_conn_pool_size > new_conn_pool_size) {
    MRConnManager_Shrink(&ioRuntime->conn_mgr, new_conn_pool_size);
  } else if (old_conn_pool_size < new_conn_pool_size) {
    MRConnManager_Expand(&ioRuntime->conn_mgr, new_conn_pool_size, IORuntimeCtx_GetLoop(ioRuntime));
  }
}
