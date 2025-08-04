/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "rq.h"
#include "conn.h"
#include <uv.h>
#include "util/arr.h"
#include "cluster_topology.h"

#ifdef __cplusplus
extern "C" {
#endif

// `*50` for following the previous behavior
// #define MAX_CONCURRENT_REQUESTS (MR_CONN_POOL_SIZE * 50)
#define PENDING_FACTOR 50

typedef struct {
  bool loop_th_ready; /* set to true when the event loop thread is ready to process requests.
  * This is set to false when a new topology is applied, and set to true
  * when the topology check is done. */
  bool io_runtime_started_or_starting; /* Set to true when the IO Runtime is starting or already started. We know that at least one thread (main or worker) is initializing the thread so we are sure (by having atomic access)
  * that the thread will be started only once.*/
  uv_async_t async;
  uv_loop_t loop;
  uv_thread_t loop_th;
  uv_timer_t topologyValidationTimer, topologyFailureTimer;
  uv_async_t topologyAsync;
  uv_async_t shutdownAsync;

  // Thread creation / joining synchronization. Avoid race condition of joining a thread that was not created.
  bool loop_th_created;
  bool loop_th_creation_failed;
  uv_mutex_t loop_th_created_mutex;
  uv_cond_t loop_th_created_cond;
} UVRuntime;

//Structure to encapsulate the IO Runtime context for MR operations to take place
typedef struct {
  // Connectivity / topology structures
  MRConnManager conn_mgr;
  struct MRClusterTopology *topo;

  // Request queue and topology requests
  MRWorkQueue *queue;
  struct queueItem *pendingTopo; // The pending topology to be applied
  bool pendingItems; // Are there any pending items waiting for Topology to be applied

  //UV runtime
  UVRuntime uv_runtime;

} IORuntimeCtx;

struct UpdateTopologyCtx {
  IORuntimeCtx *ioRuntime;
  struct MRClusterTopology *new_topo;
};

IORuntimeCtx *IORuntimeCtx_Create(size_t conn_pool_size, struct MRClusterTopology *initialTopology, size_t id, bool take_topo_ownership);
void IORuntimeCtx_Start(IORuntimeCtx *io_runtime_ctx);
void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx);
void IORuntimeCtx_FireShutdown(IORuntimeCtx *io_runtime_ctx);

//TODO: Have it return int status (return error if thread not created)
void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata);

void IORuntimeCtx_RequestCompleted(IORuntimeCtx *io_runtime_ctx);

// Clears the pendingTopology request that may be queued to be updated, and return the topology that was pending.
void IORuntimeCtx_Debug_ClearPendingTopo(IORuntimeCtx *io_runtime_ctx);
uv_loop_t* IORuntimeCtx_GetLoop(IORuntimeCtx *io_runtime_ctx);
int IORuntimeCtx_ConnectAll(IORuntimeCtx *ioRuntime);
void IORuntimeCtx_UpdateNodes(IORuntimeCtx *ioRuntime);
/* Update the topology by calling the topology provider explicitly with ctx. If ctx is NULL, the
 * provider's current context is used. Otherwise, we call its function with the given context */
int IORuntimeCtx_UpdateNodesAndConnectAll(IORuntimeCtx *ioRuntime);
void IORuntimeCtx_Schedule_Topology(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, struct MRClusterTopology *topo, bool take_topo_ownership);
void IORuntimeCtx_UpdateConnPoolSize(IORuntimeCtx *ioRuntime, size_t new_conn_pool_size);

#ifdef __cplusplus
}
#endif
