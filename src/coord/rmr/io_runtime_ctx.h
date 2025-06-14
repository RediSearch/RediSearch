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

// `*50` for following the previous behavior
// #define MAX_CONCURRENT_REQUESTS (MR_CONN_POOL_SIZE * 50)
#define PENDING_FACTOR 50

struct MRClusterTopology;

//Structure to encapsulate the IO Runtime context for MR operations to take place
typedef struct {
  MRWorkQueue *queue;
  MRConnManager *conn_mgr;
  uv_async_t async;
  uv_loop_t loop;
  uv_thread_t loop_th;
  // Synchronization for loop thread state
  uv_mutex_t loop_th_mutex;
  uv_cond_t loop_th_cond;
  bool loop_th_running; /* set to true when the event loop thread is running and has all the initialization done.
  * It is used to synchronize between the main thread and the event loop thread. (when the Thread is lazily started to be know when can the async event be scheduled).
  * It is protected by the mutex and condition*/

  bool loop_th_ready; /* set to true when the event loop thread is ready to process requests.
  * This is set to false when a new topology is applied, and set to true
  * when the topology check is done. */
  uv_timer_t topologyValidationTimer, topologyFailureTimer;
  uv_async_t topologyAsync;
  uv_async_t shutdownAsync;
  struct queueItem *pendingTopo;
  arrayof(uv_async_t *) pendingQueues;
} IORuntimeCtx;

IORuntimeCtx *IORuntimeCtx_Create(size_t num_connections_per_shard, struct MRClusterTopology *topo, size_t id);
void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx);
void IORuntimeCtx_FireShutdown(IORuntimeCtx *io_runtime_ctx);

void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata);

void IORuntimeCtx__Debug_ClearPendingTopo(IORuntimeCtx *io_runtime_ctx);
uv_loop_t* IORuntimeCtx_GetLoop(IORuntimeCtx *io_runtime_ctx);
int IORuntimeCtx_ConnectAll(IORuntimeCtx *ioRuntime);
void IORuntimeCtx_UpdateNodes(IORuntimeCtx *ioRuntime, struct MRClusterTopology *topo);
/* Update the topology by calling the topology provider explicitly with ctx. If ctx is NULL, the
 * provider's current context is used. Otherwise, we call its function with the given context */
int IORuntimeCtx_UpdateNodesAndConnectAll(IORuntimeCtx *ioRuntime, struct MRClusterTopology *topo);
