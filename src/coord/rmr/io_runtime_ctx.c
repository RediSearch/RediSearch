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


IORuntimeCtx *IORuntimeCtx_Create(size_t num_connections_per_shard, int max_pending, size_t id) {
  IORuntimeCtx *io_runtime_ctx = rm_malloc(sizeof(IORuntimeCtx));
  io_runtime_ctx->queue = RQ_New(max_pending, id);
  io_runtime_ctx->conn_mgr = MRConnManager_New(num_connections_per_shard);
  return io_runtime_ctx;
}

void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx) {
  RQ_Free(io_runtime_ctx->queue);
  MRConnManager_Free(io_runtime_ctx->conn_mgr);
  rm_free(io_runtime_ctx);
}

void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata) {
  RQ_Push(io_runtime_ctx->queue, cb, privdata);
}
