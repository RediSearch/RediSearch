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

//Structure to encapsulate the IO Runtime context for MR operations to take place
typedef struct {
  MRWorkQueue *queue;
  MRConnManager *conn_mgr;
} IORuntimeCtx;

IORuntimeCtx *IORuntimeCtx_Create(size_t num_connections_per_shard, int max_pending, size_t id);

void IORuntimeCtx_Free(IORuntimeCtx *io_runtime_ctx);

void IORuntimeCtx_Schedule(IORuntimeCtx *io_runtime_ctx, MRQueueCallback cb, void *privdata);
