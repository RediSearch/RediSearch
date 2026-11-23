/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef COORD_SEARCH_REDUCER_H__
#define COORD_SEARCH_REDUCER_H__

#include "module.h"
#include "info/global_stats.h"
#include "rmr/rmr.h"

/* Reducer state is private; its borrowed replies remain owned by the request's MRCtx. */
void CoordSearchReducer_Free(searchRequestCtx *request);
/* Schedules reduction while retaining the transport through worker completion. */
int CoordSearchReducer_Schedule(struct MRCtx *ctx, int count, MRReply **replies);

/* Blocked-client callbacks share the request's result-production claim. */
int CoordSearchReducer_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CoordSearchReducer_TimeoutFail(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CoordSearchReducer_TimeoutPartial(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Atomic (relaxed) access to the FT.SEARCH MR execution-phase marker: the BG
// handler advances it, the main-thread timeout callbacks read it.
static inline void searchReqCtx_SetExecutionStage(searchRequestCtx *req, QueryTimeoutStage stage) {
  QueryRequest_SetExecutionPhase(&req->base, (int)stage);
}

#endif
