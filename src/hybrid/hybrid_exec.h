/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef __HYBRID_EXECUTION_H__
#define __HYBRID_EXECUTION_H__

#include "redismodule.h"
#include "hybrid_request.h"
#include "search_ctx.h"
#include "aggregate/aggregate.h"
#include "query_error.h"
#include "cursor.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Main command handler for FT.HYBRID command.
 *
 * Parses command arguments, builds hybrid request structure, constructs execution pipeline,
 * and prepares for hybrid search execution.
 *
 * @param ctx Redis module context
 * @param argv Command arguments array (starting with "FT.HYBRID")
 * @param argc Number of arguments in argv
 * @param internal Whether the request is internal (true - shard in cluster setup, false - Coordinator in cluster setup or standalone)
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool internal);

void HybridRequest_StartCursor(HybridRequest *req, RedisModuleCtx *ctx, arrayof(ResultProcessor*) depleters, QueryError *status, bool coord);

void HybridRequest_Execute(HybridRequest *hreq, RedisModuleCtx *ctx, RedisSearchCtx *sctx);

void sendChunk_hybrid(HybridRequest *hreq, RedisModule_Reply *reply, size_t limit, cachedVars cv);

/**
 * Helper function to get the search context from a hybrid request.
 *
 * @param hreq The hybrid request
 * @return RedisSearchCtx pointer from the hybrid request
 */
static inline RedisSearchCtx *HREQ_SearchCtx(struct HybridRequest *hreq) {
  return hreq->sctx;
}

/**
 * Helper function to get the request flags from a hybrid request.
 *
 * @param hreq The hybrid request
 * @return Request flags from the hybrid request
 */
static inline uint32_t HREQ_RequestFlags(HybridRequest *hreq) {
  return hreq->reqflags;
}

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_EXECUTION_H__
