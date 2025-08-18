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
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/**
 * Activates the pipeline embedded in `hreq`, and serializes the appropriate
 * response to the client, according to the RESP protocol used (2/3).
 *
 * Note: Currently this is used only by the `FT.HYBRID` command, that does
 * not support cursors and profiling, thus this function does not handle
 * those cases. Support should be added as these features are added.
 *
 * @param hreq The hybrid request with built pipeline
 * @param reply Redis module reply object
 * @param limit Maximum number of results to return
 * @param cv Cached variables for result processing
 */
void sendChunk_hybrid(HybridRequest *hreq, RedisModule_Reply *reply, size_t limit, cachedVars cv);

/**
 * Execute the hybrid search pipeline and send results to the client.
 * This function uses the hybrid-specific result serialization functions.
 *
 * @param hreq The HybridRequest with built pipeline
 * @param ctx Redis module context for sending the reply
 * @param sctx Redis search context
 */
void HREQ_Execute(HybridRequest *hreq, RedisModuleCtx *ctx, RedisSearchCtx *sctx);

/**
 * Create a new blocked client context for background hybrid execution.
 *
 * @param hreq The hybrid request to execute
 * @param blockedClient The blocked Redis client
 * @param spec Strong reference to the index spec
 * @return New blocked client hybrid context
 */
blockedClientHybridCtx *blockedClientHybridCtx_New(HybridRequest *hreq,
                                                   RedisModuleBlockedClient *blockedClient,
                                                   StrongRef spec);

/**
 * Destroy a blocked client hybrid context and clean up resources.
 *
 * @param BCHCtx The blocked client context to destroy
 */
void blockedClientHybridCtx_destroy(blockedClientHybridCtx *BCHCtx);

/**
 * Background execution callback for hybrid requests.
 * This function is called by the worker thread to execute hybrid requests.
 *
 * @param BCHCtx The blocked client context containing the request
 */
void HREQ_Execute_Callback(blockedClientHybridCtx *BCHCtx);

/**
 * Helper function to get the search context from a hybrid request.
 *
 * @param hreq The hybrid request
 * @return RedisSearchCtx pointer from the hybrid request parameters
 */
static inline RedisSearchCtx *HREQ_SearchCtx(struct HybridRequest *hreq) {
  return hreq->hybridParams->aggregationParams.common.sctx;
}

/**
 * Helper function to get the request flags from a hybrid request.
 *
 * @param hreq The hybrid request
 * @return Request flags from the hybrid request parameters
 */
static inline uint32_t HREQ_RequestFlags(HybridRequest *hreq) {
  return hreq->hybridParams->aggregationParams.common.reqflags;
}

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_EXECUTION_H__
