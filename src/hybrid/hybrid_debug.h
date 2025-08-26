/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "redismodule.h"
#include "hybrid_request.h"
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Debug Mechanism for FT.HYBRID Command
 *
 * This mechanism extends the debug functionality to support FT.HYBRID queries,
 * allowing simulation of timeouts during hybrid search execution for testing purposes.
 *
 * **Syntax:**
 *   _FT.DEBUG FT.HYBRID <index> SEARCH <query> VSIM <vector_args> [options] <DEBUG_PARAMS> DEBUG_PARAMS_COUNT <count>
 *
  * **Parameters:**
 *   - `TIMEOUT_AFTER_N_SEARCH <N>`: Timeout after N results from search component
 *   - `TIMEOUT_AFTER_N_VSIM <N>`: Timeout after N results from vector component
 *
 * **Usage Examples:**
 *   # Search component timeout only
 *   _FT.DEBUG FT.HYBRID idx SEARCH "hello" VSIM @vec $blob TIMEOUT_AFTER_N_SEARCH 5 DEBUG_PARAMS_COUNT 2
 *
 *   # Vector component timeout only
 *   _FT.DEBUG FT.HYBRID idx SEARCH "hello" VSIM @vec $blob TIMEOUT_AFTER_N_VSIM 8 DEBUG_PARAMS_COUNT 2
 *
 *   # Both component timeouts
 *   _FT.DEBUG FT.HYBRID idx SEARCH "hello" VSIM @vec $blob TIMEOUT_AFTER_N_SEARCH 5 TIMEOUT_AFTER_N_VSIM 10 DEBUG_PARAMS_COUNT 4
 *
 * Note: Currently supports single shard mode only. Coordinator-shards support will be added later.
 */

// Debug parameters structure for hybrid queries
typedef struct {
  RedisModuleString **debug_argv;
  unsigned long long debug_params_count;

  // Component-specific timeouts only
  unsigned long long search_timeout_count;
  unsigned long long vsim_timeout_count;
  int search_timeout_set;
  int vsim_timeout_set;
} HybridDebugParams;

// Wrapper structure for hybrid request with debug capabilities
typedef struct {
  HybridRequest *hreq;                     // Base hybrid request
  HybridDebugParams debug_params;          // Debug parameters
} HybridRequest_Debug;

/**
 * Create a new debug-enabled hybrid request from command arguments.
 * Parses both the hybrid query and debug parameters.
 *
 * @param ctx Redis module context
 * @param argv Command arguments (starting with "FT.HYBRID")
 * @param argc Number of arguments
 * @param status Output parameter for error reporting
 * @return HybridRequest_Debug* on success, NULL on error
 */
HybridRequest_Debug* HybridRequest_Debug_New(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, QueryError *status);

/**
 * Free hybrid debug request structure and all associated resources.
 *
 * @param debug_req The debug request to free
 */
void HybridRequest_Debug_Free(HybridRequest_Debug *debug_req);

/**
 * Parse debug parameters from command arguments.
 * Expects arguments in format: [...] DEBUG_PARAMS_COUNT <count>
 *
 * @param argv Command arguments
 * @param argc Number of arguments
 * @param status Output parameter for error reporting
 * @return HybridDebugParams structure (debug_params_count=0 on error)
 */
HybridDebugParams parseHybridDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status);

/**
 * Parse debug parameters from debug request (without applying them).
 * This should be called BEFORE building the hybrid pipeline.
 *
 * @param debug_req The debug request containing parameters to parse
 * @param status Output parameter for error reporting
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int parseHybridDebugParams(HybridRequest_Debug *debug_req, QueryError *status);

/**
 * Apply debug parameters to the already-built hybrid pipeline.
 * This should be called AFTER HybridRequest_BuildPipeline().
 *
 * @param debug_req The debug request containing parameters to apply
 * @param status Output parameter for error reporting
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int applyHybridDebugToBuiltPipelines(HybridRequest_Debug *debug_req, QueryError *status);

/**
 * Apply component-specific timeouts to hybrid request pipelines.
 * Inserts timeout result processors into the search and vector pipelines.
 * A timeout value of 0 means no timeout for that component.
 *
 * @param hreq The hybrid request to modify
 * @param search_timeout Number of results after which search pipeline times out (0 = no timeout)
 * @param vsim_timeout Number of results after which vector pipeline times out (0 = no timeout)
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int applyHybridTimeout(HybridRequest *hreq, unsigned long long search_timeout,
                       unsigned long long vsim_timeout);

/**
 * Extract component-specific timeout values from debug parameters.
 * Returns the timeout value if set, otherwise returns 0 (no timeout).
 *
 * @param params The debug parameters containing timeout settings
 * @param search_timeout Output parameter for search timeout (0 if not set)
 * @param vsim_timeout Output parameter for vector timeout (0 if not set)
 */
void extractHybridTimeoutValues(const HybridDebugParams *params,
                                unsigned long long *search_timeout,
                                unsigned long long *vsim_timeout);

/**
 * Debug command handler for FT.HYBRID (single shard mode).
 *
 * @param ctx Redis module context
 * @param argv Command arguments (starting with "FT.HYBRID")
 * @param argc Number of arguments
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int DEBUG_hybridCommandHandler(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#ifdef __cplusplus
}
#endif
