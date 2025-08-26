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
 *   _FT.DEBUG FT.HYBRID <index> SEARCH <query> VSIM <vector_args> [options] TIMEOUT_AFTER_N <N> DEBUG_PARAMS_COUNT 2
 *
 * **Parameters:**
 *   - `TIMEOUT_AFTER_N <N>`: Simulates a timeout after processing N results
 *
 * **Usage Example:**
 *   _FT.DEBUG FT.HYBRID idx SEARCH "hello" VSIM @vec $blob TIMEOUT_AFTER_N 10 DEBUG_PARAMS_COUNT 2
 *
 * Note: Currently supports single shard mode only. Coordinator-shards support will be added later.
 */

// Debug parameters structure for hybrid queries
typedef struct {
  RedisModuleString **debug_argv;
  unsigned long long debug_params_count;
  unsigned long long timeout_count;        // Parsed TIMEOUT_AFTER_N value
  int timeout_set;                         // Flag to indicate if timeout was set
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
 * Apply timeout after count to hybrid request components.
 * Inserts timeout result processors into the appropriate parts of the pipeline.
 *
 * @param hreq The hybrid request to modify
 * @param timeout_count Number of results after which to timeout
 * @return REDISMODULE_OK on success, REDISMODULE_ERR on error
 */
int applyHybridTimeout(HybridRequest *hreq, unsigned long long timeout_count);

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
