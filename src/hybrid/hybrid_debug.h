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
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

struct HybridRequest;

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
 *   - `TIMEOUT_AFTER_N_TAIL <N>`: Timeout after N results from tail pipeline (merger)
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
 *   # Tail pipeline timeout
 *   _FT.DEBUG FT.HYBRID idx SEARCH "hello" VSIM @vec $blob TIMEOUT_AFTER_N_TAIL 3 DEBUG_PARAMS_COUNT 2
 *
 * Supports both single shard (standalone) and multi-shard (cluster) modes.
 */

// Debug parameters structure for hybrid queries
typedef struct {
  RedisModuleString **debug_argv;
  unsigned long long debug_params_count;

  unsigned long long search_timeout_count;
  unsigned long long vsim_timeout_count;
  unsigned long long tail_timeout_count;
  int search_timeout_set;
  int vsim_timeout_set;
  int tail_timeout_set;
} HybridDebugParams;

/**
 * Parse DEBUG_PARAMS_COUNT and debug_argv pointer from the end of argv.
 * Does NOT parse individual debug params (TIMEOUT_AFTER_N_SEARCH, etc.).
 * Sets status on error; returns a zeroed struct with debug_params_count==0 on failure.
 */
HybridDebugParams parseHybridDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status);

/**
 * Parse individual debug parameters (TIMEOUT_AFTER_N_SEARCH, etc.) from
 * the debug_argv region already identified by parseHybridDebugParamsCount.
 */
int parseHybridDebugParams(HybridDebugParams *params, QueryError *status);

/**
 * Apply parsed debug timeouts to the built pipelines of a HybridRequest.
 */
int applyHybridDebugTimeout(struct HybridRequest *hreq, const HybridDebugParams *params);

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
