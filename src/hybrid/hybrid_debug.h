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
 * Note: Currently supports single shard mode only. Coordinator-shards support will be added later.
 */

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
