/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "query_error.h"
#include "aggregate.h"

/*
 * Debugging Mechanism for Query Execution
 *
 * This mechanism provides a way to simulate and test specific behaviors in query execution
 * that cannot be easily controlled through the standard user API.
 * The framework is designed to be extendable for additional debugging scenarios requiring direct
 * code intervention.
 * NOTE: Only supported in SA mode
 *
 * -----------------------------------------------------------------------------
 * ### How to Use:
 *
 * **Syntax:**
 *   _FT.DEBUG <QUERY> <DEBUG_QUERY_ARGS> DEBUG_PARAMS_COUNT <COUNT>
 *
 * **Parameters:**
 *   - `<QUERY>`:
 *     - Any valid `FT.SEARCH` or `FT.AGGREGATE` command.
 *     - Supported in both standalone (SA) and cluster mode.
 *
 *   - `<DEBUG_QUERY_ARGS>`:
 *     - Currently supports:
 *       - **`TIMEOUT_AFTER_N <N> **:
 *         - Simulates a timeout after processing `<N>` results.
 *         - Internally inserts a result processor (RP) as the downstream processor
 *           of the final execution step (e.g., `RP_INDEX`).
 *
 *   - `<DEBUG_PARAMS_COUNT>`:
 *     - Specifies the number of expected arguments in `<DEBUG_QUERY_ARGS>`.
 *     - Ensures correct parsing of debug arguments.
 *
 * **Usage Example:**
 *   - To simulate a timeout after processing 100 results:
 *   ```
 *   _FT.DEBUG FT.SEARCH idx "*" TIMEOUT_AFTER_N 100 DEBUG_PARAMS_COUNT 2
 *   ```
 *
 * -----------------------------------------------------------------------------
 *
 * ### Limitations:
 * - `_FT.DEBUG` does not support `FT.PROFILE`.
 *
 * -----------------------------------------------------------------------------
 *
 * ### Debug Params Order:
 * - All debug parameters must be placed at the end of the command. This is required because the
 *   query itself is extracted from the command to be processed using the regular query execution
 *   pipeline.
 *
 * -----------------------------------------------------------------------------
 *
 * ### Current Capabilities:
 *
 * #### Timeout Simulation:
 * Allows simulating query execution timeouts in standalone (SA) mode.
 *
 * - The timeout is applied after processing `N` results.
 * - If the number of available documents matching the query is less than `N`, execution reaches EOF
 *   instead of simulating a timeout.
 *
 */


typedef struct {
  RedisModuleString **debug_argv;
  unsigned long long debug_params_count;  // not including the DEBUG_PARAMS_COUNT <count> args
} AREQ_Debug_params;

typedef struct {
  AREQ r;
  AREQ_Debug_params debug_params;
} AREQ_Debug;

// Will hold AREQ by value, so we can use AREQ_Debug->r in all functions
// expecting AREQ, including AREQ_Free
AREQ_Debug *AREQ_Debug_New(RedisModuleString **argv, int argc, QueryError *status);
AREQ_Debug_params parseDebugParamsCount(RedisModuleString **argv, int argc, QueryError *status);
int parseAndCompileDebug(AREQ_Debug *debug_req, QueryError *status);

// Debug command to wrap single shard FT.AGGREGATE
int DEBUG_RSAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Debug command to wrap single shard FT.SEARCH
int DEBUG_RSSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
