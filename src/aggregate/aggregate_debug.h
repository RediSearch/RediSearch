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
 *       - **`TIMEOUT_AFTER_N <N> [INTERNAL_ONLY]`**:
 *         - Simulates a timeout after processing `<N>` results.
 *         - Internally inserts a result processor (RP) as the downstream processor
 *           of the final execution step (e.g., `RP_INDEX` in SA or `RP_NETWORK` in the
 * coordinator).
 *       - **`INTERNAL_ONLY` (optional)**:
 *         - Only applicable in FT.AGGREGATE cluster mode.
 *         - If specified, the timeout applies solely to internal shard queries,
 *           without affecting the coordinator pipeline.
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
 *  ### Limitations:
 * - `_FT.DEBUG` does not support `FT.PROFILE`.
 *
 * -----------------------------------------------------------------------------
 *
 * ### Debug Params Order:
 * - All debug parameters must be placed at the end of the command. This is required because the query itself is extracted
 *   from the command to be processed using the regular query execution pipeline.
 *
 * -----------------------------------------------------------------------------
 *
 * ### Current Capabilities:
 *
 * #### Timeout Simulation:
 * Allows simulating query execution timeouts in both standalone (SA) and cluster modes.
 *
 * **Standalone Mode:**
 * - The timeout is applied after processing `N` results.
 * - If the number of available documents matching the query is less than `N`, execution reaches EOF
 *   instead of simulating a timeout.
 *
 * **Cluster Mode:**
 *
 * - **`FT.SEARCH`**
 *   - The coordinator does not check for timeouts, and there is no query pipeline in `FT.SEARCH`.
 *   - Timeout simulation is applied only at the shard level.
 *   - Each shard processes `N` results before returning a timeout warning.
 *   - Since the coordinator aggregates all shard responses, the final result will contain
 *     `N * number_of_shards` results and a timeout warning.
 *
 * - **`FT.AGGREGATE` in Cluster Mode**
 *   - There is no strict guarantee on the exact number of results returned before a timeout occurs.
 *   - This happens because the coordinator only checks for timeouts before processing a new shard
 * response, rather than after every individual result.
 *   - As a result, execution may continue slightly beyond the expected threshold before the timeout
 * is applied.
 *
 *   **Example Scenario (N = 10):**
 *   1. The first shard returns 8 results (since only 8 documents match the query).
 *      The coordinator checks for a timeout, but none has occurred yet.
 *   2. The coordinator requests another shard’s response and starts processing it.
 *   3. During processing (after returning 2 results), the timeout is set,
 *      but it will only be checked after the entire shard’s response is processed.
 *   4. Suppose this second shard returns 10 results. Since the timeout is only checked after
 * processing, all 10 results are returned before stopping execution.
 *   5. When requesting the next shard’s response, the timeout check finally halts execution,
 * returning a timeout warning.
 *
 *   **Outcome:**
 *   - Instead of stopping after `N = 10` results, execution may continue up to `8 + 10 = 18`
 * results.
 *   - This behavior is sufficient, as the mechanism's primary goal is to simulate a timeout rather
 * than enforce an exact result count.
 *
 *   **Recommendations:**
 *   - In `FT.AGGREGATE` (cluster mode), do not expect an exact number of results unless
 *     you fully know what you are doing.
 *   - If precise control over the result count is required, ensure that all shards contain at
 *     least `N` matching documents. This way, a timeout occurs after processing the first shard's
 * response.
 *
 * - **`INTERNAL_ONLY` Flag:**
 *   - Enforces timeout only at the shard level, without affecting the coordinator.
 *   - If a shard returns an empty result, the coordinator is not notified, which could cause it to
 *     hang indefinitely. To avoid this, if N == 0, a real timeout is enforced.
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
