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
 *           coordinator).
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
 * - All debug parameters must be placed at the end of the command. This is required because the
 *   query itself is extracted from the command to be processed using the regular query execution pipeline.
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
 *   - When the timeout policy is non strict, the coordinator does not check for timeouts, and there is no query pipeline in `FT.SEARCH`.
 *   - Timeout simulation is applied only at the shard level.
 *   - Each shard processes `N` results before returning a timeout warning.
 *   - Since the coordinator aggregates all shard responses, the final result will contain
 *     `N * number_of_shards` results and a timeout warning.
 *
 * - **`FT.AGGREGATE` in Cluster Mode**
 *
 * 1. Timeout Checkpoints In RPNetNext (production code):
 *    The coordinator does not continuously check for timeouts. Instead, it checks at specific
 *    points:
 *    - Before requesting a new shard’s reply, based on elapsed time.
 *    - When returning the last document of a shard’s reply, based on whether the reply contains a
 *      timeout warning. This means that once a shard’s reply is received, all results from that
 *      reply are processed before checking for a timeout.
 *
 * 2. The timeout time is set by the timeout rp when the total number of results returned crosses
 *    N. However, as mentioned above, if we are in the middle of consuming a shard’s
 *    reply when we exceed N, we do not immediately check for a timeout. Instead, we
 *    finish consuming the entire reply before performing a timeout check.
 *
 * 3. Standard Behavior: Returning Exactly N Results
 *    In a regular scenario, if all shards contain enough results to fully answer the query,
 *    the first shard’s reply will return exactly `N` results and trigger a timeout warning.
 *
 *    It is important that **all shards** have sufficient results to ensure tests are not flaky,
 *    as the order of replies depends on timing. If a shard with insufficient results replies
 *    first (EOF), the results will not align with `N`, leading to potential inconsistencies. See details below.
 *
 * 4. When Does Result Length Not Align with N
 *    - If the first shard’s reply contains fewer than N results due to EOF,
 *      subsequent replies might push the total accumulated results past N, and the
 *      exact alignment with N is lost.
 *    - This can result in a timeout warning being issued after more than N
 *      results have been returned, or not being issued at all.
 *
 *    Since checks only occur at specific points, exceeding N alone does not immediately
 *    trigger a timeout. If total accumulated results exceed N, whether the final result
 *    contains a timeout warning depends on:
 *
 *    - **A timeout warning exists in the current reply:**
 *      If the current reply contained a timeout warning and pushed the accumulated results past
 *      N, the coordinator propagates this timeout when returning the last document of the
 *      reply.
 *
 *      Example:
 *        - `timeout_res_count = 10`
 *        - First reply: 5 results (EOF)
 *        - Second reply: 10 results (TIMEOUT)
 *        - Total results = 15, timeout warning triggered.
 *
 *    - **Elapsed time before fetching a new reply:**
 *      If the current reply did not contain a timeout warning but was returned due to EOF, the
 *      coordinator must request another shard’s reply. Before making this request, it checks the elapsed
 *      time. Since the timeout time was already set when we reached N, this check will
 *      trigger a timeout status.
 *
 *    *Example of timeout warning due to elapsed time:*
 *      - `timeout_res_count = 10`
 *      - First reply: 5 results (EOF)
 *      - Second reply: 7 results (EOF)
 *      - Total results = 12, timeout warning triggered.
 *
 *    *Example of no timeout warning, despite exceeding N:*
 *      - `timeout_res_count = 10`
 *      - First reply: 5 results (EOF)
 *      - Second reply: 4 results (EOF)
 *      - Third reply: 3 results (EOF)
 *      - Total results = 12, no timeout warning.
 *
 * **Recommendations:**
 * - In `FT.AGGREGATE` (cluster mode), do not expect an exact number of results unless
 *   you fully understand how the timeout mechanism works.
 * - If precise control over the result count is required, ensure that all shards contain at
 *   least `N` matching documents. This way, a timeout occurs after processing the first shard's
 *   response.
 * - When using `WITHCURSOR` be mindful to the last `FT.CURSOR READ` iterations. Some shards might
 *   run out of docs and return fewer than `N` results (EOF), causing the result content to be harder to predict.
 *
 * - **`INTERNAL_ONLY` Flag:**
 *   - Enforces timeout only at the shard level, without affecting the coordinator.
 *   - If a shard returns an empty result, the coordinator is not notified, which could cause it to
 *     hang indefinitely. To avoid this, if `N == 0`, a real timeout is enforced.
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
