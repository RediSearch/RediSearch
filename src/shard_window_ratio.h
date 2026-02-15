/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include <stddef.h>
#include <math.h>
#include <stdbool.h>
#include <string.h>
#include "redismodule.h"
#include "coord/rmr/command.h"
#include "coord/special_case_ctx.h"
#include "config.h"
#include "vector_index.h"
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Validate a SHARD_K_RATIO value string.
 *
 * Parses the string as a double and validates it's within the valid range:
 * (MIN_SHARD_WINDOW_RATIO, MAX_SHARD_WINDOW_RATIO] (exclusive min, inclusive max)
 *
 * @param value The string value to parse and validate
 * @param ratio Output parameter for the parsed ratio value
 * @param status QueryError to populate on failure
 * @return true on success, false on failure (with status populated)
 */
bool ValidateShardKRatio(const char *value, double *ratio, QueryError *status);

/**
 * Calculate effective K value for shard window ratio optimization.
 *
 * Implements the PRD formula: k_per_shard = max(top_k/#shards, ceil(top_k × ratio))
 * This ensures:
 * - Minimum guarantee: Each shard returns at least top_k/#shards results
 * - Optimization: If ceil(top_k × ratio) > top_k/#shards, use the larger value
 *
 * @param originalK The original K value requested
 * @param ratio The shard window ratio (any value, function handles validation)
 * @param numShards The number of shards in the cluster
 * @return Effective K value per shard
 */
static inline size_t calculateEffectiveK(size_t originalK, double ratio, size_t numShards) {
  // No optimization if ratio is invalid or > 1.0, or if numShards is 0
  RS_LOG_ASSERT_FMT(ratio >= MIN_SHARD_WINDOW_RATIO && ratio <= MAX_SHARD_WINDOW_RATIO, "Invalid shard window ratio: %f", ratio);

  // We should not get here if numShards == 1
  RS_LOG_ASSERT(numShards > 1, "Should not calculate effective K for single shard");

  if (ratio == MAX_SHARD_WINDOW_RATIO) {
    return originalK;
  }

  // Calculate minimum K per shard to ensure we can return full originalK results
  // Use ceiling division: (originalK + numShards - 1) / numShards
  size_t minKPerShard = (originalK + numShards - 1) / numShards;

  // Calculate ratio-based K per shard
  double exactRatioK = (double)originalK * ratio;
  size_t ratioKPerShard = (size_t)ceil(exactRatioK);

  // Apply PRD formula: max(top_k/#shards, ceil(top_k × ratio))
  size_t effectiveK = (ratioKPerShard > minKPerShard) ? ratioKPerShard : minKPerShard;

  return effectiveK;
}

/**
 * Modify KNN command for shard distribution by replacing K value.
 *
 * This function handles two cases:
 * 1. Literal K (e.g., "KNN 50") - uses saved position for exact replacement
 * 2. Parameter K (e.g., "KNN $k") - replaces parameter reference in query string
 *
 * @param cmd The MRCommand to modify
 * @param query_arg_index Index of the query string argument in cmd
 * @param effectiveK The calculated effective K value for shards
 * @param vq The VectorQuery containing K position information

 */
void modifyKNNCommand(MRCommand *cmd, size_t query_arg_index, size_t effectiveK, VectorQuery *vq);

/**
 * Modify VSIM KNN K value in a built command.
 *
 * This function replaces the K value argument at the specified index with
 * the calculated effective K value for shard distribution.
 *
 * @param cmd The MRCommand to modify
 * @param kArgIndex Index of the K value argument in cmd (as returned by MRCommand_appendVsim)
 * @param effectiveK The calculated effective K value for shards
 * @param originalK The original K value from the VectorQuery
 */
void modifyVsimKNN(MRCommand *cmd, int kArgIndex, size_t effectiveK, size_t originalK);

#ifdef __cplusplus
}
#endif
