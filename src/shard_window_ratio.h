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
#include "special_case_ctx.h"

#ifdef __cplusplus
extern "C" {
#endif

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
  if (ratio <= 0.0 || ratio > 1.0 || numShards == 0) {
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

  return effectiveK > 0 ? effectiveK : 1;  // Ensure at least 1 result
}

/**
 * Modify KNN command for shard distribution by replacing K value.
 *
 * This function handles two cases:
 * 1. Literal K (e.g., "KNN 50") - uses saved position for exact replacement
 * 2. Parameter K (e.g., "KNN $k") - modifies parameter value in PARAMS section
 *
 * @param cmd The MRCommand to modify
 * @param originalK The original K value from the coordinator
 * @param effectiveK The calculated effective K value for shards
 * @param knnCtx Contains the QueryNode used to find K position and parameter name for modification
 * @return 0 on success, -1 on error
 */
int modifyKNNCommand(MRCommand *cmd, size_t originalK, size_t effectiveK, knnContext *knnCtx);

#ifdef __cplusplus
}
#endif
