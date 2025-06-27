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

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Check if a command is a shard-level command (_FT.*) vs coordinator command (FT.*).
 *
 * @param argv Command arguments array
 * @param argc Number of arguments
 * @return true if this is a shard-level command, false otherwise
 */
static inline bool isShardLevelCommand(RedisModuleString **argv, int argc) {
  if (argc == 0 || argv == NULL) {
    return false;
  }

  size_t cmdLen;
  const char *cmdName = RedisModule_StringPtrLen(argv[0], &cmdLen);
  return (cmdLen > 3 && cmdName[0] == '_' && strncasecmp(cmdName + 1, "FT.", 3) == 0);
}

/**
 * Calculate effective K value for shard window ratio optimization.
 *
 * This function handles all edge cases and validation internally:
 * - If ratio is <= 0.0 or >= 1.0, returns originalK (no optimization)
 * - If ratio is valid (0.0 < ratio < 1.0), applies optimization
 * - Ensures result is at least 1
 *
 * @param originalK The original K value requested
 * @param ratio The shard window ratio (any value, function handles validation)
 * @return Effective K value (originalK if no optimization, calculated value otherwise)
 */
static inline size_t calculateEffectiveK(size_t originalK, double ratio) {
  // No optimization if ratio is invalid or >= 1.0
  if (ratio <= 0.0 || ratio >= 1.0) {
    return originalK;
  }

  // Apply optimization for valid ratios (0.0 < ratio < 1.0)
  // Use floating point math to ensure proper rounding
  double exactK = (double)originalK * ratio;
  size_t effectiveK = (size_t)ceil(exactK);

  return effectiveK > 0 ? effectiveK : 1;  // Ensure at least 1 result
}

/**
 * Get the appropriate K value based on context (shard vs coordinator).
 *
 * @param originalK The original K value requested
 * @param ratio The shard window ratio
 * @param isShardCommand Whether this is a shard-level command
 * @return Effective K for shards, original K for coordinator
 */
static inline size_t getContextualK(size_t originalK, double ratio, bool isShardCommand) {
  return isShardCommand ? calculateEffectiveK(originalK, ratio) : originalK;
}

#ifdef __cplusplus
}
#endif
