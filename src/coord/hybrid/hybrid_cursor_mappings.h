/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "rmr/rmr.h"
#include "util/references.h"
#include "../../config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum  {
  TYPE_SEARCH,
  TYPE_VSIM,
} MappingType;

typedef struct {
  char * targetShard;
  uint16_t targetShardIdx;
  long long cursorId;
} CursorMapping;

typedef struct {
  MappingType type;
  arrayof(CursorMapping) mappings;
} CursorMappings;

// forward declaration of QueryError
typedef struct QueryError QueryError;

/**
 * Context for SHARD_K_RATIO optimization in FT.HYBRID commands.
 * Contains information needed to calculate and apply effectiveK.
 */
typedef struct {
  size_t originalK;         // Original K value from query
  double shardWindowRatio;  // Ratio for shard window optimization
  int kArgIndex;            // Index of the K value argument in the MRCommand
} HybridKnnContext;

/**
 * Command modifier callback for SHARD_K_RATIO optimization.
 * This callback is called from the IO thread (in iterStartCb) before commands are sent,
 * allowing effectiveK to be calculated based on the actual topology.
 *
 * @param cmd The command to modify
 * @param numShards The actual number of shards from the IO thread's topology
 * @param privateData Pointer to HybridKnnContext (if present)
 */
void HybridKnnCommandModifier(MRCommand *cmd, size_t numShards, void *privateData);

/**
 * Process hybrid cursor mappings synchronously
 * Populates the searchMappings and vsimMappings arrays with cursor mappings from all shards.
 * Handles shard errors by recording them in the status parameter while continuing to process all shards.
 * Returns true even if all shards fail with warnings (e.g., OOM), resulting in empty mapping arrays and allowing the caller to handle the warnings.
 *
 * Note: The number of expected shards is obtained from the IO thread's topology
 * snapshot via a privateDataInit callback. This ensures we wait for exactly as
 * many responses as commands were actually sent, avoiding race conditions with
 * topology changes.
 *
 * @param cmd The MRCommand to execute
 * @param searchMappings Empty array to populate with search cursor mappings
 * @param vsimMappings Empty array to populate with vector similarity cursor mappings
 * @param knnCtx KNN context for SHARD_K_RATIO optimization (NULL if not applicable)
 * @param status QueryError pointer to store warning/error information
 * @param oomPolicy OOM policy to determine error handling behavior
 * @return true if processing completed (even with warnings), false on fatal errors; status will contain error/warning information
 */
bool ProcessHybridCursorMappings(const MRCommand *cmd,
                                 StrongRef searchMappings,
                                 StrongRef vsimMappings,
                                 HybridKnnContext *knnCtx,
                                 QueryError *status,
                                 RSOomPolicy oomPolicy);

/**
 * Release resources associated with a cursor mapping
 */
void CursorMapping_Release(CursorMapping *mapping);

#ifdef __cplusplus
}
#endif
