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
#include "../../query_error.h"
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
  MappingType type;
  arrayof(CursorMapping) mappings;
} CursorMappings;

/**
 * Process hybrid cursor mappings synchronously
 * Populates the searchMappings and vsimMappings arrays with cursor mappings from all shards.
 * Handles shard errors by recording them in the status parameter while continuing to process all shards.
 * Returns true even if all shards fail with warnings (e.g., OOM), resulting in empty mapping arrays and allowing the caller to handle the warnings.
 * @param cmd The MRCommand to execute
 * @param numShards Expected number of shards (determines expected callbacks)
 * @param searchMappings Empty array to populate with search cursor mappings
 * @param vsimMappings Empty array to populate with vector similarity cursor mappings
 * @param status QueryError pointer to store warning/error information
 * @param oomPolicy OOM policy to determine error handling behavior
 * @return true if processing completed (even with warnings), false on fatal errors; status will contain error/warning information
 */
bool ProcessHybridCursorMappings(const MRCommand *cmd,int numShards, StrongRef searchMappings, StrongRef vsimMappings, QueryError *status, RSOomPolicy oomPolicy);

#ifdef __cplusplus
}
#endif
