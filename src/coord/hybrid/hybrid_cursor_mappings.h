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
 * @param cmd The MRCommand to execute
 * @param numShards Expected number of shards (determines expected callbacks)
 * @param searchMappings Empty array to populate with search cursor mappings
 * @param vsimMappings Empty array to populate with vector similarity cursor mappings
 * @param status QueryError pointer to store error information on failure
 * @return true on success, false otherwise, on failure status will contain error information
 */
bool ProcessHybridCursorMappings(const MRCommand *cmd,int numShards, StrongRef searchMappings, StrongRef vsimMappings, QueryError *status);

#ifdef __cplusplus
}
#endif
