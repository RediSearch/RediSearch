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
 * @return RS_RESULT_OK on success, error code otherwise
 */
int ProcessHybridCursorMappings(const MRCommand *cmd,int numShards, StrongRef searchMappings, StrongRef vsimMappings);

#ifdef __cplusplus
}
#endif
