/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef __HYBRID_MERGE_UTILS_H__
#define __HYBRID_MERGE_UTILS_H__

#include "result_processor.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Merge flags from source flags into target flags (in-place).
 * Modifies target_flags by ORing it with source_flags.
 */
void MergeFlags(uint8_t *target_flags, const uint8_t *source_flags);

/**
 * Simple RLookup union - copy fields from source row to target row.
 * No conflict resolution is performed, assuming no conflicts (all keys have same values)
 */
void UnionRLookupRows(RLookupRow *target_row, const RLookupRow *source_row, const RLookup *lookup);

#ifdef __cplusplus
}
#endif

#endif // __HYBRID_MERGE_UTILS_H__