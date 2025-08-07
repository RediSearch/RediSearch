/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "merge_utils.h"
#include "query_error.h"
#include <string.h>

/**
 * Merge flags from source into target (in-place).
 * Modifies target by ORing it with source flags.
 */
void MergeFlags(uint8_t *target_flags, const uint8_t *source_flags) {
  if (!target_flags || !source_flags) {
    return;
  }

  *target_flags |= *source_flags;
}

/**
 * Union RLookup rows - copy fields from source row to target row.
 * No conflict resolution is performed, assuming no conflicts (all keys have same values).
 */
void UnionRLookupRows(RLookupRow *target_row, const RLookupRow *source_row, const RLookup *lookup) {
  if (!target_row || !source_row || !lookup) {
    return;
  }

  // Union all fields from source row into target row
  for (const RLookupKey *key = lookup->head; key; key = key->next) {
    if (!key->name) continue;  // Skip overridden keys

    RSValue *sourceValue = RLookup_GetItem(key, source_row);
    if (!sourceValue) continue;  // Skip if source doesn't have this field

    RSValue *existingValue = RLookup_GetItem(key, target_row);
    if (!existingValue) {
      // Field doesn't exist in target - add it
      RLookup_WriteKey(key, target_row, sourceValue);
    } else {
      // Field exists - assert that values are the same (our assumption)
      // This validates that "first upstream wins" == "no conflict resolution needed"
      QueryError err;
      QueryError_Init(&err);
      int equal = RSValue_Equal(existingValue, sourceValue, &err);
      QueryError_ClearError(&err);  // Clean up any error details
      RS_ASSERT(equal == 1);
    }
  }
}
