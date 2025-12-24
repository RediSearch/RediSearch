/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "search_disk_utils.h"
#include "search_disk.h"

/**
 * @brief Check if the number of indexes is within the limit
 *
 * @return true if the number of indexes is within the limit, false otherwise
 */
bool SearchDisk_CheckLimitNumberOfIndexes(size_t nIndexes) {
  if (!SearchDisk_IsEnabled(NULL)) {
    return true;
  }
  return nIndexes < FLEX_MAX_INDEX_COUNT;
}
