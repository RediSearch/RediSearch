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

bool SearchDisk_CheckLimitNumberOfIndexes(size_t nIndexes) {
  if (!SearchDisk_IsEnabledForValidation()) {
    return true;
  }
  return nIndexes <= FLEX_MAX_INDEX_COUNT;
}

bool SearchDisk_MarkUnsupportedField(const char *fieldTypeStr, const FieldSpec *fs, QueryError *status) {
  if (SearchDisk_IsEnabledForValidation()) {
    QueryError_SetWithoutUserDataFmt(status, QUERY_ERROR_CODE_FLEX_UNSUPPORTED_FIELD, "%s fields are not supported in Flex indexes", fieldTypeStr);
    return false;
  }
  return true;
}
