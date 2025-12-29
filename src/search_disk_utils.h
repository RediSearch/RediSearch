/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include "field_spec.h"
#include "query_error.h"

#define FLEX_MAX_INDEX_COUNT 10

/**
 * @brief Check if the number of indexes is within the limit
 *
 * @return true if the number of indexes is within the limit, false otherwise
 */
bool SearchDisk_CheckLimitNumberOfIndexes(size_t nIndexes);

/**
 * @brief Mark a field as unsupported in Flex indexes
 *
 * @param fieldTypeStr Field type string
 * @param fs Field specification
 * @param status Query error status
 * @return true if the field type is supported, false otherwise
 */
bool SearchDisk_MarkUnsupportedFieldIfDiskEnabled(const char *fieldTypeStr, const FieldSpec *fs, QueryError *status);
