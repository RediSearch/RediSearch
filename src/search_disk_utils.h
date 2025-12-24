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

#define FLEX_MAX_INDEX_COUNT 10

/**
 * @brief Check if the number of indexes is within the limit
 *
 * @return true if the number of indexes is within the limit, false otherwise
 */
bool SearchDisk_CheckLimitNumberOfIndexes(size_t nIndexes);
