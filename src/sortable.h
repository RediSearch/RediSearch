/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "value.h"
#include "sorting_vector_rs.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Load a sorting vector from RDB. Used by legacy RDB load only */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb);

/* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str);

#ifdef __cplusplus
}
#endif
