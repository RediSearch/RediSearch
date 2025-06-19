#pragma once

#include "redismodule.h"
#include "sortable_rs.h"

/* Load a sorting vector from RDB. Used by legacy RDB load only */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb);

/* Normalize sorting string for storage. This folds everything to unicode equivalent strings. The
 * allocated return string needs to be freed later */
char *normalizeStr(const char *str);