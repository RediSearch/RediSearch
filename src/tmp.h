#pragma once

#include "redismodule.h"
#include "sortable_rs.h"

/* Load a sorting vector from RDB. Used by legacy RDB load only */
RSSortingVector *SortingVector_RdbLoad(RedisModuleIO *rdb);