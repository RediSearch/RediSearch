/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "info/info_redis/types/blocked_queries.h"

#include <inttypes.h>

#include "info/info_redis/block_client.h"
#include "obfuscation/obfuscation_api.h"
#include "query_request.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"

BlockedQueries *BlockedQueries_Init() {
  BlockedQueries* blockedQueries = rm_calloc(1, sizeof(BlockedQueries));
  dllist_init(&blockedQueries->queries);
  dllist_init(&blockedQueries->cursors);
  return blockedQueries;
}

static size_t PrintActiveQueries(BlockedQueries *blockedQueries) {
  size_t count = 0;
  DLLIST_FOREACH(node, &blockedQueries->queries) {
    QueryRequest *at = DLLIST_ITEM(node, QueryRequest, registryInfo.node);
    ++count;
    char buffer[MAX_OBFUSCATED_INDEX_NAME];
    RedisModule_Log(NULL, "warning", "Active query on index %s, started at %ld",
                    QueryRequest_ReportIndexName(at, buffer), at->registryInfo.cycle_start);
  }
  return count;
}

static size_t PrintActiveCursors(BlockedQueries *blockedQueries) {
  size_t count = 0;
  DLLIST_FOREACH(node, &blockedQueries->cursors) {
    QueryRequest *at = DLLIST_ITEM(node, QueryRequest, registryInfo.node);
    ++count;
    char buffer[MAX_OBFUSCATED_INDEX_NAME];
    RedisModule_Log(NULL, "warning", "Active cursor %" PRIu64 ", on index %s, started at %ld",
                    at->cursorInfo.id, QueryRequest_ReportIndexName(at, buffer), at->registryInfo.cycle_start);
  }
  return count;
}

void BlockedQueries_Free(BlockedQueries *blockedQueries) {
  const size_t numQueries = PrintActiveQueries(blockedQueries);
  const size_t numCursors = PrintActiveCursors(blockedQueries);
  RS_LOG_ASSERT_FMT(numQueries == 0 && numCursors == 0,
    "There are %zu active queries and %zu active cursors. This is a bug. Please report it to https://github.com/RediSearch/RediSearch/issues",
    numQueries, numCursors);
  rm_free(blockedQueries);
}
