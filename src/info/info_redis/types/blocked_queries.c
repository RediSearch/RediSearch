/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "info/info_redis/types/blocked_queries.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"
#include "rmutil/rm_assert.h"

BlockedQueries *BlockedQueries_Init() {
  BlockedQueries* blockedQueries = rm_calloc(1, sizeof(BlockedQueries));
  dllist_init(&blockedQueries->queries);
  dllist_init(&blockedQueries->cursors);
  return blockedQueries;
}

static size_t PrintActiveQueries(BlockedQueries *blockedQueries) {
  size_t count = 0;
  DLLIST_FOREACH(node, &blockedQueries->queries) {
    BlockedQueryNode *at = DLLIST_ITEM(node, BlockedQueryNode, llnode);
    IndexSpec *sp = StrongRef_Get(at->spec);
    ++count; // increment regardless if sp is valid, the fact we have a valid node is problematic
    const char *indexName = sp ? IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog) : "n/a";
    const char *query = at->query && !RSGlobalConfig.hideUserDataFromLog ? at->query : "n/a";
    RedisModule_Log(NULL, "warning", "Active query on index %s, query: %s, started at %ld", indexName, query, at->start);
  }
  return count;
}

static size_t PrintActiveCursors(BlockedQueries *blockedQueries) {
  size_t count = 0;
  DLLIST_FOREACH(node, &blockedQueries->cursors) {
    BlockedCursorNode *at = DLLIST_ITEM(node, BlockedCursorNode, llnode);
    IndexSpec *sp = StrongRef_Get(at->spec);
    ++count; // increment regardless if sp is valid, the fact we have a valid node is problematic
    const char *indexName = sp ? IndexSpec_FormatName(sp, RSGlobalConfig.hideUserDataFromLog) : "n/a";
    const char *query = at->query && !RSGlobalConfig.hideUserDataFromLog ? at->query : "n/a";
    RedisModule_Log(NULL, "warning", "Active cursor %zu, on index %s, query: %s, started at %ld", at->cursorId, indexName, query, at->start);
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

BlockedQueryNode* BlockedQueries_AddQuery(BlockedQueries* blockedQueries, StrongRef spec, QueryAST* ast) {
  BlockedQueryNode* blockedQueryNode = rm_calloc(1, sizeof(BlockedQueryNode));
  blockedQueryNode->spec = StrongRef_Clone(spec);
  blockedQueryNode->start = time(NULL);
  blockedQueryNode->query = QAST_DumpExplain(ast, StrongRef_Get(spec));
  dllist_prepend(&blockedQueries->queries, &blockedQueryNode->llnode);
  return blockedQueryNode;
}

BlockedCursorNode* BlockedQueries_AddCursor(BlockedQueries* blockedQueries, WeakRef spec, uint64_t cursorId, QueryAST* ast, size_t count) {
  BlockedCursorNode* blockedCursorNode = rm_calloc(1, sizeof(BlockedCursorNode));
  if (spec.rm) {
    // we don't want cursors to block index deletion, so we don't take a strong ref
    // not entirely sure we clean cursors on index drop, so better be safe than sorry
    blockedCursorNode->spec = WeakRef_Promote(spec);
    IndexSpec *sp = StrongRef_Get(blockedCursorNode->spec);
    if (sp) {
      blockedCursorNode->query = QAST_DumpExplain(ast, sp);
    }
  }
  blockedCursorNode->cursorId = cursorId;
  blockedCursorNode->count = count;
  blockedCursorNode->start = time(NULL);
  dllist_prepend(&blockedQueries->cursors, &blockedCursorNode->llnode);
  return blockedCursorNode;
}

void BlockedQueries_RemoveQuery(BlockedQueryNode* blockedQueryNode) {
  // Main thread manages the lifetime of specs, so spec must be valid
  StrongRef_Release(blockedQueryNode->spec);
  dllist_delete(&blockedQueryNode->llnode);
}

void BlockedQueries_RemoveCursor(BlockedCursorNode* blockedCursorNode) {
  dllist_delete(&blockedCursorNode->llnode);
}
