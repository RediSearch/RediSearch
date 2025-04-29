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

BlockedQueries *BlockedQueries_Init() {
  BlockedQueries* blockedQueries = rm_calloc(1, sizeof(BlockedQueries));
  dllist_init(&blockedQueries->queries);
  dllist_init(&blockedQueries->cursors);
  return blockedQueries;
}

void BlockedQueries_Free(BlockedQueries *blockedQueries) {
  // Assert that the lists are empty
  RS_LOG_ASSERT((blockedQueries->queries.prev == blockedQueries->queries.next) &&
                (blockedQueries->queries.next == &blockedQueries->queries),
                "Active queries list is not empty");
  RS_LOG_ASSERT((blockedQueries->cursors.prev == blockedQueries->cursors.next) &&
              (blockedQueries->cursors.next == &blockedQueries->cursors),
              "Active cursor list is not empty");
  rm_free(blockedQueries);
}

BlockedQueryNode* BlockedQueries_AddQuery(BlockedQueries* blockedQueries, StrongRef spec, QueryAST* ast) {
  BlockedQueryNode* blockedQueryNode = rm_calloc(1, sizeof(BlockedQueryNode));
  blockedQueryNode->spec = StrongRef_Clone(spec);
  blockedQueryNode->start = time(NULL);
  dllist_prepend(&blockedQueries->queries, &blockedQueryNode->llnode);
  return blockedQueryNode;
}

BlockedCursorNode* BlockedQueries_AddCursor(BlockedQueries* blockedQueries, WeakRef spec, uint64_t cursorId, size_t count) {
  BlockedCursorNode* blockedCursorNode = rm_calloc(1, sizeof(BlockedCursorNode));
  if (spec.rm) {
    blockedCursorNode->spec = WeakRef_Promote(spec);
  }
  blockedCursorNode->cursorId = cursorId;
  blockedCursorNode->count = count;
  blockedCursorNode->start = time(NULL);
  dllist_prepend(&blockedQueries->cursors, &blockedCursorNode->llnode);
  return blockedCursorNode;
}

void BlockedQueries_RemoveQuery(BlockedQueryNode* blockedQueryNode) {
  StrongRef_Release(blockedQueryNode->spec);
  dllist_delete(&blockedQueryNode->llnode);
}

void BlockedQueries_RemoveCursor(BlockedCursorNode* blockedCursorNode) {
  dllist_delete(&blockedCursorNode->llnode);
}
