/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifdef __linux__
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "active_queries/active_queries.h"
#include "rmutil/rm_assert.h"
#include "redismodule.h"

ActiveQueries *activeQueries = NULL;

/**
 * @brief Initializes the active threads data structure.
 *
 * This function allocates memory for the `ActiveQueries` structure and
 * initializes the doubly-linked list for storing `ActiveThread` objects. It
 * also initializes the mutex used for thread-safe operations on the
 * `ActiveQueries` structure.
 */
ActiveQueries *ActiveQueries_Init() {
  ActiveQueries* activeQueries = rm_calloc(1, sizeof(ActiveQueries));
  dllist_init(&activeQueries->queries);
  dllist_init(&activeQueries->cursors);
  return activeQueries;
}

/**
 * @brief Frees the active threads data structure.
 *
 * This function releases the doubly-linked list and destroys the mutex used
 * for thread-safe operations on the `ActiveQueries` structure.
 */
void ActiveQueries_Free(ActiveQueries *activeQueries) {
  // Assert that the list is empty
  RS_LOG_ASSERT((activeQueries->queries.prev == activeQueries->queries.next) &&
                (activeQueries->queries.next == &activeQueries->queries),
                "Active queries list is not empty");
  RS_LOG_ASSERT((activeQueries->cursors.prev == activeQueries->cursors.next) &&
              (activeQueries->cursors.next == &activeQueries->cursors),
              "Active cursor list is not empty");
  rm_free(activeQueries);
}

ActiveQueryNode* ActiveQueries_AddQuery(ActiveQueries* activeQueries, StrongRef spec, QueryAST* ast) {
  ActiveQueryNode* activeQueryNode = rm_calloc(1, sizeof(ActiveQueryNode));
  activeQueryNode->spec = spec;
  activeQueryNode->ast = ast;
  activeQueryNode->start = time(NULL);
  dllist_prepend(&activeQueries->queries, &activeQueryNode->llnode);
  return activeQueryNode;
}

ActiveCursorNode* ActiveQueries_AddCursor(ActiveQueries* activeQueries, uint64_t cursorId, size_t count) {
  ActiveCursorNode* activeCursorNode = rm_calloc(1, sizeof(ActiveCursorNode));
  activeCursorNode->cursorId = cursorId;
  activeCursorNode->count = count;
  activeCursorNode->start = time(NULL);
  dllist_prepend(&activeQueries->cursors, &activeCursorNode->llnode);
  return activeCursorNode;
}

void ActiveQueries_RemoveQuery(ActiveQueryNode* activeQueryNode) {
  dllist_delete(&activeQueryNode->llnode);
}

void ActiveQueries_RemoveCursor(ActiveCursorNode* activeCursorNode) {
  dllist_delete(&activeCursorNode->llnode);
}