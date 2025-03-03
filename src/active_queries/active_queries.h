/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include <pthread.h>

#include "util/dllist.h"
#include "util/references.h"
#include "query.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Represents all the active qureis.
 *
 * This structure is used to store information about an active query, including
 * the query itself, and a strong reference to the `IndexSpec`
 * associated with the query. Since we use the StrongRef, we know that we can
 * safely access the `IndexSpec` upon crashing.
 */
typedef struct {
  DLLIST_node llnode; // Node in the doubly-linked list
  StrongRef spec;     // Thread information
  QueryAST *ast;      // The query AST
  time_t start;       // Time node was added into list
} ActiveQueryNode;

typedef struct {
  DLLIST_node llnode; // Node in the doubly-linked list
  uint64_t cursorId;  // cursor id
  size_t count;       // cursor count
  time_t start;       // Time node was added into list
} ActiveCursorNode;

/**
 * @brief Represents a list of active queries.
 *
 * This structure is used to store a list of active queries. It contains a
 * doubly-linked list of `ActiveQueryNode` objects
 * It is not threads safe and should be manipulated from a single thread
 */
typedef struct ActiveQueries {
  DLLIST queries;
  DLLIST cursors;
} ActiveQueries;

/**
 * @brief Initializes the active queries data structure.
 *
 * This function allocates memory for the `activeQueries` structure and
 * initializes the doubly-linked list for storing `ActiveQueries` objects.
 */
ActiveQueries* ActiveQueries_Init();

/**
 * @brief Frees the active queries data structure.
 *
 * This function releases the doubly-linked list
 */
void ActiveQueries_Free(ActiveQueries*);

ActiveQueryNode* ActiveQueries_AddQuery(ActiveQueries* list, StrongRef spec, QueryAST* ast);
ActiveCursorNode* ActiveQueries_AddCursor(ActiveQueries* list, uint64_t cursorId, size_t count);
void ActiveQueries_RemoveQuery(ActiveQueryNode* node);
void ActiveQueries_RemoveCursor(ActiveCursorNode* node);

#ifdef __cplusplus
}
#endif
