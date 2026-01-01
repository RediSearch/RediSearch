/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
 * @brief Represents all the active queries.
 *
 * This structure is used to store information about an active query, including
 * the query itself, and a strong reference to the `IndexSpec`
 * associated with the query. Since we use the StrongRef, we know that we can
 * safely access the `IndexSpec` upon crashing.
 */
typedef struct {
  DLLIST_node llnode; // Node in the doubly-linked list
  StrongRef spec;     // IndexSpec strong ref
  time_t start;       // Time node was added into list
} BlockedQueryNode;

typedef struct {
  DLLIST_node llnode; // Node in the doubly-linked list
  StrongRef spec;     // IndexSpec strong ref
  uint64_t cursorId;  // cursor id
  size_t count;       // cursor count
  time_t start;       // Time node was added into list
} BlockedCursorNode;

/**
 * @brief Represents a list of active queries.
 *
 * This structure is used to store a list of active queries. It contains a
 * doubly-linked list of `ActiveQueryNode` and `ActiveCursorNode` objects
 * It is not thread safe and should be manipulated from a single thread
 */
typedef struct ActiveQueries {
  DLLIST queries;
  DLLIST cursors;
} BlockedQueries;

/**
 * @brief Initializes the active queries data structure.
 *
 * This function allocates memory for the `ActiveQueries` structure and
 * initializes the doubly-linked list for storing `ActiveQueries` objects.
 */
BlockedQueries* BlockedQueries_Init();

/**
 * @brief Frees the active queries data structure.
 *
 * This function destroys the doubly-linked lists and frees the active queries pointer
 */
void BlockedQueries_Free(BlockedQueries*);

BlockedQueryNode* BlockedQueries_AddQuery(BlockedQueries* list, StrongRef spec, QueryAST* ast);
BlockedCursorNode* BlockedQueries_AddCursor(BlockedQueries* list, WeakRef spec, uint64_t cursorId, size_t count);
void BlockedQueries_RemoveQuery(BlockedQueryNode* node);
void BlockedQueries_RemoveCursor(BlockedCursorNode* node);

#ifdef __cplusplus
}
#endif
