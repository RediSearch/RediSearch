/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "util/dllist.h"

#ifdef __cplusplus
extern "C" {
#endif

struct RequestSyncCtx;

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

void BlockedQueries_LinkQuery(BlockedQueries *list, struct RequestSyncCtx *rsc);
void BlockedQueries_LinkCursor(BlockedQueries *list, struct RequestSyncCtx *rsc);
void BlockedQueries_Unlink(struct RequestSyncCtx *rsc);

#ifdef __cplusplus
}
#endif
