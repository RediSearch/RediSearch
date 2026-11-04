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

/**
 * @brief The registry of in-flight blocked-client cycles (crash reports).
 *
 * Two intrusive lists of QueryRequests, linked through their `registryInfo`
 * by BeginCycle and unlinked by EndCycle. The registry owns nothing: the
 * walkers read everything they report through the linked request itself.
 * It is not thread safe and must be manipulated from the main thread only.
 */
typedef struct ActiveQueries {
  DLLIST queries;
  DLLIST cursors;
} BlockedQueries;

/**
 * @brief Initializes the blocked queries data structure.
 */
BlockedQueries* BlockedQueries_Init();

/**
 * @brief Frees the blocked queries data structure.
 *
 * Logs any request still registered and asserts emptiness — a linked request
 * at teardown is a bug.
 */
void BlockedQueries_Free(BlockedQueries*);

#ifdef __cplusplus
}
#endif
