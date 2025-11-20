/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "aggregate.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Count total results for a cursor query using the LIMIT 0 0 approach.
 * 
 * This function creates a separate counting pipeline that rebuilds the query
 * from the AST and executes it with RPCounter to get an accurate count of
 * all results without storing them in memory.
 * 
 * This is used for FT.AGGREGATE + WITHCOUNT + WITHCURSOR queries where we
 * need to return an accurate total_results count on the first cursor response,
 * but cannot use the depleter approach (which would consume all results and
 * defeat the purpose of cursors).
 * 
 * @param req The original AREQ (will NOT be modified)
 * @return The total count of results, or 0 on error
 */
uint32_t countTotalResults(AREQ *req);

#ifdef __cplusplus
}
#endif

