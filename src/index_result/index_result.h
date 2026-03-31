/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __INDEX_RESULT_H__
#define __INDEX_RESULT_H__

#include "varint.h"
#include "rmalloc.h"
#define DEFAULT_RECORDLIST_SIZE 4

#ifdef __cplusplus
extern "C" {
#endif

/* Get the minimal delta between the terms in the result */
int IndexResult_MinOffsetDelta(const RSIndexResult *r);

/* Fill an array of max capacity cap with all the matching text terms for the result. The number of
 * matching terms is returned */
size_t IndexResult_GetMatchedTerms(const RSIndexResult *r, RSQueryTerm **arr, size_t cap);

/* Return 1 if the the result is within a given slop range, inOrder determines whether the tokens
 * need to be ordered as in the query or not */
int IndexResult_IsWithinRange(RSIndexResult *r, int maxSlop, int inOrder);

#ifdef __cplusplus
}
#endif
#endif
