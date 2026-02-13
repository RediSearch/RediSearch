/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "redisearch.h"

#ifdef __cplusplus
extern "C" {
#endif

RSQueryTerm *NewQueryTerm(RSToken *tok, int id);
void Term_Free(RSQueryTerm *t);

/* Get the idf of the term. This is used for scoring and ranking results. */
double QueryTerm_Idf(RSQueryTerm const *t);

/* Set the idf of the term. */
void QueryTerm_SetIdf(RSQueryTerm *t, double idf);

/* Get the BM25 idf of the term. This is used for BM25 scoring. */
double QueryTerm_Bm25Idf(RSQueryTerm const *t);

/* Set the BM25 idf of the term. */
void QueryTerm_SetBm25Idf(RSQueryTerm *t, double bm25_idf);

/* Get the id of the term. */
int RSQueryTerm_GetId(const RSQueryTerm *term);

#ifdef __cplusplus
}
#endif
