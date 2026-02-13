/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "query_term.h"
#include "rmalloc.h"


RSQueryTerm *NewQueryTerm(RSToken *tok, int id) {
  RSQueryTerm *ret = rm_malloc(sizeof(RSQueryTerm));
  ret->idf = 1;
  ret->str = tok->str ? rm_strndup(tok->str, tok->len) : NULL;
  ret->len = tok->len;
  ret->flags = tok->flags;
  ret->id = id;
  return ret;
}

void Term_Free(RSQueryTerm *t) {
  if (t) {
    if (t->str) rm_free(t->str);
    rm_free(t);
  }
}

double QueryTerm_Idf(RSQueryTerm const *t) {
  return t->idf;
}

void QueryTerm_SetIdf(RSQueryTerm *const t, double idf) {
    t->idf = idf;
}

double QueryTerm_Bm25Idf(RSQueryTerm const *t) {
  return t->bm25_idf;
}

void QueryTerm_SetBm25Idf(RSQueryTerm *const t, double bm25_idf) {
    t->bm25_idf = bm25_idf;
}

int RSQueryTerm_GetId(const RSQueryTerm *term) {
  return term->id;
}
