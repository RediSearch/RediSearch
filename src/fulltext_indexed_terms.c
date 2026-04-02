/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "fulltext_indexed_terms.h"
#include "redisearch.h"
#include "forward_index.h"
#include "inverted_index.h"
#include "phonetic_manager.h"
#include "redis_index.h"
#include "search_ctx.h"
#include "spec.h"
#include "stemmer.h"
#include "synonym_map.h"
#include "rmalloc.h"

#include <string.h>

struct RSFulltextIndexedTerms {
  size_t buflen;
  char *buf;
};

static int skip_indexed_term(const ForwardIndexEntry *entry) {
  if (entry->len == 0 || entry->term == NULL) {
    return 1;
  }
  char c = entry->term[0];
  if (c == STEM_PREFIX || c == PHONETIC_PREFIX || c == SYNONYM_PREFIX_CHAR) {
    return 1;
  }
  return 0;
}

static size_t packed_size(ForwardIndex *fw) {
  ForwardIndexIterator it = ForwardIndex_Iterate(fw);
  size_t n = 0;
  size_t bytes = sizeof(uint32_t);
  for (ForwardIndexEntry *e = ForwardIndexIterator_Next(&it); e != NULL;
       e = ForwardIndexIterator_Next(&it)) {
    if (skip_indexed_term(e)) {
      continue;
    }
    n++;
    bytes += sizeof(uint32_t) + e->len;
  }
  if (n == 0) {
    return 0;
  }
  return bytes;
}

void RSFulltextIndexedTerms_Free(RSFulltextIndexedTerms *terms) {
  if (terms == NULL) {
    return;
  }
  rm_free(terms->buf);
  rm_free(terms);
}

RSFulltextIndexedTerms *RSFulltextIndexedTerms_CreateFromForwardIndex(ForwardIndex *fw) {
  if (fw == NULL) {
    return NULL;
  }
  size_t sz = packed_size(fw);
  if (sz == 0) {
    return NULL;
  }
  RSFulltextIndexedTerms *t = rm_calloc(1, sizeof(*t));
  t->buflen = sz;
  t->buf = rm_malloc(sz);
  char *w = t->buf;
  ForwardIndexIterator it = ForwardIndex_Iterate(fw);
  uint32_t count = 0;
  size_t count_offs = 0;
  memcpy(w, &count, sizeof(count));
  w += sizeof(count);

  for (ForwardIndexEntry *e = ForwardIndexIterator_Next(&it); e != NULL;
       e = ForwardIndexIterator_Next(&it)) {
    if (skip_indexed_term(e)) {
      continue;
    }
    count++;
    uint32_t len = (uint32_t)e->len;
    memcpy(w, &len, sizeof(len));
    w += sizeof(len);
    memcpy(w, e->term, e->len);
    w += e->len;
  }
  if (count == 0) {
    RSFulltextIndexedTerms_Free(t);
    return NULL;
  }
  memcpy(t->buf + count_offs, &count, sizeof(count));
  return t;
}

void RSFulltextIndexedTerms_DecrementLive(RedisSearchCtx *sctx, RSFulltextIndexedTerms *terms) {
  if (terms == NULL || sctx == NULL || sctx->spec == NULL) {
    return;
  }
  IndexSpec *spec = sctx->spec;
  if (spec->diskSpec || spec->keysDict == NULL) {
    return;
  }
  const char *p = terms->buf;
  const char *end = p + terms->buflen;
  if ((size_t)(end - p) < sizeof(uint32_t)) {
    return;
  }
  uint32_t n = 0;
  memcpy(&n, p, sizeof(n));
  p += sizeof(n);
  for (uint32_t i = 0; i < n && p < end; i++) {
    if ((size_t)(end - p) < sizeof(uint32_t)) {
      break;
    }
    uint32_t len = 0;
    memcpy(&len, p, sizeof(len));
    p += sizeof(len);
    if (len == 0 || (size_t)(end - p) < len) {
      break;
    }
    InvertedIndex *iv = Redis_OpenInvertedIndex(sctx, p, len, 0, NULL);
    if (iv) {
      InvertedIndex_DecrementLiveUniqueDocs(iv, 1);
    }
    p += len;
  }
}

void DMD_SetFulltextIndexedTerms(RSDocumentMetadata *dmd, RSFulltextIndexedTerms *terms) {
  if (dmd == NULL) {
    RSFulltextIndexedTerms_Free(terms);
    return;
  }
  if (dmd->fulltextIndexedTerms) {
    RSFulltextIndexedTerms_Free(dmd->fulltextIndexedTerms);
  }
  dmd->fulltextIndexedTerms = terms;
}
