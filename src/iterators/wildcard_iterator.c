/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "wildcard_iterator.h"
#include "inverted_index_iterator.h"
#include "iterators_rs.h"
#include "search_disk.h"

bool IsWildcardIterator(const QueryIterator *it) {
  return (it && (it->type == WILDCARD_ITERATOR || it->type == INV_IDX_WILDCARD_ITERATOR));
}

QueryIterator *NewWildcardIterator_Optimized(const RedisSearchCtx *sctx, double weight) {
  RS_ASSERT(sctx->spec->rule->index_all);
  if (sctx->spec->existingDocs) {
    return NewInvIndIterator_WildcardQuery(sctx->spec->existingDocs, sctx, weight);
  } else {
    return NewEmptyIterator(); // Index all and no index, means the spec is currently empty.
  }
}

// Returns a new wildcard iterator.
// If the spec tracks all existing documents, it will return an iterator over those documents.
// Otherwise, it will return a non-optimized wildcard iterator
QueryIterator *NewWildcardIterator(const QueryEvalCtx *q, double weight) {
  if (q->sctx->spec->diskSpec) {
    return SearchDisk_NewWildcardIterator(q->sctx->spec->diskSpec, weight);
  }
  if (q->sctx->spec->rule && q->sctx->spec->rule->index_all == true) { // LLAPI spec may not have a rule
    return NewWildcardIterator_Optimized(q->sctx, weight);
  } else {
    // Non-optimized wildcard iterator, using a simple doc-id increment as its base.
    return NewWildcardIterator_NonOptimized(q->docTable->maxDocId, weight);
  }
}
