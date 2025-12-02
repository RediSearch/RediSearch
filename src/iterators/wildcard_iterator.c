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
#include "empty_iterator.h"
#include "search_disk.h"

/* Free a wildcard iterator */
static void WI_Free(QueryIterator *base) {
  IndexResult_Free(base->current);
  rm_free(base);
}

static size_t WI_NumEstimated(QueryIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;
  return wi->topId;
}

/* Read reads the next consecutive id, unless we're at the end */
static IteratorStatus WI_Read(QueryIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;
  if (wi->currentId >= wi->topId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }
  base->lastDocId = base->current->docId = ++wi->currentId;
  return ITERATOR_OK;
}

/* Skipto for wildcard iterator - always succeeds, but this should normally not happen as it has
 * no meaning */
static IteratorStatus WI_SkipTo(QueryIterator *base, t_docId docId) {
  WildcardIterator *wi = (WildcardIterator *)base;

  if (wi->currentId > wi->topId || docId > wi->topId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  wi->currentId = docId;
  base->lastDocId = base->current->docId = docId;
  return ITERATOR_OK;
}

static void WI_Rewind(QueryIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;
  wi->currentId = 0;
  base->atEOF = false;
  base->lastDocId = 0;
}

bool IsWildcardIterator(QueryIterator *it) {
  if (it && it->type == WILDCARD_ITERATOR) {
    return true;
  }
  if (it && it->type == INV_IDX_ITERATOR) {
    InvIndIterator *invIdxIt = (InvIndIterator *)it;
    return invIdxIt->isWildcard;
  }
  return false;
}

/* Create a new wildcard iterator */
QueryIterator *NewWildcardIterator_NonOptimized(t_docId maxId, double weight) {
  WildcardIterator *wi = rm_calloc(1, sizeof(*wi));
  wi->currentId = 0;
  wi->topId = maxId;
  QueryIterator *ret = &wi->base;
  ret->current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  ret->current->freq = 1;
  ret->atEOF = false;
  ret->lastDocId = 0;
  ret->type = WILDCARD_ITERATOR;
  ret->Rewind = WI_Rewind;
  ret->Free = WI_Free;
  ret->Read = WI_Read;
  ret->SkipTo = WI_SkipTo;
  ret->NumEstimated = WI_NumEstimated;
  ret->Revalidate = Default_Revalidate;
  return ret;
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
