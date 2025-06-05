/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "wildcard_iterator.h"

/* Free a wildcard iterator */
static void WI_Free(QueryIterator *base) {
  IndexResult_Free(base->current);
  rm_free(base);
}

static size_t WI_NumEstimated(QueryIterator *base) {
  WildcardIterator *wi = (WildcardIterator *)base;
  return wi->numDocs;
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

/* Create a new wildcard iterator */
QueryIterator *IT_V2(NewWildcardIterator_NonOptimized)(t_docId maxId, size_t numDocs) {
  WildcardIterator *wi = rm_calloc(1, sizeof(*wi));
  wi->currentId = 0;
  wi->topId = maxId;
  wi->numDocs = numDocs;
  QueryIterator *ret = &wi->base;
  ret->current = NewVirtualResult(1, RS_FIELDMASK_ALL);
  ret->current->freq = 1;
  ret->atEOF = false;
  ret->lastDocId = 0;
  ret->type = WILDCARD_ITERATOR;
  ret->Rewind = WI_Rewind;
  ret->Free = WI_Free;
  ret->Read = WI_Read;
  ret->SkipTo = WI_SkipTo;
  ret->NumEstimated = WI_NumEstimated;
  return ret;
}
