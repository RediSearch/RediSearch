/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "not_iterator.h"
#include "wildcard_iterator.h"
#include "empty_iterator.h"

static void NI_Rewind(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  base->current->docId = 0;
  base->atEOF = false;
  base->lastDocId = 0;
  if (ni->wcii) {
    ni->wcii->Rewind(ni->wcii);
  }
  ni->child->Rewind(ni->child);
}

static void NI_Free(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  ni->child->Free(ni->child);
  if (ni->wcii) {
    ni->wcii->Free(ni->wcii);
  }
  IndexResult_Free(base->current);
  rm_free(base);
}

static size_t NI_NumEstimated(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  return ni->maxDocId;
}

/* Read from a NOT iterator - Non-Optimized version. We simply read until max
 * docId, skipping docIds that exist in the child */
static IteratorStatus NI_Read_NotOptimized(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  // Check if we reached the end
  if (base->atEOF || base->lastDocId >= ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc;

  // If child has not read any element, read one
  if (ni->child->lastDocId == 0) {
    // Read at least the first entry of the child
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
  }

  // Advance to the next potential docId
  base->lastDocId++;

  // Search for a document that's not in the child iterator
  while (base->lastDocId <= ni->maxDocId) {
    // Case 1: Current docID is less than child's docID or child is exhauster.
    // This means we found a document that is not in the child iterator
    if (base->lastDocId < ni->child->lastDocId || ni->child->atEOF) {
      ni->timeoutCtx.counter = 0;
      base->current->docId = base->lastDocId;
      return ITERATOR_OK;
    }

    // Case 2: Current docID is equal to child's docID.
    // If we're at the same docId as the child, we need to advance both and in next loop iteration we will see how they compare
    else if (base->lastDocId == ni->child->lastDocId) {
      rc = ni->child->Read(ni->child);
      if (rc == ITERATOR_TIMEOUT) return rc;
      base->lastDocId++;
    }
    // Case 3: Current docID is ahead of child's docID
    // This means we need to advance the child until it catches up, and in next loop we will see how they compare
    else { // (base->lastDocId > ni->child->lastDocId)
      // If our docId is greater than child's, advance the child until it catches up
      while (!ni->child->atEOF && ni->child->lastDocId < base->lastDocId) {
        rc = ni->child->Read(ni->child);
        if (rc == ITERATOR_TIMEOUT) return rc;
      }
    }

    // Check for timeout periodically
    if (TimedOut_WithCtx_Gran(&ni->timeoutCtx, 5000)) {
      base->atEOF = true;
      return ITERATOR_TIMEOUT;
    }
  }

  // If we've reached here, we've exceeded the maximum docId
  base->atEOF = true;
  return ITERATOR_EOF;
}

/* SkipTo for NOT iterator - Non-optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static IteratorStatus NI_SkipTo_NotOptimized(QueryIterator *base, t_docId docId) {
  NotIterator *ni = (NotIterator *)base;
  // do not skip beyond max doc id
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  if (docId > ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }
  RS_ASSERT(base->lastDocId < docId);

  // Case 1: Child is ahead or at EOF - docId is not in child
  if (ni->child->lastDocId > docId || ni->child->atEOF) {
    base->lastDocId = base->current->docId = docId;
    return ITERATOR_OK;
  }
  // Case 2: Child is behind docId - need to check if docId is in child
  else if (ni->child->lastDocId < docId) {
    IteratorStatus rc = ni->child->SkipTo(ni->child, docId);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
    if (rc != ITERATOR_OK) {
      // Child does not have docID, so is valid match
      base->lastDocId = base->current->docId = docId;
      return ITERATOR_OK;
    }
  }

  // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
  // We need to return NOTFOUND and set the current result to the next valid docId
  base->current->docId = base->lastDocId = docId;
  IteratorStatus rc = NI_Read_NotOptimized(base);

  return rc == ITERATOR_OK ? ITERATOR_NOTFOUND : rc;
}

QueryIterator *IT_V2(NewNotIterator)(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q) {
  NotIterator *ni = rm_calloc(1, sizeof(*ni));
  QueryIterator *ret = &ni->base;
  /*bool optimized = q && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    //TODO(Joan): Once all the WildCards are migrated, call the generic one
    ni->wcii = IT_V2(NewWildcardIterator_NonOptimized)(q->docTable->maxDocId, q->docTable->size);
  }*/
  ni->child = it ? it : IT_V2(NewEmptyIterator)();
  ni->maxDocId = maxDocId;          // Valid for the optimized case as well, since this is the maxDocId of the embedded wildcard iterator
  ni->timeoutCtx = (TimeoutCtx){ .timeout = timeout, .counter = 0 };

  ret->current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  ret->current->docId = 0;
  ret->atEOF = false;
  ret->type = NOT_ITERATOR;
  ret->lastDocId = 0;
  ret->NumEstimated = NI_NumEstimated;
  ret->Free = NI_Free;
  /*ret->Read = optimized ? NI_Read_Optimized : NI_Read_NotOptimized;
  ret->SkipTo = optimized ? NI_SkipTo_Optimized : NI_SkipTo_NotOptimized;*/
  ret->Read = NI_Read_NotOptimized;
  ret->SkipTo = NI_SkipTo_NotOptimized;
  ret->Rewind = NI_Rewind;

  return ret;
}

// LCOV_EXCL_START
QueryIterator *IT_V2(_New_NotIterator_With_WildCardIterator)(QueryIterator *child, QueryIterator *wcii, t_docId maxDocId, double weight, struct timespec timeout) {
  NotIterator *ni = rm_calloc(1, sizeof(*ni));
  QueryIterator *ret = &ni->base;
  ni->child = child;
  ni->wcii = wcii;
  ni->maxDocId = maxDocId;          // Valid for the optimized case as well, since this is the maxDocId of the embedded wildcard iterator
  ni->timeoutCtx = (TimeoutCtx){ .timeout = timeout, .counter = 0 };

  ret->current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  ret->current->docId = 0;
  ret->atEOF = false;
  ret->type = NOT_ITERATOR;
  ret->lastDocId = 0;
  ret->NumEstimated = NI_NumEstimated;
  ret->Free = NI_Free;
  ret->Read = NI_Read_NotOptimized;
  ret->SkipTo = NI_SkipTo_NotOptimized;
  ret->Rewind = NI_Rewind;

  return ret;
}
// LCOV_EXCL_STOP
