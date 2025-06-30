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
  return ni->wcii ? ni->wcii->NumEstimated(ni->wcii) : ni->maxDocId;
}

/* Read from a NOT iterator - Non-Optimized version. This is applicable only if
 * the only or leftmost node of a query is a NOT node. We simply read until max
 * docId, skipping docIds that exist in the child */
static IteratorStatus NI_Read_NotOptimized(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  // Check if we reached the end
  if (base->atEOF || base->lastDocId >= ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc;
  if (base->lastDocId == ni->child->lastDocId) {
    // read next entry from child, or EOF
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
  }

  while (base->lastDocId < ni->maxDocId) {
    base->lastDocId++;
    if (base->lastDocId < ni->child->lastDocId || ni->child->atEOF) {
      ni->timeoutCtx.counter = 0;
      base->current->docId = base->lastDocId;
      return ITERATOR_OK;
    }
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
    // Check for timeout with low granularity (MOD-5512)
    if (TimedOut_WithCtx_Gran(&ni->timeoutCtx, 5000)) {
      base->atEOF = true;
      return ITERATOR_TIMEOUT;
    }
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

/* Read from a NOT iterator - Optimized version, utilizing the `existing docs`
 * inverted index. This is applicable only if the only or leftmost node of a
 * query is a NOT node. We simply read until max docId, skipping docIds that
 * exist in the child */
static IteratorStatus NI_Read_Optimized(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  // Check if we reached the end
  if (base->atEOF || base->lastDocId >= ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  // Advance to the next potential docId
  IteratorStatus rc = ni->wcii->Read(ni->wcii);
  if (rc == ITERATOR_TIMEOUT) {
    return ITERATOR_TIMEOUT;
  }
  // Iterate through all the documents present in the wcii until we find one that is not in child
  while (!ni->wcii->atEOF) {
    if (ni->child->atEOF || ni->wcii->lastDocId < ni->child->lastDocId) {
      // Case 1: Current docID is less than child's docID or child is exhauster.
      // This means we found a document that is not in the child iterator
      base->lastDocId = base->current->docId = ni->wcii->lastDocId;
      return ITERATOR_OK; // Found a valid difference element
    } else if (ni->wcii->lastDocId == ni->child->lastDocId) {
      // Case 2: Current docID is equal to child's docID.
      // If we're at the same docId as the child, we need to advance both and in next loop iteration we will see how they compare
      rc = ni->child->Read(ni->child);
      if (rc == ITERATOR_TIMEOUT) return rc;
      rc = ni->wcii->Read(ni->wcii);
      if (rc == ITERATOR_TIMEOUT) {
        return ITERATOR_TIMEOUT;
      }
    } else { //(ni->child->lastDocId < ni->wcii->lastDocId)
      // Case 3: Current docID is ahead of child's docID
      // This means we need to advance the child until it catches up, and in next loop we will see how they compare
      while (!ni->child->atEOF && ni->child->lastDocId < ni->wcii->lastDocId) {
        rc = ni->child->Read(ni->child);
        if (rc == ITERATOR_TIMEOUT) return rc;
      }
    }
    if (TimedOut_WithCtx_Gran(&ni->timeoutCtx, 5000)) {
      return ITERATOR_TIMEOUT;
    }
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

/* SkipTo for NOT iterator - Non-optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static IteratorStatus NI_SkipTo_NotOptimized(QueryIterator *base, t_docId docId) {
  NotIterator *ni = (NotIterator *)base;
  RS_ASSERT(base->lastDocId < docId);
  // do not skip beyond max doc id
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  if (docId > ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

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

/* SkipTo for NOT iterator - Optimized version.
 * This function attempts to skip to a specific document ID in a NOT iterator,
 * utilizing the wildcard iterator (wcii) which contains all existing documents.
 * It returns:
 * - ITERATOR_OK if the document exists in wcii but NOT in the child iterator (valid result)
 * - ITERATOR_NOTFOUND if the document IS in the child iterator (anti-match)
 * - ITERATOR_EOF if we've reached the end of the iterator
 * - ITERATOR_TIMEOUT if the operation timed out
 */
static IteratorStatus NI_SkipTo_Optimized(QueryIterator *base, t_docId docId) {
  NotIterator *ni = (NotIterator *)base;
  RS_ASSERT(base->lastDocId < docId);

  // Check if we've reached the end or if docId exceeds maximum
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  if (docId > ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }
  // Handle relative positions of wildcard and child iterators
  IteratorStatus rc = ni->wcii->SkipTo(ni->wcii, docId);
  if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
  if (rc == ITERATOR_EOF) {
    base->atEOF = true;
    return rc;
  }

  if (ni->wcii->lastDocId < ni->child->lastDocId || ni->child->atEOF) {
    // Case 1: Wildcard is behind child
    // Wildcard found a document before child's position - valid result
    base->lastDocId = base->current->docId = ni->wcii->lastDocId;
  } else if (ni->wcii->lastDocId == ni->child->lastDocId) {
    // Case 2: Both iterators at same position
    // Both at same position - find next valid result
    rc = NI_Read_Optimized(base);
    if (rc == ITERATOR_OK) {
      return ITERATOR_NOTFOUND;
    } else if (rc == ITERATOR_EOF) {
      RS_ASSERT(base->atEOF);
    }
  } else { // ni->wcii->lastDocId > ni->child->lastDocId
    // Case 3: Wildcard is ahead of child
    // Wildcard advanced past child - check if child has this new docID
    IteratorStatus child_rc = ni->child->SkipTo(ni->child, ni->wcii->lastDocId);
    if (child_rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;

    if (child_rc == ITERATOR_OK) {
      // Child has this document - find next valid result
      rc = NI_Read_Optimized(base);
      if (rc == ITERATOR_OK) {
        return ITERATOR_NOTFOUND;
      } else if (rc == ITERATOR_EOF) {
        RS_ASSERT(base->atEOF);
      }
    } else if (child_rc == ITERATOR_NOTFOUND || child_rc == ITERATOR_EOF) {
      // Child doesn't have this document - valid result
      base->lastDocId = base->current->docId = ni->wcii->lastDocId;
    }
  }

  return rc;
}

/*
 * Reduce the not iterator by applying these rules:
 * 1. If the child is an empty iterator or NULL, return a wildcard iterator
 * 2. If the child is a wildcard iterator, return an empty iterator
 * 3. Otherwise, return NULL and let the caller create the not iterator
 */
static QueryIterator* NotIteratorReducer(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q) {
  RS_ASSERT(q);
  QueryIterator *ret = NULL;
  if (!it || it->type == EMPTY_ITERATOR) {
    ret = IT_V2(NewWildcardIterator)(q, weight);
  } else if (IsWildcardIterator(it)) {
    ret = IT_V2(NewEmptyIterator)();
  }
  if (ret != NULL) {
    if (it) {
      it->Free(it);
    }
  }
  return ret;
}

QueryIterator *IT_V2(NewNotIterator)(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q) {
  QueryIterator *ret = NotIteratorReducer(it, maxDocId, weight, timeout, q);
  if (ret != NULL) {
    return ret;
  }
  NotIterator *ni = rm_calloc(1, sizeof(*ni));
  ret = &ni->base;
  bool optimized = q && q->sctx && q->sctx->spec && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    ni->wcii = IT_V2(NewWildcardIterator_Optimized)(q->sctx, weight);
  }
  ni->child = it;
  ni->maxDocId = maxDocId;          // Valid for the optimized case as well, since this is the maxDocId of the embedded wildcard iterator
  ni->timeoutCtx = (TimeoutCtx){ .timeout = timeout, .counter = 0 };

  ret->current = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  ret->current->docId = 0;
  ret->atEOF = false;
  ret->type = NOT_ITERATOR;
  ret->lastDocId = 0;
  ret->NumEstimated = NI_NumEstimated;
  ret->Free = NI_Free;
  ret->Read = optimized ? NI_Read_Optimized : NI_Read_NotOptimized;
  ret->SkipTo = optimized ? NI_SkipTo_Optimized : NI_SkipTo_NotOptimized;
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
  ret->Read = NI_Read_Optimized;
  ret->SkipTo = NI_SkipTo_Optimized;
  ret->Rewind = NI_Rewind;

  return ret;
}
// LCOV_EXCL_STOP
