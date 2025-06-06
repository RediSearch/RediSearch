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
  return ni->maxDocId - ni->child->NumEstimated(ni->child);
}

static IteratorStatus NI_ReadSorted_O(QueryIterator *base); // forward decl
static IteratorStatus NI_ReadSorted_NO(QueryIterator *base); // forward decl

/* SkipTo for NOT iterator - Non-optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static IteratorStatus NI_SkipTo_NO(QueryIterator *base, t_docId docId) {
  NotIterator *ni = (NotIterator *)base;
  // do not skip beyond max doc id
  if (docId > ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  // So we are okay!
  if (ni->child->lastDocId > docId || ni->child->atEOF) {
    base->lastDocId = base->current->docId = docId;
    return ITERATOR_OK;
  } else if (ni->child->lastDocId < docId) {
    // read the next entry from the child
    IteratorStatus rc = ni->child->SkipTo(ni->child, docId);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
    if (rc != ITERATOR_OK) {
      base->lastDocId = base->current->docId = docId;
      return ITERATOR_OK;
    }
  }
  // If the child docId is the one we are looking for, it's an anti match!
  // We need to return NOTFOUND and set hit to the next valid docId
  base->current->docId = base->lastDocId = docId;
  IteratorStatus rc = NI_ReadSorted_NO(base);
  if (rc == ITERATOR_OK) {
    return ITERATOR_NOTFOUND;
  }
  return rc;
}

/* SkipTo for NOT iterator - Optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static IteratorStatus NI_SkipTo_O(QueryIterator *base, t_docId docId) {
  NotIterator *ni = (NotIterator *)base;

  // do not skip beyond max doc id
  if (docId > ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc = ni->wcii->SkipTo(ni->wcii, docId);
  if (rc != ITERATOR_NOTFOUND && rc != ITERATOR_OK) {
    return rc;
  } else if (rc == ITERATOR_OK) {
    // A valid wildcard result was found. Let's check if the child has it
    if (ni->child->lastDocId > docId || ni->child->atEOF) {
      // If the child is ahead of the skipto id, it means the child doesn't have this id.
      // So we are okay!
      base->lastDocId = base->current->docId = docId;
      return ITERATOR_OK;
    } else if (ni->child->lastDocId < docId) {
      // read the next entry from the child
      rc = ni->child->SkipTo(ni->child, docId);
      if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
      if (rc != ITERATOR_OK) {
        base->lastDocId = base->current->docId = docId;
        return ITERATOR_OK;
      }
    }
  }

  // If the wildcard iterator is missing the docId, or the child iterator has it,
  // We need to return NOTFOUND and set hit to the next valid docId
  rc = NI_ReadSorted_O(base);
  if (rc == ITERATOR_OK) {
    return ITERATOR_NOTFOUND;
  }
  return rc;
}

/* Read from a NOT iterator - Non-Optimized version. This is applicable only if
 * the only or leftmost node of a query is a NOT node. We simply read until max
 * docId, skipping docIds that exist in the child */
static IteratorStatus NI_ReadSorted_NO(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  if (base->atEOF || base->lastDocId >= ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc;

  if (ni->child->lastDocId == 0) {
    // Read at least the first entry of the child
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
  }
  //Advance and see if it si valid
  base->lastDocId++;

  while (base->lastDocId <= ni->maxDocId) {
    // I have the risk of missing one lastDocID if childEOF is not accurate (if the child requires another Read to set the flag)
    if (base->lastDocId < ni->child->lastDocId || ni->child->atEOF) {
      ni->timeoutCtx.counter = 0;
      base->current->docId = base->lastDocId;
      return ITERATOR_OK;
    }
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
    // Check for timeout with low granularity (MOD-5512)
    //TODO(Joan): Ticket about magic number? Should it be a config?
    if (TimedOut_WithCtx_Gran(&ni->timeoutCtx, 5000)) {
      base->atEOF = true;
      return ITERATOR_TIMEOUT;
    }
    // The lastDocID that we proposed is not valid, so we need to try another one
    base->lastDocId++;
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

/* Read from a NOT iterator - Optimized version, utilizing the `existing docs`
 * inverted index. This is applicable only if the only or leftmost node of a
 * query is a NOT node. We simply read until max docId, skipping docIds that
 * exist in the child */
static IteratorStatus NI_ReadSorted_O(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
  if (base->atEOF || base->lastDocId >= ni->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc;

  if (ni->child->lastDocId == 0) {
    // Read at least the first entry of the child
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
  }
  //Advance and see if it si valid
  rc = ni->wcii->Read(ni->wcii);
  if (rc == ITERATOR_TIMEOUT) {
    base->atEOF = true;
    return ITERATOR_TIMEOUT;
  }
  base->lastDocId = base->current->docId = ni->wcii->current->docId;

  while (base->lastDocId <= ni->maxDocId) {
    // I have the risk of missing one lastDocID if childEOF is not accurate (if the child requires another Read to set the flag)
    if (base->lastDocId < ni->child->lastDocId || ni->child->atEOF) {
      ni->timeoutCtx.counter = 0;
      base->current->docId = base->lastDocId;
      return ITERATOR_OK;
    }
    rc = ni->child->Read(ni->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
    // Check for timeout with low granularity (MOD-5512)
    //TODO(Joan): Ticket about magic number? Should it be a config?
    if (TimedOut_WithCtx_Gran(&ni->timeoutCtx, 5000)) {
      base->atEOF = true;
      return ITERATOR_TIMEOUT;
    }
    // The lastDocID that we proposed is not valid, so we need to try another one
    rc = ni->wcii->Read(ni->wcii);
    if (rc == ITERATOR_TIMEOUT) {
      base->atEOF = true;
      return ITERATOR_TIMEOUT;
    }
    base->lastDocId = base->current->docId = ni->wcii->current->docId;
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

QueryIterator *IT_V2(NewNotIterator)(QueryIterator *it, t_docId maxDocId, double weight, struct timespec timeout, QueryEvalCtx *q) {
  NotIterator *ni = rm_calloc(1, sizeof(*ni));
  QueryIterator *ret = &ni->base;
  bool optimized = q && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    //TODO(Joan): Once all the WildCards are migrated, call the generic one
    ni->wcii = IT_V2(NewWildcardIterator_NonOptimized)(q->docTable->maxDocId, q->docTable->size);
  }
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
  ret->Read = optimized ? NI_ReadSorted_O : NI_ReadSorted_NO;
  ret->SkipTo = optimized ? NI_SkipTo_O : NI_SkipTo_NO;
  ret->Rewind = NI_Rewind;

  return ret;
}
