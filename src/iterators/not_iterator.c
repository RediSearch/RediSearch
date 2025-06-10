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

static IteratorStatus NI_Read_Optimized(QueryIterator *base); // forward decl
static IteratorStatus NI_Read_NotOptimized(QueryIterator *base); // forward decl

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
  // We need to return NOTFOUND and set the current result to the next valid docId
  base->current->docId = base->lastDocId = docId;
  IteratorStatus rc = NI_Read_NotOptimized(base);
  if (rc == ITERATOR_OK) {
    return ITERATOR_NOTFOUND;
  }
  return rc;
}

/* SkipTo for NOT iterator - Optimized version. If we have a match - return
 * NOTFOUND. If we don't or we're at the end - return OK */
static IteratorStatus NI_SkipTo_Optimized(QueryIterator *base, t_docId docId) {
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

  // If the child is ahead of the skipto id, it means the child doesn't have this id.
  if (ni->child->lastDocId > docId || ni->child->atEOF) {
    // We know that the Child does not have docId, so we need to check if the WC has it or not
    IteratorStatus rc = ni->wcii->SkipTo(ni->wcii, docId);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
    if (rc == ITERATOR_OK) {
      base->lastDocId = base->current->docId = ni->wcii->lastDocId;
      return ITERATOR_OK;
    } else {
      // EOF or other error
      base->atEOF = ni->wcii->atEOF;
      return rc;
    }
  } else if (ni->child->lastDocId < docId) {
    // Child is behind docId, so we need to check if docId is in child
    IteratorStatus rc = ni->child->SkipTo(ni->child, docId);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;

    if (rc == ITERATOR_OK) {
      // Child has this docId, so it's an anti-match
      // Now check if wildcard has this docId
      rc = ni->wcii->SkipTo(ni->wcii, docId);
      if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
      if (rc == ITERATOR_EOF) {
        base->atEOF = true;
        return rc;
      }
      if (ni->wcii->lastDocId > ni->child->lastDocId) {
        rc = ni->child->SkipTo(ni->child, ni->wcii->lastDocId);
        if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
        if (rc == ITERATOR_OK) {
          rc = NI_Read_Optimized(base);
          if (rc == ITERATOR_OK) {
            return ITERATOR_NOTFOUND;
          } else if (rc == ITERATOR_EOF) {
            base->atEOF = true;
          }
          return rc;
        } else if (rc == ITERATOR_NOTFOUND || rc == ITERATOR_EOF) {
          base->lastDocId = base->current->docId = ni->wcii->lastDocId;
          return base->lastDocId == docId ? ITERATOR_OK: ITERATOR_NOTFOUND;
        }
      } else if (ni->wcii->lastDocId == ni->child->lastDocId) {
        rc = NI_Read_Optimized(base);
        if (rc == ITERATOR_OK) {
          return ITERATOR_NOTFOUND;
        } else if (rc == ITERATOR_EOF) {
          base->atEOF = true;
        }
        return rc;
      }
    } else {
      // Child doesn't have this exact docId but skipped to a higher one
      // Check if wildcard has this docId
      rc = ni->wcii->SkipTo(ni->wcii, docId);
      if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
      if (rc == ITERATOR_OK) {
        base->lastDocId = base->current->docId = ni->wcii->lastDocId;
        return ITERATOR_OK;
      } else {
        // We now make sure we did not surpass the child docId? or consider if Child reached EOF
        if (ni->wcii->lastDocId > ni->child->lastDocId) {
          rc = ni->child->SkipTo(ni->child, ni->wcii->lastDocId);
          if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
          if (rc == ITERATOR_OK) {
            rc = NI_Read_Optimized(base);
            if (rc == ITERATOR_OK) {
              return ITERATOR_NOTFOUND;
            } else if (rc == ITERATOR_EOF) {
              base->atEOF = true;
            }
            return rc;
          } else if (rc == ITERATOR_NOTFOUND || rc == ITERATOR_EOF) {
            base->lastDocId = base->current->docId = ni->wcii->lastDocId;
            return ITERATOR_OK;
          }
        }
        base->atEOF = ni->wcii->atEOF;
        return rc;
      }
    }
  } else {
    // Child is exactly at docId, so it's an anti-match
    // Check if wildcard has this docId
    IteratorStatus rc = ni->wcii->SkipTo(ni->wcii, docId);
    if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
    if (rc == ITERATOR_OK) {
      // Both Child and WC have this docID, just Read the next
      rc = NI_Read_Optimized(base);
      if (rc == ITERATOR_OK) {
        return ITERATOR_NOTFOUND;
      } else if (rc == ITERATOR_EOF) {
        base->atEOF = true;
      }
      return rc;
    } else if (rc == ITERATOR_EOF) {
      base->atEOF = true;
      return rc;
    } else if (rc == ITERATOR_NOTFOUND) {
      // Here it means that the WC advanced the Child
      rc = ni->child->SkipTo(ni->child, ni->wcii->lastDocId);
      if (rc == ITERATOR_TIMEOUT) return ITERATOR_TIMEOUT;
      if (rc == ITERATOR_OK) {
        rc = NI_Read_Optimized(base);
        if (rc == ITERATOR_OK) {
          return ITERATOR_NOTFOUND;
        } else if (rc == ITERATOR_EOF) {
          base->atEOF = true;
        }
        return rc;
      } else if (rc == ITERATOR_NOTFOUND || rc == ITERATOR_EOF) {
        base->lastDocId = base->current->docId = ni->wcii->lastDocId;
        return ITERATOR_NOTFOUND;
      }
    }
  }
}

/* Read from a NOT iterator - Non-Optimized version. We simply read until max
 * docId, skipping docIds that exist in the child */
static IteratorStatus NI_Read_NotOptimized(QueryIterator *base) {
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
  // Advance and see if it is valid
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
static IteratorStatus NI_Read_Optimized(QueryIterator *base) {
  NotIterator *ni = (NotIterator *)base;
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

  // Advance wcii to next element
  rc = ni->wcii->Read(ni->wcii);
  if (rc == ITERATOR_TIMEOUT) {
    base->atEOF = true;
    return ITERATOR_TIMEOUT;
  }
  // Keep advancing wcii until we find an element not in child
  while (!ni->wcii->atEOF) {
    // If child is exhausted or its current value is greater than wcii's,
    // then wcii's current value is not in child
    if (ni->child->atEOF || ni->wcii->lastDocId < ni->child->lastDocId) {
      base->lastDocId = base->current->docId = ni->wcii->lastDocId;
      return ITERATOR_OK; // Found a valid difference element
    }

    // If both pointers are at the same element, advance wcii
    else if (ni->wcii->lastDocId == ni->child->lastDocId) {
      rc = ni->wcii->Read(ni->wcii);
      if (rc == ITERATOR_TIMEOUT) {
        base->atEOF = true;
        return ITERATOR_TIMEOUT;
      }
    }
    // If child is behind, advance it until it catches up or passes wcii
    //else if (ni->child->lastDocId < ni->wcii->lastDocId) {
    else {
      rc = ni->child->Read(ni->child);
      if (rc == ITERATOR_TIMEOUT) {
        base->atEOF = true;
        return ITERATOR_TIMEOUT;
      }
    }
    if (TimedOut_WithCtx_Gran(&ni->timeoutCtx, 5000)) {
      base->atEOF = true;
      return ITERATOR_TIMEOUT;
    }
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
  ret->Read = optimized ? NI_Read_Optimized : NI_Read_NotOptimized;
  ret->SkipTo = optimized ? NI_SkipTo_Optimized : NI_SkipTo_NotOptimized;
  ret->Rewind = NI_Rewind;

  return ret;
}
