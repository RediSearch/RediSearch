/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "optional_iterator.h"
#include "iterator_api.h"
#include "types_rs.h"
#include "wildcard_iterator.h"
#include "iterators_rs.h"

typedef struct {
  QueryIterator base;     // base index iterator
  QueryIterator *child;   // child index iterator
  QueryIterator *wcii;    // wildcard child iterator, used for optimization
  RSIndexResult *virt;
  t_docId maxDocId;
  double weight;
} OptionalOptimizedIterator;

static void OI_Free(QueryIterator *base) {
  OptionalOptimizedIterator *oi = (OptionalOptimizedIterator *)base;

  oi->child->Free(oi->child);
  if (oi->wcii) {
    oi->wcii->Free(oi->wcii);
  }
  // Only free our virtual result, not the child's result
  IndexResult_Free(oi->virt);
  rm_free(base);
}

static size_t OI_NumEstimated(const QueryIterator *base) {
  const OptionalOptimizedIterator *oi = (const OptionalOptimizedIterator *)base;
  return oi->wcii ? oi->wcii->NumEstimated(oi->wcii) : oi->maxDocId;
}

static void OI_Rewind(QueryIterator *base) {
  OptionalOptimizedIterator *oi = (OptionalOptimizedIterator *)base;
  base->atEOF = false;
  base->lastDocId = 0;
  oi->virt->docId = 0;

  oi->child->Rewind(oi->child);
  if (oi->wcii) {
    oi->wcii->Rewind(oi->wcii);
  }
}

// SkipTo for OPTIONAL iterator - Optimized version.
static IteratorStatus OI_SkipTo_Optimized(QueryIterator *base, t_docId docId) {
  OptionalOptimizedIterator *oi = (OptionalOptimizedIterator *)base;
  RS_ASSERT(docId > base->lastDocId);
  RS_ASSERT(docId > oi->wcii->lastDocId);

  if (docId > oi->maxDocId || base->atEOF) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  // Promote the wildcard iterator to the requested docId if the docId
  IteratorStatus rc = oi->wcii->SkipTo(oi->wcii, docId);
  if (rc != ITERATOR_OK && rc != ITERATOR_NOTFOUND) {
    base->atEOF = (rc == ITERATOR_EOF);
    return rc;
  }

  // Update the docId target to the last docId of the wildcard iterator
  // If the SkipTo returned ITERATOR_NOTFOUND, this is the relevant docId.
  docId = oi->wcii->lastDocId;

  if (docId > oi->child->lastDocId) {
    IteratorStatus crc = oi->child->SkipTo(oi->child, docId);
    if (crc == ITERATOR_TIMEOUT) return crc;
  }

  if (docId == oi->child->lastDocId) {
    // Has a real hit on the child iterator
    oi->base.current = oi->child->current;
    oi->base.current->weight = oi->weight;
  } else {
    oi->virt->docId = oi->wcii->lastDocId;
    oi->base.current = oi->virt;
  }

  oi->base.lastDocId = docId;
  return rc;
}

// Read from optional iterator - Optimized version, utilizing the `existing docs`
// inverted index.
static IteratorStatus OI_Read_Optimized(QueryIterator *base) {
  OptionalOptimizedIterator *oi = (OptionalOptimizedIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  if (base->lastDocId >= oi->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  // Get the next docId
  IteratorStatus wcii_rc = oi->wcii->Read(oi->wcii);
  if (wcii_rc != ITERATOR_OK) {
    // EOF, set invalid
    base->atEOF = (wcii_rc == ITERATOR_EOF);
    return wcii_rc;
  }

  // We loop over this condition, since it reflects that the index is not up to date.
  while (oi->wcii->lastDocId > oi->child->lastDocId && !oi->child->atEOF) {
    IteratorStatus rc = oi->child->Read(oi->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
  }

  if (oi->wcii->lastDocId != oi->child->lastDocId) {
    oi->base.current = oi->virt;
  } else {
    oi->base.current = oi->child->current;
    oi->base.current->weight = oi->weight;
  }

  base->lastDocId = oi->base.current->docId = oi->wcii->lastDocId;
  return ITERATOR_OK;
}

// Revalidate for OPTIONAL iterator - Optimized version.
static ValidateStatus OI_Revalidate_Optimized(QueryIterator *base) {
  OptionalOptimizedIterator *oi = (OptionalOptimizedIterator *)base;

  // 1. Revalidate the wildcard iterator first
  ValidateStatus wcii_status = oi->wcii->Revalidate(oi->wcii);
  base->atEOF = oi->wcii->atEOF; // Update atEOF based on wildcard iterator status
  if (wcii_status == VALIDATE_ABORTED) {
    return VALIDATE_ABORTED; // If wildcard iterator is aborted, we must abort too
  }

  // 2. Revalidate the child iterator
  ValidateStatus child_status = oi->child->Revalidate(oi->child);
  if (child_status == VALIDATE_ABORTED) {
    // Free child and replace with empty iterator
    oi->child->Free(oi->child);
    oi->child = NewEmptyIterator();
  }

  // 3. Validate the current result
  if (wcii_status == VALIDATE_OK) {
    // If the wildcard iterator was not moved, we can handle the current state similarly to the non-optimized version.
    // If the child iterator was not moved, or if the current result is virtual, we can return VALIDATE_OK.
    if (child_status == VALIDATE_OK || base->current == oi->virt) {
      return VALIDATE_OK;
    }
    // If the child iterator was moved and the current result is real,
    // we need to read to get the next valid result.
    base->Read(base);
    return VALIDATE_MOVED;
  } else {
    RS_ASSERT(wcii_status == VALIDATE_MOVED);
    // If the wildcard iterator was moved, we need to advance the iterator
    // to the next valid result, which may be a real hit or a virtual hit.
    // We cannot just read the iterator, because it will advance the wildcard iterator again
    if (oi->wcii->lastDocId > oi->child->lastDocId) {
      oi->child->SkipTo(oi->child, oi->wcii->lastDocId);
    }
    if (oi->child->lastDocId == oi->wcii->lastDocId) {
      // If the child iterator has a hit on the same docId, we can return VALIDATE_MOVED
      base->current = oi->child->current;
      base->current->weight = oi->weight;
    } else {
      // If the child iterator does not have a hit on the same docId,
      // we return the virtual result.
      oi->virt->docId = oi->wcii->lastDocId;
      base->current = oi->virt;
    }
    base->lastDocId = oi->wcii->lastDocId;
    return VALIDATE_MOVED;
  }
}

/**
 * Reduce the optional iterator by applying these rules:
 * 1. If the child is an empty iterator or NULL, return a wildcard iterator
 * 2. If the child is a wildcard iterator, return it
 * 3. Otherwise, return NULL and let the caller create the optional iterator
 */
static QueryIterator* OptionalIteratorReducer(QueryIterator *it, QueryEvalCtx *q, double weight) {
  QueryIterator *ret = NULL;
  if (!it || it->type == EMPTY_ITERATOR) {
    // If the child is NULL, we return a wildcard iterator. All will be virtual hits
    ret = NewWildcardIterator(q, 0);
    if (it) {
      it->Free(it);
    }
  } else if (IsWildcardIterator(it)) {
    // All will be real hits
    ret = it;
    ret->current->weight = weight;
  }
  return ret;
}

// Create a new OPTIONAL iterator - Non-Optimized version.
QueryIterator *NewOptionalIterator(QueryIterator *it, QueryEvalCtx *q, double weight) {
  RS_ASSERT(q && q->sctx && q->sctx->spec && q->docTable);
  QueryIterator *ret = OptionalIteratorReducer(it, q, weight);
  if (ret != NULL) {
    return ret;
  }

  bool optimized = q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  optimized |= q && q->sctx && q->sctx->spec && q->sctx->spec->diskSpec;
  t_docId maxDocId = q->docTable->maxDocId;

  if (optimized) {
    OptionalOptimizedIterator *oi = rm_calloc(1, sizeof(*oi));
    oi->wcii = NewWildcardIterator_Optimized(q->sctx, 0);
    oi->child = it;
    oi->virt = NewVirtualResult(0, RS_FIELDMASK_ALL);
    oi->virt->freq = 1;
    oi->maxDocId = maxDocId;
    oi->weight = weight;
    ret = &oi->base;
    ret->type = OPTIONAL_OPTIMIZED_ITERATOR;
    ret->atEOF = false;
    ret->lastDocId = 0;
    ret->current = oi->virt;
    ret->NumEstimated = OI_NumEstimated;
    ret->Free = OI_Free;
    ret->Rewind = OI_Rewind;
    ret->Read = OI_Read_Optimized;
    ret->SkipTo = OI_SkipTo_Optimized;
    ret->Revalidate = OI_Revalidate_Optimized;
  } else {
    ret = NewOptionalNonOptimizedIterator(it, maxDocId, weight);
  }

  return ret;
}

QueryIterator const* GetOptionalIteratorChild(const QueryIterator *base) {
    if (base->type == OPTIONAL_OPTIMIZED_ITERATOR) {
        OptionalOptimizedIterator const*it = (OptionalOptimizedIterator *)base;
        return it->child;
    } else {
        return GetOptionalNonOptimizedIteratorChild(base);
    }
}

QueryIterator *TakeOptionalIteratorChild(QueryIterator *base) {
    if (base->type == OPTIONAL_OPTIMIZED_ITERATOR) {
        OptionalOptimizedIterator *it = (OptionalOptimizedIterator *)base;
        QueryIterator* child = it->child;
        it->child = NULL;
        return child;
    } else {
        return TakeOptionalNonOptimizedIteratorChild(base);
    }
}

void SetOptionalIteratorChild(QueryIterator *base, QueryIterator *newChild) {
    if (base->type == OPTIONAL_OPTIMIZED_ITERATOR) {
        OptionalOptimizedIterator *it = (OptionalOptimizedIterator *)base;
        if (it->child) {
            it->child->Free(it->child);
        }
        it->child = newChild;
    } else {
        SetOptionalNonOptimizedIteratorChild(base, newChild);
    }
}

QueryIterator const* GetOptionalOptimizedIteratorWildcard(QueryIterator *base) {
    RS_ASSERT (base->type == OPTIONAL_OPTIMIZED_ITERATOR);
    OptionalOptimizedIterator const*it = (OptionalOptimizedIterator *)base;
    return it->wcii;
}

void SetOptionalOptimizedIteratorWildcard(QueryIterator *base, QueryIterator *newWcii) {
    RS_ASSERT (base->type == OPTIONAL_OPTIMIZED_ITERATOR);
    OptionalOptimizedIterator *it = (OptionalOptimizedIterator *)base;
    if (it->wcii) {
        it->wcii->Free(it->wcii);
    }
    it->wcii = newWcii;
}
