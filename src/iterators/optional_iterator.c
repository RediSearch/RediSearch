/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "optional_iterator.h"
#include "wildcard_iterator.h"

static void OI_Free(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;

  oi->child->Free(oi->child);
  if (oi->wcii) {
    oi->wcii->Free(oi->wcii);
  }
  // Only free our virtual result, not the child's result
  IndexResult_Free(oi->virt);
  rm_free(base);
}

static size_t OI_NumEstimated(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
  return oi->wcii ? oi->wcii->NumEstimated(oi->wcii) : oi->numDocs;
}

static void OI_Rewind(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
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
  OptionalIterator *oi = (OptionalIterator *)base;
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
  OptionalIterator *oi = (OptionalIterator *)base;
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

// SkipTo for OPTIONAL iterator - Non-optimized version.
// Skip to a specific docId. If the child has a hit on this docId, return it.
// Otherwise, return a virtual hit.
static IteratorStatus OI_SkipTo_NotOptimized(QueryIterator *base, t_docId docId) {
  RS_ASSERT(docId > base->lastDocId);
  OptionalIterator *oi = (OptionalIterator *)base;

  if (docId > oi->maxDocId || base->atEOF) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  if (docId > oi->child->lastDocId) {
    IteratorStatus rc = oi->child->SkipTo(oi->child, docId);
    if (rc == ITERATOR_TIMEOUT) return rc;
  }

  if (docId == oi->child->lastDocId) {
    // Has a real hit on the child iterator
    base->current = oi->child->current;
    base->current->weight = oi->weight;
  } else {
    // Virtual hit
    oi->virt->docId = docId;
    base->current = oi->virt;
  }
  // Set the current ID
  base->lastDocId = docId;
  return ITERATOR_OK;
}

// Read from an OPTIONAL iterator - Non-Optimized version.
static IteratorStatus OI_Read_NotOptimized(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
  if (base->atEOF || base->lastDocId >= oi->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  if (base->lastDocId == oi->child->lastDocId) {
    IteratorStatus rc = oi->child->Read(oi->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
  }

  // Point to next doc
  base->lastDocId++;

  if (base->lastDocId == oi->child->lastDocId) {
    base->current = oi->child->current;
    base->current->weight = oi->weight;
  } else {
    oi->virt->docId = base->lastDocId;
    base->current = oi->virt;
  }
  return ITERATOR_OK;
}

// Create a new OPTIONAL iterator - Non-Optimized version.
QueryIterator *IT_V2(NewOptionalIterator)(QueryIterator *it, QueryEvalCtx *q, double weight) {
  RS_ASSERT(it != NULL);
  OptionalIterator *oi = rm_calloc(1, sizeof(*oi));
  bool optimized = q && q->sctx->spec->rule && q->sctx->spec->rule->index_all;
  if (optimized) {
    oi->wcii = IT_V2(NewWildcardIterator_Optimized)(q->sctx);
  }
  oi->child = it;
  oi->virt = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  oi->maxDocId = q->docTable->maxDocId;
  oi->numDocs = q->docTable->size;
  oi->virt->freq = 1;
  oi->weight = weight;

  QueryIterator *ret = &oi->base;
  ret->type = OPTIONAL_ITERATOR;
  ret->atEOF = false;
  ret->lastDocId = 0;
  ret->current = oi->virt;
  ret->NumEstimated = OI_NumEstimated;
  ret->Free = OI_Free;
  ret->Rewind = OI_Rewind;
  if (optimized) {
    ret->Read = OI_Read_Optimized;
    ret->SkipTo = OI_SkipTo_Optimized;
  } else {
    ret->Read = OI_Read_NotOptimized;
    ret->SkipTo = OI_SkipTo_NotOptimized;
  }

  return ret;
}
