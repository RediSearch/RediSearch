#include "optional_iterator.h"

static void OI_Free(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
  if (oi->child) {
    oi->child->Free(oi->child);
  }
  // Only free our virtual result, not the child's result
  if (oi->virt) {
    IndexResult_Free(oi->virt);
  }
  rm_free(base);
}

static size_t OI_NumEstimated(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
  return oi->maxDocId;
}

static void OI_Rewind(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
  base->atEOF = false;
  base->lastDocId = 0;
  oi->virt->docId = 0;
  if (oi->child) {
    oi->child->Rewind(oi->child);
  }
}



// SkipTo for OPTIONAL iterator - Non-optimized version.
// Skip to a specific docId. If the child has a hit on this docId, return it.
// Otherwise, return a virtual hit.
static IteratorStatus OI_SkipTo_NotOptimized(QueryIterator *base, t_docId docId) {
  assert(docId > 0);
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
static IteratorStatus OI_ReadSorted_NotOptimized(QueryIterator *base) {
  OptionalIterator *oi = (OptionalIterator *)base;
  if (base->atEOF || base->lastDocId >= oi->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  // Point to next doc
  base->lastDocId++;

  if (base->lastDocId > oi->child->lastDocId) {
    IteratorStatus rc = oi->child->Read(oi->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
  }

  if (base->lastDocId != oi->child->lastDocId) {
    base->current = oi->virt;
  } else {
    base->current = oi->child->current;
    base->current->weight = oi->weight;
  }

  base->current->docId = base->lastDocId;
  return ITERATOR_OK;
}

// Create a new OPTIONAL iterator - Non-Optimized version.
QueryIterator *IT_V2(NewOptionalIterator)(QueryIterator *it, t_docId maxDocId, size_t numDocs, double weight) {
  assert(it != NULL);
  OptionalIterator *oi = rm_calloc(1, sizeof(*oi));
  oi->child = it;
  oi->maxDocId = maxDocId;
  oi->virt = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  oi->virt->freq = 1;
  oi->weight = weight;

  QueryIterator *ret = &oi->base;
  ret->type = OPTIONAL_ITERATOR;
  ret->atEOF = false;
  ret->lastDocId = 0;
  ret->current = oi->virt;
  ret->NumEstimated = OI_NumEstimated;
  ret->Free = OI_Free;
  ret->Read = OI_ReadSorted_NotOptimized;
  ret->SkipTo = OI_SkipTo_NotOptimized;
  ret->Rewind = OI_Rewind;

  return ret;
}
