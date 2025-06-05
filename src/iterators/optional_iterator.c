#include "optional_iterator.h"
#include "empty_iterator.h"

static void OI_Free(QueryIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (nc->child) {
    nc->child->Free(nc->child);
  }
  // Only free our virtual result, not the child's result
  if (nc->virt) {
    IndexResult_Free(nc->virt);
  }
  rm_free(base);
}

static size_t OI_NumEstimated(QueryIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  return nc->maxDocId;
}

static void OI_Rewind(QueryIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  base->atEOF = false;
  base->lastDocId = 0;
  nc->virt->docId = 0;
  if (nc->child) {
    nc->child->Rewind(nc->child);
  }
}



// SkipTo for OPTIONAL iterator - Non-optimized version.
static IteratorStatus OI_SkipTo_NO(QueryIterator *base, t_docId docId) {
  OptionalIterator *nc = (OptionalIterator *)base;

  if (docId > nc->maxDocId || base->atEOF) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  if (docId > nc->child->lastDocId) {
    IteratorStatus rc = nc->child->SkipTo(nc->child, docId);
    if (rc == ITERATOR_TIMEOUT) return rc;
  }

  if (docId > 0 && docId == nc->child->lastDocId) {
    // Has a real hit on the child iterator
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
  } else {
    // Virtual hit
    nc->virt->docId = docId;
    nc->base.current = nc->virt;
  }
  // Set the current ID
  base->lastDocId = docId;
  return ITERATOR_OK;
}

// Read from an OPTIONAL iterator - Non-Optimized version.
static IteratorStatus OI_ReadSorted_NO(QueryIterator *base) {
  OptionalIterator *nc = (OptionalIterator *)base;
  if (base->atEOF || base->lastDocId >= nc->maxDocId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  // Increase the size by one
  base->lastDocId++;

  if (base->lastDocId > nc->child->lastDocId && !base->atEOF) {
    IteratorStatus rc = nc->child->Read(nc->child);
    if (rc == ITERATOR_TIMEOUT) return rc;
  }

  if (base->lastDocId != nc->child->lastDocId) {
    nc->base.current = nc->virt;
  } else {
    nc->base.current = nc->child->current;
    nc->base.current->weight = nc->weight;
  }

  nc->base.current->docId = base->lastDocId;
  return ITERATOR_OK;
}

// Create a new OPTIONAL iterator - Non-Optimized version.
QueryIterator *IT_V2(NewOptionalIterator_NonOptimized)(QueryIterator *it, t_docId maxDocId, size_t numDocs, double weight) {
  OptionalIterator *nc = rm_calloc(1, sizeof(*nc));
  nc->child = it ? it : IT_V2(NewEmptyIterator)();
  nc->maxDocId = maxDocId;
  nc->virt = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  nc->virt->freq = 1;
  nc->weight = weight;

  QueryIterator *ret = &nc->base;
  ret->type = OPTIONAL_ITERATOR;
  ret->atEOF = false;
  ret->lastDocId = 0;
  ret->current = nc->virt;
  ret->NumEstimated = OI_NumEstimated;
  ret->Free = OI_Free;
  ret->Read = OI_ReadSorted_NO;
  ret->SkipTo = OI_SkipTo_NO;
  ret->Rewind = OI_Rewind;

  return ret;
}
