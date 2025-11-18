/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "inverted_index_iterator.h"
#include "redis_index.h"

void InvIndIterator_Free(QueryIterator *it) {
  if (!it) return;
  IndexResult_Free(it->current);
  IndexReader_Free(((InvIndIterator *)it)->reader);
  rm_free(it);
}

void InvIndIterator_Rewind(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  base->atEOF = false;
  base->lastDocId = 0;
  base->current->docId = 0;

  IndexReader_Reset(it->reader);
}

size_t InvIndIterator_NumEstimated(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  return IndexReader_NumEstimated(it->reader);
}

static ValidateStatus NumericCheckAbort(QueryIterator *base) {
  NumericInvIndIterator *nit = (NumericInvIndIterator *)base;
  InvIndIterator *it = (InvIndIterator *)base;

  if (!it->sctx) {
    return VALIDATE_OK;
  }

  if (nit->rt->revisionId != nit->revisionId) {
    // The numeric tree was either completely deleted or a node was split or removed.
    // The cursor is invalidated.
    return VALIDATE_ABORTED;
  }

  return VALIDATE_OK;
}

static ValidateStatus TermCheckAbort(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (!it->sctx) {
    return VALIDATE_OK;
  }
  RSQueryTerm *term = IndexResult_QueryTermRef(base->current);
  InvertedIndex *idx = Redis_OpenInvertedIndex(it->sctx, term->str, term->len, false, NULL);
  if (!idx || !IndexReader_IsIndex(it->reader, idx)) {
    // The inverted index was collected entirely by GC.
    // All the documents that were inside were deleted and new ones were added.
    // We will not continue reading those new results and instead abort reading
    // for this specific inverted index.
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus TagCheckAbort(QueryIterator *base) {
  TagInvIndIterator *it = (TagInvIndIterator *)base;
  if (!it->base.sctx) {
    return VALIDATE_OK;
  }
  size_t sz;
  RSQueryTerm *term = IndexResult_QueryTermRef(base->current);
  InvertedIndex *idx = TagIndex_OpenIndex(it->tagIdx, term->str, term->len, false, &sz);
  if (idx == TRIEMAP_NOTFOUND || !IndexReader_IsIndex(it->base.reader, idx)) {
    // The inverted index was collected entirely by GC.
    // All the documents that were inside were deleted and new ones were added.
    // We will not continue reading those new results and instead abort reading
    // for this specific inverted index.

    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus WildcardCheckAbort(QueryIterator *base) {
  // Check if the wildcard iterator is still valid
  InvIndIterator *wi = (InvIndIterator *)base;
  RS_ASSERT(wi->sctx && wi->sctx->spec);

  if (!IndexReader_IsIndex(wi->reader, wi->sctx->spec->existingDocs)) {
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus MissingCheckAbort(QueryIterator *base) {
  // Check if the missing iterator is still valid
  InvIndIterator *mi = (InvIndIterator *)base;
  RS_ASSERT(mi->sctx && mi->sctx->spec);
  RS_ASSERT(mi->sctx->spec->missingFieldDict);
  RS_ASSERT(mi->sctx->spec->numFields > mi->filterCtx.field.index);

  const HiddenString *fieldName = mi->sctx->spec->fields[mi->filterCtx.field.index].fieldName;
  const InvertedIndex *missingII = dictFetchValue(mi->sctx->spec->missingFieldDict, fieldName);

  if (!IndexReader_IsIndex(mi->reader, missingII)) {
    // The inverted index was collected entirely by GC.
    // All the documents that were inside were deleted and new ones were added.
    // We will not continue reading those new results and instead abort reading
    // for this specific inverted index.
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus InvIndIterator_Revalidate(QueryIterator *base) {
  // Here we should apply the specifics of Term, Tag and Numeric

  InvIndIterator *it = (InvIndIterator *)base;
  ValidateStatus ret = it->CheckAbort(it);

  if (ret != VALIDATE_OK) {
    return ret;
  }

  if (IndexReader_Revalidate(it->reader)) {
    // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
    // anymore. This means that we need to seek the last docId we were at

    // keep the last docId we were at
    t_docId lastDocId = base->lastDocId;
    // reset the state of the reader
    base->Rewind(base);
    if (lastDocId && base->SkipTo(base, lastDocId) != ITERATOR_OK) { // Cannot skip to 0!
      ret = VALIDATE_MOVED;
    }
  }

  return ret;
}

// Used to determine if the field mask for the given doc id are valid based on their ttl:
// it->filterCtx.predicate
// returns true if the we don't have expiration information for the document
// otherwise will return the same as DocTable_CheckFieldExpirationPredicate
// if predicate is default then it means at least one of the fields need to not be expired for us to return true
// if predicate is missing then it means at least one of the fields needs to be expired for us to return true
static inline bool VerifyFieldMaskExpirationForCurrent(InvIndIterator *it) {
  // If the field is not a mask, then we can just check the single field
  if (it->filterCtx.field.tag == FieldMaskOrIndex_Index) {
    return DocTable_CheckFieldExpirationPredicate(
      &it->sctx->spec->docs,
      it->base.current->docId,
      it->filterCtx.field.index,
      it->filterCtx.predicate,
      &it->sctx->time.current
    );
  } else if (IndexReader_Flags(it->reader) & Index_WideSchema) {
    return DocTable_CheckWideFieldMaskExpirationPredicate(
      &it->sctx->spec->docs,
      it->base.current->docId,
      it->base.current->fieldMask & it->filterCtx.field.mask,
      it->filterCtx.predicate,
      &it->sctx->time.current,
      it->sctx->spec->fieldIdToIndex
    );
  } else {
    return DocTable_CheckFieldMaskExpirationPredicate(
      &it->sctx->spec->docs,
      it->base.current->docId,
      it->base.current->fieldMask & it->filterCtx.field.mask,
      it->filterCtx.predicate,
      &it->sctx->time.current,
      it->sctx->spec->fieldIdToIndex
    );
  }
}

/************************************* Read Implementations *************************************/

// 1. Default read implementation, without any additional filtering.
IteratorStatus InvIndIterator_Read_Default(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;

  if (IndexReader_Next(it->reader, record)) {
    base->lastDocId = record->docId;
    return ITERATOR_OK;
  }

  // Exit outer loop => we reached the end of the last block
  base->atEOF = true;
  return ITERATOR_EOF;
}

// 2. Read implementation that skips multi-value entries from the same document.
IteratorStatus InvIndIterator_Read_SkipMulti(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;

  while (IndexReader_Next(it->reader, record)) {
    if (base->lastDocId == record->docId) {
      // Avoid returning the same doc
      // Currently the only relevant predicate for multi-value is `any`, therefore only the first match in each doc is needed.
      // More advanced predicates, such as `at least <N>` or `exactly <N>`, will require adding more logic.
      continue;
    }

    base->lastDocId = record->docId;
    return ITERATOR_OK;
  }

  // Exit outer loop => we reached the end of the last block
  base->atEOF = true;
  return ITERATOR_EOF;
}

// 3. Read implementation that skips entries based on field mask expiration.
IteratorStatus InvIndIterator_Read_CheckExpiration(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;

  while (IndexReader_Next(it->reader, record)) {
    if (!VerifyFieldMaskExpirationForCurrent(it)) {
      continue;
    }

    base->lastDocId = record->docId;
    return ITERATOR_OK;
  }

  // Exit outer loop => we reached the end of the last block
  base->atEOF = true;
  return ITERATOR_EOF;
}

// 4. Read implementation that combines skipping multi-value entries and checking field mask expiration.
IteratorStatus InvIndIterator_Read_SkipMulti_CheckExpiration(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;

  while (IndexReader_Next(it->reader, record)) {
    if (base->lastDocId == record->docId) {
      // Avoid returning the same doc
      // Currently the only relevant predicate for multi-value is `any`, therefore only the first match in each doc is needed.
      // More advanced predicates, such as `at least <N>` or `exactly <N>`, will require adding more logic.
      continue;
    }

    if (!VerifyFieldMaskExpirationForCurrent(it)) {
      continue;
    }

    base->lastDocId = record->docId;
    return ITERATOR_OK;
  }

  // Exit outer loop => we reached the end of the last block
  base->atEOF = true;
  return ITERATOR_EOF;
}

/************************************ SkipTo Implementations ************************************/

// SkipTo implementation that uses a seeker to find the next valid docId, no additional filtering.
IteratorStatus InvIndIterator_SkipTo(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  InvIndIterator *it = (InvIndIterator*)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  if (!IndexReader_SkipTo(it->reader, docId)) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc;
  if (IndexReader_Seek(it->reader, docId, base->current)) {
    // The seeker found a doc id that is greater or equal to the requested doc id
    // in the current block
    base->lastDocId = base->current->docId;
    rc = (base->current->docId == docId) ? ITERATOR_OK : ITERATOR_NOTFOUND;
  } else {
    // The seeker did not find a doc id that is greater or equal to the requested doc id
    // in the current block, we need to read the next valid result
    // Even if we need to skip multi-values, we know the target docId is greater than the lastDocId,
    // so we use the default read implementation without any additional filtering.
    rc = InvIndIterator_Read_Default(base);
    if (rc == ITERATOR_OK) {
      rc = ITERATOR_NOTFOUND;
    }
  }
  return rc;
}

// SkipTo implementation that uses a seeker and checks for field expiration.
IteratorStatus InvIndIterator_SkipTo_CheckExpiration(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  InvIndIterator *it = (InvIndIterator*)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  if (!IndexReader_SkipTo(it->reader, docId)) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  IteratorStatus rc;
  if (IndexReader_Seek(it->reader, docId, base->current) &&
      VerifyFieldMaskExpirationForCurrent(it)) {
    // The seeker found a doc id that is greater or equal to the requested doc id
    // in the current block, and the doc id is valid
    base->lastDocId = base->current->docId;
    rc = (base->current->docId == docId) ? ITERATOR_OK : ITERATOR_NOTFOUND;
  } else {
    // The seeker did not find a doc id that is greater or equal to the requested doc id
    // in the current block, or the doc id is not valid (expired).
    // We need to read the next valid result
    rc = InvIndIterator_Read_CheckExpiration(base);
    if (rc == ITERATOR_OK) {
      rc = ITERATOR_NOTFOUND;
    }
  }
  return rc;
}

// Returns true if the iterator should check for expiration of the field/s
static inline bool HasExpiration(const InvIndIterator *it) {
  // TODO: better estimation
  // check if the specific field/fieldMask has expiration, according to the `filterCtx`
  return it->sctx && it->sctx->spec->docs.ttl && it->sctx->spec->monitorFieldExpiration &&                // Has TTL info
         (it->filterCtx.field.tag == FieldMaskOrIndex_Mask || it->filterCtx.field.index != RS_INVALID_FIELD_INDEX);  // Context is a field mask or a valid index
}

// Returns true if the iterator should skip multi-values from the same document
static inline bool ShouldSkipMulti(const InvIndIterator *it) {
  return it->skipMulti &&                       // Skip multi-values is requested
        IndexReader_HasMulti(it->reader); // The index holds multi-values (if not, no need to check)
}

static QueryIterator *InitInvIndIterator(InvIndIterator *it, const InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                                        bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx, ValidateStatus (*checkAbortFn)(QueryIterator *)) {
  it->reader = NewIndexReader(idx, *decoderCtx);
  it->skipMulti = skipMulti; // Original request, regardless of what implementation is chosen
  it->sctx = sctx;
  it->filterCtx = *filterCtx;
  it->isWildcard = false;
  it->CheckAbort = (ValidateStatus (*)(struct InvIndIterator *))checkAbortFn;

  QueryIterator *base = &it->base;
  base->current = res;
  base->type = INV_IDX_ITERATOR;
  base->atEOF = false;
  base->lastDocId = 0;
  base->NumEstimated = InvIndIterator_NumEstimated;
  base->Free = InvIndIterator_Free;
  base->Rewind = InvIndIterator_Rewind;
  base->Revalidate = InvIndIterator_Revalidate;

  // Choose the Read and SkipTo methods for best performance
  skipMulti = ShouldSkipMulti(it);
  bool hasExpiration = HasExpiration(it);

  // Read function choice:
  // skip multi     |  no                   |  yes
  // ------------------------------------------------------------------------
  // no expiration  |  Read_Default         |  Read_SkipMulti
  // expiration     |  Read_CheckExpiration |  Read_SkipMulti_CheckExpiration

  if (skipMulti && hasExpiration) {
    base->Read = InvIndIterator_Read_SkipMulti_CheckExpiration;
  } else if (skipMulti) { // skipMulti && !hasExpiration
    base->Read = InvIndIterator_Read_SkipMulti;
  } else if (hasExpiration) { // !skipMulti && hasExpiration
    base->Read = InvIndIterator_Read_CheckExpiration;
  } else { // !skipMulti && !hasExpiration
    base->Read = InvIndIterator_Read_Default;
  }

  if (hasExpiration) {
    base->SkipTo = InvIndIterator_SkipTo_CheckExpiration;
  } else {
    base->SkipTo = InvIndIterator_SkipTo;
  }

  return base;
}

static QueryIterator *NewInvIndIterator(const InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                                        bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx, ValidateStatus (*checkAbortFn)(QueryIterator *)) {
  RS_ASSERT(idx);
  InvIndIterator *it = rm_calloc(1, sizeof(*it));
  return InitInvIndIterator(it, idx, res, filterCtx, skipMulti, sctx, decoderCtx, checkAbortFn);
}

static QueryIterator *NewInvIndIterator_NumericRange(const InvertedIndex *idx, RSIndexResult *res, const NumericRangeTree *rt, const FieldFilterContext *filterCtx,
                bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx) {
  RS_ASSERT(idx);
  NumericInvIndIterator *it = rm_calloc(1, sizeof(*it));

  // Initialize the iterator first
  InitInvIndIterator(&it->base, idx, res, filterCtx, skipMulti, sctx, decoderCtx, NumericCheckAbort);

  if (rt) {
    it->revisionId = rt->revisionId;
    it->rt = rt;
  }

  return &it->base.base;
}

QueryIterator *NewInvIndIterator_NumericFull(const InvertedIndex *idx) {
  FieldFilterContext fieldCtx = {
    .field = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.tag = IndexDecoderCtx_None};
  return NewInvIndIterator_NumericRange(idx, NewNumericResult(), NULL, &fieldCtx, false, NULL, &decoderCtx);
}

QueryIterator *NewInvIndIterator_TermFull(const InvertedIndex *idx) {
  FieldFilterContext fieldCtx = {
    .field = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *res = NewTokenRecord(NULL, 1);
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  return NewInvIndIterator(idx, res, &fieldCtx, false, NULL, &decoderCtx, TermCheckAbort);
}

QueryIterator *NewInvIndIterator_TagFull(const InvertedIndex *idx, const TagIndex *tagIdx) {
  FieldFilterContext fieldCtx = {
    .field = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *res = NewTokenRecord(NULL, 1);
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  TagInvIndIterator *it = rm_calloc(1, sizeof(*it));
  it->tagIdx = tagIdx;
  return InitInvIndIterator(&it->base, idx, res, &fieldCtx, false, NULL, &decoderCtx, TagCheckAbort);
}

QueryIterator *NewInvIndIterator_NumericQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, const FieldFilterContext* fieldCtx,
                                              const NumericFilter *flt, const NumericRangeTree *rt, double rangeMin, double rangeMax) {
  IndexDecoderCtx decoderCtx = {.tag = IndexDecoderCtx_None};

  if (flt) {
    decoderCtx = (IndexDecoderCtx){.numeric_tag = IndexDecoderCtx_Numeric, .numeric = flt};
  }

  QueryIterator *ret = NewInvIndIterator_NumericRange(idx, NewNumericResult(), rt, fieldCtx, true, sctx, &decoderCtx);
  InvIndIterator *it = (InvIndIterator *)ret;
  it->profileCtx.numeric.rangeMin = rangeMin;
  it->profileCtx.numeric.rangeMax = rangeMax;
  return ret;
}

static inline double CalculateIDF(size_t totalDocs, size_t termDocs) {
  return logb(1.0F + totalDocs / (double)(termDocs ?: 1));
}

// IDF computation for BM25 standard scoring algorithm (which is slightly different from the regular
// IDF computation).
static inline double CalculateIDF_BM25(size_t totalDocs, size_t termDocs) {
  // totalDocs should never be less than termDocs, as that causes an underflow
  // wraparound in the below calculation.
  // Yet, that can happen in some scenarios of deletions/updates, until fixed in
  // the next GC run.
  // In that case, we set totalDocs to termDocs, as a temporary fix.
  totalDocs = MAX(totalDocs, termDocs);
  return log(1.0F + (totalDocs - termDocs + 0.5F) / (termDocs + 0.5F));
}

QueryIterator *NewInvIndIterator_TermQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight) {
  FieldFilterContext fieldCtx = {
    .field = fieldMaskOrIndex,
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  if (term && sctx) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sctx->spec->docs.size, InvertedIndex_NumDocs(idx)); // FIXME: docs.size starts at 1???
    term->bm25_idf = CalculateIDF_BM25(sctx->spec->stats.numDocuments, InvertedIndex_NumDocs(idx));
  }

  RSIndexResult *record = NewTokenRecord(term, weight);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {.tag = IndexDecoderCtx_FieldMask};
  if (fieldMaskOrIndex.tag == FieldMaskOrIndex_Mask) {
    dctx.field_mask = fieldMaskOrIndex.mask;
  } else {
    dctx.field_mask = RS_FIELDMASK_ALL; // Also covers the case of a non-wide schema
  }

  return NewInvIndIterator(idx, record, &fieldCtx, true, sctx, &dctx, TermCheckAbort);
}

QueryIterator *NewInvIndIterator_TagQuery(const InvertedIndex *idx, const TagIndex *tagIdx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight) {

  FieldFilterContext fieldCtx = {
    .field = fieldMaskOrIndex,
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  if (term && sctx) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sctx->spec->docs.size, InvertedIndex_NumDocs(idx)); // FIXME: docs.size starts at 1???
    term->bm25_idf = CalculateIDF_BM25(sctx->spec->stats.numDocuments, InvertedIndex_NumDocs(idx));
  }

  RSIndexResult *record = NewTokenRecord(term, weight);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {.tag = IndexDecoderCtx_FieldMask};
  if (fieldMaskOrIndex.tag == FieldMaskOrIndex_Mask) {
    dctx.field_mask = fieldMaskOrIndex.mask;
  } else {
    dctx.field_mask = RS_FIELDMASK_ALL; // Also covers the case of a non-wide schema
  }

  TagInvIndIterator *it = rm_calloc(1, sizeof(*it));
  it->tagIdx = tagIdx;
  return InitInvIndIterator(&it->base, idx, record, &fieldCtx, true, sctx, &dctx, TagCheckAbort);
}

QueryIterator *NewInvIndIterator_WildcardQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, double weight) {
  FieldFilterContext fieldCtx = {
    .field = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL};
  RSIndexResult *record = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  record->freq = 1;

  InvIndIterator *it = rm_calloc(1, sizeof(*it));
  InitInvIndIterator(it, idx, record, &fieldCtx, true, sctx, &decoderCtx, WildcardCheckAbort);
  it->isWildcard = true; // Mark as wildcard iterator
  return &it->base;
}

QueryIterator *NewInvIndIterator_MissingQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, t_fieldIndex fieldIndex) {
  FieldFilterContext fieldCtx = {
    .field = {.index_tag = FieldMaskOrIndex_Index, .index = fieldIndex},
    .predicate = FIELD_EXPIRATION_MISSING, // Missing predicate
  };
  IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *record = NewVirtualResult(0.0, RS_FIELDMASK_ALL);
  record->freq = 1;

  return NewInvIndIterator(idx, record, &fieldCtx, true, sctx, &decoderCtx, MissingCheckAbort);
}
