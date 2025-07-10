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

// pointer to the current block while reading the index
#define CURRENT_BLOCK(it) ((it)->idx->blocks[(it)->currentBlock])
#define CURRENT_BLOCK_READER_AT_END(it) BufferReader_AtEnd(&(it)->blockReader.buffReader)

void InvIndIterator_Free(QueryIterator *it) {
  if (!it) return;
  IndexResult_Free(it->current);
  rm_free(it);
}

static inline void SetCurrentBlockReader(InvIndIterator *it) {
  it->blockReader = (IndexBlockReader) {
    NewBufferReader(&CURRENT_BLOCK(it).buf),
    CURRENT_BLOCK(it).firstId,
  };
}

static inline void AdvanceBlock(InvIndIterator *it) {
  it->currentBlock++;
  SetCurrentBlockReader(it);
}

void InvIndIterator_Rewind(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  base->atEOF = false;
  base->lastDocId = 0;
  base->current->docId = 0;
  it->currentBlock = 0;
  it->gcMarker = it->idx->gcMarker;
  SetCurrentBlockReader(it);
}

size_t InvIndIterator_NumEstimated(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  return it->idx->numDocs;
}

#define FIELD_MASK_BIT_COUNT (sizeof(t_fieldMask) * 8)

static ValidateStatus EmptyCheckAbort(QueryIterator *base) {
  return VALIDATE_OK;
}

static ValidateStatus NumericCheckAbort(QueryIterator *base) {
  NumericInvIndIterator *nit = (NumericInvIndIterator *)base;
  InvIndIterator *it = (InvIndIterator *)base;

  // For numeric fields, we need to get the field name from the filter context
  // We need to use the field spec directly from the numeric filter
  const NumericFilter *filter = it->decoderCtx.filter;
  if (!filter) {
    // No filter means we can't check for revision changes
    return VALIDATE_OK;
  }

  RedisModuleString *numField = IndexSpec_GetFormattedKey(it->sctx->spec, filter->fieldSpec, INDEXFLD_T_NUMERIC);
  NumericRangeTree *rt = openNumericKeysDict(it->sctx->spec, numField, DONT_CREATE_INDEX);

  if (!rt || rt->revisionId != nit->revisionId) {
    // The numeric tree was either completely deleted or a node was split or removed.
    // The cursor is invalidated.
    base->isAborted = true;
    return VALIDATE_ABORTED;
  }

  return VALIDATE_OK;
}

static ValidateStatus TermCheckAbort(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  InvertedIndex *idx = Redis_OpenInvertedIndex(it->sctx, base->current->data.term.term->str,
      base->current->data.term.term->len, false, NULL);
  if (!idx || idx == TRIEMAP_NOTFOUND || it->idx != idx) {
    // The inverted index was collected entirely by GC.
    // All the documents that were inside were deleted and new ones were added.
    // We will not continue reading those new results and instead abort reading
    // for this specific inverted index.
    base->isAborted = true;
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus TagCheckAbort(QueryIterator *base) {
  TagInvIndIterator *it = (TagInvIndIterator *)base;
  size_t sz;
  // TODO(Joan): This is wrong, We may need to have another class for tag iterators where we include the TagIndex there
  InvertedIndex *idx = TagIndex_OpenIndex(it->tagIdx, base->current->data.term.term->str,
      base->current->data.term.term->len, false, &sz);
  if (!idx || idx == TRIEMAP_NOTFOUND || it->base.idx != idx) {
    // The inverted index was collected entirely by GC.
    // All the documents that were inside were deleted and new ones were added.
    // We will not continue reading those new results and instead abort reading
    // for this specific inverted index.

    base->isAborted = true;
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus InvIndIterator_Revalidate(QueryIterator *base) {
  // Here we should apply the specifics of Term, Tag and Numeric

  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    // Save time and state if we are already at the end
    return VALIDATE_OK;
  }

  ValidateStatus ret = it->CheckAbort(it);

  if(ret != VALIDATE_OK) {
    return ret;
  }

  // the gc marker tells us if there is a chance the keys has undergone GC while we were asleep
  if (it->gcMarker == it->idx->gcMarker) {
    // no GC - we just go to the same offset we were at
    size_t offset = it->blockReader.buffReader.pos;
    SetCurrentBlockReader(it);
    it->blockReader.buffReader.pos = offset;
    ret = VALIDATE_OK;
  } else {
    // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
    // anymore. This means that we need to seek to last docId we were at

    // keep the last docId we were at
    t_docId lastDocId = base->lastDocId;
    // reset the state of the reader
    base->Rewind(base);
    IteratorStatus rc = base->SkipTo(base, lastDocId);
    if (rc == ITERATOR_NOTFOUND) {
      ret = VALIDATE_MOVED;
    } else if (rc == ITERATOR_EOF) {
      ret = VALIDATE_OK;
    }
  }

  return ret;
}

// Used to determine if the field mask for the given doc id are valid based on their ttl:
// it->filterCtx.predicate
// returns true if the we don't have expiration information for the document
// otherwise will return the same as DocTable_VerifyFieldExpirationPredicate
// if predicate is default then it means at least one of the fields need to not be expired for us to return true
// if predicate is missing then it means at least one of the fields needs to be expired for us to return true
static inline bool VerifyFieldMaskExpirationForDocId(InvIndIterator *it, t_docId docId, t_fieldMask docFieldMask) {
  // If there isn't a ttl information then the doc fields are valid
  if (!it->sctx || !it->sctx->spec || !DocTable_HasExpiration(&it->sctx->spec->docs, docId)) {
    return true;
  }


  // TODO: move to constructor

  // doc has expiration information, create a field id array to check for expiration predicate
  size_t numFieldIndices = 0;
  // Use a stack allocated array for the field indices, if the field mask is not a single field
  t_fieldIndex fieldIndicesArray[FIELD_MASK_BIT_COUNT];
  t_fieldIndex* sortedFieldIndices = fieldIndicesArray;
  if (it->filterCtx.field.isFieldMask) {
    const t_fieldMask relevantMask = docFieldMask & it->filterCtx.field.value.mask;
    numFieldIndices = IndexSpec_TranslateMaskToFieldIndices(it->sctx->spec,
                                                            relevantMask,
                                                            fieldIndicesArray);
  } else if (it->filterCtx.field.value.index != RS_INVALID_FIELD_INDEX) {
    sortedFieldIndices = &it->filterCtx.field.value.index;
    numFieldIndices = 1;
  }
  return DocTable_VerifyFieldExpirationPredicate(&it->sctx->spec->docs, docId,
                                                 sortedFieldIndices, numFieldIndices,
                                                 it->filterCtx.predicate, &it->sctx->time.current);
}

IteratorStatus InvIndIterator_Read(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;
  while (true) {
    // if needed - advance to the next block
    if (CURRENT_BLOCK_READER_AT_END(it)) {
      if (it->currentBlock + 1 == it->idx->size) {
        // We're at the end of the last block...
        break;
      }
      AdvanceBlock(it);
    }

    // The decoder also acts as a filter. If the decoder returns false, the
    // current record should not be processed.
    // Since we are not at the end of the block (previous check), the decoder is guaranteed
    // to read a record (advanced by at least one entry).
    if (!it->decoders.decoder(&it->blockReader, &it->decoderCtx, record)) {
      continue;
    }

    if (it->skipMulti && base->lastDocId == record->docId) {
      // Avoid returning the same doc
      // Currently the only relevant predicate for multi-value is `any`, therefore only the first match in each doc is needed.
      // More advanced predicates, such as `at least <N>` or `exactly <N>`, will require adding more logic.
      continue;
    }

    if (!VerifyFieldMaskExpirationForDocId(it, record->docId, record->fieldMask)) {
      continue;
    }

    base->lastDocId = record->docId;
    return ITERATOR_OK;
  }
  base->atEOF = true;
  return ITERATOR_EOF;
}

#define BLOCK_MATCHES(blk, docId) ((blk).firstId <= docId && docId <= (blk).lastId)

// Assumes there is a valid block to skip to (matching or past the requested docId)
static inline void SkipToBlock(InvIndIterator *it, t_docId docId) {
  const InvertedIndex *idx = it->idx;
  uint32_t top = idx->size - 1;
  uint32_t bottom = it->currentBlock + 1;

  if (docId <= idx->blocks[bottom].lastId) {
    // the next block is the one we're looking for, although it might not contain the docId
    it->currentBlock = bottom;
    goto new_block;
  }

  uint32_t i;
  while (bottom <= top) {
    i = (bottom + top) / 2;
    if (BLOCK_MATCHES(idx->blocks[i], docId)) {
      it->currentBlock = i;
      goto new_block;
    }

    if (docId < idx->blocks[i].firstId) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
  }

  // We didn't find a matching block. According to the assumptions, there must be a block past the
  // requested docId, and the binary search brought us to it or the one before it.
  it->currentBlock = i;
  if (CURRENT_BLOCK(it).lastId < docId) {
    it->currentBlock++; // It's not the current block. Advance
    RS_ASSERT(CURRENT_BLOCK(it).firstId > docId); // Not a match but has to be past it
  }

new_block:
  RS_LOG_ASSERT(it->currentBlock < idx->size, "Invalid block index");
  SetCurrentBlockReader(it);
}

IteratorStatus InvIndIterator_SkipTo_Default(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  InvIndIterator *it = (InvIndIterator*)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  if (docId > it->idx->lastId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  if (CURRENT_BLOCK(it).lastId < docId) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(it, docId);
  }

  while (ITERATOR_EOF != InvIndIterator_Read(base)) {
    if (base->lastDocId < docId) continue;
    if (base->lastDocId == docId) return ITERATOR_OK;
    return ITERATOR_NOTFOUND;
  }
  return ITERATOR_EOF; // Assumes the call to "Read" set the `atEOF` flag
}

// Will use the seeker to reach a valid doc id that is greater or equal to the requested doc id
// returns true if a valid doc id was found, false if eof was reached
// The validity of the document relies on the predicate the reader was initialized with.
// Invariant: We only go forward, never backwards
static inline bool ReadWithSeeker(InvIndIterator *it, t_docId docId) {
  bool found = false;
  while (!found) {
    // if found is true we found a doc id that is greater or equal to the searched doc id
    // if found is false we need to continue scanning the inverted index, possibly advancing to the next block
    if (CURRENT_BLOCK_READER_AT_END(it)) {
      if (it->currentBlock + 1 < it->idx->size) {
        // We reached the end of the current block but we have more blocks to advance to
        // advance to the next block and continue the search using the seeker from there
        AdvanceBlock(it);
      } else {
        // we reached the end of the inverted index
        // we are at the end of the last block
        // break out of the loop and return found (found = false)
        break;
      }
    }

    // try and find docId using seeker
    found = it->decoders.seeker(&it->blockReader, &it->decoderCtx, docId, it->base.current);
    // ensure the entry is valid
    if (found && !VerifyFieldMaskExpirationForDocId(it, it->base.current->docId, it->base.current->fieldMask)) {
      // the doc id is not valid, filter out the doc id and continue scanning
      // we set docId to be the next doc id to search for to avoid infinite loop
      // IMPORTANT:
      // we still perform the AtEnd check to avoid the case the non valid doc id was at the end of the block
      // block: [1, 4, 7, ..., 564]
      //                        ^-- we are here, and 564 is not valid
      found = false;
      docId = it->base.current->docId + 1;
    }
  }
  // if found is true we found a doc id that is greater or equal to the searched doc id
  // if found is false we are at the end of the inverted index, no more blocks or doc ids
  // we could not find a valid doc id that is greater or equal to the doc id we were called with
  return found;
}

IteratorStatus InvIndIterator_SkipTo_withSeeker(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  InvIndIterator *it = (InvIndIterator*)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  if (docId > it->idx->lastId) {
    goto eof;
  }

  if (CURRENT_BLOCK(it).lastId < docId) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(it, docId);
  }

  // the seeker will return 1 only when it found a docid which is greater or equals the
  // searched docid and the field mask matches the searched fields mask. We need to continue
  // scanning only when we found such an id or we reached the end of the inverted index.
  if (!ReadWithSeeker(it, docId)) {
    goto eof;
  }
  // Found a document that match the field mask and greater or equal the searched docid
  base->lastDocId = it->base.current->docId;
  return (it->base.current->docId == docId) ? ITERATOR_OK : ITERATOR_NOTFOUND;

eof:
  base->atEOF = true;
  return ITERATOR_EOF;
}

static QueryIterator *InitInvIndIterator(InvIndIterator *it, InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                                        bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx, ValidateStatus (*checkAbortFn)(QueryIterator *)) {
  it->idx = idx;
  it->currentBlock = 0;
  it->gcMarker = idx->gcMarker;
  it->decoders = InvertedIndex_GetDecoder(idx->flags);
  it->decoderCtx = *decoderCtx;
  it->skipMulti = skipMulti;
  it->sctx = sctx;
  it->filterCtx = *filterCtx;
  it->isWildcard = false;
  it->CheckAbort = (ValidateStatus (*)(struct InvIndIterator *))checkAbortFn;

  SetCurrentBlockReader(it);

  it->base.current = res;
  it->base.isAborted = false;
  it->base.type = READ_ITERATOR;
  it->base.atEOF = false;
  it->base.lastDocId = 0;
  it->base.NumEstimated = InvIndIterator_NumEstimated;
  it->base.Read = InvIndIterator_Read;
  it->base.SkipTo = it->decoders.seeker ? InvIndIterator_SkipTo_withSeeker : InvIndIterator_SkipTo_Default;
  it->base.Free = InvIndIterator_Free;
  it->base.Rewind = InvIndIterator_Rewind;
  it->base.Revalidate = InvIndIterator_Revalidate;
  return &it->base;
}

static QueryIterator *NewInvIndIterator(InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                                        bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx, ValidateStatus (*checkAbortFn)(QueryIterator *)) {
  RS_ASSERT(idx && idx->size > 0);
  InvIndIterator *it = rm_calloc(1, sizeof(*it));
  return InitInvIndIterator(it, idx, res, filterCtx, skipMulti, sctx, decoderCtx, checkAbortFn);
}

static QueryIterator *NewInvIndIterator_NumericRange(InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx) {
  RS_ASSERT(idx && idx->size > 0);
  NumericInvIndIterator *it = rm_calloc(1, sizeof(*it));

  // Initialize the iterator first
  InitInvIndIterator(&it->base, idx, res, filterCtx, skipMulti, sctx, decoderCtx, NumericCheckAbort);

  // Get the numeric field key and retrieve the NumericRangeTree
  const NumericFilter *filter = decoderCtx->filter;
  RS_ASSERT(filter);
  RedisModuleString *numField = IndexSpec_GetFormattedKey(sctx->spec, filter->fieldSpec, INDEXFLD_T_NUMERIC);
  NumericRangeTree *rt = openNumericKeysDict(sctx->spec, numField, DONT_CREATE_INDEX);
  if (rt) {
    it->revisionId = rt->revisionId;
  }

  return &it->base.base;
}

QueryIterator *NewInvIndIterator_NumericFull(InvertedIndex *idx) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.filter = NULL};
  return NewInvIndIterator_NumericRange(idx, NewNumericResult(), &fieldCtx, false, NULL, &decoderCtx);
}

QueryIterator *NewInvIndIterator_TermFull(InvertedIndex *idx) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.wideMask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *res = NewTokenRecord(NULL, 1);
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  return NewInvIndIterator(idx, res, &fieldCtx, false, NULL, &decoderCtx, TermCheckAbort);
}

QueryIterator *NewInvIndIterator_TagFull(InvertedIndex *idx, TagIndex *tagIdx) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.wideMask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *res = NewTokenRecord(NULL, 1);
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  TagInvIndIterator *it = rm_calloc(1, sizeof(*it));
  it->tagIdx = tagIdx;
  return InitInvIndIterator(&it->base, idx, res, &fieldCtx, false, NULL, &decoderCtx, TagCheckAbort);
}

QueryIterator *NewInvIndIterator_NumericQuery(InvertedIndex *idx, const RedisSearchCtx *sctx, const FieldFilterContext* fieldCtx,
                                              const NumericFilter *flt, double rangeMin, double rangeMax) {
  IndexDecoderCtx decoderCtx = {.filter = flt};
  QueryIterator *ret = NewInvIndIterator_NumericRange(idx, NewNumericResult(), fieldCtx, true, sctx, &decoderCtx);
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
  return log(1.0F + (totalDocs - termDocs + 0.5F) / (termDocs + 0.5F));
}

QueryIterator *NewInvIndIterator_TermQuery(InvertedIndex *idx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight) {
  FieldFilterContext fieldCtx = {
    .field = fieldMaskOrIndex,
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  if (term && sctx) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sctx->spec->docs.size, idx->numDocs);
    term->bm25_idf = CalculateIDF_BM25(sctx->spec->docs.size, idx->numDocs);
  }

  RSIndexResult *record = NewTokenRecord(term, weight);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {0};
  if (fieldMaskOrIndex.isFieldMask && (idx->flags & Index_WideSchema))
    dctx.wideMask = fieldMaskOrIndex.value.mask;
  else if (fieldMaskOrIndex.isFieldMask)
    dctx.mask = fieldMaskOrIndex.value.mask;
  else
    dctx.wideMask = RS_FIELDMASK_ALL; // Also covers the case of a non-wide schema

  return NewInvIndIterator(idx, record, &fieldCtx, true, sctx, &dctx, TermCheckAbort);
}

QueryIterator *NewInvIndIterator_TagQuery(InvertedIndex *idx, TagIndex *tagIdx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight) {

  FieldFilterContext fieldCtx = {
    .field = fieldMaskOrIndex,
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };

  RSIndexResult *record = NewTokenRecord(term, weight);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {0};
  if (fieldMaskOrIndex.isFieldMask && (idx->flags & Index_WideSchema))
    dctx.wideMask = fieldMaskOrIndex.value.mask;
  else if (fieldMaskOrIndex.isFieldMask)
    dctx.mask = fieldMaskOrIndex.value.mask;
  else
    dctx.wideMask = RS_FIELDMASK_ALL; // Also covers the case of a non-wide schema

  TagInvIndIterator *it = rm_calloc(1, sizeof(*it));
  it->tagIdx = tagIdx;
  return InitInvIndIterator(&it->base, idx, record, &fieldCtx, true, sctx, &dctx, TagCheckAbort);
}


QueryIterator *NewInvIndIterator_GenericQuery(InvertedIndex *idx, const RedisSearchCtx *sctx, t_fieldIndex fieldIndex,
                                              enum FieldExpirationPredicate predicate, double weight) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = fieldIndex}},
    .predicate = predicate,
  };
  IndexDecoderCtx decoderCtx = {.wideMask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *record = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  record->freq = (predicate == FIELD_EXPIRATION_MISSING) ? 0 : 1; // TODO: is this required?
  return NewInvIndIterator(idx, record, &fieldCtx, true, sctx, &decoderCtx, EmptyCheckAbort);
}
