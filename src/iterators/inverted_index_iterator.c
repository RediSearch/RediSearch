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
    NewBufferReader(IndexBlock_Buffer(&CURRENT_BLOCK(it))),
    IndexBlock_FirstId(&CURRENT_BLOCK(it)),
  };
}

static inline void AdvanceBlock(InvIndIterator *it) {
  it->currentBlock++;
  SetCurrentBlockReader(it);
}

// A while-loop helper to advance the iterator to the next block or break if we are at the end.
static __attribute__((always_inline)) inline bool NotAtEnd(InvIndIterator *it) {
  if (!CURRENT_BLOCK_READER_AT_END(it)) {
    return true; // still have entries in the current block
  }
  if (it->currentBlock + 1 < it->idx->size) {
    // we have more blocks to read, so we can advance to the next block
    AdvanceBlock(it);
    return true;
  }
  // no more blocks to read, so we are at the end
  return false;
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

static ValidateStatus EmptyCheckAbort(QueryIterator *base) {
  return VALIDATE_OK;
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
  InvertedIndex *idx = Redis_OpenInvertedIndex(it->sctx, base->current->data.term.term->str,
      base->current->data.term.term->len, false, NULL);
  if (!idx || it->idx != idx) {
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
  InvertedIndex *idx = TagIndex_OpenIndex(it->tagIdx, base->current->data.term.term->str,
      base->current->data.term.term->len, false, &sz);
  if (idx == TRIEMAP_NOTFOUND || it->base.idx != idx) {
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
  RS_ASSERT(wi->idx);
  RS_ASSERT(wi->sctx && wi->sctx->spec);

  if (wi->sctx->spec->existingDocs != wi->idx) {
    return VALIDATE_ABORTED;
  }
  return VALIDATE_OK;
}

static ValidateStatus MissingCheckAbort(QueryIterator *base) {
  // Check if the missing iterator is still valid
  InvIndIterator *mi = (InvIndIterator *)base;
  RS_ASSERT(mi->idx);
  RS_ASSERT(mi->sctx && mi->sctx->spec);
  RS_ASSERT(mi->sctx->spec->missingFieldDict);
  RS_ASSERT(mi->sctx->spec->numFields > mi->filterCtx.field.value.index);

  const HiddenString *fieldName = mi->sctx->spec->fields[mi->filterCtx.field.value.index]->fieldName;
  const InvertedIndex *missingII = dictFetchValue(mi->sctx->spec->missingFieldDict, fieldName);

  if (mi->idx != missingII) {
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

  // the gc marker tells us if there is a chance the keys have undergone GC while we were asleep
  if (it->gcMarker == it->idx->gcMarker) {
    // no GC - we just go to the same offset we were at.
    // Reset the buffer pointer in case it was reallocated
    it->blockReader.buffReader.buf = IndexBlock_Buffer(&CURRENT_BLOCK(it));
  } else {
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
  if (!it->filterCtx.field.isFieldMask) {
    return DocTable_CheckFieldExpirationPredicate(
      &it->sctx->spec->docs,
      it->base.current->docId,
      it->filterCtx.field.value.index,
      it->filterCtx.predicate,
      &it->sctx->time.current
    );
  } else if (it->idx->flags & Index_WideSchema) {
    return DocTable_CheckWideFieldMaskExpirationPredicate(
      &it->sctx->spec->docs,
      it->base.current->docId,
      it->base.current->fieldMask & it->filterCtx.field.value.mask,
      it->filterCtx.predicate,
      &it->sctx->time.current,
      it->sctx->spec->fieldIdToIndex
    );
  } else {
    return DocTable_CheckFieldMaskExpirationPredicate(
      &it->sctx->spec->docs,
      it->base.current->docId,
      it->base.current->fieldMask & it->filterCtx.field.value.mask,
      it->filterCtx.predicate,
      &it->sctx->time.current,
      it->sctx->spec->fieldIdToIndex
    );
  }
}

#define BLOCK_MATCHES(blk, docId) (IndexBlock_FirstId(&blk) <= docId && docId <= IndexBlock_LastId(&blk))

// Assumes there is a valid block to skip to (matching or past the requested docId)
static inline void SkipToBlock(InvIndIterator *it, t_docId docId) {
  const InvertedIndex *idx = it->idx;
  uint32_t top = idx->size - 1;
  uint32_t bottom = it->currentBlock + 1;

  if (docId <= IndexBlock_LastId(&idx->blocks[bottom])) {
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

    t_docId firstId = IndexBlock_FirstId(&idx->blocks[i]);
    if (docId < firstId) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
  }

  // We didn't find a matching block. According to the assumptions, there must be a block past the
  // requested docId, and the binary search brought us to it or the one before it.
  it->currentBlock = i;
  t_docId lastId = IndexBlock_LastId(&CURRENT_BLOCK(it));
  if (lastId < docId) {
    it->currentBlock++; // It's not the current block. Advance
    RS_ASSERT(IndexBlock_FirstId(&CURRENT_BLOCK(it)) > docId); // Not a match but has to be past it
  }

new_block:
  RS_LOG_ASSERT(it->currentBlock < idx->size, "Invalid block index");
  SetCurrentBlockReader(it);
}

/************************************* Read Implementations *************************************/

// 1. Default read implementation, without any additional filtering.
IteratorStatus InvIndIterator_Read_Default(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;
  while (NotAtEnd(it)) {
    // The decoder also acts as a filter. If the decoder returns false, the
    // current record should not be processed.
    // Since we are not at the end of the block (previous check), the decoder is guaranteed
    // to read a record (advanced by at least one entry).
    if (it->decoders.decoder(&it->blockReader, &it->decoderCtx, record)) {
      base->lastDocId = record->docId;
      return ITERATOR_OK;
    }
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
  while (NotAtEnd(it)) {
    // The decoder also acts as a filter. If the decoder returns false, the
    // current record should not be processed.
    // Since we are not at the end of the block (previous check), the decoder is guaranteed
    // to read a record (advanced by at least one entry).
    if (!it->decoders.decoder(&it->blockReader, &it->decoderCtx, record)) {
      continue;
    }

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
  while (NotAtEnd(it)) {
    // The decoder also acts as a filter. If the decoder returns false, the
    // current record should not be processed.
    // Since we are not at the end of the block (previous check), the decoder is guaranteed
    // to read a record (advanced by at least one entry).
    if (!it->decoders.decoder(&it->blockReader, &it->decoderCtx, record)) {
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

// 4. Read implementation that combines skipping multi-value entries and checking field mask expiration.
IteratorStatus InvIndIterator_Read_SkipMulti_CheckExpiration(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }
  RSIndexResult *record = base->current;
  while (NotAtEnd(it)) {
    // The decoder also acts as a filter. If the decoder returns false, the
    // current record should not be processed.
    // Since we are not at the end of the block (previous check), the decoder is guaranteed
    // to read a record (advanced by at least one entry).
    if (!it->decoders.decoder(&it->blockReader, &it->decoderCtx, record)) {
      continue;
    }

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

// 1. Default SkipTo implementation, without any additional filtering.
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

  t_docId lastId = IndexBlock_LastId(&CURRENT_BLOCK(it));
  if (lastId < docId) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(it, docId);
  }

  // Even if we need to skip multi-values, we know the target docId is greater than the lastDocId,
  // so we use the default read implementation without any additional filtering.
  while (ITERATOR_EOF != InvIndIterator_Read_Default(base)) {
    if (base->lastDocId < docId) continue;
    if (base->lastDocId == docId) return ITERATOR_OK;
    return ITERATOR_NOTFOUND;
  }
  return ITERATOR_EOF; // Assumes the call to "Read" set the `atEOF` flag
}

// 2. SkipTo implementation that filters out expired results
IteratorStatus InvIndIterator_SkipTo_CheckExpiration(QueryIterator *base, t_docId docId) {
  RS_ASSERT(base->lastDocId < docId);
  InvIndIterator *it = (InvIndIterator*)base;
  if (base->atEOF) {
    return ITERATOR_EOF;
  }

  if (docId > it->idx->lastId) {
    base->atEOF = true;
    return ITERATOR_EOF;
  }

  t_docId lastId = IndexBlock_LastId(&CURRENT_BLOCK(it));
  if (lastId < docId) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(it, docId);
  }

  // Even if we need to skip multi-values, we know the target docId is greater than the lastDocId,
  // so we use the default read implementation without any additional filtering.
  while (ITERATOR_EOF != InvIndIterator_Read_CheckExpiration(base)) {
    if (base->lastDocId < docId) continue;
    if (base->lastDocId == docId) return ITERATOR_OK;
    return ITERATOR_NOTFOUND;
  }
  return ITERATOR_EOF; // Assumes the call to "Read" set the `atEOF` flag
}

// 3. SkipTo implementation that uses a seeker to find the next valid docId, no additional filtering.
IteratorStatus InvIndIterator_SkipTo_withSeeker(QueryIterator *base, t_docId docId) {
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

  IteratorStatus rc;
  if (it->decoders.seeker(&it->blockReader, &it->decoderCtx, docId, it->base.current)) {
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

// 4. SkipTo implementation that uses a seeker and checks for field expiration.
IteratorStatus InvIndIterator_SkipTo_withSeeker_CheckExpiration(QueryIterator *base, t_docId docId) {
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

  IteratorStatus rc;
  if (it->decoders.seeker(&it->blockReader, &it->decoderCtx, docId, it->base.current) &&
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
         (it->filterCtx.field.isFieldMask || it->filterCtx.field.value.index != RS_INVALID_FIELD_INDEX);  // Context is a field mask or a valid index
}

// Returns true if the iterator should skip multi-values from the same document
static inline bool ShouldSkipMulti(const InvIndIterator *it) {
  return it->skipMulti &&                       // Skip multi-values is requested
        (it->idx->flags & Index_HasMultiValue); // The index holds multi-values (if not, no need to check)
}

static QueryIterator *InitInvIndIterator(InvIndIterator *it, const InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                                        bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx, ValidateStatus (*checkAbortFn)(QueryIterator *)) {
  it->idx = idx;
  it->currentBlock = 0;
  it->gcMarker = idx->gcMarker;
  it->decoders = InvertedIndex_GetDecoder(idx->flags);
  it->decoderCtx = *decoderCtx;
  it->skipMulti = skipMulti; // Original request, regardless of what implementation is chosen
  it->sctx = sctx;
  it->filterCtx = *filterCtx;
  it->isWildcard = false;
  it->CheckAbort = (ValidateStatus (*)(struct InvIndIterator *))checkAbortFn;

  SetCurrentBlockReader(it);

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
  bool hasSeeker = it->decoders.seeker != NULL;
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

  // SkipTo function choice:
  // has seeker     |  no                      |  yes
  // ------------------------------------------------------------------------------
  // no expiration  |  SkipTo_Default          |  SkipTo_withSeeker
  // expiration     |  SkipTo_CheckExpiration  |  SkipTo_withSeeker_CheckExpiration

  if (hasSeeker && hasExpiration) {
    base->SkipTo = InvIndIterator_SkipTo_withSeeker_CheckExpiration;
  } else if (hasSeeker) { // hasSeeker && !hasExpiration
    base->SkipTo = InvIndIterator_SkipTo_withSeeker;
  } else if (hasExpiration) { // !hasSeeker && hasExpiration
    base->SkipTo = InvIndIterator_SkipTo_CheckExpiration;
  } else { // !hasSeeker && !hasExpiration
    base->SkipTo = InvIndIterator_SkipTo_Default;
  }

  return base;
}

static QueryIterator *NewInvIndIterator(const InvertedIndex *idx, RSIndexResult *res, const FieldFilterContext *filterCtx,
                                        bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx, ValidateStatus (*checkAbortFn)(QueryIterator *)) {
  RS_ASSERT(idx && idx->size > 0);
  InvIndIterator *it = rm_calloc(1, sizeof(*it));
  return InitInvIndIterator(it, idx, res, filterCtx, skipMulti, sctx, decoderCtx, checkAbortFn);
}

static QueryIterator *NewInvIndIterator_NumericRange(const InvertedIndex *idx, RSIndexResult *res, const FieldSpec* fieldSpec, const FieldFilterContext *filterCtx,
                bool skipMulti, const RedisSearchCtx *sctx, IndexDecoderCtx *decoderCtx) {
  RS_ASSERT(idx && idx->size > 0);
  NumericInvIndIterator *it = rm_calloc(1, sizeof(*it));

  // Initialize the iterator first
  InitInvIndIterator(&it->base, idx, res, filterCtx, skipMulti, sctx, decoderCtx, NumericCheckAbort);

  if (fieldSpec) {
    RedisModuleString *numField = IndexSpec_GetFormattedKey(sctx->spec, fieldSpec, INDEXFLD_T_NUMERIC);
    NumericRangeTree *rt = openNumericKeysDict(sctx->spec, numField, DONT_CREATE_INDEX);
    RS_ASSERT(rt);
    it->revisionId = rt->revisionId;
    it->rt = rt;
  } else {
    it->rt = NULL;
    it->revisionId = 0;
    it->base.CheckAbort = (ValidateStatus (*)(struct InvIndIterator *))EmptyCheckAbort;
  }

  return &it->base.base;
}

QueryIterator *NewInvIndIterator_NumericFull(const InvertedIndex *idx) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.filter = NULL};
  return NewInvIndIterator_NumericRange(idx, NewNumericResult(), NULL, &fieldCtx, false, NULL, &decoderCtx);
}

QueryIterator *NewInvIndIterator_TermFull(const InvertedIndex *idx) {
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

QueryIterator *NewInvIndIterator_TagFull(const InvertedIndex *idx, const TagIndex *tagIdx) {
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

QueryIterator *NewInvIndIterator_NumericQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, const FieldFilterContext* fieldCtx,
                                              const NumericFilter *flt, const FieldSpec *fieldSpec, double rangeMin, double rangeMax) {
  IndexDecoderCtx decoderCtx = {.filter = flt};
  QueryIterator *ret = NewInvIndIterator_NumericRange(idx, NewNumericResult(), fieldSpec, fieldCtx, true, sctx, &decoderCtx);
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

QueryIterator *NewInvIndIterator_TermQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight) {
  FieldFilterContext fieldCtx = {
    .field = fieldMaskOrIndex,
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  if (term && sctx) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sctx->spec->docs.size, idx->numDocs); // FIXME: docs.size starts at 1???
    term->bm25_idf = CalculateIDF_BM25(sctx->spec->stats.numDocuments, idx->numDocs);
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

QueryIterator *NewInvIndIterator_TagQuery(const InvertedIndex *idx, const TagIndex *tagIdx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                           RSQueryTerm *term, double weight) {

  FieldFilterContext fieldCtx = {
    .field = fieldMaskOrIndex,
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  if (term && sctx) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sctx->spec->docs.size, idx->numDocs); // FIXME: docs.size starts at 1???
    term->bm25_idf = CalculateIDF_BM25(sctx->spec->stats.numDocuments, idx->numDocs);
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

  TagInvIndIterator *it = rm_calloc(1, sizeof(*it));
  it->tagIdx = tagIdx;
  return InitInvIndIterator(&it->base, idx, record, &fieldCtx, true, sctx, &dctx, TagCheckAbort);
}

QueryIterator *NewInvIndIterator_WildcardQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, double weight) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}},
    .predicate = FIELD_EXPIRATION_DEFAULT,
  };
  IndexDecoderCtx decoderCtx = {.wideMask = RS_FIELDMASK_ALL};
  RSIndexResult *record = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  record->freq = 1;

  InvIndIterator *it = rm_calloc(1, sizeof(*it));
  InitInvIndIterator(it, idx, record, &fieldCtx, true, sctx, &decoderCtx, WildcardCheckAbort);
  it->isWildcard = true; // Mark as wildcard iterator
  return &it->base;
}

QueryIterator *NewInvIndIterator_MissingQuery(const InvertedIndex *idx, const RedisSearchCtx *sctx, t_fieldIndex fieldIndex) {
  FieldFilterContext fieldCtx = {
    .field = {.isFieldMask = false, .value = {.index = fieldIndex}},
    .predicate = FIELD_EXPIRATION_MISSING, // Missing predicate
  };
  IndexDecoderCtx decoderCtx = {.wideMask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  RSIndexResult *record = NewVirtualResult(0.0, RS_FIELDMASK_ALL);
  record->freq = 1;

  return NewInvIndIterator(idx, record, &fieldCtx, true, sctx, &decoderCtx, MissingCheckAbort);
}
