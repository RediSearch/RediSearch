/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "inverted_index_iterator.h"

// pointer to the current block while reading the index
#define CURRENT_BLOCK(it) ((it)->idx->blocks[(it)->currentBlock])

void InvIndIterator_Free(QueryIterator *it) {
  if (!it) return;
  IndexResult_Free(it->current);
  rm_free(it);
}

void InvIndIterator_Rewind(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  QITER_CLEAR_EOF(base);
  base->LastDocId = 0;
  it->currentBlock = 0;
  it->gcMarker = it->idx->gcMarker;
  it->br = NewBufferReader(&CURRENT_BLOCK(it).buf);
  it->lastId = CURRENT_BLOCK(it).firstId;
  it->sameId = 0;
}

size_t InvIndIterator_NumEstimated(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  return it->idx->numDocs;
}

static inline void AdvanceBlock(InvIndIterator *it) {
  it->currentBlock++;
  it->br = NewBufferReader(&CURRENT_BLOCK(it).buf);
  it->lastId = CURRENT_BLOCK(it).firstId;
}

IteratorStatus InvIndIterator_Read(QueryIterator *base) {
  InvIndIterator *it = (InvIndIterator *)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }
  while (true) {
    // if needed - advance to the next block
    if (BufferReader_AtEnd(&it->br)) {
      if (it->currentBlock + 1 == it->idx->size) {
        // We're at the end of the last block...
        break;
      }
      AdvanceBlock(it);
    }

    t_docId offset = (it->decoders.decoder != readRawDocIdsOnly) ? it->lastId : CURRENT_BLOCK(it).firstId;
    bool relevant = it->decoders.decoder(&it->br, &it->decoderCtx, it->base.current, offset);
    RSIndexResult *record = it->base.current;
    it->lastId = record->docId;

    // The decoder also acts as a filter. A zero return value means that the
    // current record should not be processed.
    if (!relevant) {
      continue;
    }

    if (it->skipMulti) {
      // Avoid returning the same doc
      // Currently the only relevant predicate for multi-value is `any`, therefore only the first match in each doc is needed.
      // More advanced predicates, such as `at least <N>` or `exactly <N>`, will require adding more logic.
      if (it->sameId == it->lastId) {
        continue;
      }
      it->sameId = it->lastId;
    }

    // TODO: move to constructor
    if (it->sctx && it->sctx->spec && DocTable_HasExpiration(&it->sctx->spec->docs, record->docId)) {
      size_t numFieldIndices = 0;
      // Use a stack allocated array for the field indices, if the field mask is not a single field
      t_fieldIndex fieldIndicesArray[FIELD_MASK_BIT_COUNT];
      t_fieldIndex* sortedFieldIndices = fieldIndicesArray;
      if (it->filterCtx.field.isFieldMask) {
        numFieldIndices = IndexSpec_TranslateMaskToFieldIndices(it->sctx->spec, it->filterCtx.field.value.mask, fieldIndicesArray);
      } else if (it->filterCtx.field.value.index != RS_INVALID_FIELD_INDEX) {
        sortedFieldIndices = &it->filterCtx.field.value.index;
        ++numFieldIndices;
      }
      const bool validValue = DocTable_VerifyFieldExpirationPredicate(&it->sctx->spec->docs, record->docId, sortedFieldIndices, numFieldIndices, it->filterCtx.predicate, &it->sctx->time.current);
      if (!validValue) {
        continue;
      }
    }

    base->LastDocId = record->docId;
    return ITERATOR_OK;
  }
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}

#define BLOCK_MATCHES(blk, docId) ((blk).firstId <= docId && docId <= (blk).lastId)

// Assumes there is a valid block to skip to (maching or past the requested docId)
static inline void SkipToBlock(InvIndIterator *it, t_docId docId) {
  InvertedIndex *idx = it->idx;
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
    const IndexBlock *blk = idx->blocks + i;
    if (BLOCK_MATCHES(*blk, docId)) {
      it->currentBlock = i;
      goto new_block;
    }

    if (docId < blk->firstId) {
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
  }

new_block:
  RS_LOG_ASSERT(it->currentBlock < idx->size, "Invalid block index");
  it->lastId = CURRENT_BLOCK(it).firstId;
  it->br = NewBufferReader(&CURRENT_BLOCK(it).buf);
}

IteratorStatus InvIndIterator_SkipTo_Default(QueryIterator *base, t_docId docId) {
  InvIndIterator *it = (InvIndIterator*)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }

  if (docId > it->idx->lastId) {
    QITER_SET_EOF(base);
    return ITERATOR_EOF;
  }

  if (CURRENT_BLOCK(it).lastId < docId || BufferReader_AtEnd(&it->br)) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(it, docId);
  }

  while (ITERATOR_EOF != InvIndIterator_Read(base)) {
    if (it->lastId < docId) continue;
    if (it->lastId == docId) return ITERATOR_OK;
    return ITERATOR_NOTFOUND;
  }
  return ITERATOR_EOF;
}

IteratorStatus InvIndIterator_SkipTo_withSeeker(QueryIterator *base, t_docId docId) {
  InvIndIterator *it = (InvIndIterator*)base;
  if (QITER_AT_EOF(base)) {
    return ITERATOR_EOF;
  }

  if (docId > it->idx->lastId) {
    goto eof;
  }

  if (CURRENT_BLOCK(it).lastId < docId || BufferReader_AtEnd(&it->br)) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(it, docId);
  }

  // the seeker will return 1 only when it found a docid which is greater or equals the
  // searched docid and the field mask matches the searched fields mask. We need to continue
  // scanning only when we found such an id or we reached the end of the inverted index.
  while (!it->decoders.seeker(&it->br, &it->decoderCtx, it, docId, it->base.current)) {
    if (it->currentBlock + 1 < it->idx->size) {
      AdvanceBlock(it);
    } else {
      goto eof;
    }
  }
  // Found a document that match the field mask and greater or equal the searched docid
  base->LastDocId = it->base.current->docId;
  return (it->base.current->docId == docId) ? ITERATOR_OK : ITERATOR_NOTFOUND;

eof:
  QITER_SET_EOF(base);
  return ITERATOR_EOF;
}
