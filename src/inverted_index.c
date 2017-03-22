#include "offset_vector.h"
#include "inverted_index.h"
#include "math.h"
#include "varint.h"
#include <stdio.h>
#include "rmalloc.h"

#define INDEX_BLOCK_SIZE 100
#define INDEX_BLOCK_INITIAL_CAP 2

#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])
#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

size_t __readEntry(BufferReader *br, IndexFlags idxflags, t_docId lastId, t_docId *docId,
                   uint32_t *freq, uint32_t *fieldMask, RSOffsetVector *offsets,
                   int singleWordMode);

void InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId) {

  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  idx->blocks[idx->size - 1] = (IndexBlock){.firstId = firstId, .lastId = 0, .numDocs = 0};
  Buffer_Init(&(INDEX_LAST_BLOCK(idx).data), INDEX_BLOCK_INITIAL_CAP);
}

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock) {
  InvertedIndex *idx = rm_malloc(sizeof(InvertedIndex));
  idx->blocks = NULL;
  idx->size = 0;
  idx->lastId = 0;
  idx->flags = flags;
  idx->numDocs = 0;
  if (initBlock) {
    InvertedIndex_AddBlock(idx, 0);
  }
  return idx;
}

void indexBlock_Free(IndexBlock *blk) {
  Buffer_Free(&blk->data);
}

void InvertedIndex_Free(void *ctx) {
  InvertedIndex *idx = ctx;
  for (uint32_t i = 0; i < idx->size; i++) {
    indexBlock_Free(&idx->blocks[i]);
  }
  rm_free(idx->blocks);
  rm_free(idx);
}

size_t __writeEntry(BufferWriter *bw, IndexFlags idxflags, t_docId docId, uint8_t flags,
                    uint32_t freq, size_t offsetsSz, RSOffsetVector *offsets) {
  size_t ret = WriteVarint(docId, bw);
  // encode len

  // ret += WriteVarint(len, &w->bw);
  // encode freq
  // printf("writing freq %d\n", freq);
  ret += WriteVarint(freq, bw);

  if (idxflags & Index_StoreFieldFlags) {
    // encode flags
    ret += Buffer_Write(bw, &flags, 1);
  }

  if (idxflags & Index_StoreTermOffsets) {
    ret += WriteVarint(offsetsSz, bw);
    // write offsets size
    ret += Buffer_Write(bw, offsets->data, offsetsSz);
  }
  return ret;
}

/* Write a forward-index entry to an index writer */
size_t InvertedIndex_WriteEntry(InvertedIndex *idx,
                                ForwardIndexEntry *ent) {  // VVW_Truncate(ent->vw);

  // printf("writing %s docId %d, lastDocId %d\n", ent->term, ent->docId, idx->lastId);
  IndexBlock *blk = &INDEX_LAST_BLOCK(idx);

  // see if we need to grow the current block
  if (blk->numDocs >= INDEX_BLOCK_SIZE) {
    InvertedIndex_AddBlock(idx, ent->docId);
    blk = &INDEX_LAST_BLOCK(idx);
  }
  // // this is needed on the first block
  if (blk->firstId == 0) {
    blk->firstId = ent->docId;
  }
  size_t ret = 0;

  RSOffsetVector offsets = (RSOffsetVector){ent->vw->bw.buf->data, ent->vw->bw.buf->offset};

  BufferWriter bw = NewBufferWriter(&blk->data);
  //   if (idx->flags & Index_StoreScoreIndexes) {
  //     ScoreIndexWriter_AddEntry(&w->scoreWriter, ent->freq, BufferOffset(w->bw.buf), w->lastId);
  //   }
  // quantize the score to compress it to max 4 bytes
  // freq is between 0 and 1
  // int quantizedScore =
  //     floorl(ent->freq * ent->docScore * (double)FREQ_QUANTIZE_FACTOR);

  ret = __writeEntry(&bw, idx->flags, ent->docId - blk->lastId, ent->flags, ent->freq, offsets.len,
                     &offsets);

  idx->lastId = ent->docId;
  blk->lastId = ent->docId;
  ++blk->numDocs;
  ++idx->numDocs;

  return ret;
}

inline int IR_HasNext(void *ctx) {
  IndexReader *ir = ctx;

  // if we're at an end of block - check if this is the last block
  if (BufferReader_AtEnd(&ir->br)) {
    return ir->currentBlock < ir->idx->size - 1;
  }
  return 1;
}

void indexReader_advanceBlock(IndexReader *ir) {
  ir->currentBlock++;
  ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).data);
  ir->lastId = 0;  // IR_CURRENT_BLOCK(ir).firstId;
}

inline size_t __readEntry(BufferReader *br, IndexFlags idxflags, t_docId lastId, t_docId *docId,
                          uint32_t *freq, uint32_t *flags, RSOffsetVector *offsets,
                          int singleWordMode) {
  size_t startPos = BufferReader_Offset(br);
  *docId = ReadVarint(br) + lastId;

  // printf("IR %s read docId %d, last id %d\n", ir->term->str, *docId,
  // ir->lastId);
  *freq = ReadVarint(br);

  if (idxflags & Index_StoreFieldFlags) {
    Buffer_ReadByte(br, (char *)flags);
  } else {
    *flags = 0xFF;
  }

  if (idxflags & Index_StoreTermOffsets) {

    size_t offsetsLen = ReadVarint(br);

    // If needed - read offset vectors
    if (offsets != NULL && !singleWordMode) {
      offsets->data = br->pos;
      offsets->len = offsetsLen;
    }
    Buffer_Skip(br, offsetsLen);
  }
  return BufferReader_Offset(br) - startPos;
}

inline int IR_GenericRead(IndexReader *ir, t_docId *docId, uint32_t *freq, uint32_t *fieldMask,
                          RSOffsetVector *offsets) {
  if (!IR_HasNext(ir)) {
    return INDEXREAD_EOF;
  }
  // if we're at the end of the block
  if (BufferReader_AtEnd(&ir->br)) {
    indexReader_advanceBlock(ir);
  }
  BufferReader *br = &ir->br;
  uint32_t dummyFreq;
  __readEntry(br, ir->flags, ir->lastId, docId, freq ? freq : &dummyFreq, fieldMask, offsets,
              ir->singleWordMode);

  ir->lastId = *docId;
  return INDEXREAD_OK;
}

inline int IR_TryRead(IndexReader *ir, t_docId *docId, t_docId expectedDocId) {
  if (!IR_HasNext(ir)) {
    return INDEXREAD_EOF;
  }
  // if we're at the end of the block
  if (BufferReader_AtEnd(&ir->br)) {

    *docId = expectedDocId + 1;
    return INDEXREAD_NOTFOUND;
  }

  BufferReader *br = &ir->br;
  *docId = ReadVarint(br) + ir->lastId;

  ReadVarint(br);  // read quantized score
  uint32_t flags = 0xffffffff;

  // pseudo-read flags
  if (ir->flags & Index_StoreFieldFlags) {
    Buffer_ReadByte(br, (char *)&flags);
  }

  // pseudo read offsets
  if (ir->flags & Index_StoreTermOffsets) {
    size_t len = ReadVarint(br);
    Buffer_Skip(br, len);
  }

  ir->lastId = *docId;
  // printf("Tryread expected %d, got: %d\n", expectedDocId, ir->lastId);

  if ((*docId != expectedDocId && expectedDocId != 0) || !(flags & ir->fieldMask)) {
    return INDEXREAD_NOTFOUND;
  }

  return INDEXREAD_OK;
}

int IR_Read(void *ctx, RSIndexResult *e) {

  IndexReader *ir = ctx;

  RSOffsetVector *offsets = NULL;
  if (!ir->singleWordMode) {
    offsets = &ir->record->term.offsets;
  }
  int rc;
  do {
    rc = IR_GenericRead(ir, &ir->record->docId, &ir->record->freq, &ir->record->fieldMask, offsets);

    // add the record to the current result
    if (rc == INDEXREAD_OK) {
      if (!(ir->record->fieldMask & ir->fieldMask)) {
        continue;
      }

      ++ir->len;
      ir->lastId = ir->record->docId;
      AggregateResult_AddChild(e, ir->record);
      

      // printf("IR LOOP %s Read docId %d, lastId %d rc %d\n", ir->term->str, e->docId,
      // ir->lastId,rc);
      return INDEXREAD_OK;
    }
  } while (rc != INDEXREAD_EOF);

  return rc;
}

inline void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId) {
  Buffer_Seek(&ir->br, offset);
  ir->lastId = docId;
}

int _isPos(InvertedIndex *idx, uint32_t i, t_docId docId) {
  if (idx->blocks[i].firstId <= docId &&
      (i == idx->size - 1 || idx->blocks[i + 1].firstId > docId)) {
    return 1;
  }
  return 0;
}

int indexReader_skipToBlock(IndexReader *ir, t_docId docId) {

  InvertedIndex *idx = ir->idx;
  if (idx->size == 0 || docId < idx->blocks[0].firstId) {
    return 0;
  }
  // if we don't need to move beyond the current block
  if (_isPos(idx, ir->currentBlock, docId)) {
    return 1;
  }
  if (docId >= idx->blocks[idx->size - 1].firstId) {
    ir->currentBlock = idx->size - 1;
    goto found;
  }
  uint32_t top = idx->size, bottom = ir->currentBlock;
  uint32_t i = bottom;
  uint32_t newi;

  while (bottom < top) {
    // LG_DEBUG("top %d, bottom: %d idx %d, i %d, docId %d\n", top, bottom,
    // idx->entries[i].docId, i, docId );
    if (_isPos(idx, i, docId)) {
      ir->currentBlock = i;
      goto found;
    }

    if (docId < idx->blocks[i].firstId) {
      top = i;
    } else {
      bottom = i;
    }
    newi = (bottom + top) / 2;
    // LG_DEBUG("top %d, bottom: %d, new i: %d\n", top, bottom, newi);
    if (newi == i) {
      break;
    }
    i = newi;
  }
  ir->currentBlock = i;

found:
  ir->lastId = 0;
  ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).data);
  return 1;
}

/**
Skip to the given docId, or one place after it
@param ctx IndexReader context
@param docId docId to seek to
@param hit an index hit we put our reads into
@return INDEXREAD_OK if found, INDEXREAD_NOTFOUND if not found, INDEXREAD_EOF
if
at EOF
*/
int IR_SkipTo(void *ctx, u_int32_t docId, RSIndexResult *hit) {
  IndexReader *ir = ctx;

  // printf("IR %s skipTo %d\n", ir->term->str, docId);
  /* If we are skipping to 0, it's just like a normal read */
  if (docId == 0) {
    return IR_Read(ctx, hit);
  }

  /* check if the id is out of range */
  if (docId > ir->idx->lastId) {
    return INDEXREAD_EOF;
  }
  // try to skip to the current block
  if (!indexReader_skipToBlock(ir, docId)) {
    IR_Read(ir, hit);
    return INDEXREAD_NOTFOUND;
  }

  int rc;
  t_docId lastId = ir->lastId, readId = 0;
  t_offset offset = BufferReader_Offset(&ir->br);
  do {

    // do a quick-read until we hit or pass the desired document
    rc = IR_TryRead(ir, &readId, docId);
    if (rc == INDEXREAD_EOF) {
      return rc;
    }
    // rewind 1 document and re-read it...
    if (rc == INDEXREAD_OK || readId > docId) {

      IR_Seek(ir, offset, lastId);

      int _rc = IR_Read(ir, hit);
      // rc might be NOTFOUND and _rc EOF
      return _rc == INDEXREAD_NOTFOUND ? INDEXREAD_NOTFOUND : rc;
    }
    lastId = readId;
    offset = BufferReader_Offset(&ir->br);
  } while (rc != INDEXREAD_EOF);

  return INDEXREAD_EOF;
}

size_t IR_NumDocs(void *ctx) {
  IndexReader *ir = ctx;

  // in single word optimized mode we only know the size of the record from
  // the header.
  if (ir->singleWordMode) {
    return ir->idx->numDocs;
  }

  // otherwise we use our counter
  return ir->len;
}

IndexReader *NewIndexReader(InvertedIndex *idx, DocTable *docTable, uint32_t fieldMask,
                            IndexFlags flags, RSQueryTerm *term, int singleWordMode) {
  IndexReader *ret = rm_malloc(sizeof(IndexReader));
  ret->currentBlock = 0;

  ret->idx = idx;
  ret->term = term;

  if (term) {
    // compute IDF based on num of docs in the header
    ret->term->idf = logb(1.0F + docTable->size / (idx->numDocs ? idx->numDocs : (double)1));
  }

  ret->record = NewTokenRecord(term);
  ret->lastId = 0;
  ret->docTable = docTable;
  ret->len = 0;
  ret->singleWordMode = singleWordMode;

  ret->fieldMask = fieldMask;
  ret->flags = flags;
  ret->br = NewBufferReader(&IR_CURRENT_BLOCK(ret).data);
  return ret;
}

void IR_Free(IndexReader *ir) {
  Term_Free(ir->term);
  rm_free(ir);
}

void ReadIterator_Free(IndexIterator *it) {
  if (it == NULL) {
    return;
  }

  IR_Free(it->ctx);
  rm_free(it);
}

inline t_docId IR_LastDocId(void *ctx) {
  return ((IndexReader *)ctx)->lastId;
}

IndexIterator *NewReadIterator(IndexReader *ir) {
  IndexIterator *ri = rm_malloc(sizeof(IndexIterator));
  ri->ctx = ir;
  ri->Read = IR_Read;
  ri->SkipTo = IR_SkipTo;
  ri->LastDocId = IR_LastDocId;
  ri->HasNext = IR_HasNext;
  ri->Free = ReadIterator_Free;
  ri->Len = IR_NumDocs;
  return ri;
}

typedef struct {
  InvertedIndex *idx;
  uint32_t currentBlock;
  DocTable *docs;
  int numRepaired;

} RepairContext;

int IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags) {
  t_docId lastReadId = 0;
  blk->lastId = 0;
  Buffer repair = blk->data;
  repair.offset = 0;

  BufferReader br = NewBufferReader(&blk->data);
  BufferWriter bw = NewBufferWriter(&repair);

  t_docId docId;
  uint32_t fflags;
  int frags = 0;

  RSOffsetVector offsets;
  int qscore;
  while (!BufferReader_AtEnd(&br)) {
    size_t sz = __readEntry(&br, flags, lastReadId, &docId, &qscore, &fflags, &offsets, 0);

    RSDocumentMetadata *md = DocTable_Get(dt, docId);
    lastReadId = docId;
    if (md->flags & Document_Deleted) {
      frags += 1;
      printf("ignoring hole in doc %d, frags now %d\n", docId, frags);
    } else {

      if (frags) {
        printf("Writing entry %d, last read id %d, last blk id %d\n", docId, lastReadId,
               blk->lastId);
        __writeEntry(&bw, flags, docId - blk->lastId, fflags, qscore, offsets.len, &offsets);
      } else {
        bw.buf->offset += sz;
        bw.pos += sz;
      }
      blk->lastId = docId;
    }
  }
  if (frags) {
    blk->numDocs -= frags;
    blk->data = repair;
    Buffer_Truncate(&blk->data, 0);
  }
  // IndexReader *ir = NewIndexReader()
  return frags;
}

int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num) {
  int n = 0;
  while (startBlock < idx->size && (num <= 0 || n < num)) {
    int rep = IndexBlock_Repair(&idx->blocks[startBlock], dt, idx->flags);
    if (rep) {
      printf("Repaired %d holes in block %d\n", rep, startBlock);
    }
    n++;
    startBlock++;
  }

  return startBlock < idx->size ? startBlock : 0;
}