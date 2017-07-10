#include "inverted_index.h"
#include "math.h"
#include "varint.h"
#include <stdio.h>
#include "rmalloc.h"
#include "qint.h"

#define INDEX_BLOCK_SIZE 100
#define INDEX_BLOCK_INITIAL_CAP 2

#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])
#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

size_t readEntry(BufferReader *__restrict__ br, IndexFlags idxflags, RSIndexResult *res,
                 int singleWordMode);

size_t writeEntry(BufferWriter *bw, IndexFlags idxflags, t_docId docId, t_fieldMask fieldMask,
                  uint32_t freq, uint32_t offsetsSz, RSOffsetVector *offsets);

void InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId) {

  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  idx->blocks[idx->size - 1] = (IndexBlock){.firstId = firstId, .lastId = 0, .numDocs = 0};
  INDEX_LAST_BLOCK(idx).data = NewBuffer(INDEX_BLOCK_INITIAL_CAP);
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
  Buffer_Free(blk->data);
  free(blk->data);
}

void InvertedIndex_Free(void *ctx) {
  InvertedIndex *idx = ctx;
  for (uint32_t i = 0; i < idx->size; i++) {
    indexBlock_Free(&idx->blocks[i]);
  }
  rm_free(idx->blocks);
  rm_free(idx);
}

size_t writeEntry(BufferWriter *bw, IndexFlags idxflags, t_docId docId, t_fieldMask fieldMask,
                  uint32_t freq, uint32_t offsetsSz, RSOffsetVector *offsets) {
  size_t sz = 0;
  switch (idxflags & (Index_StoreFieldFlags | Index_StoreTermOffsets)) {
    // Full encoding - docId, freq, flags, offset
    case Index_StoreTermOffsets | Index_StoreFieldFlags:
      sz = qint_encode4(bw, docId, (uint32_t)freq, (uint32_t)fieldMask, (uint32_t)offsetsSz);
      sz += Buffer_Write(bw, offsets->data, offsetsSz);
      break;

    // Store term offsets but not field flags
    case Index_StoreTermOffsets:
      sz = qint_encode3(bw, docId, (uint32_t)freq, (uint32_t)offsetsSz);
      sz += Buffer_Write(bw, offsets->data, offsetsSz);
      break;

    // Store field mask but not term offsets
    case Index_StoreFieldFlags:
      sz = qint_encode3(bw, docId, (uint32_t)freq, (uint32_t)fieldMask);
      break;
    // Store neither -we store just freq and docId
    default:
      sz = qint_encode2(bw, docId, (uint32_t)freq);
      break;
  }

  return sz;
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

  BufferWriter bw = NewBufferWriter(blk->data);

  ret = writeEntry(&bw, idx->flags, ent->docId - blk->lastId, ent->fieldMask, ent->freq,
                   offsets.len, &offsets);

  idx->lastId = ent->docId;
  blk->lastId = ent->docId;
  ++blk->numDocs;
  ++idx->numDocs;

  return ret;
}

inline int IR_HasNext(void *ctx) {
  IndexReader *ir = ctx;
  return !ir->atEnd;
}

void indexReader_advanceBlock(IndexReader *ir) {
  ir->currentBlock++;
  ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
  ir->lastId = 0;  // IR_CURRENT_BLOCK(ir).firstId;
}

inline size_t readEntry(BufferReader *__restrict__ br, IndexFlags idxflags, RSIndexResult *res,
                        int singleWordMode) {

  size_t startPos = BufferReader_Offset(br);

  switch ((uint32_t)idxflags) {
    // Full encoding - load docId, freq, flags, offset
    case Index_StoreTermOffsets | Index_StoreFieldFlags:

      // size_t sz =
      //     streamvbyte_decode((const uint8_t *)BufferReader_Current(br), arr, 4);
      qint_decode(br, (uint32_t *)res, 4);
      res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
      Buffer_Skip(br, res->offsetsSz);

      break;

    // load term offsets but not field flags
    case Index_StoreTermOffsets:
      qint_decode3(br, &res->docId, &res->freq, &res->offsetsSz);
      res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
      Buffer_Skip(br, res->offsetsSz);
      break;

    // Load field mask but not term offsets
    case Index_StoreFieldFlags:
      qint_decode(br, (uint32_t *)res, 3);
      break;

    // Load neither -we load just freq and docId
    default:
      qint_decode(br, (uint32_t *)res, 2);
      break;
  }

  return BufferReader_Offset(br) - startPos;
}

int IR_Read(void *ctx, RSIndexResult **e) {

  IndexReader *ir = ctx;

  int rc;
  BufferReader *br = &ir->br;

  do {
    if (BufferReader_AtEnd(br)) {
      // We're at the end of the last block...
      if (ir->currentBlock + 1 == ir->idx->size) {
        goto eof;
      }
      indexReader_advanceBlock(ir);
      br = &ir->br;
    }

    readEntry(br, ir->readFlags, ir->record, ir->singleWordMode);
    ir->lastId = ir->record->docId += ir->lastId;

    // The record doesn't match the field filter. Continue to the next one
    if (!(ir->record->fieldMask & ir->fieldMask)) {
      continue;
    }

    ++ir->len;
    *e = ir->record;
    return INDEXREAD_OK;

  } while (1);
eof:

  // Mark the reader as at EOF and return EOF
  ir->atEnd = 1;

  return INDEXREAD_EOF;
}

RSIndexResult *IR_Current(void *ctx) {
  return ((IndexReader *)ctx)->record;
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

  uint32_t top = idx->size, bottom = ir->currentBlock;
  uint32_t i = bottom;
  uint32_t newi;

  while (bottom <= top) {
    if (_isPos(idx, i, docId)) {
      ir->currentBlock = i;
      goto found;
    }

    if (docId < idx->blocks[i].firstId) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }
  ir->currentBlock = i;

found:
  ir->lastId = 0;
  ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
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
int IR_SkipTo(void *ctx, uint32_t docId, RSIndexResult **hit) {
  IndexReader *ir = ctx;

  // printf("IR %s skipTo %d\n", ir->term->str, docId);
  /* If we are skipping to 0, it's just like a normal read */
  if (docId == 0) {
    return IR_Read(ctx, hit);
  }

  /* check if the id is out of range */
  if (docId > ir->idx->lastId) {
    ir->atEnd = 1;
    return INDEXREAD_EOF;
  }
  // try to skip to the current block
  if (!indexReader_skipToBlock(ir, docId)) {
    if (IR_Read(ir, hit) == INDEXREAD_EOF) {
      return INDEXREAD_EOF;
    }
    return INDEXREAD_NOTFOUND;
  }

  int rc;
  t_docId rid;
  while (INDEXREAD_EOF != (rc = IR_Read(ir, hit))) {
    rid = (*hit)->docId;
    if (ir->lastId < docId) continue;
    if (rid == docId) return INDEXREAD_OK;
    return INDEXREAD_NOTFOUND;
  }
  ir->atEnd = 1;
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

IndexReader *NewIndexReader(InvertedIndex *idx, DocTable *docTable, t_fieldMask fieldMask,
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
  ret->atEnd = 0;

  ret->fieldMask = fieldMask;
  ret->flags = flags;
  ret->readFlags = (uint32_t)flags & (Index_StoreFieldFlags | Index_StoreTermOffsets);
  ret->br = NewBufferReader(IR_CURRENT_BLOCK(ret).data);
  return ret;
}

void IR_Free(IndexReader *ir) {

  IndexResult_Free(ir->record);

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
  ri->Current = IR_Current;
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
  Buffer repair = *blk->data;
  repair.offset = 0;

  BufferReader br = NewBufferReader(blk->data);
  BufferWriter bw = NewBufferWriter(&repair);

  RSIndexResult *res = NewTokenRecord(NULL);
  int frags = 0;

  while (!BufferReader_AtEnd(&br)) {
    size_t sz = readEntry(&br, flags & (Index_StoreFieldFlags | Index_StoreTermOffsets), res, 0);
    lastReadId = res->docId += lastReadId;
    RSDocumentMetadata *md = DocTable_Get(dt, res->docId);

    if (md->flags & Document_Deleted) {
      frags += 1;
      // printf("ignoring hole in doc %d, frags now %d\n", docId, frags);
    } else {

      if (frags) {
        // printf("Writing entry %d, last read id %d, last blk id %d\n", docId, lastReadId,
        //        blk->lastId);
        writeEntry(&bw, flags, res->docId - blk->lastId, res->fieldMask, res->freq,
                   res->term.offsets.len, &res->term.offsets);
      } else {
        bw.buf->offset += sz;
        bw.pos += sz;
      }
      blk->lastId = res->docId;
    }
  }
  if (frags) {
    blk->numDocs -= frags;
    *blk->data = repair;
    Buffer_Truncate(blk->data, 0);
  }
  // IndexReader *ir = NewIndexReader()
  return frags;
}

int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num) {
  int n = 0;
  while (startBlock < idx->size && (num <= 0 || n < num)) {
    int rep = IndexBlock_Repair(&idx->blocks[startBlock], dt, idx->flags);
    if (rep) {
      // printf("Repaired %d holes in block %d\n", rep, startBlock);
    }
    n++;
    startBlock++;
  }

  return startBlock < idx->size ? startBlock : 0;
}