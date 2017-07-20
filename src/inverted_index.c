#define QINT_API static
#include "inverted_index.h"
#include "math.h"
#include "varint.h"
#include <stdio.h>
#include "rmalloc.h"
#include "qint.h"
#include "qint.c"

#define INDEX_BLOCK_SIZE 100
#define INDEX_BLOCK_INITIAL_CAP 2

#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])
#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

static size_t writeEntry(BufferWriter *bw, IndexFlags idxflags, t_docId docId,
                         t_fieldMask fieldMask, uint32_t freq, RSOffsetVector *offsets);

static void InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId) {

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

// Formatting options:

// 1. (freq, field, offset)
// 2. (freq, field)
// 3. (freq)

// 4. (field),
// 5. (field, offset)

// 6. (offset)
// 7. (freq, offset)
//
// 0. (empty)

size_t encodeFull(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  size_t sz = 0;
  sz = qint_encode4(bw, delta, res->freq, res->fieldMask, res->offsetsSz);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 2. (Frequency, Field)
size_t encodeFreqsFields(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return qint_encode3(bw, (uint32_t)delta, (uint32_t)res->freq, (uint32_t)res->fieldMask);
}

// 3. Frequencies only
size_t encodeFreqsOnly(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return qint_encode2(bw, (uint32_t)delta, (uint32_t)res->freq);
}

// 4. Field mask only
size_t encodeFieldsOnly(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return qint_encode2(bw, (uint32_t)delta, (uint32_t)res->fieldMask);
}

// 5. (field, offset)
size_t encodeFieldsOffsets(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->fieldMask, res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 6. Offsets only
size_t encodeOffsetsOnly(BufferWriter *bw, t_docId delta, RSIndexResult *res) {

  size_t sz = qint_encode2(bw, delta, res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 7. Offsets and freqs
size_t encodeFreqsOffsets(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->freq, (uint32_t)res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

size_t encodeDocIdsOnly(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return WriteVarint(delta, bw);
}
static IndexEncoder InvertedIndex_GetEncoder(IndexFlags idxflags) {

  switch (idxflags & INDEX_STORAGE_MASK) {
    // 1. Full encoding - docId, freq, flags, offset
    case Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags:
      return encodeFull;
    // 2. (Frequency, Field)
    case Index_StoreFreqs | Index_StoreFieldFlags:
      return encodeFreqsFields;

    // 3. Frequencies only
    case Index_StoreFreqs:
      return encodeFreqsOnly;

    // 4. Field only
    case Index_StoreFieldFlags:
      return encodeFieldsOnly;

    // 5. (field, offset)
    case Index_StoreFieldFlags | Index_StoreTermOffsets:
      return encodeFieldsOffsets;

    // 6. (offset)
    case Index_StoreTermOffsets:
      return encodeOffsetsOnly;

    // 7. (freq, offset) Store term offsets but not field flags
    case Index_StoreFreqs | Index_StoreTermOffsets:
      return encodeFreqsOffsets;

    // 0. docid only
    case Index_DocIdsOnly:
      return encodeDocIdsOnly;

    default:
      abort();
  }

  return 0;
}

/* Write a forward-index entry to an index writer */
size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder, t_docId docId,
                                       RSIndexResult *entry) {

  // printf("writing %s docId %d, lastDocId %d\n", ent->term, ent->docId, idx->lastId);
  IndexBlock *blk = &INDEX_LAST_BLOCK(idx);

  // see if we need to grow the current block
  if (blk->numDocs >= INDEX_BLOCK_SIZE) {
    InvertedIndex_AddBlock(idx, docId);
    blk = &INDEX_LAST_BLOCK(idx);
  }
  // // this is needed on the first block
  if (blk->firstId == 0) {
    blk->firstId = docId;
  }

  BufferWriter bw = NewBufferWriter(blk->data);

  //
  size_t ret = encoder(&bw, docId - idx->lastId, entry);

  idx->lastId = docId;
  blk->lastId = docId;
  ++blk->numDocs;
  ++idx->numDocs;

  return ret;
}

size_t InvertedIndex_WriteForwardIndexEntry(InvertedIndex *idx, IndexEncoder encoder,
                                            ForwardIndexEntry *ent) {
  RSIndexResult rec = (RSIndexResult){
      .type = RSResultType_Term,
      .docId = ent->docId,
      .offsetsSz = ent->vw->bw.buf->offset,
      .freq = ent->freq,
      .fieldMask = ent->fieldMask,
      .term = (RSTermRecord){.term = NULL,
                             .offsets = ent->vw ? (RSOffsetVector){ent->vw->bw.buf->data,
                                                                   ent->vw->bw.buf->offset}
                                                : (RSOffsetVector){0, 0}},

  };
  return InvertedIndex_WriteEntryGeneric(idx, encoder, ent->docId, &rec);
}

inline int IR_HasNext(void *ctx) {
  IndexReader *ir = ctx;
  return !ir->atEnd;
}

static void IndexReader_AdvanceBlock(IndexReader *ir) {
  ir->currentBlock++;
  ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
  ir->lastId = 0;  // IR_CURRENT_BLOCK(ir).firstId;
}

#define DECODER(name) static int name(BufferReader *br, IndexDecoderCtx ctx, RSIndexResult *res)

DECODER(readFreqsFlags) {
  qint_decode(br, (uint32_t *)res, 3);
  // qint_decode3(br, &res->docId, &res->freq, &res->fieldMask);
  return res->fieldMask & ctx.num;
}

DECODER(readFreqOffsetsFlags) {
  qint_decode(br, (uint32_t *)res, 4);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return res->fieldMask & ctx.num;
}

DECODER(readFreqs) {
  qint_decode(br, (uint32_t *)res, 2);
  return 1;
}

DECODER(readFlags) {
  qint_decode2(br, &res->docId, &res->fieldMask);
  return res->fieldMask & ctx.num;
}

DECODER(readFlagsOffsets) {
  qint_decode3(br, &res->docId, &res->fieldMask, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return res->fieldMask & ctx.num;
}

DECODER(readOffsets) {
  qint_decode2(br, &res->docId, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

DECODER(readFreqsOffsets) {
  qint_decode3(br, &res->docId, &res->freq, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

DECODER(readDocIdsOnly) {
  res->docId = ReadVarint(br);
  res->freq = 1;
  return 1;  // Don't care about field mask
}

static IndexDecoder getDecoder(uint32_t flags) {
  switch (flags & INDEX_STORAGE_MASK) {

    // (freqs, fields, offset)
    case Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets:
      return readFreqOffsetsFlags;

    // (freqs)
    case Index_StoreFreqs:
      return readFreqs;

    // (offsets)
    case Index_StoreTermOffsets:
      return readOffsets;

    // (fields)
    case Index_StoreFieldFlags:
      return readFlags;

    // ()
    case Index_DocIdsOnly:
      return readDocIdsOnly;

    // (freqs, offsets)
    case Index_StoreFreqs | Index_StoreTermOffsets:
      return readFreqsOffsets;

    // (freqs, fields)
    case Index_StoreFreqs | Index_StoreFieldFlags:
      return readFreqsFlags;

    // (fields, offsets)
    case Index_StoreFieldFlags | Index_StoreTermOffsets:
      return readFlagsOffsets;

    default:
      fprintf(stderr, "No decoder for flags %x\n", flags & INDEX_STORAGE_MASK);
      abort();
      return NULL;
  }
}

int IR_Read(void *ctx, RSIndexResult **e) {

  IndexReader *ir = ctx;

  do {
    if (BufferReader_AtEnd(&ir->br)) {
      // We're at the end of the last block...
      if (ir->currentBlock + 1 == ir->idx->size) {
        ir->atEnd = 1;
        return INDEXREAD_EOF;
      }
      IndexReader_AdvanceBlock(ir);
    }

    int rv = ir->decoder(&ir->br, ir->decoderCtx, ir->record);
    ir->lastId = ir->record->docId += ir->lastId;
    // The decoder also acts as a filter. A zero return value means that the
    // current record should not be processed.
    if (!rv) {
      continue;
    }

    ++ir->len;
    *e = ir->record;
    return INDEXREAD_OK;

  } while (1);
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

static int IndexReader_SkipToBlock(IndexReader *ir, t_docId docId) {

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
  if (!IndexReader_SkipToBlock(ir, docId)) {
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
  // otherwise we use our counter
  return ir->len;
}

IndexReader *NewTermIndexReader(InvertedIndex *idx, DocTable *docTable, t_fieldMask fieldMask,
                                RSQueryTerm *term) {

  if (term) {
    // compute IDF based on num of docs in the header
    term->idf = logb(1.0F + docTable->size / (idx->numDocs ? idx->numDocs : (double)1));
  }

  RSIndexResult *record = NewTokenRecord(term);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {.num = (uint32_t)fieldMask};

  uint32_t readFlags = (uint32_t)idx->flags & INDEX_STORAGE_MASK;
  IndexDecoder decoder = getDecoder(readFlags);

  return NewIndexReaderGeneric(idx, decoder, dctx, record);
}

IndexReader *NewIndexReaderGeneric(InvertedIndex *idx, IndexDecoder decoder,
                                   IndexDecoderCtx decoderCtx, RSIndexResult *record) {
  IndexReader *ret = rm_malloc(sizeof(IndexReader));
  ret->currentBlock = 0;
  ret->lastId = 0;
  ret->idx = idx;

  ret->record = record;
  ret->len = 0;
  ret->atEnd = 0;
  ret->br = NewBufferReader(IR_CURRENT_BLOCK(ret).data);
  ret->decoder = decoder;
  ret->decoderCtx = decoderCtx;
  return ret;
}

void IR_Free(IndexReader *ir) {

  IndexResult_Free(ir->record);
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

  uint32_t readFlags = flags & INDEX_STORAGE_MASK;
  IndexDecoder decoder = getDecoder(readFlags);
  while (!BufferReader_AtEnd(&br)) {
    const char *bufBegin = BufferReader_Current(&br);
    decoder(&br, (IndexDecoderCtx){}, res);
    size_t sz = BufferReader_Current(&br) - bufBegin;
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
                   &res->term.offsets);
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