#define QINT_API static
#include "inverted_index.h"
#include "math.h"
#include "varint.h"
#include <stdio.h>
#include "rmalloc.h"
#include "qint.h"
#include "qint.c"
#include "redis_index.h"
#include "numeric_filter.h"

// The number of entries in each index block. A new block will be created after every N entries
#define INDEX_BLOCK_SIZE 100

// Initial capacity (in bytes) of a new block
#define INDEX_BLOCK_INITIAL_CAP 6

// The last block of the index
#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])

// pointer to the current block while reading the index
#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

static IndexReader *NewIndexReaderGeneric(InvertedIndex *idx, IndexDecoder decoder,
                                          IndexDecoderCtx decoderCtx, RSIndexResult *record);

/* Add a new block to the index with a given document id as the initial id */
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

/* A callback called from the ConcurrentSearchCtx after regaining execution and reopening the
 * underlying term key. We check for changes in the underlying key, or possible deletion of it */
void IndexReader_OnReopen(RedisModuleKey *k, void *privdata) {

  IndexReader *ir = privdata;
  // If the key has been deleted we'll get a NULL here, so we just mark ourselves as EOF
  if (k == NULL || RedisModule_ModuleTypeGetType(k) != InvertedIndexType) {
    ir->atEnd = 1;
    ir->idx = NULL;
    ir->br.buf = NULL;
    return;
  }

  // If the key is valid, we just reset the reader's buffer reader to the current block pointer
  ir->idx = RedisModule_ModuleTypeGetValue(k);
  size_t offset = ir->br.pos;
  ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
  ir->br.pos = offset;
}

/******************************************************************************
 * Index Encoders Implementations.
 *
 * We have 9 distinct ways to encode the index records. Based on the index flags we select the
 * correct encoder when writing to the index
 *
 ******************************************************************************/

#define ENCODER(f) static size_t f(BufferWriter *bw, t_docId delta, RSIndexResult *res)

// 1. Encode the full data of the record, delta, frequency, field mask and offset vector
ENCODER(encodeFull) {
  size_t sz = 0;
  sz = qint_encode4(bw, delta, res->freq, res->fieldMask, res->offsetsSz);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 2. (Frequency, Field)
ENCODER(encodeFreqsFields) {
  return qint_encode3(bw, (uint32_t)delta, (uint32_t)res->freq, (uint32_t)res->fieldMask);
}

// 3. Frequencies only
ENCODER(encodeFreqsOnly) {
  return qint_encode2(bw, (uint32_t)delta, (uint32_t)res->freq);
}

// 4. Field mask only
ENCODER(encodeFieldsOnly) {
  return qint_encode2(bw, (uint32_t)delta, (uint32_t)res->fieldMask);
}

// 5. (field, offset)
ENCODER(encodeFieldsOffsets) {
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->fieldMask, res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 6. Offsets only
ENCODER(encodeOffsetsOnly) {

  size_t sz = qint_encode2(bw, delta, res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 7. Offsets and freqs
ENCODER(encodeFreqsOffsets) {
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->freq, (uint32_t)res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 8. Encode only the doc ids
ENCODER(encodeDocIdsOnly) {
  return WriteVarint(delta, bw);
}

// 9. Special encoder for numeric values
ENCODER(encodeNumeric) {
  size_t sz = WriteVarint(delta, bw);

  sz += Buffer_Write(bw, (char *)&res->num.encoded, sizeof(uint32_t));
  return sz;
}

/* Get the appropriate encoder based on index flags */
IndexEncoder InvertedIndex_GetEncoder(IndexFlags flags) {

  switch (flags & INDEX_STORAGE_MASK) {
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

    // invalid encoder - we will fail
    default:
      break;
  }

  return NULL;
}

/* Write a forward-index entry to an index writer */
size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder, t_docId docId,
                                       RSIndexResult *entry) {

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

  //  printf("Writing docId %d, delta %d, flags %x\n", docId, docId - idx->lastId, (int)idx->flags);
  size_t ret = encoder(&bw, docId - blk->lastId, entry);

  idx->lastId = docId;
  blk->lastId = docId;
  ++blk->numDocs;
  ++idx->numDocs;

  return ret;
}

/** Write a forward-index entry to the index */
size_t InvertedIndex_WriteForwardIndexEntry(InvertedIndex *idx, IndexEncoder encoder,
                                            ForwardIndexEntry *ent) {
  RSIndexResult rec = {.type = RSResultType_Term,
                       .docId = ent->docId,
                       .offsetsSz = VVW_GetByteLength(ent->vw),
                       .freq = ent->freq,
                       .fieldMask = ent->fieldMask};

  rec.term.term = NULL;
  if (ent->vw) {
    rec.term.offsets.data = VVW_GetByteData(ent->vw);
    rec.term.offsets.len = VVW_GetByteLength(ent->vw);
  }
  return InvertedIndex_WriteEntryGeneric(idx, encoder, ent->docId, &rec);
}

/* Write a numeric entry to the index */
size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, float value) {

  RSIndexResult rec = (RSIndexResult){
      .docId = docId, .type = RSResultType_Numeric, .num = (RSNumericRecord){.value = value},
  };
  return InvertedIndex_WriteEntryGeneric(idx, encodeNumeric, docId, &rec);
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

/******************************************************************************
 * Index Decoder Implementations.
 *
 * We have 9 distinct ways to decode the index records. Based on the index flags we select the
 * correct decoder for creating an index reader. A decoder both decodes the entry and does initial
 * filtering, returning 1 if the record is ok or 0 if it is filtered.
 *
 * Term indexes can filter based on fieldMask, and

 *
 ******************************************************************************/

#define DECODER(name) static int name(BufferReader *br, IndexDecoderCtx ctx, RSIndexResult *res)

#define CHECK_FLAGS(ctx, res)        \
  if (ctx.num != RS_FIELDMASK_ALL) { \
    return res->fieldMask & ctx.num; \
  }                                  \
  return 1;

DECODER(readFreqsFlags) {
  qint_decode(br, (uint32_t *)res, 3);
  // qint_decode3(br, &res->docId, &res->freq, &res->fieldMask);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFreqOffsetsFlags) {
  qint_decode(br, (uint32_t *)res, 4);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(ctx, res);
}

// special decoder for decoding numeric results
DECODER(readNumeric) {
  res->docId = ReadVarint(br);
  Buffer_Read(br, &res->num.encoded, sizeof(uint32_t));
  // qint_decode2(br, &res->docId, &res->num.encoded);
  // printf("Decoded %u -> %f\n", res->num.encoded, res->num.val)
  NumericFilter *f = ctx.ptr;
  if (f) {
    return NumericFilter_Match(f, res->num.value);
  }
  return 1;
}

DECODER(readFreqs) {
  qint_decode(br, (uint32_t *)res, 2);
  return 1;
}

DECODER(readFlags) {
  qint_decode2(br, &res->docId, &res->fieldMask);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFlagsOffsets) {
  qint_decode3(br, &res->docId, &res->fieldMask, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(ctx, res);
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

IndexDecoder InvertedIndex_GetDecoder(uint32_t flags) {

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

    case Index_StoreNumeric:
      return readNumeric;

    default:
      fprintf(stderr, "No decoder for flags %x\n", flags & INDEX_STORAGE_MASK);
      return NULL;
  }
}

IndexReader *NewNumericReader(InvertedIndex *idx, NumericFilter *flt) {
  RSIndexResult *res = NewNumericResult();
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  res->num.value = 0;

  IndexDecoderCtx ctx = {.ptr = flt};
  return NewIndexReaderGeneric(idx, readNumeric, ctx, res);
}

int IR_Read(void *ctx, RSIndexResult **e) {

  IndexReader *ir = ctx;
  if (ir->atEnd) {
    goto eof;
  }
  do {
    if (BufferReader_AtEnd(&ir->br)) {
      // We're at the end of the last block...
      if (ir->currentBlock + 1 == ir->idx->size) {
        goto eof;
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
eof:
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

  if (ir->atEnd) {
    goto eof;
  }

  /* check if the id is out of range */
  if (docId > ir->idx->lastId) {
    goto eof;
  }
  // try to skip to the current block
  if (!IndexReader_SkipToBlock(ir, docId)) {
    if (IR_Read(ir, hit) == INDEXREAD_EOF) {
      goto eof;
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
eof:
  ir->atEnd = 1;
  return INDEXREAD_EOF;
}

size_t IR_NumDocs(void *ctx) {
  IndexReader *ir = ctx;
  // otherwise we use our counter
  return ir->len;
}

static IndexReader *NewIndexReaderGeneric(InvertedIndex *idx, IndexDecoder decoder,
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

IndexReader *NewTermIndexReader(InvertedIndex *idx, DocTable *docTable, t_fieldMask fieldMask,
                                RSQueryTerm *term) {
  if (term) {
    // compute IDF based on num of docs in the header
    term->idf = logb(1.0F + docTable->size / (idx->numDocs ? idx->numDocs : (double)1));
  }

  // Get the decoder
  IndexDecoder decoder = InvertedIndex_GetDecoder((uint32_t)idx->flags & INDEX_STORAGE_MASK);
  if (!decoder) {
    return NULL;
  }

  RSIndexResult *record = NewTokenRecord(term);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {.num = (uint32_t)fieldMask};

  return NewIndexReaderGeneric(idx, decoder, dctx, record);
}

void IR_Free(IndexReader *ir) {

  IndexResult_Free(ir->record);
  rm_free(ir);
}

void IR_Abort(void *ctx) {
  IndexReader *it = ctx;
  it->atEnd = 1;
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
  ri->Abort = IR_Abort;
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
  IndexDecoder decoder = InvertedIndex_GetDecoder(readFlags);
  IndexEncoder encoder = InvertedIndex_GetEncoder(readFlags);

  if (!encoder || !decoder) {
    fprintf(stderr, "Could not get decoder/encoder for index\n");
    return -1;
  }
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
        encoder(&bw, res->docId - blk->lastId, res);

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
    // we couldn't repair the block - return 0
    if (rep == -1) {
      return 0;
    }
    if (rep) {
      // printf("Repaired %d holes in block %d\n", rep, startBlock);
    }
    n++;
    startBlock++;
  }

  return startBlock < idx->size ? startBlock : 0;
}