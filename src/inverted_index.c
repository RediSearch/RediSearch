#define QINT_API static
#include "inverted_index.h"
#include "math.h"
#include "varint.h"
#include <stdio.h>
#include <float.h>
#include "rmalloc.h"
#include "qint.h"
#include "qint.c"
#include "redis_index.h"
#include "numeric_filter.h"
#include "redismodule.h"
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
  idx->gcMarker = 0;
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

  // the gc marker tells us if there is a chance the keys has undergone GC while we were asleep
  if (ir->gcMarker == ir->idx->gcMarker) {
    // no GC - we just go to the same offset we were at
    size_t offset = ir->br.pos;
    ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
    ir->br.pos = offset;
  } else {
    // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
    // anymore. This means that we need to seek to last docId we were at

    // reset the state of the reader
    t_docId lastId = ir->lastId;
    ir->br = NewBufferReader(IR_CURRENT_BLOCK(ir).data);
    ir->lastId = 0;

    // seek to the previous last id
    RSIndexResult *dummy = NULL;
    IR_SkipTo(ir, lastId, &dummy);
  }
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

/**
 * DeltaType{1,2} Float{3}(=1), IsInf{4}   -  Sign{5} IsDouble{6} Unused{7,8}
 * DeltaType{1,2} Float{3}(=0), Tiny{4}(1) -  Number{5,6,7,8}
 * DeltaType{1,2} Float{3}(=0), Tiny{4}(0) -  NumEncoding{5,6,7} Sign{8}
 */

#define NUM_TINYENC_MASK 0x07  // This flag is set if the number is 'tiny'

typedef struct {
  uint8_t deltaEncoding : 2;
  uint8_t zero : 2;
  uint8_t valueByteCount : 3;
  uint8_t sign : 1;
} NumEncodingInt;

typedef struct {
  uint8_t deltaEncoding : 2;
  uint8_t zero : 1;
  uint8_t isTiny : 1;
  uint8_t tinyValue : 4;
} NumEncodingTiny;

typedef struct {
  uint8_t deltaEncoding : 2;
  uint8_t isFloat : 1;  // Always set to 1
  uint8_t isInf : 1;    // -INFINITY has the 'sign' bit set too
  uint8_t sign : 1;
  uint8_t isDouble : 1;  // Read 8 bytes rather than 4
} NumEncodingFloat;

typedef struct {
  uint8_t deltaEncoding : 2;
  uint8_t isFloat : 1;
  uint8_t specific : 5;
} NumEncodingCommon;

typedef union {
  uint8_t storage;
  NumEncodingCommon encCommon;
  NumEncodingInt encInt;
  NumEncodingTiny encTiny;
  NumEncodingFloat encFloat;
} EncodingHeader;

#define NUM_TINY_MAX 0xF  // Mask/Limit for 'Tiny' value
static void dumpBits(uint64_t value, size_t numBits, FILE *fp) {
  while (numBits) {
    fprintf(fp, "%d", !!(value & (1 << (numBits - 1))));
    numBits--;
  }
}

static void dumpEncoding(EncodingHeader header, FILE *fp) {
  fprintf(fp, "DeltaBytes: %u\n", header.encCommon.deltaEncoding + 1);
  fprintf(fp, "Type: ");
  if (header.encCommon.isFloat) {
    fprintf(fp, " FLOAT\n");
    fprintf(fp, "  SubType: %s\n", header.encFloat.isDouble ? "Double" : "Float");
    fprintf(fp, "  INF: %s\n", header.encFloat.isInf ? "Yes" : "No");
    fprintf(fp, "  Sign: %c\n", header.encFloat.sign ? '-' : '+');
  } else if (header.encTiny.isTiny) {
    fprintf(fp, " TINY\n");
    fprintf(fp, "  Value: %u\n", header.encTiny.tinyValue);
  } else {
    fprintf(fp, " INT\n");
    fprintf(fp, "  Size: %u\n", header.encInt.valueByteCount + 1);
    fprintf(fp, "  Sign: %c\n", header.encInt.sign ? '-' : '+');
  }
}

// 9. Special encoder for numeric values
ENCODER(encodeNumeric) {
  const double absVal = fabs(res->num.value);
  const double realVal = res->num.value;
  const float f32Num = absVal;
  uint64_t u64Num = (uint64_t)absVal;
  const uint8_t tinyNum = ((uint8_t)absVal) & NUM_TINYENC_MASK;

  EncodingHeader header = {.storage = 0};

  size_t pos = BufferWriter_Offset(bw);
  size_t sz = Buffer_Write(bw, "\0", 1);

  // Write the delta
  size_t numDeltaBytes = 0;
  do {
    sz += Buffer_Write(bw, &delta, 1);
    numDeltaBytes++;
    delta >>= 8;
  } while (delta);
  header.encCommon.deltaEncoding = numDeltaBytes - 1;

  if ((double)tinyNum == realVal) {
    // Number is small enough to fit?
    header.encTiny.tinyValue = tinyNum;
    header.encTiny.isTiny = 1;

  } else if ((double)(uint64_t)absVal == absVal) {
    // Is a whole number
    uint64_t wholeNum = absVal;
    NumEncodingInt *encInt = &header.encInt;

    if (realVal < 0) {
      encInt->sign = 1;
    }

    size_t numValueBytes = 0;
    do {
      sz += Buffer_Write(bw, &u64Num, 1);
      numValueBytes++;
      u64Num >>= 8;
    } while (u64Num);
    encInt->valueByteCount = numValueBytes - 1;

  } else if (!isfinite(realVal)) {
    header.encCommon.isFloat = 1;
    header.encFloat.isInf = 1;
    if (realVal == -INFINITY) {
      header.encFloat.sign = 1;
    }

  } else {
    // Floating point
    NumEncodingFloat *encFloat = &header.encFloat;
    if (fabs(absVal - f32Num) < 0.01) {
      sz += Buffer_Write(bw, (void *)&f32Num, 4);
      encFloat->isDouble = 0;
    } else {
      sz += Buffer_Write(bw, (void *)&absVal, 8);
      encFloat->isDouble = 1;
    }

    encFloat->isFloat = 1;
    if (realVal < 0) {
      encFloat->sign = 1;
    }
  }

  *BufferWriter_PtrAt(bw, pos) = header.storage;
  // printf("== Encoded ==\n");
  // dumpEncoding(header, stdout);

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

  //  printf("Writing docId %d, delta %d, flags %x\n", docId, docId - idx->lastId,
  //  (int)idx->flags);
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
size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, double value) {

  RSIndexResult rec = (RSIndexResult){
      .docId = docId, .type = RSResultType_Numeric, .num = (RSNumericRecord){.value = value},
  };
  return InvertedIndex_WriteEntryGeneric(idx, encodeNumeric, docId, &rec);
}

int IR_HasNext(void *ctx) {
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

#define CHECK_FLAGS(ctx, res) return (res->fieldMask & ctx.num)

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
  EncodingHeader header;
  Buffer_Read(br, &header, 1);

  res->docId = 0;
  Buffer_Read(br, &res->docId, header.encCommon.deltaEncoding + 1);

  if (header.encCommon.isFloat) {
    if (header.encFloat.isInf) {
      res->num.value = INFINITY;
    } else if (header.encFloat.isDouble) {
      Buffer_Read(br, &res->num.value, 8);
    } else {
      float f;
      Buffer_Read(br, &f, 4);
      res->num.value = f;
    }
    if (header.encFloat.sign) {
      res->num.value = -res->num.value;
    }
  } else if (header.encTiny.isTiny) {
    // Is embedded into the header
    res->num.value = header.encTiny.tinyValue;

  } else {
    // Is a whole number
    uint64_t num = 0;
    Buffer_Read(br, &num, header.encInt.valueByteCount + 1);
    res->num.value = num;
    if (header.encInt.sign) {
      res->num.value = -res->num.value;
    }
  }

  // printf("== Decoded ==\n");
  // dumpEncoding(header, stdout);

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

    // if needed - skip to the next block (skipping empty blocks that may appear here due to GC)
    while (BufferReader_AtEnd(&ir->br)) {
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

#define BLOCK_MATCHES(blk, docId) (blk.firstId <= docId && docId <= blk.lastId)

static int IndexReader_SkipToBlock(IndexReader *ir, t_docId docId) {

  InvertedIndex *idx = ir->idx;

  if (!idx->size || docId < idx->blocks[0].firstId) {
    return 0;
  }

  // if we don't need to move beyond the current block
  if (BLOCK_MATCHES(IR_CURRENT_BLOCK(ir), docId)) return 1;

  uint32_t top = idx->size - 1;
  uint32_t bottom = ir->currentBlock + 1;
  uint32_t i = bottom;  //(bottom + top) / 2;
  while (bottom <= top) {
    if (BLOCK_MATCHES(idx->blocks[i], docId)) {
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

  if (!docId) {
    return IR_Read(ctx, hit);
  }
  if (ir->atEnd) goto eof;
  if (docId > ir->idx->lastId) goto eof;

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
    rid = ir->lastId;
    if (rid < docId) continue;
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

  ret->gcMarker = idx->gcMarker;
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
    term->idf = CalculateIDF(docTable->size, idx->numDocs);
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

/* Repair an index block by removing garbage - records pointing at deleted documents.
 * Returns the number of records collected, and puts the number of bytes collected in the given
 * pointer. If an error occurred - returns -1
 */
int IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags, size_t *bytesCollected) {
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

    // If we found a deleted document, we increment the number of found "frags",
    // and not write anything, so the reader will advance but the writer won't.
    // this will close the "hole" in the index
    if (!md || md->flags & Document_Deleted) {
      ++frags;
      if (bytesCollected) *bytesCollected += sz;
    } else {  // valid document

      // If we're already operating in a repaired block, we do nothing if we found no holes yet, or
      // write back the record at the writer's top end if we've found a hole before
      if (frags) {

        // In this case we are already closing holes, so we need to write back the record at the
        // writer's position. We also calculate the delta again
        encoder(&bw, res->docId - blk->lastId, res);

      } else {
        // Nothing to do - this block is not fragmented as of now, so we just advance the writer
        bw.buf->offset += sz;
        bw.pos += sz;
      }
      blk->lastId = res->docId;
    }
  }
  if (frags) {
    // If we deleted stuff from this block, we need to chagne the number of docs and the data
    // pointer
    blk->numDocs -= frags;
    *blk->data = repair;
    Buffer_Truncate(blk->data, 0);
  }
  IndexResult_Free(res);
  return frags;
}

int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock, int num,
                         size_t *bytesCollected, size_t *recordsRemoved) {
  int n = 0;

  while (startBlock < idx->size && (num <= 0 || n < num)) {
    int repaired = IndexBlock_Repair(&idx->blocks[startBlock], dt, idx->flags, bytesCollected);
    // We couldn't repair the block - return 0
    if (repaired == -1) {
      return 0;
    }

    if (repaired > 0) {
      // Record the number of records removed for gc stats
      *recordsRemoved += repaired;

      // Increase the GC marker so other queries can tell that we did something
      ++idx->gcMarker;
    }
    n++;
    startBlock++;
  }

  return startBlock < idx->size ? startBlock : 0;
}