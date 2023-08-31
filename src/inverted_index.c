/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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
#include "rmutil/rm_assert.h"
#include "geo_index.h"
#include "module.h"

uint64_t TotalIIBlocks = 0;

// The number of entries in each index block. A new block will be created after every N entries
#define INDEX_BLOCK_SIZE 100
#define INDEX_BLOCK_SIZE_DOCID_ONLY 1000

// Initial capacity (in bytes) of a new block
#define INDEX_BLOCK_INITIAL_CAP 6

// The last block of the index
#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])

// pointer to the current block while reading the index
#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

static IndexReader *NewIndexReaderGeneric(const IndexSpec *sp, InvertedIndex *idx,
                                          IndexDecoderProcs decoder, IndexDecoderCtx decoderCtx, int skipMulti,
                                          RSIndexResult *record);

/* Add a new block to the index with a given document id as the initial id */
IndexBlock *InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId) {
  TotalIIBlocks++;
  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  IndexBlock *last = idx->blocks + (idx->size - 1);
  memset(last, 0, sizeof(*last));  // for msan
  last->firstId = last->lastId = firstId;
  Buffer_Init(&INDEX_LAST_BLOCK(idx).buf, INDEX_BLOCK_INITIAL_CAP);
  return &INDEX_LAST_BLOCK(idx);
}

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock) {
  int useFieldMask = flags & Index_StoreFieldFlags;
  int useNumEntries = flags & Index_StoreNumeric;
  RedisModule_Assert(!(useFieldMask && useNumEntries));
  // Avoid some of the allocation if not needed
  size_t size = (useFieldMask || useNumEntries) ? sizeof(InvertedIndex) :
                                                  sizeof(InvertedIndex) - sizeof(t_fieldMask);

  InvertedIndex *idx = rm_malloc(size);
  idx->blocks = NULL;
  idx->size = 0;
  idx->lastId = 0;
  idx->gcMarker = 0;
  idx->flags = flags;
  idx->numDocs = 0;
  if (useFieldMask) {
    idx->fieldMask = (t_fieldMask)0;
  } else if (useNumEntries) {
    idx->numEntries = 0;
  }
  if (initBlock) {
    InvertedIndex_AddBlock(idx, 0);
  }
  return idx;
}

void indexBlock_Free(IndexBlock *blk) {
  Buffer_Free(&blk->buf);
}

void InvertedIndex_Free(void *ctx) {
  InvertedIndex *idx = ctx;
  TotalIIBlocks -= idx->size;
  for (uint32_t i = 0; i < idx->size; i++) {
    indexBlock_Free(&idx->blocks[i]);
  }
  rm_free(idx->blocks);
  rm_free(idx);
}

static void IR_SetAtEnd(IndexReader *r, int value) {
  if (r->isValidP) {
    *r->isValidP = !value;
  }
  r->atEnd_ = value;
}
#define IR_IS_AT_END(ir) (ir)->atEnd_

/* A callback called from the ConcurrentSearchCtx after regaining execution and reopening the
 * underlying term key. We check for changes in the underlying key, or possible deletion of it */
void IndexReader_OnReopen(void *privdata) {

  IndexReader *ir = privdata;
  if (ir->record->type == RSResultType_Term) {
    // we need to reopen the inverted index to make sure its stil valid.
    // the GC might have deleted it by now.
    RedisSearchCtx sctx = SEARCH_CTX_STATIC(RSDummyContext, (IndexSpec *)ir->sp);
    InvertedIndex *idx = Redis_OpenInvertedIndexEx(&sctx, ir->record->term.term->str,
                                                   ir->record->term.term->len, 0, NULL, NULL);
    if (!idx || ir->idx != idx) {
      // the inverted index was collected entirely by GC, lets stop searching.
      // notice, it might be that a new inverted index was created, we will not
      // continue read those results and we are not promise that documents
      // that was added during cursor life will be returned by the cursor.
      IR_Abort(ir);
      return;
    }
  }

  // the gc marker tells us if there is a chance the keys has undergone GC while we were asleep
  if (ir->gcMarker == ir->idx->gcMarker) {
    // no GC - we just go to the same offset we were at
    size_t offset = ir->br.pos;
    ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).buf);
    ir->br.pos = offset;
  } else {
    // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
    // anymore. This means that we need to seek to last docId we were at

    // reset the state of the reader
    t_docId lastId = ir->lastId;
    ir->currentBlock = 0;
    ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).buf);
    ir->lastId = IR_CURRENT_BLOCK(ir).firstId;

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

#define ENCODER(f) static size_t f(BufferWriter *bw, uint32_t delta, RSIndexResult *res)

// 1. Encode the full data of the record, delta, frequency, field mask and offset vector
ENCODER(encodeFull) {
  size_t sz = qint_encode4(bw, delta, res->freq, (uint32_t)res->fieldMask, res->offsetsSz);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

ENCODER(encodeFullWide) {
  size_t sz = qint_encode3(bw, delta, res->freq, res->offsetsSz);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

// 2. (Frequency, Field)
ENCODER(encodeFreqsFields) {
  return qint_encode3(bw, (uint32_t)delta, (uint32_t)res->freq, (uint32_t)res->fieldMask);
}

ENCODER(encodeFreqsFieldsWide) {
  size_t sz = qint_encode2(bw, (uint32_t)delta, (uint32_t)res->freq);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
  return sz;
}

// 3. Frequencies only
ENCODER(encodeFreqsOnly) {
  return qint_encode2(bw, (uint32_t)delta, (uint32_t)res->freq);
}

// 4. Field mask only
ENCODER(encodeFieldsOnly) {
  return qint_encode2(bw, (uint32_t)delta, (uint32_t)res->fieldMask);
}

ENCODER(encodeFieldsOnlyWide) {
  size_t sz = WriteVarint((uint32_t)delta, bw);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
  return sz;
}

// 5. (field, offset)
ENCODER(encodeFieldsOffsets) {
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->fieldMask, res->term.offsets.len);
  sz += Buffer_Write(bw, res->term.offsets.data, res->term.offsets.len);
  return sz;
}

ENCODER(encodeFieldsOffsetsWide) {
  size_t sz = qint_encode2(bw, delta, res->term.offsets.len);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
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

// 9. Encode only the doc ids
ENCODER(encodeRawDocIdsOnly) {
  return Buffer_Write(bw, &delta, 4);
}

/**
 * DeltaType{1,2} Float{3}(=1), IsInf{4}   -  Sign{5} IsDouble{6} Unused{7,8}
 * DeltaType{1,2} Float{3}(=0), Tiny{4}(1) -  Number{5,6,7,8}
 * DeltaType{1,2} Float{3}(=0), Tiny{4}(0) -  NumEncoding{5,6,7} Sign{8}
 */

#define NUM_TINYENC_MASK 0x07  // This flag is set if the number is 'tiny'

#define NUM_ENCODING_COMMON_TYPE_TINY           0
#define NUM_ENCODING_COMMON_TYPE_FLOAT          1
#define NUM_ENCODING_COMMON_TYPE_POSITIVE_INT   2
#define NUM_ENCODING_COMMON_TYPE_NEG_INT        3


typedef struct {
  // Common fields
  uint8_t deltaEncoding : 3;  // representing a zero-based number of bytes that stores the docId delta (delta from the previous docId)
                              // (zero delta is required to store multiple values in the same doc)
                              // Max delta size is 7 bytes (values between 0 to 7), allowing for max delta value of 2^((2^3-1)*8)-1
  uint8_t type : 2; // (tiny, float, posint, negint)
  // Specific fields
  uint8_t specific : 3; // dummy field
} NumEncodingCommon;

typedef struct {
  uint8_t deltaEncoding : 3;
  uint8_t type : 2;
  // Specific fields
  uint8_t valueByteCount : 3; //1 to 8 (encoded as 0-7, since value 0 is represented as tiny)
} NumEncodingInt;

typedef struct {
  uint8_t deltaEncoding : 3;
  uint8_t type : 2;
  // Specific fields
  uint8_t tinyValue : 3;  // corresponds to NUM_TINYENC_MASK
} NumEncodingTiny;

typedef struct {
  uint8_t deltaEncoding : 3;
  uint8_t type : 2;
  // Specific fields
  uint8_t isInf : 1;    // -INFINITY has the 'sign' bit set too
  uint8_t sign : 1;
  uint8_t isDouble : 1;  // Read 8 bytes rather than 4
} NumEncodingFloat;

// EncodingHeader is used for encodind/decoding Inverted Index numeric values.
// This header is written/read to/from Inverted Index entries, followed by the actual bytes representing the delta (if not zero),
// followed by the actual bytes representing the numeric value (if not tiny)
// (see encoder `encodeNumeric` and decoder `readNumeric`)
// EncodingHeader internal structs must all be of the same size, beginning with common "base" fields, followed by specific fields per "derived" struct.
// The specific types are:
//  tiny - for tiny positive integers, including zero (the value is encoded in the header itself)
//  posint and negint - for none-zero integer nubmers
//  float - for floating point numbers
typedef union {
  // Alternative representation as a primitive number (used for writing)
  uint8_t storage;
  // Common struct
  NumEncodingCommon encCommon;
  // Specific structs
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
  fprintf(fp, "DeltaBytes: %u\n", header.encCommon.deltaEncoding);
  fprintf(fp, "Type: ");
  if (header.encCommon.type == NUM_ENCODING_COMMON_TYPE_FLOAT) {
    fprintf(fp, " FLOAT\n");
    fprintf(fp, "  SubType: %s\n", header.encFloat.isDouble ? "Double" : "Float");
    fprintf(fp, "  INF: %s\n", header.encFloat.isInf ? "Yes" : "No");
    fprintf(fp, "  Sign: %c\n", header.encFloat.sign ? '-' : '+');
  } else if (header.encCommon.type == NUM_ENCODING_COMMON_TYPE_TINY) {
    fprintf(fp, " TINY\n");
    fprintf(fp, "  Value: %u\n", header.encTiny.tinyValue);
  } else {
    fprintf(fp, " INT\n");
    fprintf(fp, "  Size: %u\n", header.encInt.valueByteCount + 1);
    fprintf(fp, "  Sign: %c\n", header.encCommon.type == NUM_ENCODING_COMMON_TYPE_NEG_INT ? '-' : '+');
  }
}

#ifdef _DEBUG
void InvertedIndex_Dump(InvertedIndex *idx, int indent) {
  PRINT_INDENT(indent);
  printf("InvertedIndex {\n");
  ++indent;
  PRINT_INDENT(indent);
  printf("numDocs %u, lastId %ld, size %u\n", idx->numDocs, idx->lastId, idx->size);

  RSIndexResult *res = NULL;
  IndexReader *ir = NewNumericReader(NULL, idx, NULL ,0, 0, false);
  while (INDEXREAD_OK == IR_Read(ir, &res)) {
    PRINT_INDENT(indent);
    printf("value %f, docId %lu\n", res->num.value, res->docId);
  }
  IR_Free(ir);
  --indent;
  PRINT_INDENT(indent);
  printf("}\n");
}


void IndexBlock_Dump(IndexBlock *b, int indent) {
  PRINT_INDENT(indent);
  printf("IndexBlock {\n");
  ++indent;
  PRINT_INDENT(indent);
  printf("numEntries %u, firstId %lu, lastId %lu, \n", b->numEntries, b->firstId, b->lastId);
  --indent;
  PRINT_INDENT(indent);
  printf("}\n");
}
#endif // #ifdef _DEBUG

// 9. Special encoder for numeric values
ENCODER(encodeNumeric) {
  const double absVal = fabs(res->num.value);
  const double realVal = res->num.value;
  const float f32Num = absVal;
  uint64_t u64Num = (uint64_t)absVal;
  const uint8_t tinyNum = ((uint8_t)absVal) & NUM_TINYENC_MASK;

  EncodingHeader header = {.storage = 0};

  // Write a placeholder for the header and mark its position
  size_t pos = BufferWriter_Offset(bw);
  size_t sz = Buffer_Write(bw, "\0", 1);

  // Write the delta (if not zero)
  size_t numDeltaBytes = 0;
  while (delta) {
    sz += Buffer_Write(bw, &delta, 1);
    numDeltaBytes++;
    delta >>= 8;
  }
  header.encCommon.deltaEncoding = numDeltaBytes;

  // Write the numeric value
  if ((double)tinyNum == realVal) {
    // Number is small enough to fit?
    header.encTiny.tinyValue = tinyNum;
    header.encCommon.type = NUM_ENCODING_COMMON_TYPE_TINY;

  } else if ((double)(uint64_t)absVal == absVal) {
    // Is a whole number
    uint64_t wholeNum = absVal;
    NumEncodingInt *encInt = &header.encInt;

    if (realVal < 0) {
      encInt->type = NUM_ENCODING_COMMON_TYPE_NEG_INT;
    } else {
      encInt->type = NUM_ENCODING_COMMON_TYPE_POSITIVE_INT;
    }

    size_t numValueBytes = 0;
    do {
      sz += Buffer_Write(bw, &u64Num, 1);
      numValueBytes++;
      u64Num >>= 8;
    } while (u64Num);
    encInt->valueByteCount = numValueBytes - 1;

  } else if (!isfinite(realVal)) {
    header.encCommon.type = NUM_ENCODING_COMMON_TYPE_FLOAT;
    header.encFloat.isInf = 1;
    if (realVal == -INFINITY) {
      header.encFloat.sign = 1;
    }

  } else {
    // Floating point
    NumEncodingFloat *encFloat = &header.encFloat;
    if (absVal == f32Num || (RSGlobalConfig.numericCompress == true &&
                             fabs(absVal - f32Num) < 0.01)) {
      sz += Buffer_Write(bw, (void *)&f32Num, 4);
      encFloat->isDouble = 0;
    } else {
      sz += Buffer_Write(bw, (void *)&absVal, 8);
      encFloat->isDouble = 1;
    }

    encFloat->type = NUM_ENCODING_COMMON_TYPE_FLOAT;
    if (realVal < 0) {
      encFloat->sign = 1;
    }
  }

  // Write the header at its marked position
  *BufferWriter_PtrAt(bw, pos) = header.storage;

  return sz;
}

/* Get the appropriate encoder based on index flags */
IndexEncoder InvertedIndex_GetEncoder(IndexFlags flags) {
  switch (flags & INDEX_STORAGE_MASK) {
    // 1. Full encoding - docId, freq, flags, offset
    case Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags:
      return encodeFull;

    case Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_WideSchema:
      return encodeFullWide;

    // 2. (Frequency, Field)
    case Index_StoreFreqs | Index_StoreFieldFlags:
      return encodeFreqsFields;

    case Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema:
      return encodeFreqsFieldsWide;

    // 3. Frequencies only
    case Index_StoreFreqs:
      return encodeFreqsOnly;

    // 4. Field only
    case Index_StoreFieldFlags:
      return encodeFieldsOnly;

    case Index_StoreFieldFlags | Index_WideSchema:
      return encodeFieldsOnlyWide;

    // 5. (field, offset)
    case Index_StoreFieldFlags | Index_StoreTermOffsets:
      return encodeFieldsOffsets;

    case Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
      return encodeFieldsOffsetsWide;

    // 6. (offset)
    case Index_StoreTermOffsets:
      return encodeOffsetsOnly;

    // 7. (freq, offset) Store term offsets but not field flags
    case Index_StoreFreqs | Index_StoreTermOffsets:
      return encodeFreqsOffsets;

    // 0. docid only
    case Index_DocIdsOnly:
      if (RSGlobalConfig.invertedIndexRawDocidEncoding) {
        return encodeRawDocIdsOnly;
      } else {
        return encodeDocIdsOnly;
      }

    case Index_StoreNumeric:
      return encodeNumeric;

    // invalid encoder - we will fail
    default:
      break;
  }

  return NULL;
}

/* Write a forward-index entry to an index writer */
size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder, t_docId docId,
                                       RSIndexResult *entry) {

  int same_doc = 0;
  if (idx->lastId && idx->lastId == docId) {
    if (encoder != encodeNumeric) {
      // do not allow the same document to be written to the same index twice.
      // this can happen with duplicate tags for example
      return 0;
    } else {
      // for numeric it is allowed (to support multi values)
      same_doc = 1;
    }
  }

  t_docId delta = 0;
  IndexBlock *blk = &INDEX_LAST_BLOCK(idx);

  // use proper block size. Index_DocIdsOnly == 0x00
  uint16_t blockSize = (idx->flags & INDEX_STORAGE_MASK) ?
          INDEX_BLOCK_SIZE :
          INDEX_BLOCK_SIZE_DOCID_ONLY;

  // see if we need to grow the current block
  if (blk->numEntries >= blockSize && !same_doc) {
    // If same doc can span more than a single block - need to adjust IndexReader_SkipToBlock
    blk = InvertedIndex_AddBlock(idx, docId);
  } else if (blk->numEntries == 0) {
    blk->firstId = blk->lastId = docId;
  }

  if (encoder != encodeRawDocIdsOnly) {
    delta = docId - blk->lastId;
  } else {
    delta = docId - blk->firstId;
  }

  // For non-numeric encoders the maximal delta is UINT32_MAX (since it is encoded with 4 bytes)
  //
  // For numeric encoder the maximal delta is practically not a limit (see structs `EncodingHeader` and `NumEncodingCommon`)
  if (delta > UINT32_MAX && encoder != encodeNumeric) {
    blk = InvertedIndex_AddBlock(idx, docId);
    delta = 0;
  }

  BufferWriter bw = NewBufferWriter(&blk->buf);

  size_t ret = encoder(&bw, delta, entry);

  idx->lastId = docId;
  blk->lastId = docId;
  ++blk->numEntries;
  if (!same_doc) {
    ++idx->numDocs;
  }
  if (encoder == encodeNumeric) {
    ++idx->numEntries;
  }

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
      .docId = docId,
      .type = RSResultType_Numeric,
      .num = (RSNumericRecord){.value = value},
  };
  return InvertedIndex_WriteEntryGeneric(idx, encodeNumeric, docId, &rec);
}

static void IndexReader_AdvanceBlock(IndexReader *ir) {
  ir->currentBlock++;
  ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).buf);
  ir->lastId = IR_CURRENT_BLOCK(ir).firstId;
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

#define DECODER(name) \
  static int name(BufferReader *br, const IndexDecoderCtx *ctx, RSIndexResult *res)

/**
 * Skipper implements SkipTo. It is an optimized version of DECODER which reads
 * the document ID first, and skips ahead if the result does not match the
 * expected one.
 */
#define SKIPPER(name)                                                                           \
  static int name(BufferReader *br, const IndexDecoderCtx *ctx, IndexReader *ir, t_docId expid, \
                  RSIndexResult *res)

#define CHECK_FLAGS(ctx, res) return ((res->fieldMask & ctx->num) != 0)

DECODER(readFreqsFlags) {
  qint_decode3(br, (uint32_t *)&res->docId, &res->freq, (uint32_t *)&res->fieldMask);
  // qint_decode3(br, &res->docId, &res->freq, &res->fieldMask);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFreqsFlagsWide) {
  uint32_t maskSz;
  qint_decode2(br, (uint32_t *)&res->docId, &res->freq);
  res->fieldMask = ReadVarintFieldMask(br);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFreqOffsetsFlags) {
  qint_decode4(br, (uint32_t *)&res->docId, &res->freq, (uint32_t *)&res->fieldMask,
               &res->offsetsSz);
  res->term.offsets.data = BufferReader_Current(br);
  res->term.offsets.len = res->offsetsSz;
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(ctx, res);
}

SKIPPER(seekFreqOffsetsFlags) {
  uint32_t did = 0, freq = 0, offsz = 0;
  t_fieldMask fm = 0;
  t_docId lastId = ir->lastId;
  int rc = 0;

  t_fieldMask num = ctx->num;

  if (!BufferReader_AtEnd(br)) {
    size_t oldpos = br->pos;
    qint_decode4(br, &did, &freq, (uint32_t *)&fm, &offsz);
    Buffer_Skip(br, offsz);

    if (oldpos == 0 && did != 0) {
      // Old RDB: Delta is not 0, but the docid itself
      lastId = did;
    } else {
      lastId = (did += lastId);
    }

    if (num & fm) {
      if (did >= expid) {
        // overshoot
        rc = 1;
        goto done;
      }
    }
  }

  if (!BufferReader_AtEnd(br)) {
    while (!BufferReader_AtEnd(br)) {
      qint_decode4(br, &did, &freq, (uint32_t *)&fm, &offsz);
      Buffer_Skip(br, offsz);
      lastId = (did += lastId);
      if (!(num & fm)) {
        continue;  // we just ignore it if it does not match the field mask
      }
      if (did >= expid) {
        // Overshoot!
        rc = 1;
        break;
      }
    }
  }

done:
  res->docId = did;
  res->freq = freq;
  res->fieldMask = fm;
  res->offsetsSz = offsz;
  res->term.offsets.data = BufferReader_Current(br) - offsz;
  res->term.offsets.len = offsz;

  // sync back!
  ir->lastId = lastId;
  return rc;
}

DECODER(readFreqOffsetsFlagsWide) {
  uint32_t maskSz;

  qint_decode3(br, (uint32_t *)&res->docId, &res->freq, &res->offsetsSz);
  res->fieldMask = ReadVarintFieldMask(br);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(ctx, res);
}

// special decoder for decoding numeric results
DECODER(readNumeric) {
  EncodingHeader header;
  Buffer_Read(br, &header, 1);

  res->docId = 0;
  // Read the delta (if not zero)
  if (header.encCommon.deltaEncoding) {
    Buffer_Read(br, &res->docId, header.encCommon.deltaEncoding);
  }

  switch (header.encCommon.type) {
    case NUM_ENCODING_COMMON_TYPE_FLOAT:
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
      break;

    case NUM_ENCODING_COMMON_TYPE_TINY:
      // Is embedded into the header
      res->num.value = header.encTiny.tinyValue;
      break;

    case NUM_ENCODING_COMMON_TYPE_POSITIVE_INT:
    case NUM_ENCODING_COMMON_TYPE_NEG_INT:
      {
        // Is a none-zero integer (zero is represented as tiny)
        uint64_t num = 0;
        Buffer_Read(br, &num, header.encInt.valueByteCount + 1);
        res->num.value = num;
        if (header.encCommon.type == NUM_ENCODING_COMMON_TYPE_NEG_INT) {
          res->num.value = -res->num.value;
        }
      }
      break;
  }

  NumericFilter *f = ctx->ptr;
  if (f) {
    if (NumericFilter_IsNumeric(f)) {
      return NumericFilter_Match(f, res->num.value);
    } else {
      return isWithinRadius(f->geoFilter, res->num.value, &res->num.value);
    }
  }

  return 1;
}

DECODER(readFreqs) {
  qint_decode2(br, (uint32_t *)&res->docId, &res->freq);
  return 1;
}

DECODER(readFlags) {
  qint_decode2(br, (uint32_t *)&res->docId, (uint32_t *)&res->fieldMask);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFlagsWide) {
  res->docId = ReadVarint(br);
  res->freq = 1;
  res->fieldMask = ReadVarintFieldMask(br);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFlagsOffsets) {
  qint_decode3(br, (uint32_t *)&res->docId, (uint32_t *)&res->fieldMask, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(ctx, res);
}

DECODER(readFlagsOffsetsWide) {

  qint_decode2(br, (uint32_t *)&res->docId, &res->offsetsSz);
  res->fieldMask = ReadVarintFieldMask(br);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};

  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(ctx, res);
}

DECODER(readOffsets) {
  qint_decode2(br, (uint32_t *)&res->docId, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

DECODER(readFreqsOffsets) {
  qint_decode3(br, (uint32_t *)&res->docId, &res->freq, &res->offsetsSz);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

SKIPPER(seekRawDocIdsOnly) {
  int64_t delta = expid - IR_CURRENT_BLOCK(ir).firstId;

  Buffer_Read(br, &res->docId, 4);
  if (res->docId >= delta || delta < 0) {
    goto final;
  }

  uint32_t *buf = (uint32_t *)br->buf->data;
  size_t start = br->pos / 4;
  size_t end = (br->buf->offset - 4) / 4;
  size_t cur = start;
  uint32_t curVal = buf[cur];

  // perform binary search
  while (start < end) {
    if (curVal == delta) {
      break;
    }
    if (curVal > delta) {
      end = cur - 1;
    } else {
      start = cur + 1;
    }
    cur = (end + start) / 2;
    curVal = buf[cur];
  }

  // we cannot get out of range since we check in
  if (curVal < delta) {
    cur++;

#if 1
	// TODO: consider adding a fix
    // Fixes test_optimizer:testCoordinator with raw DocID encoding
    // TODO: explain why it is so
    if (cur >= br->buf->offset / 4) {
      return 0;
    }
#endif // 1
  }

  // skip to position and read
  Buffer_Seek(br, cur * 4);
  Buffer_Read(br, &res->docId, 4);

final:
  res->docId += IR_CURRENT_BLOCK(ir).firstId;
  res->freq = 1;
  return 1;
}

DECODER(readRawDocIdsOnly) {
  Buffer_Read(br, &res->docId, 4);
  res->freq = 1;
  return 1;  // Don't care about field mask
}

DECODER(readDocIdsOnly) {
  res->docId = ReadVarint(br);
  res->freq = 1;
  return 1;  // Don't care about field mask
}

IndexDecoderProcs InvertedIndex_GetDecoder(uint32_t flags) {
#define RETURN_DECODERS(reader, seeker_) \
  procs.decoder = reader;                \
  procs.seeker = seeker_;                \
  return procs;

  IndexDecoderProcs procs = {0};
  switch (flags & INDEX_STORAGE_MASK) {

    // (freqs, fields, offset)
    case Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets:
      RETURN_DECODERS(readFreqOffsetsFlags, seekFreqOffsetsFlags);

    case Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
      RETURN_DECODERS(readFreqOffsetsFlagsWide, NULL);

    // (freqs)
    case Index_StoreFreqs:
      RETURN_DECODERS(readFreqs, NULL);

    // (offsets)
    case Index_StoreTermOffsets:
      RETURN_DECODERS(readOffsets, NULL);

    // (fields)
    case Index_StoreFieldFlags:
      RETURN_DECODERS(readFlags, NULL);

    case Index_StoreFieldFlags | Index_WideSchema:
      RETURN_DECODERS(readFlagsWide, NULL);

    // ()
    case Index_DocIdsOnly:
      if (RSGlobalConfig.invertedIndexRawDocidEncoding) {
        RETURN_DECODERS(readRawDocIdsOnly, seekRawDocIdsOnly);
      } else {
        RETURN_DECODERS(readDocIdsOnly, NULL);
      }

    // (freqs, offsets)
    case Index_StoreFreqs | Index_StoreTermOffsets:
      RETURN_DECODERS(readFreqsOffsets, NULL);

    // (freqs, fields)
    case Index_StoreFreqs | Index_StoreFieldFlags:
      RETURN_DECODERS(readFreqsFlags, NULL);

    case Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema:
      RETURN_DECODERS(readFreqsFlagsWide, NULL);

    // (fields, offsets)
    case Index_StoreFieldFlags | Index_StoreTermOffsets:
      RETURN_DECODERS(readFlagsOffsets, NULL);

    case Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
      RETURN_DECODERS(readFlagsOffsetsWide, NULL);

    case Index_StoreNumeric:
      RETURN_DECODERS(readNumeric, NULL);

    default:
      fprintf(stderr, "No decoder for flags %x\n", flags & INDEX_STORAGE_MASK);
      RETURN_DECODERS(NULL, NULL);
  }
}

IndexReader *NewNumericReader(const IndexSpec *sp, InvertedIndex *idx, const NumericFilter *flt,
                              double rangeMin, double rangeMax, int skipMulti) {
  RSIndexResult *res = NewNumericResult();
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  res->num.value = 0;

  IndexDecoderCtx ctx = {.ptr = (void *)flt, .rangeMin = rangeMin, .rangeMax = rangeMax};
  IndexDecoderProcs procs = {.decoder = readNumeric};
  return NewIndexReaderGeneric(sp, idx, procs, ctx, skipMulti, res);
}

typedef struct {
  IndexCriteriaTester base;
  union {
    NumericFilter nf;
    struct {
      char *term;
      size_t termLen;
      t_fieldMask fieldMask;
    } tf;
  };
  const IndexSpec *spec;
} IR_CriteriaTester;

static int IR_TestNumeric(IndexCriteriaTester *ct, t_docId id) {
  IR_CriteriaTester *irct = (IR_CriteriaTester *)ct;
  const IndexSpec *sp = irct->spec;
  size_t len;
  const sds externalId = DocTable_GetKey((DocTable *)&sp->docs, id, &len);
  double doubleValue;
  int ret = sp->getValue(sp->getValueCtx, irct->nf.fieldName, externalId, NULL, &doubleValue);
  RS_LOG_ASSERT(ret == RSVALTYPE_DOUBLE, "RSvalue type should be a double");
  sdsfree(externalId);
  return ((irct->nf.min < doubleValue || (irct->nf.inclusiveMin && irct->nf.min == doubleValue)) &&
          (irct->nf.max > doubleValue || (irct->nf.inclusiveMax && irct->nf.max == doubleValue)));
}

static void IR_TesterFreeNumeric(IndexCriteriaTester *ct) {
  IR_CriteriaTester *irct = (IR_CriteriaTester *)ct;
  rm_free(irct->nf.fieldName);
  rm_free(irct);
}

static int IR_TestTerm(IndexCriteriaTester *ct, t_docId id) {
  IR_CriteriaTester *irct = (IR_CriteriaTester *)ct;
  const IndexSpec *sp = irct->spec;
  size_t len;
  int ret = 0;
  const sds externalId = DocTable_GetKey(&sp->docs, id, &len);
  for (int i = 0; i < sp->numFields; ++i) {
    FieldSpec *field = sp->fields + i;
    if (!(FIELD_BIT(field) & irct->tf.fieldMask)) {
      // field is not requested, we are not checking this field!!
      continue;
    }
    char *strValue;
    int type = sp->getValue(sp->getValueCtx, field->name, externalId, &strValue, NULL);
    RS_LOG_ASSERT(type == RSVALTYPE_STRING, "RSvalue type should be a string");
    if (strcmp(irct->tf.term, strValue) == 0) {
      ret = 1;
      break;
    }
  }

  sdsfree(externalId);
  return ret;
}

static void IR_TesterFreeTerm(IndexCriteriaTester *ct) {
  IR_CriteriaTester *irct = (IR_CriteriaTester *)ct;
  rm_free(irct->tf.term);
  rm_free(irct);
}

IndexCriteriaTester *IR_GetCriteriaTester(void *ctx) {
  IndexReader *ir = ctx;
  if (!ir->sp || !ir->sp->getValue) {
    return NULL;  // CriteriaTester is not supported!!!
  }
  if (ir->decoders.decoder == readNumeric) {
    // for now, if the iterator did not took the numric filter
    // we will avoid using the CT.
    // TODO: save the numeric filter in the numeric iterator to support CT anyway.
    if (!ir->decoderCtx.ptr) {
      return NULL;
    }
  }
  IR_CriteriaTester *irct = rm_malloc(sizeof(*irct));
  irct->spec = ir->sp;
  if (ir->decoders.decoder == readNumeric) {
    irct->nf = *(NumericFilter *)ir->decoderCtx.ptr;
    irct->nf.fieldName = rm_strdup(irct->nf.fieldName);
    irct->base.Test = IR_TestNumeric;
    irct->base.Free = IR_TesterFreeNumeric;
  } else {
    irct->tf.term = rm_strdup(ir->record->term.term->str);
    irct->tf.termLen = ir->record->term.term->len;
    irct->tf.fieldMask = ir->decoderCtx.num;
    irct->base.Test = IR_TestTerm;
    irct->base.Free = IR_TesterFreeTerm;
  }
  return &irct->base;
}

size_t IR_NumEstimated(void *ctx) {
  IndexReader *ir = ctx;
  return ir->idx->numDocs;
}

int IR_Read(void *ctx, RSIndexResult **e) {

  IndexReader *ir = ctx;
  if (IR_IS_AT_END(ir)) {
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

    size_t pos = ir->br.pos;
    int rv = ir->decoders.decoder(&ir->br, &ir->decoderCtx, ir->record);
    RSIndexResult *record = ir->record;

    // We write the docid as a 32 bit number when decoding it with qint.
    uint32_t delta = *(uint32_t *)&record->docId;
    if (ir->decoders.decoder != readRawDocIdsOnly) {
      ir->lastId = record->docId = ir->lastId + delta;
    } else {
      ir->lastId = record->docId = IR_CURRENT_BLOCK(ir).firstId + delta;
    }

    // The decoder also acts as a filter. A zero return value means that the
    // current record should not be processed.
    if (!rv) {
      continue;
    }

    if (ir->skipMulti) {
    // Avoid returning the same doc
    //
    // Currently the only relevant predicate for multi-value is `any`, therefore only the first match in each doc is needed.
    // More advanced predicates, such as `at least <N>` or `exactly <N>`, will require adding more logic.
      if( ir->sameId == ir->lastId) {
        continue;
      }
      ir->sameId = ir->lastId;
    }


    ++ir->len;
    *e = record;
    return INDEXREAD_OK;

  } while (1);
eof:
  IR_SetAtEnd(ir, 1);
  return INDEXREAD_EOF;
}

#define BLOCK_MATCHES(blk, docId) ((blk).firstId <= docId && docId <= (blk).lastId)

static int IndexReader_SkipToBlock(IndexReader *ir, t_docId docId) {
  int rc = 0;
  InvertedIndex *idx = ir->idx;

  // the current block doesn't match and it's the last one - no point in searching
  if (ir->currentBlock + 1 == idx->size) {
    return 0;
  }

  uint32_t top = idx->size - 1;
  uint32_t bottom = ir->currentBlock + 1;
  uint32_t i = bottom;  //(bottom + top) / 2;
  while (bottom <= top) {
    const IndexBlock *blk = idx->blocks + i;
    if (BLOCK_MATCHES(*blk, docId)) {
      ir->currentBlock = i;
      rc = 1;
      goto new_block;
    }

    if (docId < blk->firstId) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
    i = (bottom + top) / 2;
  }

  ir->currentBlock = i;

new_block:
  ir->lastId = IR_CURRENT_BLOCK(ir).firstId;
  ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).buf);
  return rc;
}

int IR_SkipTo(void *ctx, t_docId docId, RSIndexResult **hit) {
  IndexReader *ir = ctx;
  if (!docId) {
    return IR_Read(ctx, hit);
  }

  if (IR_IS_AT_END(ir)) {
    goto eof;
  }

  if (docId > ir->idx->lastId || ir->idx->size == 0) {
    goto eof;
  }

  if (!BLOCK_MATCHES(IR_CURRENT_BLOCK(ir), docId)) {
    IndexReader_SkipToBlock(ir, docId);
  } else if (BufferReader_AtEnd(&ir->br)) {
    // Current block, but there's nothing here
    if (IR_Read(ir, hit) == INDEXREAD_EOF) {
      goto eof;
    } else {
      return INDEXREAD_NOTFOUND;
    }
  }

  /**
   * We need to replicate the effects of IR_Read() without actually calling it
   * continuously.
   *
   * The seeker function saves CPU by avoiding unnecessary function
   * calls and pointer derefences/accesses if the requested ID is
   * not found. Because less checking is required
   *
   * We:
   * 1. Call IR_Read() at least once
   * 2. IR_Read seeks ahead to the first non-empty block
   * 3. IR_Read reads the current record
   * 4. If the current record's flags do not match the fieldmask, it
   *    continues to step 2
   * 5. If the current record's flags match, the function exits
   * 6. The returned ID is examined. If:
   *    - ID is smaller than requested, continue to step 1
   *    - ID is larger than requested, return NOTFOUND
   *    - ID is equal, return OK
   */

  if (ir->decoders.seeker) {
    // // if needed - skip to the next block (skipping empty blocks that may appear here due to GC)
    while (BufferReader_AtEnd(&ir->br)) {
      // We're at the end of the last block...
      if (ir->currentBlock + 1 == ir->idx->size) {
        goto eof;
      }
      IndexReader_AdvanceBlock(ir);
    }

    // the seeker will return 1 only when it found a docid which is greater or equals the
    // searched docid and the field mask matches the searched fields mask. We need to continue
    // scanning only when we found such an id or we reached the end of the inverted index.
    while (!ir->decoders.seeker(&ir->br, &ir->decoderCtx, ir, docId, ir->record)) {
      if (BufferReader_AtEnd(&ir->br)) {
        if (ir->currentBlock < ir->idx->size - 1) {
          IndexReader_AdvanceBlock(ir);
        } else {
          return INDEXREAD_EOF;
        }
      }
    }
    // Found a document that match the field mask and greater or equal the searched docid
    *hit = ir->record;
    return (ir->record->docId == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
  } else {
    int rc;
    t_docId rid;
    while (INDEXREAD_EOF != (rc = IR_Read(ir, hit))) {
      rid = ir->lastId;
      if (rid < docId) continue;
      if (rid == docId) return INDEXREAD_OK;
      return INDEXREAD_NOTFOUND;
    }
  }
eof:
  IR_SetAtEnd(ir, 1);
  return INDEXREAD_EOF;
}

size_t IR_NumDocs(void *ctx) {
  IndexReader *ir = ctx;
  // otherwise we use our counter
  return ir->len;
}

static void IndexReader_Init(const IndexSpec *sp, IndexReader *ret, InvertedIndex *idx,
                             IndexDecoderProcs decoder, IndexDecoderCtx decoderCtx, int skipMulti,
                             RSIndexResult *record) {
  ret->currentBlock = 0;
  ret->idx = idx;
  ret->gcMarker = idx->gcMarker;
  ret->record = record;
  ret->len = 0;
  ret->lastId = IR_CURRENT_BLOCK(ret).firstId;
  ret->sameId = 0;
  ret->skipMulti = skipMulti;
  ret->br = NewBufferReader(&IR_CURRENT_BLOCK(ret).buf);
  ret->decoders = decoder;
  ret->decoderCtx = decoderCtx;
  ret->isValidP = NULL;
  ret->sp = sp;
  IR_SetAtEnd(ret, 0);
}

static IndexReader *NewIndexReaderGeneric(const IndexSpec *sp, InvertedIndex *idx,
                                          IndexDecoderProcs decoder, IndexDecoderCtx decoderCtx, int skipMulti,
                                          RSIndexResult *record) {
  IndexReader *ret = rm_malloc(sizeof(IndexReader));
  IndexReader_Init(sp, ret, idx, decoder, decoderCtx, skipMulti, record);
  return ret;
}

IndexReader *NewTermIndexReader(InvertedIndex *idx, IndexSpec *sp, t_fieldMask fieldMask,
                                RSQueryTerm *term, double weight) {
  if (term && sp) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sp->docs.size, idx->numDocs);
    term->bm25_idf = CalculateIDF_BM25(sp->stats.numDocuments, idx->numDocs);
  }

  // Get the decoder
  IndexDecoderProcs decoder = InvertedIndex_GetDecoder((uint32_t)idx->flags & INDEX_STORAGE_MASK);
  if (!decoder.decoder) {
    return NULL;
  }

  RSIndexResult *record = NewTokenRecord(term, weight);
  record->fieldMask = RS_FIELDMASK_ALL;
  record->freq = 1;

  IndexDecoderCtx dctx = {.num = fieldMask};

  return NewIndexReaderGeneric(sp, idx, decoder, dctx, false, record);
}

void IR_Free(IndexReader *ir) {

  IndexResult_Free(ir->record);
  rm_free(ir);
}

void IR_Abort(void *ctx) {
  IndexReader *it = ctx;
  IR_SetAtEnd(it, 1);
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

void IR_Rewind(void *ctx) {

  IndexReader *ir = ctx;
  IR_SetAtEnd(ir, 0);
  ir->currentBlock = 0;
  ir->gcMarker = ir->idx->gcMarker;
  ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).buf);
  ir->lastId = IR_CURRENT_BLOCK(ir).firstId;
}

IndexIterator *NewReadIterator(IndexReader *ir) {
  IndexIterator *ri = rm_malloc(sizeof(IndexIterator));
  ri->ctx = ir;
  ri->mode = MODE_SORTED;
  ri->type = READ_ITERATOR;
  ri->NumEstimated = IR_NumEstimated;
  ri->GetCriteriaTester = IR_GetCriteriaTester;
  ri->Read = IR_Read;
  ri->SkipTo = IR_SkipTo;
  ri->LastDocId = IR_LastDocId;
  ri->Free = ReadIterator_Free;
  ri->Len = IR_NumDocs;
  ri->Abort = IR_Abort;
  ri->Rewind = IR_Rewind;
  ri->HasNext = NULL;
  ri->isValid = !ir->atEnd_;
  ri->current = ir->record;

  ir->isValidP = &ri->isValid;
  return ri;
}

/* Repair an index block by removing garbage - records pointing at deleted documents.
 * Returns the number of records collected, and puts the number of bytes collected in the given
 * pointer. If an error occurred - returns -1
 */
int IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags, IndexRepairParams *params) {
  t_docId firstReadId = blk->firstId;
  t_docId lastReadId = blk->firstId;
  bool isFirstRes = true;

  t_docId oldFirstBlock = blk->lastId;
  blk->lastId = blk->firstId = 0;
  Buffer repair = {0};
  BufferReader br = NewBufferReader(&blk->buf);
  BufferWriter bw = NewBufferWriter(&repair);

  RSIndexResult *res = flags == Index_StoreNumeric ? NewNumericResult() : NewTokenRecord(NULL, 1);
  size_t frags = 0;
  int isLastValid = 0;

  uint32_t readFlags = flags & INDEX_STORAGE_MASK;
  IndexDecoderProcs decoders = InvertedIndex_GetDecoder(readFlags);
  IndexEncoder encoder = InvertedIndex_GetEncoder(readFlags);

  if (!encoder || !decoders.decoder) {
    fprintf(stderr, "Could not get decoder/encoder for index\n");
    return -1;
  }

  params->bytesBeforFix = blk->buf.offset;

  int docExists;
  while (!BufferReader_AtEnd(&br)) {
    static const IndexDecoderCtx empty = {0};
    const char *bufBegin = BufferReader_Current(&br);
    decoders.decoder(&br, &empty, res);
    size_t sz = BufferReader_Current(&br) - bufBegin;
    if (!(isFirstRes && res->docId != 0)) {
      // if we are entering this here
      // then its not the first entry or its
      // not an old rdb version
      // on an old rdb version, the first entry is the docid itself and not
      // the delta, so no need to increase by the lastReadId
      if (decoders.decoder != readRawDocIdsOnly) {
        res->docId = (*(uint32_t *)&res->docId) + lastReadId;
      } else {
        res->docId = (*(uint32_t *)&res->docId) + firstReadId;
      }
    }
    // Increment frags only when moving to the next doc
    // (do not increment when moving to the next entry in the same doc)
    int fragsIncr = (isFirstRes || (lastReadId != res->docId)) ? 1 : 0;
    isFirstRes = false;
    lastReadId = res->docId;

    // Lookup the doc (for the same doc use the previous result)
    docExists = fragsIncr ? DocTable_Exists(dt, res->docId) : docExists;

    // If we found a deleted document, we increment the number of found "frags",
    // and not write anything, so the reader will advance but the writer won't.
    // this will close the "hole" in the index
    if (!docExists) {
      if (!frags) {
        // First invalid doc; copy everything prior to this to the repair
        // buffer
        Buffer_Write(&bw, blk->buf.data, bufBegin - blk->buf.data);
      }
      frags += fragsIncr;
      params->bytesCollected += sz;
      ++params->entriesCollected;
      isLastValid = 0;
    } else {
      if (params->RepairCallback) {
        params->RepairCallback(res, blk, params->arg);
      }
      // Valid document, but we're rewriting the block:
      if (frags) {

        // In this case we are already closing holes, so we need to write back the record at the
        // writer's position. We also calculate the delta again
        if (!blk->lastId) {
          blk->lastId = res->docId;
        }
        if (encoder != encodeRawDocIdsOnly) {
          if (isLastValid) {
            Buffer_Write(&bw, bufBegin, sz);
          } else {
            encoder(&bw, res->docId - blk->lastId, res);
          }
        } else { // encoder == encodeRawDocIdsOnly
          if (!blk->firstId) {
            blk->firstId = res->docId;
          }
            encoder(&bw, res->docId - blk->firstId, res);
          }
      }

      // Update these for every valid document, even for those which
      // are not repaired
      if (blk->firstId == 0) {
        blk->firstId = res->docId;
      }
      blk->lastId = res->docId;
      isLastValid = 1;
    }
  }
  if (frags) {
    // If we deleted stuff from this block, we need to change the number of entries and the data
    // pointer
    blk->numEntries -= params->entriesCollected;
    Buffer_Free(&blk->buf);
    blk->buf = repair;
    Buffer_ShrinkToSize(&blk->buf);
  }
  if (blk->numEntries == 0) {
    // if we left with no elements we do need to keep the
    // first id so the binary search on the block will still working.
    // The last_id will turn zero indicating there is no records in
    // this block. We will not save empty blocks in rdb and also we
    // will not read empty block from rdb (in case we read a corrunpted
    // rdb from older versions).
    blk->firstId = oldFirstBlock;
  }

  params->bytesAfterFix = blk->buf.offset;

  IndexResult_Free(res);
  return frags;
}

int InvertedIndex_Repair(InvertedIndex *idx, DocTable *dt, uint32_t startBlock,
                         IndexRepairParams *params) {
  size_t limit = params->limit ? params->limit : SIZE_MAX;
  size_t blocksProcessed = 0;
  for (; startBlock < idx->size && blocksProcessed < limit; ++startBlock, ++blocksProcessed) {
    IndexBlock *blk = idx->blocks + startBlock;
    if (blk->lastId - blk->firstId > UINT32_MAX) {
      // Skip over blocks which have a wide variation. In the future we might
      // want to split a block into two (or more) on high-delta boundaries.
      continue;
    }
    int repaired = IndexBlock_Repair(&idx->blocks[startBlock], dt, idx->flags, params);
    // We couldn't repair the block - return 0
    if (repaired == -1) {
      return 0;
    } else if (repaired > 0) {
      // Record the number of records removed for gc stats
      params->docsCollected += repaired;
      idx->numDocs -= repaired;

      // Increase the GC marker so other queries can tell that we did something
      ++idx->gcMarker;
    }
  }

  return startBlock < idx->size ? startBlock : 0;
}
