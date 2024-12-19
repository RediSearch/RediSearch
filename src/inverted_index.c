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

// The last block of the index
#define INDEX_LAST_BLOCK(idx) (idx->blocks[idx->size - 1])

// pointer to the current block while reading the index
#define IR_CURRENT_BLOCK(ir) (ir->idx->blocks[ir->currentBlock])

static IndexReader *NewIndexReaderGeneric(const RedisSearchCtx *sctx, InvertedIndex *idx,
                                          IndexDecoderProcs decoder, IndexDecoderCtx decoderCtx, bool skipMulti,
                                          RSIndexResult *record, const FieldFilterContext* filterCtx);

IndexBlock *InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId, size_t *memsize) {
  TotalIIBlocks++;
  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  IndexBlock *last = idx->blocks + (idx->size - 1);
  memset(last, 0, sizeof(*last));  // for msan
  last->firstId = last->lastId = firstId;
  Buffer_Init(&INDEX_LAST_BLOCK(idx).buf, INDEX_BLOCK_INITIAL_CAP);
  (*memsize) += sizeof(IndexBlock) + INDEX_BLOCK_INITIAL_CAP;
  return &INDEX_LAST_BLOCK(idx);
}

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock, size_t *memsize) {
  RedisModule_Assert(memsize != NULL);
  int useFieldMask = flags & Index_StoreFieldFlags;
  int useNumEntries = flags & Index_StoreNumeric;
  RedisModule_Assert(!(useFieldMask && useNumEntries));
  size_t size = sizeof_InvertedIndex(flags);
  InvertedIndex *idx = rm_malloc(size);
  *memsize = size;
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
    InvertedIndex_AddBlock(idx, 0, memsize);
  }
  return idx;
}

size_t indexBlock_Free(IndexBlock *blk) {
  return Buffer_Free(&blk->buf);
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

static void IR_SetAtEnd(IndexReader *r, bool value) {
  if (r->isValidP) {
    *r->isValidP = !value;
  }
  r->atEnd_ = value;
}
#define IR_IS_AT_END(ir) (ir)->atEnd_

/* A callback called from the ConcurrentSearchCtx after regaining execution and reopening the
 * underlying term key. We check for changes in the underlying key, or possible deletion of it */
void TermReader_OnReopen(void *privdata) {
  IndexReader *ir = privdata;
  if (ir->record->type == RSResultType_Term) {
    // we need to reopen the inverted index to make sure its still valid.
    // the GC might have deleted it by now.
    InvertedIndex *idx = Redis_OpenInvertedIndex(ir->sctx, ir->record->term.term->str,
                                                 ir->record->term.term->len, 0, NULL);
    if (!idx || ir->idx != idx) {
      // The inverted index was collected entirely by GC.
      // All the documents that were inside were deleted and new ones were added.
      // We will not continue reading those new results and instead abort reading
      // for this specific inverted index.
      IR_Abort(ir);
      return;
    }
  }

  IndexReader_OnReopen(ir);
}

void IndexReader_OnReopen(IndexReader *ir) {
  if (IR_IS_AT_END(ir)) {
    // Save time and state if we are already at the end
    return;
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

    // keep the last docId we were at
    t_docId lastId = ir->lastId;
    // reset the state of the reader
    IR_Rewind(ir);
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

// 9. Special encoder for numeric values
ENCODER(encodeNumeric) {
  const double absVal = fabs(res->num.value);
  const double realVal = res->num.value;
  const float f32Num = absVal;
  uint64_t u64Num = (uint64_t)absVal;
  const uint8_t tinyNum = u64Num & NUM_TINYENC_MASK;

  EncodingHeader header = {.storage = 0};

  // Write a placeholder for the header and mark its position
  size_t pos = BufferWriter_Offset(bw); // save the current position to the buffer. here we will store the header
  size_t sz = Buffer_Write(bw, "\0", sizeof(EncodingHeader)); // promote the buffer by the header size (1 byte)

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

  } else if ((double)u64Num == absVal) {
    // Is a whole number
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
      RS_LOG_ASSERT_FMT(0, "Invalid encoder flags: %d", flags);
      return NULL;
  }
}

/* Write a forward-index entry to an index writer */
size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder, t_docId docId,
                                       RSIndexResult *entry) {
  size_t sz = 0;
  bool same_doc = 0;
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
    blk = InvertedIndex_AddBlock(idx, docId, &sz);
  } else if (blk->numEntries == 0) {
    blk->firstId = blk->lastId = docId;
  }

  if (encoder != encodeRawDocIdsOnly) {
    delta = docId - blk->lastId;
  } else {
    delta = docId - blk->firstId;
  }

  // For non-numeric encoders the maximal delta is UINT32_MAX (since it is encoded with 4 bytes)
  // For numeric encoder the maximal delta has to fit in 7 bytes (since it is encoded with 0-7 bytes)
  const t_docId maxDelta = encoder == encodeNumeric ? (UINT64_MAX >> 8) : UINT32_MAX;
  if (delta > maxDelta) {
    blk = InvertedIndex_AddBlock(idx, docId, &sz);
    delta = 0;
  }

  BufferWriter bw = NewBufferWriter(&blk->buf);

  sz += encoder(&bw, delta, entry);

  idx->lastId = docId;
  blk->lastId = docId;
  ++blk->numEntries;
  if (!same_doc) {
    ++idx->numDocs;
  }
  if (encoder == encodeNumeric) {
    ++idx->numEntries;
  }

  return sz;
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
  static bool name(BufferReader *br, const IndexDecoderCtx *ctx, RSIndexResult *res, t_docId offset)

/**
 * Skipper implements SkipTo. It is an optimized version of DECODER which reads
 * the document ID first, and skips ahead if the result does not match the
 * expected one.
 */
#define SKIPPER(name)                                                                            \
  static bool name(BufferReader *br, const IndexDecoderCtx *ctx, IndexReader *ir, t_docId expid, \
                   RSIndexResult *res)

DECODER(readFreqsFlags) {
  uint32_t delta, fieldMask;
  qint_decode3(br, &delta, &res->freq, &fieldMask);
  res->docId = delta + offset;
  res->fieldMask = fieldMask;
  return fieldMask & ctx->mask;
}

DECODER(readFreqsFlagsWide) {
  uint32_t delta;
  qint_decode2(br, &delta, &res->freq);
  res->docId = delta + offset;
  res->fieldMask = ReadVarintFieldMask(br);
  return res->fieldMask & ctx->wideMask;
}

DECODER(readFreqOffsetsFlags) {
  uint32_t delta, fieldMask;
  qint_decode4(br, &delta, &res->freq, &fieldMask, &res->offsetsSz);
  res->docId = delta + offset;
  res->fieldMask = fieldMask;
  res->term.offsets.data = BufferReader_Current(br);
  res->term.offsets.len = res->offsetsSz;
  Buffer_Skip(br, res->offsetsSz);
  return fieldMask & ctx->mask;
}

SKIPPER(seekFreqOffsetsFlags) {
  uint32_t did = 0, freq = 0, offsz = 0, fm = 0;
  bool rc = false;

  while (!BufferReader_AtEnd(br)) {
    qint_decode4(br, &did, &freq, &fm, &offsz);
    Buffer_Skip(br, offsz);
    ir->lastId = (did += ir->lastId);
    if (!(ctx->mask & fm)) {
      continue;  // we just ignore it if it does not match the field mask
    }
    if (did >= expid) {
      // Overshoot!
      rc = true;
      break;
    }
  }

  res->docId = did;
  res->freq = freq;
  res->fieldMask = fm;
  res->offsetsSz = offsz;
  res->term.offsets.data = BufferReader_Current(br) - offsz;
  res->term.offsets.len = offsz;

  return rc;
}

DECODER(readFreqOffsetsFlagsWide) {
  uint32_t delta;
  qint_decode3(br, &delta, &res->freq, &res->offsetsSz);
  res->docId = delta + offset;
  res->fieldMask = ReadVarintFieldMask(br);
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return res->fieldMask & ctx->wideMask;
}

// special decoder for decoding numeric results
DECODER(readNumeric) {
  EncodingHeader header;
  Buffer_Read(br, &header, 1);

  // Read the delta (if not zero)
  t_docId delta = 0;
  Buffer_Read(br, &delta, header.encCommon.deltaEncoding);
  res->docId = offset + delta;

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

  const NumericFilter *f = ctx->filter;
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
  uint32_t delta;
  qint_decode2(br, &delta, &res->freq);
  res->docId = delta + offset;
  return 1;
}

DECODER(readFlags) {
  uint32_t delta, mask;
  qint_decode2(br, &delta, &mask);
  res->docId = delta + offset;
  res->fieldMask = mask;
  return mask & ctx->mask;
}

DECODER(readFlagsWide) {
  res->docId = ReadVarint(br) + offset;
  res->freq = 1;
  res->fieldMask = ReadVarintFieldMask(br);
  return res->fieldMask & ctx->wideMask;
}

DECODER(readFlagsOffsets) {
  uint32_t delta, mask;
  qint_decode3(br, &delta, &mask, &res->offsetsSz);
  res->fieldMask = mask;
  res->docId = delta + offset;
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return mask & ctx->mask;
}

DECODER(readFlagsOffsetsWide) {
  uint32_t delta;
  qint_decode2(br, &delta, &res->offsetsSz);
  res->fieldMask = ReadVarintFieldMask(br);
  res->docId = delta + offset;
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};

  Buffer_Skip(br, res->offsetsSz);
  return res->fieldMask & ctx->wideMask;
}

DECODER(readOffsets) {
  uint32_t delta;
  qint_decode2(br, &delta, &res->offsetsSz);
  res->docId = delta + offset;
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

DECODER(readFreqsOffsets) {
  uint32_t delta;
  qint_decode3(br, &delta, &res->freq, &res->offsetsSz);
  res->docId = delta + offset;
  res->term.offsets = (RSOffsetVector){.data = BufferReader_Current(br), .len = res->offsetsSz};
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

SKIPPER(seekRawDocIdsOnly) {
  int64_t delta = expid - IR_CURRENT_BLOCK(ir).firstId;

  uint32_t curVal;
  Buffer_Read(br, &curVal, sizeof(curVal));
  if (curVal >= delta || delta < 0) {
    goto final;
  }

  uint32_t *buf = (uint32_t *)br->buf->data;
  size_t start = br->pos / 4;
  size_t end = (br->buf->offset - 4) / 4;
  size_t cur;

  // perform binary search
  while (start <= end) {
    cur = (end + start) / 2;
    curVal = buf[cur];
    if (curVal == delta) {
      goto found;
    }
    if (curVal > delta) {
      end = cur - 1;
    } else {
      start = cur + 1;
    }
  }

  // we didn't find the value, so we need to return the first value that is greater than the delta.
  // Assuming we are at the right block, such value must exist.
  // if got here, curVal is either the last value smaller than delta, or the first value greater
  // than delta. If it is the last value smaller than delta, we need to skip to the next value.
  if (curVal < delta) {
    cur++;
    curVal = buf[cur];
  }

found:
  // skip to next position
  Buffer_Seek(br, (cur + 1) * sizeof(uint32_t));

final:
  res->docId = curVal + IR_CURRENT_BLOCK(ir).firstId;
  res->freq = 1;
  return 1;
}

DECODER(readRawDocIdsOnly) {
  uint32_t delta;
  Buffer_Read(br, &delta, sizeof delta);
  res->docId = delta + offset;
  res->freq = 1;
  return 1;  // Don't care about field mask
}

DECODER(readDocIdsOnly) {
  res->docId = ReadVarint(br) + offset;
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
      RS_LOG_ASSERT_FMT(0, "Invalid index flags: %d", flags);
      RETURN_DECODERS(NULL, NULL);
  }
}

IndexReader *NewNumericReader(const RedisSearchCtx *sctx, InvertedIndex *idx, const NumericFilter *flt,
                              double rangeMin, double rangeMax, bool skipMulti,
                              const FieldFilterContext* fieldCtx) {
  RSIndexResult *res = NewNumericResult();
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;
  res->num.value = 0;

  IndexDecoderCtx ctx = {.filter = flt};
  IndexDecoderProcs procs = {.decoder = readNumeric};
  IndexReader *ir = NewIndexReaderGeneric(sctx, idx, procs, ctx, skipMulti, res, fieldCtx);
  ir->profileCtx.numeric.rangeMax = rangeMax;
  ir->profileCtx.numeric.rangeMin = rangeMin;
  return ir;
}

IndexReader *NewMinimalNumericReader(InvertedIndex *idx, bool skipMulti) {
  FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
  FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
  return NewNumericReader(NULL, idx, NULL, 0, 0, skipMulti, &fieldCtx);
}

size_t IR_NumEstimated(void *ctx) {
  IndexReader *ir = ctx;
  return ir->idx->numDocs;
}

#define FIELD_MASK_BIT_COUNT (sizeof(t_fieldMask) * 8)

int IR_Read(void *ctx, RSIndexResult **e) {

  IndexReader *ir = ctx;
  if (IR_IS_AT_END(ir)) {
    goto eof;
  }
  do {

    // if needed - skip to the next block (skipping empty blocks that may appear here due to GC)
    while (BufferReader_AtEnd(&ir->br)) {
      if (ir->currentBlock + 1 == ir->idx->size) {
        // We're at the end of the last block...
        goto eof;
      }
      IndexReader_AdvanceBlock(ir);
    }
    t_docId offset = (ir->decoders.decoder != readRawDocIdsOnly) ? ir->lastId : IR_CURRENT_BLOCK(ir).firstId;
    int rv = ir->decoders.decoder(&ir->br, &ir->decoderCtx, ir->record, offset);
    RSIndexResult *record = ir->record;
    ir->lastId = record->docId;

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

    if (ir->sctx && ir->sctx->spec && DocTable_HasExpiration(&ir->sctx->spec->docs, record->docId)) {
      size_t numFieldIndices = 0;
      // Use a stack allocated array for the field indices, if the field mask is not a single field
      t_fieldIndex fieldIndicesArray[FIELD_MASK_BIT_COUNT];
      t_fieldIndex* sortedFieldIndices = fieldIndicesArray;
      if (ir->filterCtx.field.isFieldMask) {
        numFieldIndices = IndexSpec_TranslateMaskToFieldIndices(ir->sctx->spec, ir->filterCtx.field.value.mask, fieldIndicesArray);
      } else if (ir->filterCtx.field.value.index != RS_INVALID_FIELD_INDEX) {
        sortedFieldIndices = &ir->filterCtx.field.value.index;
        ++numFieldIndices;
      }
      const bool validValue = DocTable_VerifyFieldExpirationPredicate(&ir->sctx->spec->docs, record->docId, sortedFieldIndices, numFieldIndices, ir->filterCtx.predicate, &ir->sctx->time.current);
      if (!validValue) {
        continue;
      }
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

// Assumes there is a valid block to skip to (maching or past the requested docId)
static void IndexReader_SkipToBlock(IndexReader *ir, t_docId docId) {
  InvertedIndex *idx = ir->idx;
  uint32_t top = idx->size - 1;
  uint32_t bottom = ir->currentBlock + 1;

  if (docId <= idx->blocks[bottom].lastId) {
    // the next block is the one we're looking for, although it might not contain the docId
    ir->currentBlock = bottom;
    goto new_block;
  }

  uint32_t i;
  while (bottom <= top) {
    i = (bottom + top) / 2;
    const IndexBlock *blk = idx->blocks + i;
    if (BLOCK_MATCHES(*blk, docId)) {
      ir->currentBlock = i;
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
  ir->currentBlock = i;
  if (IR_CURRENT_BLOCK(ir).lastId < docId) {
    ir->currentBlock++; // It's not the current block. Advance
  }

new_block:
  ir->lastId = IR_CURRENT_BLOCK(ir).firstId;
  ir->br = NewBufferReader(&IR_CURRENT_BLOCK(ir).buf);
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

  if (IR_CURRENT_BLOCK(ir).lastId < docId || BufferReader_AtEnd(&ir->br)) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    IndexReader_SkipToBlock(ir, docId);
  }

  /**
   * We need to replicate the effects of IR_Read() without actually calling it
   * continuously.
   *
   * The seeker function saves CPU by avoiding unnecessary function
   * calls and pointer dereferences/accesses if the requested ID is
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
    // the seeker will return 1 only when it found a docid which is greater or equals the
    // searched docid and the field mask matches the searched fields mask. We need to continue
    // scanning only when we found such an id or we reached the end of the inverted index.
    while (!ir->decoders.seeker(&ir->br, &ir->decoderCtx, ir, docId, ir->record)) {
      if (ir->currentBlock < ir->idx->size - 1) {
        IndexReader_AdvanceBlock(ir);
      } else {
        goto eof;
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
  return ir->len;
}

static void IndexReader_Init(const RedisSearchCtx *sctx, IndexReader *ret, InvertedIndex *idx,
                             IndexDecoderProcs decoder, IndexDecoderCtx decoderCtx, bool skipMulti,
                             RSIndexResult *record, const FieldFilterContext* filterCtx) {
  // The default ctx is needed because filterCtx can be null in the case of NewOptimizerIterator
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
  ret->filterCtx = *filterCtx;
  ret->isValidP = NULL;
  ret->sctx = sctx;
  IR_SetAtEnd(ret, 0);
}

static IndexReader *NewIndexReaderGeneric(const RedisSearchCtx *sctx, InvertedIndex *idx,
                                          IndexDecoderProcs decoder, IndexDecoderCtx decoderCtx, bool skipMulti,
                                          RSIndexResult *record, const FieldFilterContext* filterCtx) {
  IndexReader *ret = rm_malloc(sizeof(IndexReader));
  IndexReader_Init(sctx, ret, idx, decoder, decoderCtx, skipMulti, record, filterCtx);
  return ret;
}

IndexReader *NewTermIndexReaderEx(InvertedIndex *idx, const RedisSearchCtx *sctx, FieldMaskOrIndex fieldMaskOrIndex,
                                RSQueryTerm *term, double weight) {
  if (term && sctx) {
    // compute IDF based on num of docs in the header
    term->idf = CalculateIDF(sctx->spec->docs.size, idx->numDocs);
    term->bm25_idf = CalculateIDF_BM25(sctx->spec->stats.numDocuments, idx->numDocs);
  }

  // Get the decoder
  IndexDecoderProcs decoder = InvertedIndex_GetDecoder(idx->flags);

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

  FieldFilterContext filterCtx = {.field = fieldMaskOrIndex,
                                  .predicate = FIELD_EXPIRATION_DEFAULT};
  return NewIndexReaderGeneric(sctx, idx, decoder, dctx, false, record, &filterCtx);
}

IndexReader *NewTermIndexReader(InvertedIndex *idx) {
  FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
  return NewTermIndexReaderEx(idx, NULL, fieldMaskOrIndex, NULL, 1);
}

IndexReader *NewGenericIndexReader(InvertedIndex *idx, const RedisSearchCtx *sctx, double weight, uint32_t freq,
                                   t_fieldIndex fieldIndex, enum FieldExpirationPredicate predicate) {
  IndexDecoderCtx dctx = {.wideMask = RS_FIELDMASK_ALL}; // Also covers the case of a non-wide schema
  IndexDecoderProcs decoder = InvertedIndex_GetDecoder(idx->flags);
  FieldFilterContext fieldFilterCtx = {.field.isFieldMask = false, .field.value.index = fieldIndex, .predicate = predicate };
  RSIndexResult *record = NewVirtualResult(weight, RS_FIELDMASK_ALL);
  record->freq = freq;
  return NewIndexReaderGeneric(sctx, idx, decoder, dctx, false, record, &fieldFilterCtx);
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
  ir->sameId = 0;
}

IndexIterator *NewReadIterator(IndexReader *ir) {
  IndexIterator *ri = rm_malloc(sizeof(IndexIterator));
  ri->ctx = ir;
  ri->type = READ_ITERATOR;
  ri->NumEstimated = IR_NumEstimated;
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

/* Repair an index block by removing garbage - records pointing at deleted documents,
 * and write valid entries in their place.
 * Returns the number of docs collected, and puts the number of bytes collected in the given
 * pointer.
 */
size_t IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags, IndexRepairParams *params) {
  t_docId firstReadId = blk->firstId;
  t_docId lastReadId = blk->firstId;
  bool isFirstRes = true;

  blk->lastId = blk->firstId = 0;
  Buffer repair = {0};
  BufferReader br = NewBufferReader(&blk->buf);
  BufferWriter bw = NewBufferWriter(&repair);

  RSIndexResult *res = flags == Index_StoreNumeric ? NewNumericResult() : NewTokenRecord(NULL, 1);
  size_t frags = 0;
  bool isLastValid = false;

  uint32_t readFlags = flags & INDEX_STORAGE_MASK;
  IndexDecoderProcs decoders = InvertedIndex_GetDecoder(readFlags);
  IndexEncoder encoder = InvertedIndex_GetEncoder(readFlags);

  t_docId * const offset = (decoders.decoder != readRawDocIdsOnly) ? &lastReadId : &firstReadId;

  params->bytesBeforFix = blk->buf.cap;

  bool docExists;
  while (!BufferReader_AtEnd(&br)) {
    static const IndexDecoderCtx empty = {0};
    const char *bufBegin = BufferReader_Current(&br);
    // read the curr entry of the buffer into res and promote the buffer to the next one.
    // if it's not a legacy version, res->docId contains the delta from the previous entry
    decoders.decoder(&br, &empty, res, *offset);
    size_t sz = BufferReader_Current(&br) - bufBegin;

    // Multi value documents are saved as individual entries that share the same docId.
    // Increment frags only when moving to the next doc
    // (do not increment when moving to the next entry in the same doc)
    unsigned fragsIncr = 0;
    if (isFirstRes || (lastReadId != res->docId)) {
      fragsIncr = 1;
      // Lookup the doc (for the same doc use the previous result)
      docExists = DocTable_Exists(dt, res->docId);
    }
    isFirstRes = false;
    lastReadId = res->docId;

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
      isLastValid = false;
    } else { // the doc exist
      if (params->RepairCallback) {
        params->RepairCallback(res, blk, params->arg);
      }
      if (blk->firstId == 0) { // this is the first valid doc
        blk->firstId = res->docId;
        blk->lastId = res->docId;
      }

      // Valid document, but we're rewriting the block:
      if (frags) {
        if (encoder != encodeRawDocIdsOnly) {
          if (isLastValid) {
            // if the last was valid, the order of the entries didn't change. We can just copy the entry, as it already contains the correct delta.
            Buffer_Write(&bw, bufBegin, sz);
          } else { // we need to calculate the delta
            encoder(&bw, res->docId - blk->lastId, res);
          }
        } else { // encoder == encodeRawDocIdsOnly
          encoder(&bw, res->docId - blk->firstId, res);
        }
      }
      // Update the last seen valid doc id, even if we didn't write it (yet)
      blk->lastId = res->docId;
      isLastValid = true;
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

  params->bytesAfterFix = blk->buf.cap;

  IndexResult_Free(res);
  return frags;
}
