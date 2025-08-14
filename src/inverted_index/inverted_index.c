/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
#include "rmutil/rm_assert.h"
#include "geo_index.h"

uint64_t TotalIIBlocks = 0;

// The last block of the index
#define INDEX_LAST_BLOCK(idx) (InvertedIndex_BlockRef(idx, InvertedIndex_NumBlocks(idx) - 1))

IndexBlock *InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId, size_t *memsize) {
  TotalIIBlocks++;
  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  IndexBlock *last = idx->blocks + (idx->size - 1);
  memset(last, 0, sizeof(*last));  // for msan
  last->firstId = last->lastId = firstId;
  Buffer_Init(IndexBlock_Buffer(INDEX_LAST_BLOCK(idx)), INDEX_BLOCK_INITIAL_CAP);
  (*memsize) += sizeof(IndexBlock) + INDEX_BLOCK_INITIAL_CAP;
  return INDEX_LAST_BLOCK(idx);
}

InvertedIndex *NewInvertedIndex(IndexFlags flags, int initBlock, size_t *memsize) {
  RS_ASSERT(memsize != NULL);
  int useFieldMask = flags & Index_StoreFieldFlags;
  int useNumEntries = flags & Index_StoreNumeric;
  RS_ASSERT(!(useFieldMask && useNumEntries));
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

// Get a pointer to the block at the given index.
IndexBlock *InvertedIndex_BlockRef(const InvertedIndex *idx, size_t blockIndex) {
  RS_ASSERT(blockIndex < idx->size);
  return &idx->blocks[blockIndex];
}

// Take the block at the given index. This is needed by the fork GC to remove or move blocks
IndexBlock InvertedIndex_Block(InvertedIndex *idx, size_t blockIndex) {
  if (blockIndex >= idx->size) {
    return (IndexBlock){0}; // Return an empty block
  }
  return idx->blocks[blockIndex];
}

void InvertedIndex_SetBlock(InvertedIndex *idx, size_t blockIndex, IndexBlock block) {
  RS_ASSERT(blockIndex < idx->size);

  idx->blocks[blockIndex] = block;
}

void InvertedIndex_SetBlocks(InvertedIndex *idx, IndexBlock *blocks, size_t size) {
  if (idx->blocks) {
    rm_free(idx->blocks);
  }
  idx->blocks = blocks;
  idx->size = size;
}

size_t InvertedIndex_BlocksShift(InvertedIndex *idx, size_t shift) {
  size_t numBlocks = idx->size - shift;
  memmove(idx->blocks, idx->blocks + shift, numBlocks * sizeof(*idx->blocks));
  idx->size = numBlocks;
  return numBlocks;
}

size_t InvertedIndex_NumBlocks(const InvertedIndex *idx) {
  return idx->size;
}

void InvertedIndex_SetNumBlocks(InvertedIndex *idx, size_t numBlocks) {
  idx->size = numBlocks;
}

IndexFlags InvertedIndex_Flags(const InvertedIndex *idx) {
  return idx->flags;
}

t_docId InvertedIndex_LastId(const InvertedIndex *idx) {
  return idx->lastId;
}

void InvertedIndex_SetLastId(InvertedIndex *idx, t_docId lastId) {
  idx->lastId = lastId;
}

uint32_t InvertedIndex_NumDocs(const InvertedIndex *idx) {
  return idx->numDocs;
}

void InvertedIndex_SetNumDocs(InvertedIndex *idx, uint32_t numDocs) {
  idx->numDocs = numDocs;
}

uint32_t InvertedIndex_GcMarker(const InvertedIndex *idx) {
  return idx->gcMarker;
}

void InvertedIndex_SetGcMarker(InvertedIndex *idx, uint32_t marker) {
  idx->gcMarker = marker;
}

t_fieldMask InvertedIndex_FieldMask(const InvertedIndex *idx) {
  if (idx->flags & Index_StoreFieldFlags) {
    return idx->fieldMask;
  }
  return (t_fieldMask)0; // No field mask stored
}

void InvertedIndex_OrFieldMask(InvertedIndex *idx, t_fieldMask fieldMask) {
  if (idx->flags & Index_StoreFieldFlags) {
    idx->fieldMask |= fieldMask;
  }
}

uint64_t InvertedIndex_NumEntries(const InvertedIndex *idx) {
  return idx->numEntries;
}

void InvertedIndex_SetNumEntries(InvertedIndex *idx, uint64_t numEntries) {
  if (idx->flags & Index_StoreNumeric) {
    idx->numEntries = numEntries;
  }
}

size_t indexBlock_Free(IndexBlock *blk) {
  return Buffer_Free(IndexBlock_Buffer(blk));
}

t_docId IndexBlock_FirstId(const IndexBlock *b) {
  return b->firstId;
}

t_docId IndexBlock_LastId(const IndexBlock *b) {
  return b->lastId;
}

uint16_t IndexBlock_NumEntries(const IndexBlock *b) {
  return b->numEntries;
}

char *IndexBlock_Data(const IndexBlock *b) {
  return b->buf.data;
}

char **IndexBlock_DataPtr(IndexBlock *b) {
  return &b->buf.data;
}

void IndexBlock_DataFree(const IndexBlock *b) {
  rm_free(b->buf.data);
}

size_t IndexBlock_Cap(const IndexBlock *b) {
  return b->buf.cap;
}

void IndexBlock_SetCap(IndexBlock *b, size_t cap) {
  b->buf.cap = cap;
}

size_t IndexBlock_Len(const IndexBlock *b) {
  return b->buf.offset;
}

size_t *IndexBlock_LenPtr(IndexBlock *b) {
  return &b->buf.offset;
}

Buffer *IndexBlock_Buffer(IndexBlock *b) {
  return &b->buf;
}

void IndexBlock_SetBuffer(IndexBlock *b, Buffer buf) {
  b->buf = buf;
}

void InvertedIndex_Free(void *ctx) {
  InvertedIndex *idx = ctx;
  size_t numBlocks = InvertedIndex_NumBlocks(idx);
  TotalIIBlocks -= numBlocks;
  for (uint32_t i = 0; i < numBlocks; i++) {
    indexBlock_Free(&idx->blocks[i]);
  }
  rm_free(idx->blocks);
  rm_free(idx);
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
  uint32_t offsets_len;
  const RSTermRecord *term = IndexResult_TermRef(res);
  const char *offsets_data = RSOffsetVector_GetData(&term->offsets, &offsets_len);
  size_t sz = qint_encode4(bw, delta, res->freq, (uint32_t)res->fieldMask, res->offsetsSz);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

ENCODER(encodeFullWide) {
  uint32_t offsets_len;
  const RSTermRecord *term = IndexResult_TermRef(res);
  const char *offsets_data = RSOffsetVector_GetData(&term->offsets, &offsets_len);
  size_t sz = qint_encode3(bw, delta, res->freq, res->offsetsSz);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
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
  uint32_t offsets_len;
  const RSTermRecord *term = IndexResult_TermRef(res);
  const char *offsets_data = RSOffsetVector_GetData(&term->offsets, &offsets_len);
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->fieldMask, offsets_len);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

ENCODER(encodeFieldsOffsetsWide) {
  uint32_t offsets_len;
  const RSTermRecord *term = IndexResult_TermRef(res);
  const char *offsets_data = RSOffsetVector_GetData(&term->offsets, &offsets_len);
  size_t sz = qint_encode2(bw, delta, offsets_len);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

// 6. Offsets only
ENCODER(encodeOffsetsOnly) {
  uint32_t offsets_len;
  const RSTermRecord *term = IndexResult_TermRef(res);
  const char *offsets_data = RSOffsetVector_GetData(&term->offsets, &offsets_len);
  size_t sz = qint_encode2(bw, delta, offsets_len);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

// 7. Offsets and freqs
ENCODER(encodeFreqsOffsets) {
  uint32_t offsets_len;
  const RSTermRecord *term = IndexResult_TermRef(res);
  const char *offsets_data = RSOffsetVector_GetData(&term->offsets, &offsets_len);
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->freq, offsets_len);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
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
//  posint and negint - for none-zero integer numbers
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
  const double realVal = IndexResult_NumValue(res);
  const double absVal = fabs(realVal);
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

// Wrapper around the private static `encodeFull` function to expose it to benchmarking.
size_t encode_full(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFull(bw, delta, res);
}

// Wrapper around the private static `encodeFullWide` function to expose it to benchmarking.
size_t encode_full_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFullWide(bw, delta, res);
}

// Wrapper around the private static `encodeFreqsFields` function to expose it to benchmarking.
size_t encode_freqs_fields(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFreqsFields(bw, delta, res);
}

// Wrapper around the private static `encodeFreqsFieldsWide` function to expose it to benchmarking.
size_t encode_freqs_fields_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFreqsFieldsWide(bw, delta, res);
}

// Wrapper around the private static `encodeFreqsOnly` function to expose it to benchmarking.
size_t encode_freqs_only(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFreqsOnly(bw, delta, res);
}

// Wrapper around the private static `encodeFieldsOnly` function to expose it to benchmarking.
size_t encode_fields_only(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFieldsOnly(bw, delta, res);
}

// Wrapper around the private static `encodeFieldsOnlyWide` function to expose it to benchmarking.
size_t encode_fields_only_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFieldsOnlyWide(bw, delta, res);
}

// Wrapper around the private static `encodeFieldsOffsets` function to expose it to benchmarking.
size_t encode_fields_offsets(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFieldsOffsets(bw, delta, res);
}

// Wrapper around the private static `encodeFieldsOffsetsWide` function to expose it to benchmarking.
size_t encode_fields_offsets_wide(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFieldsOffsetsWide(bw, delta, res);
}

// Wrapper around the private static `encodeOffsetsOnly` function to expose it to benchmarking.
size_t encode_offsets_only(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeOffsetsOnly(bw, delta, res);
}

// Wrapper around the private static `encodeFreqsOffsets` function to expose it to benchmarking.
size_t encode_freqs_offsets(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeFreqsOffsets(bw, delta, res);
}

// Wrapper around the private static `encodeNumeric` function to expose it to benchmarking
size_t encode_numeric(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeNumeric(bw, delta, res);
}

// Wrapper around the private static `encodeDocIdsOnly` function to expose it to benchmarking.
size_t encode_docs_ids_only(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeDocIdsOnly(bw, delta, res);
}

// Wrapper around the private static `encodeRawDocIdsOnly` function to expose it to benchmarking.
size_t encode_raw_doc_ids_only(BufferWriter *bw, t_docId delta, RSIndexResult *res) {
  return encodeRawDocIdsOnly(bw, delta, res);
}

IndexBlockReader NewIndexBlockReader(BufferReader *buff, t_docId curBaseId) {
    IndexBlockReader reader = {
      .buffReader = *buff,
      .curBaseId = curBaseId,
    };

    return reader;
}

IndexDecoderCtx NewIndexDecoderCtx_NumericFilter() {
  IndexDecoderCtx ctx = {.filter = NULL};

  return ctx;
}

// Create a new IndexDecoderCtx with a mask filter. Used only in benchmarks.
IndexDecoderCtx NewIndexDecoderCtx_MaskFilter(uint32_t mask) {
  IndexDecoderCtx ctx = {.mask = mask};

  return ctx;
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
size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, IndexEncoder encoder,
                                       RSIndexResult *entry) {
  t_docId docId = entry->docId;
  size_t sz = 0;
  RS_ASSERT(docId > 0);
  const bool same_doc = idx->lastId == docId;
  if (same_doc) {
    if (encoder != encodeNumeric) {
      // do not allow the same document to be written to the same index twice.
      // this can happen with duplicate tags for example
      return 0;
    } else {
      // for numeric it is allowed (to support multi values)
      // TODO: Implement turning off this flag on GC collection
      idx->flags |= Index_HasMultiValue;
    }
  }

  t_docId delta = 0;
  IndexBlock *blk = INDEX_LAST_BLOCK(idx);

  // use proper block size. Index_DocIdsOnly == 0x00
  uint16_t blockSize = (idx->flags & INDEX_STORAGE_MASK) ?
          INDEX_BLOCK_SIZE :
          INDEX_BLOCK_SIZE_DOCID_ONLY;

  uint16_t numEntries = IndexBlock_NumEntries(blk);
  // see if we need to grow the current block
  if (numEntries >= blockSize && !same_doc) {
    // If same doc can span more than a single block - need to adjust IndexReader_SkipToBlock
    blk = InvertedIndex_AddBlock(idx, docId, &sz);
  } else if (numEntries == 0) {
    blk->firstId = blk->lastId = docId;
  }

  if (encoder != encodeRawDocIdsOnly) {
    delta = docId - IndexBlock_LastId(blk);
  } else {
    delta = docId - IndexBlock_FirstId(blk);
  }

  // For non-numeric encoders the maximal delta is UINT32_MAX (since it is encoded with 4 bytes)
  // For numeric encoder the maximal delta has to fit in 7 bytes (since it is encoded with 0-7 bytes)
  const t_docId maxDelta = encoder == encodeNumeric ? (DOCID_MAX >> 8) : UINT32_MAX;
  if (delta > maxDelta) {
    blk = InvertedIndex_AddBlock(idx, docId, &sz);
    delta = 0;
  }

  BufferWriter bw = NewBufferWriter(IndexBlock_Buffer(blk));

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

/* Write a numeric entry to the index */
size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, double value) {

  RSIndexResult rec = (RSIndexResult){
      .docId = docId,
      .data = {
        .numeric_tag = RSResultData_Numeric,
        .numeric = (RSNumericRecord){.value = value},
      },
  };
  return InvertedIndex_WriteEntryGeneric(idx, encodeNumeric, &rec);
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
  static bool name(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res)

/**
 * Skipper implements SkipTo. It is an optimized version of DECODER which reads
 * the document ID first, and skips ahead if the result does not match the
 * expected one.
 */
#define SKIPPER(name)                                                                            \
  static bool name(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, t_docId expid, RSIndexResult *res)

DECODER(readFreqsFlags) {
  uint32_t delta, fieldMask;
  qint_decode3(&blockReader->buffReader, &delta, &res->freq, &fieldMask);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = fieldMask;
  return fieldMask & ctx->mask;
}

DECODER(readFreqsFlagsWide) {
  uint32_t delta;
  qint_decode2(&blockReader->buffReader, &delta, &res->freq);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  return res->fieldMask & ctx->wideMask;
}

DECODER(readFreqOffsetsFlags) {
  uint32_t delta, fieldMask;
  qint_decode4(&blockReader->buffReader, &delta, &res->freq, &fieldMask, &res->offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = fieldMask;
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader), res->offsetsSz);
  Buffer_Skip(&blockReader->buffReader, res->offsetsSz);
  return fieldMask & ctx->mask;
}

SKIPPER(seekFreqOffsetsFlags) {
  uint32_t did = 0, freq = 0, offsz = 0, fm = 0;
  bool rc = false;

  while (!BufferReader_AtEnd(&blockReader->buffReader)) {
    qint_decode4(&blockReader->buffReader, &did, &freq, &fm, &offsz);
    Buffer_Skip(&blockReader->buffReader, offsz);
    blockReader->curBaseId = (did += blockReader->curBaseId);
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
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader) - offsz, offsz);

  return rc;
}

DECODER(readFreqOffsetsFlagsWide) {
  uint32_t delta;
  qint_decode3(&blockReader->buffReader, &delta, &res->freq, &res->offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader), res->offsetsSz);
  Buffer_Skip(&blockReader->buffReader, res->offsetsSz);
  return res->fieldMask & ctx->wideMask;
}

// special decoder for decoding numeric results
DECODER(readNumeric) {
  EncodingHeader header;
  Buffer_Read(&blockReader->buffReader, &header, 1);

  // Read the delta (if not zero)
  t_docId delta = 0;
  Buffer_Read(&blockReader->buffReader, &delta, header.encCommon.deltaEncoding);
  blockReader->curBaseId = res->docId = blockReader->curBaseId + delta;

  double value = 0;

  switch (header.encCommon.type) {
    case NUM_ENCODING_COMMON_TYPE_FLOAT:
      if (header.encFloat.isInf) {
        value = INFINITY;
      } else if (header.encFloat.isDouble) {
        Buffer_Read(&blockReader->buffReader, &value, 8);
      } else {
        float f;
        Buffer_Read(&blockReader->buffReader, &f, 4);
        value = f;
      }
      if (header.encFloat.sign) {
        value = -value;
      }
      break;

    case NUM_ENCODING_COMMON_TYPE_TINY:
      // Is embedded into the header
      value = header.encTiny.tinyValue;
      break;

    case NUM_ENCODING_COMMON_TYPE_POSITIVE_INT:
    case NUM_ENCODING_COMMON_TYPE_NEG_INT:
      {
        // Is a none-zero integer (zero is represented as tiny)
        uint64_t num = 0;
        Buffer_Read(&blockReader->buffReader, &num, header.encInt.valueByteCount + 1);
        value = num;
        if (header.encCommon.type == NUM_ENCODING_COMMON_TYPE_NEG_INT) {
          value = -value;
        }
      }
      break;
  }

  IndexResult_SetNumValue(res, value);

  const NumericFilter *f = ctx->filter;
  if (f) {
    if (NumericFilter_IsNumeric(f)) {
      return NumericFilter_Match(f, value);
    } else {
      int filtered = isWithinRadius(f->geoFilter, value, &value);

      // Update the value with the new calculated distance
      IndexResult_SetNumValue(res, value);

      return filtered;
    }
  }

  return 1;
}

DECODER(readFreqs) {
  uint32_t delta;
  qint_decode2(&blockReader->buffReader, &delta, &res->freq);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  return 1;
}

DECODER(readFlags) {
  uint32_t delta, mask;
  qint_decode2(&blockReader->buffReader, &delta, &mask);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = mask;
  return mask & ctx->mask;
}

DECODER(readFlagsWide) {
  blockReader->curBaseId = res->docId = ReadVarint(&blockReader->buffReader) + blockReader->curBaseId;
  res->freq = 1;
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  return res->fieldMask & ctx->wideMask;
}

DECODER(readFieldsOffsets) {
  uint32_t delta, mask;
  qint_decode3(&blockReader->buffReader, &delta, &mask, &res->offsetsSz);
  res->fieldMask = mask;
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader), res->offsetsSz);
  Buffer_Skip(&blockReader->buffReader, res->offsetsSz);
  return mask & ctx->mask;
}

DECODER(readFieldsOffsetsWide) {
  uint32_t delta;
  qint_decode2(&blockReader->buffReader, &delta, &res->offsetsSz);
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader), res->offsetsSz);

  Buffer_Skip(&blockReader->buffReader, res->offsetsSz);
  return res->fieldMask & ctx->wideMask;
}

DECODER(readOffsetsOnly) {
  uint32_t delta;
  qint_decode2(&blockReader->buffReader, &delta, &res->offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader), res->offsetsSz);
  Buffer_Skip(&blockReader->buffReader, res->offsetsSz);
  return 1;
}

DECODER(readFreqsOffsets) {
  uint32_t delta;
  qint_decode3(&blockReader->buffReader, &delta, &res->freq, &res->offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSTermRecord *term = IndexResult_TermRefMut(res);
  RSOffsetVector_SetData(&term->offsets, BufferReader_Current(&blockReader->buffReader), res->offsetsSz);
  Buffer_Skip(&blockReader->buffReader, res->offsetsSz);
  return 1;
}

SKIPPER(seekRawDocIdsOnly) {
  int64_t delta = expid - blockReader->curBaseId;

  uint32_t curVal;
  Buffer_Read(&blockReader->buffReader, &curVal, sizeof(curVal));
  if (curVal >= delta || delta < 0) {
    goto final;
  }

  uint32_t *buf = (uint32_t *)blockReader->buffReader.buf->data;
  size_t start = blockReader->buffReader.pos / 4;
  size_t end = (blockReader->buffReader.buf->offset - 4) / 4;
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
  Buffer_Seek(&blockReader->buffReader, (cur + 1) * sizeof(uint32_t));

final:
  res->docId = curVal + blockReader->curBaseId;
  res->freq = 1;
  return 1;
}

DECODER(readRawDocIdsOnly) {
  uint32_t delta;
  Buffer_Read(&blockReader->buffReader, &delta, sizeof delta);
  res->docId = delta + blockReader->curBaseId; // Base ID is not changing on raw docids
  res->freq = 1;
  return 1;  // Don't care about field mask
}

DECODER(readDocIdsOnly) {
  blockReader->curBaseId = res->docId = ReadVarint(&blockReader->buffReader) + blockReader->curBaseId;
  res->freq = 1;
  return 1;  // Don't care about field mask
}

// Wrapper around the private static `readFreqOffsetsFlags` function to expose it to benchmarking.
bool read_freq_offsets_flags(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFreqOffsetsFlags(blockReader, ctx, res);
}

// Wrapper around the private static `readFreqOffsetsFlagsWide` function to expose it to benchmarking.
bool read_freq_offsets_flags_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFreqOffsetsFlagsWide(blockReader, ctx, res);
}

// Wrapper around the private static `readFreqs` function to expose it to benchmarking.
bool read_freqs(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFreqs(blockReader, ctx, res);
}

// Wrapper around the private static `readFlags` function to expose it to benchmarking.
bool read_flags(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFlags(blockReader, ctx, res);
}

// Wrapper around the private static `readFlagsWide` function to expose it to benchmarking.
bool read_flags_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFlagsWide(blockReader, ctx, res);
}

// Wrapper around the private static `readFlagsOffsets` function to expose it to benchmarking.
bool read_fields_offsets(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFieldsOffsets(blockReader, ctx, res);
}

// Wrapper around the private static `readFlagsOffsetsWide` function to expose it to benchmarking.
bool read_fields_offsets_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFieldsOffsetsWide(blockReader, ctx, res);
}

// Wrapper around the private static `readOffsetsOnly` function to expose it to benchmarking.
bool read_offsets_only(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readOffsetsOnly(blockReader, ctx, res);
}

// Wrapper around the private static `readFreqsOffsets` function to expose it to benchmarking.
bool read_freqs_offsets(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFreqsOffsets(blockReader, ctx, res);
}

// Wrapper around the private static `readNumeric` function to expose it to benchmarking
bool read_numeric(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readNumeric(blockReader, ctx, res);
}

// Wrapper around the private static `readFreqsFlags` function to expose it to benchmarking.
bool read_freqs_flags(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFreqsFlags(blockReader, ctx, res);
}

// Wrapper around the private static `readNumeric` function to expose it to benchmarking
bool read_freqs_flags_wide(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readFreqsFlagsWide(blockReader, ctx, res);
}

// Wrapper around the private static `readDocIdsOnly` function to expose it to benchmarking
bool read_doc_ids_only(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readDocIdsOnly(blockReader, ctx, res);
}

// Wrapper around the private static `readRawDocIdsOnly` function to expose it to benchmarking
bool read_raw_doc_ids_only(IndexBlockReader *blockReader, const IndexDecoderCtx *ctx, RSIndexResult *res) {
  return readRawDocIdsOnly(blockReader, ctx, res);
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
      RETURN_DECODERS(readOffsetsOnly, NULL);

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
      RETURN_DECODERS(readFieldsOffsets, NULL);

    case Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
      RETURN_DECODERS(readFieldsOffsetsWide, NULL);

    case Index_StoreNumeric:
      RETURN_DECODERS(readNumeric, NULL);

    default:
      RS_LOG_ASSERT_FMT(0, "Invalid index flags: %d", flags);
      RETURN_DECODERS(NULL, NULL);
  }
}

/* Repair an index block by removing garbage - records pointing at deleted documents,
 * and write valid entries in their place.
 * Returns the number of docs collected, and puts the number of bytes collected in the given
 * pointer.
 */
size_t IndexBlock_Repair(IndexBlock *blk, DocTable *dt, IndexFlags flags, IndexRepairParams *params) {
  static const IndexDecoderCtx empty = {0};

  IndexBlockReader reader = { .buffReader = NewBufferReader(IndexBlock_Buffer(blk)), .curBaseId = IndexBlock_FirstId(blk) };
  BufferReader *br = &reader.buffReader;
  Buffer repair = {0};
  BufferWriter bw = NewBufferWriter(&repair);
  uint32_t readFlags = flags & INDEX_STORAGE_MASK;
  RSIndexResult *res = readFlags == Index_StoreNumeric ? NewNumericResult() : NewTokenRecord(NULL, 1);
  IndexDecoderProcs decoders = InvertedIndex_GetDecoder(readFlags);
  IndexEncoder encoder = InvertedIndex_GetEncoder(readFlags);

  blk->lastId = blk->firstId = 0;
  size_t frags = 0;
  t_docId lastReadId = 0;
  bool isLastValid = false;

  params->bytesBeforFix = IndexBlock_Cap(blk);

  bool docExists;
  while (!BufferReader_AtEnd(br)) {
    const char *bufBegin = BufferReader_Current(br);
    // read the curr entry of the buffer into res and promote the buffer to the next one.
    decoders.decoder(&reader, &empty, res);
    size_t sz = BufferReader_Current(br) - bufBegin;

    // Multi value documents are saved as individual entries that share the same docId.
    // Increment frags only when moving to the next doc
    // (do not increment when moving to the next entry in the same doc)
    unsigned fragsIncr = 0;
    if (lastReadId != res->docId) {
      fragsIncr = 1;
      // Lookup the doc (for the same doc use the previous result)
      docExists = DocTable_Exists(dt, res->docId);
      lastReadId = res->docId;
    }

    // If we found a deleted document, we increment the number of found "frags",
    // and not write anything, so the reader will advance but the writer won't.
    // this will close the "hole" in the index
    if (!docExists) {
      if (!frags) {
        // First invalid doc; copy everything prior to this to the repair
        // buffer
        Buffer_Write(&bw, IndexBlock_Data(blk), bufBegin - IndexBlock_Data(blk));
      }
      frags += fragsIncr;
      params->bytesCollected += sz;
      ++params->entriesCollected;
      isLastValid = false;
    } else { // the doc exist
      if (params->RepairCallback) {
        params->RepairCallback(res, blk, params->arg);
      }
      if (IndexBlock_FirstId(blk) == 0) { // this is the first valid doc
        blk->firstId = res->docId;
        blk->lastId = res->docId; // first diff should be 0
      }

      // Valid document, but we're rewriting the block:
      if (frags) {
        if (encoder != encodeRawDocIdsOnly) {
          if (isLastValid) {
            // if the last was valid, the order of the entries didn't change. We can just copy the entry, as it already contains the correct delta.
            Buffer_Write(&bw, bufBegin, sz);
          } else { // we need to calculate the delta
            encoder(&bw, res->docId - IndexBlock_LastId(blk), res);
          }
        } else { // encoder == encodeRawDocIdsOnly
          t_docId firstId = IndexBlock_FirstId(blk);
          encoder(&bw, res->docId - firstId, res);
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
    Buffer_Free(IndexBlock_Buffer(blk));
    IndexBlock_SetBuffer(blk, repair);
    Buffer_ShrinkToSize(IndexBlock_Buffer(blk));
  }

  params->bytesAfterFix = IndexBlock_Cap(blk);

  IndexResult_Free(res);
  return frags;
}
