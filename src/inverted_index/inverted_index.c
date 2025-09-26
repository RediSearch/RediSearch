/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "types_rs.h"
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

uint64_t TotalBlocks = 0;

// This is a temporary wrapper around `TotalBlocks`. When we switch over to Rust it won't be possible to access `TotalBlocks` directly. So
// this aligns the usage with the incoming Rust code.
size_t TotalIIBlocks() {
  return TotalBlocks;
}

// The last block of the index
#define INDEX_LAST_BLOCK(idx) (InvertedIndex_BlockRef(idx, InvertedIndex_NumBlocks(idx) - 1))

IndexBlock *InvertedIndex_AddBlock(InvertedIndex *idx, t_docId firstId, size_t *memsize) {
  TotalBlocks++;
  idx->size++;
  idx->blocks = rm_realloc(idx->blocks, idx->size * sizeof(IndexBlock));
  IndexBlock *last = idx->blocks + (idx->size - 1);
  memset(last, 0, sizeof(*last));  // for msan
  last->firstId = last->lastId = firstId;
  Buffer_Init(IndexBlock_Buffer(INDEX_LAST_BLOCK(idx)), INDEX_BLOCK_INITIAL_CAP);
  (*memsize) += sizeof(IndexBlock) + INDEX_BLOCK_INITIAL_CAP;
  return INDEX_LAST_BLOCK(idx);
}

InvertedIndex *NewInvertedIndex(IndexFlags flags, size_t *memsize) {
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
  InvertedIndex_AddBlock(idx, 0, memsize);
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

void InvertedIndex_Free(InvertedIndex *idx) {
  size_t numBlocks = InvertedIndex_NumBlocks(idx);
  TotalBlocks -= numBlocks;
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
  const RSOffsetVector *offsets = IndexResult_TermOffsetsRef(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  const char *offsets_data = RSOffsetVector_GetData(offsets, &offsets_len);
  size_t sz = qint_encode4(bw, delta, res->freq, (uint32_t)res->fieldMask, offsetsSz);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

ENCODER(encodeFullWide) {
  uint32_t offsets_len;
  const RSOffsetVector *offsets = IndexResult_TermOffsetsRef(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  const char *offsets_data = RSOffsetVector_GetData(offsets, &offsets_len);
  size_t sz = qint_encode3(bw, delta, res->freq, offsetsSz);
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
  const RSOffsetVector *offsets = IndexResult_TermOffsetsRef(res);
  const char *offsets_data = RSOffsetVector_GetData(offsets, &offsets_len);
  size_t sz = qint_encode3(bw, delta, (uint32_t)res->fieldMask, offsets_len);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

ENCODER(encodeFieldsOffsetsWide) {
  uint32_t offsets_len;
  const RSOffsetVector *offsets = IndexResult_TermOffsetsRef(res);
  const char *offsets_data = RSOffsetVector_GetData(offsets, &offsets_len);
  size_t sz = qint_encode2(bw, delta, offsets_len);
  sz += WriteVarintFieldMask(res->fieldMask, bw);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

// 6. Offsets only
ENCODER(encodeOffsetsOnly) {
  uint32_t offsets_len;
  const RSOffsetVector *offsets = IndexResult_TermOffsetsRef(res);
  const char *offsets_data = RSOffsetVector_GetData(offsets, &offsets_len);
  size_t sz = qint_encode2(bw, delta, offsets_len);
  sz += Buffer_Write(bw, offsets_data, offsets_len);
  return sz;
}

// 7. Offsets and freqs
ENCODER(encodeFreqsOffsets) {
  uint32_t offsets_len;
  const RSOffsetVector *offsets = IndexResult_TermOffsetsRef(res);
  const char *offsets_data = RSOffsetVector_GetData(offsets, &offsets_len);
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
  IndexDecoderCtx ctx = {.tag = IndexDecoderCtx_None};

  return ctx;
}

// Create a new IndexDecoderCtx with a mask filter. Used only in benchmarks.
IndexDecoderCtx NewIndexDecoderCtx_MaskFilter(uint32_t mask) {
  IndexDecoderCtx ctx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = mask};

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
size_t InvertedIndex_WriteEntryGeneric(InvertedIndex *idx, RSIndexResult *entry) {
  IndexEncoder encoder = InvertedIndex_GetEncoder(InvertedIndex_Flags(idx));
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
  if (idx->flags & Index_StoreFieldFlags) {
    idx->fieldMask |= entry->fieldMask;
  }

  return sz;
}

/* Write a numeric entry to the index */
size_t InvertedIndex_WriteNumericEntry(InvertedIndex *idx, t_docId docId, double value) {

  RSIndexResult rec = (RSIndexResult){
      .docId = docId,
      .data = {
        .numeric_tag = RSResultData_Numeric,
        .numeric = value,
      },
  };
  return InvertedIndex_WriteEntryGeneric(idx, &rec);
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
  return fieldMask & ctx->field_mask;
}

DECODER(readFreqsFlagsWide) {
  uint32_t delta;
  qint_decode2(&blockReader->buffReader, &delta, &res->freq);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  return res->fieldMask & ctx->field_mask;
}

DECODER(readFreqOffsetsFlags) {
  uint32_t delta, fieldMask;
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  qint_decode4(&blockReader->buffReader, &delta, &res->freq, &fieldMask, &offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = fieldMask;
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader), offsetsSz);
  Buffer_Skip(&blockReader->buffReader, offsetsSz);
  return fieldMask & ctx->field_mask;
}

SKIPPER(seekFreqOffsetsFlags) {
  uint32_t did = 0, freq = 0, offsz = 0, fm = 0;
  bool rc = false;

  while (!BufferReader_AtEnd(&blockReader->buffReader)) {
    qint_decode4(&blockReader->buffReader, &did, &freq, &fm, &offsz);
    Buffer_Skip(&blockReader->buffReader, offsz);
    blockReader->curBaseId = (did += blockReader->curBaseId);
    if (!(ctx->field_mask & fm)) {
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
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader) - offsz, offsz);

  return rc;
}

DECODER(readFreqOffsetsFlagsWide) {
  uint32_t delta;
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  qint_decode3(&blockReader->buffReader, &delta, &res->freq, &offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader), offsetsSz);
  Buffer_Skip(&blockReader->buffReader, offsetsSz);
  return res->fieldMask & ctx->field_mask;
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

  const NumericFilter *f = ctx->numeric;
  if (ctx->tag == IndexDecoderCtx_Numeric && f) {
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
  return mask & ctx->field_mask;
}

DECODER(readFlagsWide) {
  blockReader->curBaseId = res->docId = ReadVarint(&blockReader->buffReader) + blockReader->curBaseId;
  res->freq = 1;
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  return res->fieldMask & ctx->field_mask;
}

DECODER(readFieldsOffsets) {
  uint32_t delta, mask;
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  qint_decode3(&blockReader->buffReader, &delta, &mask, &offsetsSz);
  res->fieldMask = mask;
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader), offsetsSz);
  Buffer_Skip(&blockReader->buffReader, offsetsSz);
  return mask & ctx->field_mask;
}

DECODER(readFieldsOffsetsWide) {
  uint32_t delta;
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  qint_decode2(&blockReader->buffReader, &delta, &offsetsSz);
  res->fieldMask = ReadVarintFieldMask(&blockReader->buffReader);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader), offsetsSz);

  Buffer_Skip(&blockReader->buffReader, offsetsSz);
  return res->fieldMask & ctx->field_mask;
}

DECODER(readOffsetsOnly) {
  uint32_t delta;
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  qint_decode2(&blockReader->buffReader, &delta, &offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader), offsetsSz);
  Buffer_Skip(&blockReader->buffReader, offsetsSz);
  return 1;
}

DECODER(readFreqsOffsets) {
  uint32_t delta;
  RSOffsetVector *offsets = IndexResult_TermOffsetsRefMut(res);
  uint32_t offsetsSz = RSOffsetVector_Len(offsets);
  qint_decode3(&blockReader->buffReader, &delta, &res->freq, &offsetsSz);
  blockReader->curBaseId = res->docId = delta + blockReader->curBaseId;
  RSOffsetVector_SetData(offsets, BufferReader_Current(&blockReader->buffReader), offsetsSz);
  Buffer_Skip(&blockReader->buffReader, offsetsSz);
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

/* Calculate efficiency ratio of the inverted index: numEntries / numBlocks.
 * Used to measure how well the index is utilizing its block structure. */
double InvertedIndex_GetEfficiency(const InvertedIndex *idx) {
  return ((double)InvertedIndex_NumEntries(idx))/(InvertedIndex_NumBlocks(idx));
}

/* Retrieve comprehensive summary information about an inverted index.
 * Returns a stack-allocated struct containing all key metrics including:
 * - number_of_docs: Number of documents in the index
 * - number_of_entries: Total number of entries
 * - last_doc_id: Last document ID
 * - flags: Index configuration flags
 * - number_of_blocks: Number of index blocks
 * - block_efficiency: Efficiency ratio (only for numeric indexes)
 * - has_efficiency: Whether efficiency calculation is applicable */
IISummary InvertedIndex_Summary(const InvertedIndex *idx) {
  IndexFlags flags = InvertedIndex_Flags(idx);
  bool hasEfficiency = (flags & Index_StoreNumeric) ? true : false;

  IISummary summary = {
    .number_of_docs = InvertedIndex_NumDocs(idx),
    .number_of_entries = InvertedIndex_NumEntries(idx),
    .last_doc_id = InvertedIndex_LastId(idx),
    .flags = flags,
    .number_of_blocks = InvertedIndex_NumBlocks(idx),
    .block_efficiency = hasEfficiency ? InvertedIndex_GetEfficiency(idx) : 0.0,
    .has_efficiency = hasEfficiency
  };
  return summary;
}

/* Retrieve basic information about the blocks in an inverted index.
 * Returns an array with `count` entries. Each entry includes:
 * - first_doc_id: The first document ID in the block
 * - last_doc_id: The last document ID in the block
 * - number_of_entries: The number of endries in the block */
IIBlockSummary *InvertedIndex_BlocksSummary(const InvertedIndex *idx, size_t *count) {
  *count = InvertedIndex_NumBlocks(idx);
  if (*count == 0) {
    return NULL;
  }

  IIBlockSummary *summaries = rm_calloc(*count, sizeof(IIBlockSummary));
  for (size_t i = 0; i < *count; i++) {
    IndexBlock *blk = InvertedIndex_BlockRef(idx, i);
    summaries[i] = (IIBlockSummary){
      .first_doc_id = IndexBlock_FirstId(blk),
      .last_doc_id = IndexBlock_LastId(blk),
      .number_of_entries = IndexBlock_NumEntries(blk),
    };
  }

  return summaries;
}

void InvertedIndex_BlocksSummaryFree(IIBlockSummary *summaries) {
  rm_free(summaries);
}

unsigned long InvertedIndex_MemUsage(const InvertedIndex *idx) {
  unsigned long ret = sizeof_InvertedIndex(InvertedIndex_Flags(idx));

    // iterate idx blocks
  size_t numBlocks = InvertedIndex_NumBlocks(idx);
  for (size_t i = 0; i < numBlocks; i++) {
    ret += sizeof(IndexBlock);
    IndexBlock *block = InvertedIndex_BlockRef(idx, i);
    ret += IndexBlock_Cap(block);
  }
  return ret;
}

// pointer to the current block while reading the index
#define CURRENT_BLOCK(ir) (InvertedIndex_BlockRef((ir)->idx, (ir)->currentBlock))
#define CURRENT_BLOCK_READER_AT_END(ir) BufferReader_AtEnd(&(ir)->blockReader.buffReader)

static inline void SetCurrentBlockReader(IndexReader *ir) {
  ir->blockReader = (IndexBlockReader) {
    NewBufferReader(IndexBlock_Buffer(CURRENT_BLOCK(ir))),
    IndexBlock_FirstId(CURRENT_BLOCK(ir)),
  };
}

static inline void AdvanceBlock(IndexReader *ir) {
  ir->currentBlock++;
  SetCurrentBlockReader(ir);
}

// A while-loop helper to advance the iterator to the next block or break if we are at the end.
static __attribute__((always_inline)) inline bool NotAtEnd(IndexReader *ir) {
  if (!CURRENT_BLOCK_READER_AT_END(ir)) {
    return true; // still have entries in the current block
  }
  if (ir->currentBlock + 1 < InvertedIndex_NumBlocks(ir->idx)) {
    // we have more blocks to read, so we can advance to the next block
    AdvanceBlock(ir);
    return true;
  }
  // no more blocks to read, so we are at the end
  return false;
}

IndexReader *NewIndexReader(const InvertedIndex *idx, IndexDecoderCtx *ctx) {
  IndexReader *ir = rm_calloc(1, sizeof(IndexReader));

  ir->idx = idx;
  ir->currentBlock = 0;
  ir->gcMarker = InvertedIndex_GcMarker(idx);
  ir->decoders = InvertedIndex_GetDecoder(InvertedIndex_Flags(idx));
  ir->decoderCtx = *ctx;

  SetCurrentBlockReader(ir);

  return ir;
}

void IndexReader_Free(IndexReader *ir) {
  rm_free(ir);
}

void IndexReader_Reset(IndexReader *ir) {
  ir->currentBlock = 0;
  ir->gcMarker = InvertedIndex_GcMarker(ir->idx);
  SetCurrentBlockReader(ir);
}

size_t IndexReader_NumEstimated(const IndexReader *ir) {
  return InvertedIndex_NumDocs(ir->idx);
}


bool IndexReader_IsIndex(const IndexReader *ir, const InvertedIndex *idx) {
  RS_ASSERT(ir->idx);

  return ir->idx == idx;
}

bool IndexReader_Revalidate(IndexReader *ir) {
  // the gc marker tells us if there is a chance the keys have undergone GC while we were asleep
  if (ir->gcMarker == InvertedIndex_GcMarker(ir->idx)) {
    // no GC - we just go to the same offset we were at.
    // Reset the buffer pointer in case it was reallocated
    ir->blockReader.buffReader.buf = IndexBlock_Buffer(CURRENT_BLOCK(ir));

    return false;
  }

  return true;
}

bool IndexReader_HasSeeker(const IndexReader *ir) {
  return ir->decoders.seeker != NULL;
}

#define BLOCK_MATCHES(blk, docId) (IndexBlock_FirstId(blk) <= docId && docId <= IndexBlock_LastId(blk))

// Assumes there is a valid block to skip to (matching or past the requested docId)
static inline void SkipToBlock(IndexReader *ir, t_docId docId) {
  const InvertedIndex *idx = ir->idx;
  uint32_t top = InvertedIndex_NumBlocks(idx) - 1;
  uint32_t bottom = ir->currentBlock + 1;
  IndexBlock *bottomBlock = InvertedIndex_BlockRef(idx, bottom);

  if (docId <= IndexBlock_LastId(bottomBlock)) {
    // the next block is the one we're looking for, although it might not contain the docId
    ir->currentBlock = bottom;
    goto new_block;
  }

  uint32_t i;
  while (bottom <= top) {
    i = (bottom + top) / 2;
    IndexBlock *block = InvertedIndex_BlockRef(idx, i);
    if (BLOCK_MATCHES(block, docId)) {
      ir->currentBlock = i;
      goto new_block;
    }

    t_docId firstId = IndexBlock_FirstId(block);
    if (docId < firstId) {
      top = i - 1;
    } else {
      bottom = i + 1;
    }
  }

  // We didn't find a matching block. According to the assumptions, there must be a block past the
  // requested docId, and the binary search brought us to it or the one before it.
  ir->currentBlock = i;
  t_docId lastId = IndexBlock_LastId(CURRENT_BLOCK(ir));
  if (lastId < docId) {
    ir->currentBlock++; // It's not the current block. Advance
    RS_ASSERT(IndexBlock_FirstId(CURRENT_BLOCK(ir)) > docId); // Not a match but has to be past it
  }

new_block:
  RS_LOG_ASSERT(ir->currentBlock < idx->size, "Invalid block index");
  SetCurrentBlockReader(ir);
}

bool IndexReader_Next(IndexReader *ir, RSIndexResult *res) {
  while (NotAtEnd(ir)) {
    // The decoder also acts as a filter. If the decoder returns false, the
    // current record should not be processed.
    // Since we are not at the end of the block (previous check), the decoder is guaranteed
    // to read a record (advanced by at least one entry).
    if (ir->decoders.decoder(&ir->blockReader, &ir->decoderCtx, res)) {
      return true;
    }
  }

  return false;
}

bool IndexReader_SkipTo(IndexReader *ir, t_docId docId) {
  if (docId > InvertedIndex_LastId(ir->idx)) {
    return false;
  }

  t_docId lastId = IndexBlock_LastId(CURRENT_BLOCK(ir));
  if (lastId < docId) {
    // We know that `docId <= idx->lastId`, so there must be a following block that contains the
    // lastId, which either contains the requested docId or higher ids. We can skip to it.
    SkipToBlock(ir, docId);
  }

  return true;
}

bool IndexReader_Seek(IndexReader *ir, t_docId docId, RSIndexResult *res) {
  return ir->decoders.seeker(&ir->blockReader, &ir->decoderCtx, docId, res);
}

bool IndexReader_HasMulti(const IndexReader *ir) {
  return InvertedIndex_Flags(ir->idx) & Index_HasMultiValue;
}

IndexFlags IndexReader_Flags(const IndexReader *ir) {
  return InvertedIndex_Flags(ir->idx);
}

const NumericFilter *IndexReader_NumericFilter(const IndexReader *ir) {
  return ir->decoderCtx.numeric;
}

void IndexReader_SwapIndex(IndexReader *ir, const InvertedIndex *newIdx) {
  const InvertedIndex *oldIdx = ir->idx;
  ir->idx = newIdx;
  newIdx = oldIdx;
}

InvertedIndex *IndexReader_II(const IndexReader *ir) {
  return (InvertedIndex *)ir->idx;
}

// ----- GC related API

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

struct InvertedIndexGcDelta {
  IndexBlock *new_blocklist;
  size_t new_blocklist_size;

  InvertedIndex_DeletedInput *deleted;
  size_t deleted_len;

  InvertedIndex_RepairedInput *repaired;

  bool last_block_ignored;
};

static void checkLastBlock(InvertedIndex *idx, InvertedIndexGcDelta *delta,
                           II_GCScanStats *info) {
  IndexBlock *lastOld = InvertedIndex_BlockRef(idx, info->nblocksOrig - 1);
  if (info->lastblkDocsRemoved == 0) {
    // didn't touch last block in child
    return;
  }
  if (info->lastblkNumEntries == IndexBlock_NumEntries(lastOld)) {
    // didn't touch last block in parent
    return;
  }

  // Otherwise, we added new entries to the last block while the child was running. In this case we discard all
  // the child garbage collection, assuming they will take place in the next gc iteration.

  if (info->lastblkEntriesRemoved == info->lastblkNumEntries) {
    // Last block was deleted entirely while updates on the main process.
    // Remove it from delBlocks list
    delta->deleted_len--;

    // If all the blocks were deleted, there is no newblocklist. Otherwise, we need to add it to the newBlocklist.
    if (delta->new_blocklist) {
      delta->new_blocklist_size++;
      delta->new_blocklist = rm_realloc(delta->new_blocklist,
                                        sizeof(*delta->new_blocklist) * delta->new_blocklist_size);
      delta->new_blocklist[delta->new_blocklist_size - 1] = *lastOld;
    }
  } else {
    // Last block was modified on the child and on the parent. (but not entirely deleted)

    // we need to remove it from changedBlocks
    InvertedIndex_RepairedInput *rb = delta->repaired + info->nblocksRepaired - 1;
    indexBlock_Free(&rb->blk);
    info->nblocksRepaired--;

    // If newBlocklist!=NULL then the last block must be there (it was changed and not deleted),
    // prefer the parent's block.
    if (delta->new_blocklist) {
      delta->new_blocklist[delta->new_blocklist_size - 1] = *lastOld;
    }
  }

  info->ndocsCollected -= info->lastblkDocsRemoved;
  info->nbytesCollected -= info->lastblkBytesCollected;
  info->nentriesCollected -= info->lastblkEntriesRemoved;
  delta->last_block_ignored = true;

  // used to be directly on gc, now we do it via info gc stats
  info->gcBlocksDenied++;
}

void InvertedIndex_ApplyGcDelta(InvertedIndex *idx,
                                InvertedIndexGcDelta *d,
                                II_GCScanStats *info) {
  checkLastBlock(idx, d, info);

  // If the child did not touch the last block, prefer the parent's last block pointer
  if (d->new_blocklist && !info->lastblkDocsRemoved) {
      /*
      * Last block was unmodified-- let's prefer the last block's pointer
      * over our own (which may be stale).
      * If the last block was repaired, this is handled above in checkLastBlock()
      */
      d->new_blocklist[d->new_blocklist_size - 1] =
          InvertedIndex_Block(idx, info->nblocksOrig - 1);
  }

  // Free blocks that will be replaced by repaired versions
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    const int64_t oldix = d->repaired[i].oldix;
    IndexBlock *blk = InvertedIndex_BlockRef(idx, (size_t)oldix);
    indexBlock_Free(blk);
  }

  // Free raw buffers of fully deleted blocks
  for (size_t i = 0; i < d->deleted_len; ++i) {
    rm_free(d->deleted[i].ptr);
  }
  TotalBlocks -= d->deleted_len;
  rm_free(d->deleted);
  d->deleted = NULL;

  // Ensure the old index is at least as big as the new index' size
  RS_LOG_ASSERT(idx->size >= info->nblocksOrig, "Current index size should be larger or equal to original index size");

  // Reshape the block list if the child produced a new compacted list
  if (d->new_blocklist) {
    // Number of blocks added in the parent process since the last scan
    size_t newAddedLen = InvertedIndex_NumBlocks(idx) - info->nblocksOrig; // TODO: can we just decrease by number of deleted.

    // The final size is the reordered block size, plus the number of blocks
    // which we haven't scanned yet, because they were added in the parent
    size_t totalLen = d->new_blocklist_size + newAddedLen;

    if (InvertedIndex_NumBlocks(idx) > info->nblocksOrig) {
      d->new_blocklist = rm_realloc(
          d->new_blocklist,
          totalLen * sizeof(*d->new_blocklist));

      if (newAddedLen > 0) {
        memcpy(d->new_blocklist + d->new_blocklist_size,
                InvertedIndex_BlockRef(idx, info->nblocksOrig),
                newAddedLen * sizeof(*d->new_blocklist));
      }
    }
    InvertedIndex_SetBlocks(idx, d->new_blocklist, totalLen);
    // ownership of new_blocklist has moved into idx
    d->new_blocklist = NULL;
  } else if (d->deleted_len) {
    // if idxData->newBlocklist == NULL it's either because all the blocks the child has seen are gone or we didn't change the
    // size of the index (idxData->numDelBlocks == 0).
    // So if we enter here (idxData->numDelBlocks != 0) it's the first case, all blocks the child has seen need to be deleted.
    // Note that we might want to keep the last block, although deleted by the child. In this case numDelBlocks will *not include*
    // the last block.
    size_t numBlocks = InvertedIndex_BlocksShift(idx, d->deleted_len);
    if (numBlocks == 0) {
      InvertedIndex_AddBlock(idx, 0, (size_t *)&info->nbytesAdded);
    }
  }

  // Install repaired blocks at their new positions
  // TODO : can we skip if we have newBlocklist?
  for (size_t i = 0; i < info->nblocksRepaired; ++i) {
    const int64_t newix = d->repaired[i].newix;
    RS_LOG_ASSERT(newix >= 0 && (size_t)newix < InvertedIndex_NumBlocks(idx),
                  "newix out of bounds in InvertedIndex_ApplyGcDelta");
    InvertedIndex_SetBlock(idx, (size_t)newix, d->repaired[i].blk);
    // ownership of repaired[i].blk buffer is now inside idx
  }
  rm_free(d->repaired);
  d->repaired = NULL;

  // Update markers and lastId based on the final last block
  InvertedIndex_SetGcMarker(idx, InvertedIndex_GcMarker(idx) + 1);
  RS_LOG_ASSERT(InvertedIndex_NumBlocks(idx) > 0, "index must have at least one block");
  IndexBlock *last = InvertedIndex_BlockRef(idx, InvertedIndex_NumBlocks(idx) - 1);
  InvertedIndex_SetLastId(idx, IndexBlock_LastId(last));

  InvertedIndex_SetNumDocs(idx, InvertedIndex_NumDocs(idx) - info->ndocsCollected);
}

// ------------------ builders

bool InvertedIndex_GcDelta_GetLastBlockIgnored(InvertedIndexGcDelta *d) {
    return d->last_block_ignored;
}

void InvertedIndex_GcDelta_Free(InvertedIndexGcDelta *d, II_GCScanStats *info) {
  if (!d) {
      return;
  }

  if (d->new_blocklist) {
    rm_free(d->new_blocklist);
    d->new_blocklist = NULL;
    d->new_blocklist_size = 0;
  }
  if (d->deleted) {
    rm_free(d->deleted);
    d->deleted = NULL;
    d->deleted_len = 0;
  }
  if (d->repaired) {
    for (size_t ii = 0; ii < info->nblocksRepaired; ++ii) {
        IndexBlock_DataFree(&d->repaired[ii].blk);
    }
    rm_free(d->repaired);
    d->repaired = NULL;
  }

  rm_free(d);
}

// --------------------- II High Level GC API

// ---- Write Utilities (utilities which are ported over from fork_gc.c)

static void II_Gc_WriteFixed(II_GCWriter *wr, const void *buff, size_t len) {
  wr->write(wr->ctx, buff, len);
}

#define II_GC_WRITE_VAR(wr, v) II_Gc_WriteFixed(wr, &v, sizeof v)

static void II_Gc_WriteBuffer(II_GCWriter *wr, const void *buff, size_t len) {
  II_GC_WRITE_VAR(wr, len);
  if (len > 0) {
    II_Gc_WriteFixed(wr, buff, len);
  }
}

/**
 * Write instead of a string to indicate that no more buffers are to be read
 */
static void II_Gc_WriteTerminator(II_GCWriter *wr) {
  size_t smax = SIZE_MAX;
  II_GC_WRITE_VAR(wr, smax);
}

// ---- Read Utilities (utilities which are ported over from fork_gc.c)

static int II_Gc_ReadFixed(II_GCReader *rd, void *buff, size_t len) {
  return rd->read(rd->ctx, buff, len);
}

static void *RECV_BUFFER_EMPTY = (void *)0x0deadbeef;

static int __attribute__((warn_unused_result))
II_Gc_ReadBuffer(II_GCReader *rd, void **buff, size_t *len) {
  int result = II_Gc_ReadFixed(rd, len, sizeof *len);
  if (result != REDISMODULE_OK) {
      return result;
  }

  if (*len == SIZE_MAX) {
    *buff = RECV_BUFFER_EMPTY;
    return REDISMODULE_OK;
  }
  if (*len == 0) {
    *buff = NULL;
    return REDISMODULE_OK;
  }

  *buff = rm_malloc(*len + 1);
  ((char *)(*buff))[*len] = 0;
  result = II_Gc_ReadFixed(rd, *buff, *len);
  if (result != REDISMODULE_OK) {
    rm_free(*buff);
    return result;
  }

  return REDISMODULE_OK;
}

// ---------------- GC Scan Logic

/* GC Scan blocks to repair and write that info to the II_GCWriter.
 */
bool InvertedIndex_GcDelta_Scan(II_GCWriter *wr, RedisSearchCtx *sctx, InvertedIndex *idx,
                                     II_GCCallback *cb, IndexRepairParams *params) {
    size_t numBlocks = InvertedIndex_NumBlocks(idx);
    InvertedIndex_RepairedInput *fixed = array_new(InvertedIndex_RepairedInput, 10);
    InvertedIndex_DeletedInput *deleted = array_new(InvertedIndex_DeletedInput, 10);
    IndexBlock *blocklist = array_new(IndexBlock, numBlocks);
    II_GCScanStats ixmsg = {.nblocksOrig = numBlocks};
    bool rv = false;
    IndexRepairParams params_s = {0};
    if (!params) {
        params = &params_s;
    }

    for (size_t i = 0; i < numBlocks; ++i) {
    params->bytesCollected = 0;
    params->bytesBeforFix = 0;
    params->bytesAfterFix = 0;
    params->entriesCollected = 0;
    IndexBlock *blk = InvertedIndex_BlockRef(idx, i);
    t_docId firstId = IndexBlock_FirstId(blk);
    t_docId lastId = IndexBlock_LastId(blk);

    if (lastId - firstId > UINT32_MAX) {
        // Skip over blocks which have a wide variation. In the future we might
        // want to split a block into two (or more) on high-delta boundaries.
        // todo: is it ok??
        // The above TODO was written 5 years ago. We currently don't split blocks,
        // and it is also not clear why we care about high variations.
        array_append(blocklist, *blk);
        continue;
    }

    // Capture the pointer address before the block is cleared; otherwise
    // the pointer might be freed! (IndexBlock_Repair rewrites blk->buf if there were repairs)
    void *bufptr = IndexBlock_Data(blk);
    size_t nrepaired = IndexBlock_Repair(blk, &sctx->spec->docs, InvertedIndex_Flags(idx), params);
    if (nrepaired == 0) {
        // unmodified block
        array_append(blocklist, *blk);
        continue;
    }

    uint64_t curr_bytesCollected = params->bytesBeforFix - params->bytesAfterFix;

    uint16_t numEntries = IndexBlock_NumEntries(blk);
    if (numEntries == 0) {
        // this block should be removed
        InvertedIndex_DeletedInput *delmsg = array_ensure_tail(&deleted, InvertedIndex_DeletedInput);
        *delmsg = (InvertedIndex_DeletedInput){.ptr = bufptr, .oldix = i};
        curr_bytesCollected += sizeof(IndexBlock);
    } else {
        array_append(blocklist, *blk);
        InvertedIndex_RepairedInput *fixmsg = array_ensure_tail(&fixed, InvertedIndex_RepairedInput);
        fixmsg->newix = array_len(blocklist) - 1;
        fixmsg->oldix = i;
        fixmsg->blk = *blk; // TODO: consider sending the blocklist even if there weren't any deleted blocks instead of this redundant copy.
        ixmsg.nblocksRepaired++;
    }
    ixmsg.nbytesCollected += curr_bytesCollected;
    ixmsg.ndocsCollected += nrepaired;
    ixmsg.nentriesCollected += params->entriesCollected;
    // Save last block statistics because the main process might want to ignore the changes if
    // the block was modified while the fork was running.
    if (i == numBlocks - 1) {
        ixmsg.lastblkBytesCollected = curr_bytesCollected;
        ixmsg.lastblkDocsRemoved = nrepaired;
        ixmsg.lastblkEntriesRemoved = params->entriesCollected;
        // Save the original number of entries of the last block so we can compare
        // this value to the number of entries exist in the main process, to conclude if any new entries
        // were added during the fork process was running. If there were, the main process will discard the last block
        // fixes. We rely on the assumption that a block is small enough and it will be either handled in the next iteration,
        // or it will get to its maximum capacity and will no longer be the last block.
        ixmsg.lastblkNumEntries = IndexBlock_NumEntries(blk) + params->entriesCollected;
    }
    }

    if (array_len(fixed) == 0 && array_len(deleted) == 0) {
        // No blocks were removed or repaired
        goto done;
    }

    cb->call(cb->ctx);
    II_Gc_WriteFixed(wr, &ixmsg, sizeof ixmsg);
    if (array_len(blocklist) == numBlocks) {
        // no empty block, there is no need to send the blocks array. Don't send
        // any new blocks.
        size_t len = 0;
        II_GC_WRITE_VAR(wr, len);
    } else {
        II_Gc_WriteBuffer(wr, blocklist, array_len(blocklist) * sizeof(*blocklist));
    }
    // TODO: can we move it inside the if?
    II_Gc_WriteBuffer(wr, deleted, array_len(deleted) * sizeof(*deleted));

    for (size_t i = 0; i < array_len(fixed); ++i) {
        // write fix block
        const InvertedIndex_RepairedInput *msg = fixed + i;
        const IndexBlock *blk = blocklist + msg->newix;
        II_Gc_WriteFixed(wr, msg, sizeof(*msg));
        // TODO: check why we need to send the data if its part of the blk struct.
        II_Gc_WriteBuffer(wr, IndexBlock_Data(blk), IndexBlock_Len(blk));
    }
    rv = true;

done:
    array_free(fixed);
    array_free(blocklist);
    array_free(deleted);
    return rv;
}

 static int __attribute__((warn_unused_result))
 II_Gc_ReadRepairedBlock(II_GCReader* rd, InvertedIndex_RepairedInput *binfo) {
   int result = II_Gc_ReadFixed(rd, binfo, sizeof(*binfo));
   if (result != REDISMODULE_OK) {
     return result;
   }

   result = II_Gc_ReadBuffer(rd, (void **)IndexBlock_DataPtr(&binfo->blk), IndexBlock_LenPtr(&binfo->blk));
   if (result != REDISMODULE_OK) {
     return result;
   }

   IndexBlock_SetCap(&binfo->blk, IndexBlock_Len(&binfo->blk));
   return REDISMODULE_OK;
 }

InvertedIndexGcDelta *InvertedIndex_GcDelta_Read(II_GCReader *rd, II_GCScanStats *info) {
    size_t nblocksRecvd = 0;
    if (II_Gc_ReadFixed(rd, info, sizeof(*info)) != REDISMODULE_OK) {
      return NULL;
    }

    InvertedIndexGcDelta *delta = rm_calloc(1, sizeof(InvertedIndexGcDelta));

    if (II_Gc_ReadBuffer(rd, (void **)&delta->new_blocklist, &delta->new_blocklist_size) != REDISMODULE_OK) {
        goto error;
    }

    if (delta->new_blocklist_size) {
      delta->new_blocklist_size /= sizeof(*delta->new_blocklist);
    }
    if (II_Gc_ReadBuffer(rd, (void **)&delta->deleted, &delta->deleted_len) != REDISMODULE_OK) {
      goto error;
    }
    delta->deleted_len /= sizeof(*delta->deleted);
    delta->repaired = rm_malloc(sizeof(*delta->repaired) * info->nblocksRepaired);
    for (size_t i = 0; i < info->nblocksRepaired; ++i) {
      if (II_Gc_ReadRepairedBlock(rd, delta->repaired + i) != REDISMODULE_OK) {
          info->nblocksRepaired = i;
        goto error;
      }
      nblocksRecvd++;
    }
    return delta;

  error:
    InvertedIndex_GcDelta_Free(delta, info);
    return NULL;
}

// ---------------------
