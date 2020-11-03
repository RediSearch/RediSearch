
#define QINT_API static

#include "inverted_index.h"
#include "varint.h"
#include "rmalloc.h"
#include "qint.h"
#include "qint.c"
#include "redis_index.h"
#include "numeric_filter.h"
#include "redismodule.h"
#include "rmutil/rm_assert.h"

#include <stdio.h>
#include <float.h>
#include <math.h>

///////////////////////////////////////////////////////////////////////////////////////////////

uint64_t TotalIIBlocks = 0;

// The number of entries in each index block. A new block will be created after every N entries
#define INDEX_BLOCK_SIZE 100

// Initial capacity (in bytes) of a new block
#define INDEX_BLOCK_INITIAL_CAP 6

//---------------------------------------------------------------------------------------------

// The last block of the index
IndexBlock &InvertedIndex::LastBlock() {
  return blocks[size - 1];
}

//---------------------------------------------------------------------------------------------

// Add a new block to the index with a given document id as the initial id

IndexBlock *InvertedIndex::AddBlock(t_docId firstId) {
  TotalIIBlocks++;
  size++;
  blocks = rm_realloc(blocks, size * sizeof(IndexBlock));
  IndexBlock *last = blocks + (size - 1);
  memset(last, 0, sizeof(*last));  // for msan
  last->firstId = last->lastId = firstId;
  new (&LastBlock().buf) Buffer(INDEX_BLOCK_INITIAL_CAP);
  return &LastBlock();
}

//---------------------------------------------------------------------------------------------

InvertedIndex::InvertedIndex(IndexFlags flags, int initBlock) {
  blocks = NULL;
  size = 0;
  lastId = 0;
  gcMarker = 0;
  flags = flags;
  numDocs = 0;
  if (initBlock) {
    AddBlock(0);
  }
}

//---------------------------------------------------------------------------------------------

IndexBlock::~IndexBlock() {
  Buffer_Free(&buf);
}

//---------------------------------------------------------------------------------------------

InvertedIndex::~InvertedIndex() {
  TotalIIBlocks -= size;
  for (uint32_t i = 0; i < size; i++) {
    blocks[i].~IndexBlock();
  }
  rm_free(blocks);
}

///////////////////////////////////////////////////////////////////////////////////////////////

void IndexReader::SetAtEnd(int value) {
  if (isValidP) {
    *isValidP = !value;
  }
  atEnd = value;
}

//---------------------------------------------------------------------------------------------

// A callback called from the ConcurrentSearchCtx after regaining execution and reopening the
// underlying term key. We check for changes in the underlying key, or possible deletion of it.

void IndexReader::OnReopen(RedisModuleKey *k) {

  // If the key has been deleted we'll get a NULL here, so we just mark ourselves as EOF
  if (k == NULL || RedisModule_ModuleTypeGetType(k) != InvertedIndexType) {
    SetAtEnd(1);
    idx = NULL;
    br.buf = NULL;
    return;
  }

  // If the key is valid, we just reset the reader's buffer reader to the current block pointer
  idx = RedisModule_ModuleTypeGetValue(k);

  // the gc marker tells us if there is a chance the keys has undergone GC while we were asleep
  if (gcMarker == idx->gcMarker) {
    // no GC - we just go to the same offset we were at
    size_t offset = br.pos;
    br.Set(&CurrentBlock().buf, offset);
  } else {
    // if there has been a GC cycle on this key while we were asleep, the offset might not be valid
    // anymore. This means that we need to seek to last docId we were at

    // reset the state of the reader
    t_docId lastId = lastId;
    currentBlock = 0;
    br.Set(&CurrentBlock().buf);
    lastId = CurrentBlock().firstId;

    // seek to the previous last id
    RSIndexResult *dummy = NULL;
    SkipTo(lastId, &dummy);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

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

//---------------------------------------------------------------------------------------------

// Get the appropriate encoder based on index flags

IndexEncoder InvertedIndex::GetEncoder(IndexFlags flags) {
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
      return encodeDocIdsOnly;

    case Index_StoreNumeric:
      return encodeNumeric;

    // invalid encoder - we will fail
    default:
      break;
  }

  return NULL;
}

//---------------------------------------------------------------------------------------------

// Write a forward-index entry to an index writer

size_t InvertedIndex::WriteEntryGeneric(IndexEncoder encoder, t_docId docId, const RSIndexResult &entry) {
  // do not allow the same document to be written to the same index twice.
  // this can happen with duplicate tags for example
  if (lastId && lastId == docId) return 0;

  t_docId delta = 0;
  IndexBlock *blk = &LastBlock();

  // see if we need to grow the current block
  if (blk->numDocs >= INDEX_BLOCK_SIZE) {
    blk = AddBlock(docId);
  } else if (blk->numDocs == 0) {
    blk->firstId = blk->lastId = docId;
  }

  delta = docId - blk->lastId;
  if (delta > UINT32_MAX) {
    blk = AddBlock(docId);
    delta = 0;
  }

  BufferWriter bw = NewBufferWriter(&blk->buf);

  // printf("Writing docId %llu, delta %llu, flags %x\n", docId, delta, (int)idx->flags);
  size_t ret = encoder(&bw, delta, &entry);

  lastId = docId;
  blk->lastId = docId;
  ++blk->numDocs;
  ++numDocs;

  return ret;
}

//---------------------------------------------------------------------------------------------

struct ForwardIndexEntryResult : RSIndexResult {
  ForwardIndexEntryResult(const ForwardIndexEntry &ent) {
    type = RSResultType_Term;
    docId = ent.docId;
    offsetsSz = ent.vw ? ent.vw->GetByteLength() : 0;
    freq = ent.freq;
    fieldMask = ent.fieldMask;
    term.term = NULL;
    if (ent.vw) {
      term.offsets.data = ent.vw->GetByteData();
      term.offsets.len = ent.vw->GetByteLength();
    }
  }
};

//---------------------------------------------------------------------------------------------

// Write a forward-index entry to the index
size_t InvertedIndex::WriteForwardIndexEntry(IndexEncoder encoder, const ForwardIndexEntry &ent) {
  return WriteEntryGeneric(encoder, ent.docId, ForwardIndexEntryResult(ent));
}

// Write a numeric entry to the index
size_t InvertedIndex::WriteNumericEntry(t_docId docId, double value) {
  return WriteEntryGeneric(encodeNumeric, docId, RSIndexResult{docId, value});
}

///////////////////////////////////////////////////////////////////////////////////////////////

// current block while reading the index
IndexBlock &IndexReader::CurrentBlock() {
  return idx->blocks[currentBlock];
}

void IndexReader::AdvanceBlock() {
  currentBlock++;
  br.Set(&CurrentBlock().buf);
  lastId = CurrentBlock().firstId;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/* Index Decoder Implementations
 *
 * We have 9 distinct ways to decode the index records. Based on the index flags we select the
 * correct decoder for creating an index reader. A decoder both decodes the entry and does initial
 * filtering, returning 1 if the record is ok or 0 if it is filtered.
 *
 * Term indexes can filter based on fieldMask, and
 */

#define DECODER(name) \
  int IndexDecoder::name(BufferReader *br, RSIndexResult *res)

// Skipper implements SkipTo. It is an optimized version of DECODER which reads
// the document ID first, and skips ahead if the result does not match the expected one.

#define SKIPPER(name) \
  int IndexDecoder::name(BufferReader *br, IndexReader *ir, t_docId expid, RSIndexResult *res)

#define CHECK_FLAGS(res) return ((res->fieldMask & mask) != 0)

//---------------------------------------------------------------------------------------------

DECODER(readFreqsFlags) {
  qint_decode3(br, (uint32_t *)&res->docId, &res->freq, (uint32_t *)&res->fieldMask);
  // qint_decode3(br, &res->docId, &res->freq, &res->fieldMask);
  CHECK_FLAGS(res);
}

DECODER(readFreqsFlagsWide) {
  uint32_t maskSz;
  qint_decode2(br, (uint32_t *)&res->docId, &res->freq);
  res->fieldMask = ReadVarintFieldMask(br);
  CHECK_FLAGS(res);
}

DECODER(readFreqOffsetsFlags) {
  qint_decode4(br, (uint32_t *)&res->docId, &res->freq, (uint32_t *)&res->fieldMask,
               &res->offsetsSz);
  res->term.offsets.data = BufferReader_Current(br);
  res->term.offsets.len = res->offsetsSz;
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(res);
}

SKIPPER(seekFreqOffsetsFlags) {
  uint32_t did = 0, freq = 0, offsz = 0;
  t_fieldMask fm = 0;
  t_docId lastId = ir->lastId;
  int rc = 0;

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

    if (mask & fm) {
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
      if (!(mask & fm)) {
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
  CHECK_FLAGS(res);
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

  // printf("res->num.value: %lf\n", res->num.value);

  if (filter) {
    bool rv = filter->Match(res->num.value);
    // printf("Checking against filter: %d\n", rv);
    return rv ? 1 : 0;
  }
  // printf("Field matches.. hurray!\n");
  return 1;
}

DECODER(readFreqs) {
  qint_decode2(br, (uint32_t *)&res->docId, &res->freq);
  return 1;
}

DECODER(readFlags) {
  qint_decode2(br, (uint32_t *)&res->docId, (uint32_t *)&res->fieldMask);
  CHECK_FLAGS(res);
}

DECODER(readFlagsWide) {
  res->docId = ReadVarint(br);
  res->freq = 1;
  res->fieldMask = ReadVarintFieldMask(br);
  CHECK_FLAGS(res);
}

DECODER(readFlagsOffsets) {
  qint_decode3(br, (uint32_t *)&res->docId, (uint32_t *)&res->fieldMask, &res->offsetsSz);
  res->term.offsets = RSOffsetVector(BufferReader_Current(br), res->offsetsSz);
  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(res);
}

DECODER(readFlagsOffsetsWide) {

  qint_decode2(br, (uint32_t *)&res->docId, &res->offsetsSz);
  res->fieldMask = ReadVarintFieldMask(br);
  res->term.offsets = RSOffsetVector(BufferReader_Current(br), res->offsetsSz);

  Buffer_Skip(br, res->offsetsSz);
  CHECK_FLAGS(res);
}

DECODER(readOffsets) {
  qint_decode2(br, (uint32_t *)&res->docId, &res->offsetsSz);
  res->term.offsets = RSOffsetVector(BufferReader_Current(br), res->offsetsSz);
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

DECODER(readFreqsOffsets) {
  qint_decode3(br, (uint32_t *)&res->docId, &res->freq, &res->offsetsSz);
  res->term.offsets = RSOffsetVector(BufferReader_Current(br), res->offsetsSz);
  Buffer_Skip(br, res->offsetsSz);
  return 1;
}

DECODER(readDocIdsOnly) {
  res->docId = ReadVarint(br);
  res->freq = 1;
  return 1;  // Don't care about field mask
}

//---------------------------------------------------------------------------------------------

IndexDecoder InvertedIndex::GetDecoder(uint32_t flags) {
  return IndexDecoder(flags);
}

//---------------------------------------------------------------------------------------------

IndexDecoder::IndexDecoder(uint32_t flags) {
  ctor(flags);
}

IndexDecoder::IndexDecoder(uint32_t flags, t_fieldMask mask) : mask(mask) {
  ctor(flags);
}

IndexDecoder::IndexDecoder(uint32_t flags, const NumericFilter *filter) : filter(filter) {
  ctor(flags);
}

void IndexDecoder::ctor(uint32_t flags) {

#define SET_DECODERS(decoder_, seeker_) \
  decoder = decoder_; \
  seeker = seeker_; \
  return

  switch (flags & INDEX_STORAGE_MASK) {
    // (freqs, fields, offset)
    case Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets:
      SET_DECODERS(&readFreqOffsetsFlags, &seekFreqOffsetsFlags);

    case Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
      SET_DECODERS(&readFreqOffsetsFlagsWide, nullptr);

    // (freqs)
    case Index_StoreFreqs:
      SET_DECODERS(&readFreqs, nullptr);

    // (offsets)
    case Index_StoreTermOffsets:
      SET_DECODERS(&readOffsets, nullptr);

    // (fields)
    case Index_StoreFieldFlags:
      SET_DECODERS(&readFlags, nullptr);

    case Index_StoreFieldFlags | Index_WideSchema:
      SET_DECODERS(&readFlagsWide, nullptr);

    // ()
    case Index_DocIdsOnly:
      SET_DECODERS(&readDocIdsOnly, nullptr);

    // (freqs, offsets)
    case Index_StoreFreqs | Index_StoreTermOffsets:
      SET_DECODERS(&readFreqsOffsets, nullptr);

    // (freqs, fields)
    case Index_StoreFreqs | Index_StoreFieldFlags:
      SET_DECODERS(&readFreqsFlags, nullptr);

    case Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema:
      SET_DECODERS(&readFreqsFlagsWide, nullptr);

    // (fields, offsets)
    case Index_StoreFieldFlags | Index_StoreTermOffsets:
      SET_DECODERS(&readFlagsOffsets, nullptr);

    case Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
      SET_DECODERS(&readFlagsOffsetsWide, nullptr);

    case Index_StoreNumeric:
      SET_DECODERS(&readNumeric, nullptr);

    default:
      fprintf(stderr, "No decoder for flags %x\n", flags & INDEX_STORAGE_MASK);
      SET_DECODERS(nullptr, nullptr);
  }
}

//---------------------------------------------------------------------------------------------

NumericIndexReader::NumericIndexReader(InvertedIndex *idx, const IndexSpec *sp, const NumericFilter *flt) :
    IndexReader(sp, idx, IndexDecoder(Index_StoreNumeric, flt), new NumericResult(), 1.0) {
}

//---------------------------------------------------------------------------------------------

/**
 * Get the real ID, given the current delta
 * @param lastId[in, out] The last ID processed. This is updated with the
 *  current ID after computing
 * @param delta the raw delta read
 * @param isFirst whether this is the first ID in the block
 */

static t_docId calculateId(t_docId lastId, uint32_t delta, int isFirst) {
  t_docId ret;

  if (isFirst && delta != 0) {
    // this is an old version rdb, the first entry is the docid itself and
    // not the delta
    ret = delta;
  } else {
    ret = delta + lastId;
  }
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

class NumericIndexCriteriaTester : public IndexCriteriaTester {
public:
  NumericFilter nf;
  const IndexSpec *spec;

  NumericIndexCriteriaTester(IndexReader *ir, const NumericFilter &nf) : spec(ir->sp), nf(nf) {}

  int Test(t_docId id) {
    size_t len;
    const char *externalId = DocTable_GetKey((DocTable *) &spec->docs, id, &len);
    double n;
    int ret = spec->getValue(spec->getValueCtx, nf.fieldName, externalId, NULL, &n);
    RS_LOG_ASSERT(ret == RSVALTYPE_DOUBLE, "RSvalue type should be a double");
    return (nf.min < n || nf.inclusiveMin && nf.min == n) &&
           (nf.max > n || nf.inclusiveMax && nf.max == n);
  }
};

//---------------------------------------------------------------------------------------------

class TermIndexCriteriaTester : public IndexCriteriaTester {
public:
  char *term;
  size_t termLen;
  t_fieldMask fieldMask;
  const IndexSpec *spec;

  TermIndexCriteriaTester(IndexReader *ir) {
    spec = ir->sp;
    term = rm_strdup(ir->record->term.term->str);
    termLen = ir->record->term.term->len;
    fieldMask = ir->decoder.mask;
  }

  int Test(t_docId id) {
    size_t len;
    const char *externalId = DocTable_GetKey((DocTable *) &spec->docs, id, &len);
    for (int i = 0; i < spec->numFields; ++i) {
      FieldSpec *field = spec->fields + i;
      if (!(FIELD_BIT(field) & fieldMask)) {
        // field is not requested, we are not checking this field!!
        continue;
      }
      char *s;
      int ret = spec->getValue(spec->getValueCtx, field->name, externalId, &s, NULL);
      RS_LOG_ASSERT(ret == RSVALTYPE_STRING, "RSvalue type should be a string");
      if (strcmp(term, s) == 0) {
        return 1;
      }
    }
    return 0;
  }
};

//---------------------------------------------------------------------------------------------

IndexCriteriaTester *IR_GetCriteriaTester(IndexReader *ir) {
  if (!ir->sp || !ir->sp->getValue) {
    return NULL;  // CriteriaTester is not supported!!!
  }
  if (ir->decoder.decoder != &IndexDecoder::readNumeric) {
    return new TermIndexCriteriaTester(ir);
  }
  // for now, if the iterator did not took the numric filter we will avoid using the CT.
  // TODO: save the numeric filter in the numeric iterator to support CT anyway.
  if (!ir->decoder.filter) {
    return NULL;
  }
  return new NumericIndexCriteriaTester(ir, *ir->decoder.filter);
}

///////////////////////////////////////////////////////////////////////////////////////////////

size_t IndexReader::NumEstimated() const {
  return idx->numDocs;
}

//---------------------------------------------------------------------------------------------

int IndexReader::Read(RSIndexResult **e) {
  if (atEnd) {
    goto eof;
  }
  do {
    // if needed - skip to the next block (skipping empty blocks that may appear here due to GC)
    while (BufferReader_AtEnd(&br)) {
      // We're at the end of the last block...
      if (currentBlock + 1 == idx->size) {
        goto eof;
      }
      AdvanceBlock();
    }

    size_t pos = br.pos;
    int rv = (decoder.*(decoder.decoder))(&br, record);

    // We write the docid as a 32 bit number when decoding it with qint.
    uint32_t delta = *(uint32_t *)&record->docId;
    lastId = record->docId = calculateId(lastId, delta, pos == 0);

    // The decoder also acts as a filter. A zero return value means that the
    // current record should not be processed.
    if (!rv) {
      continue;
    }

    ++len;
    *e = record;
    return INDEXREAD_OK;

  } while (1);

eof:
  SetAtEnd(1);
  return INDEXREAD_EOF;
}

//---------------------------------------------------------------------------------------------

int IndexReader::SkipToBlock(t_docId docId) {
  int rc = 0;
  InvertedIndex *idx = idx;

  // the current block doesn't match and it's the last one - no point in searching
  if (currentBlock + 1 == idx->size) {
    return 0;
  }

  uint32_t top = idx->size - 1;
  uint32_t bottom = currentBlock + 1;
  uint32_t i = bottom;  //(bottom + top) / 2;
  while (bottom <= top) {
    const IndexBlock *blk = idx->blocks + i;
    if (blk->Matches(docId)) {
      currentBlock = i;
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

  currentBlock = i;

new_block:
  lastId = CurrentBlock().firstId;
  br.Set(&CurrentBlock().buf);
  return rc;
}

//---------------------------------------------------------------------------------------------

int IndexReader::SkipTo(t_docId docId, RSIndexResult **hit) {
  if (!docId) {
    return Read(hit);
  }

  if (atEnd || docId > idx->lastId || idx->size == 0) {
    goto eof;
  }

  if (!CurrentBlock().Matches(docId)) {
    SkipToBlock(docId);
  } else if (BufferReader_AtEnd(&br)) {
    // Current block, but there's nothing here
    if (Read(hit) == INDEXREAD_EOF) {
      goto eof;
    } else {
      return INDEXREAD_NOTFOUND;
    }
  }

  /**
   * We need to replicate the effects of Read() without actually calling it
   * continuously.
   *
   * The seeker function saves CPU by avoiding unnecessary function
   * calls and pointer derefences/accesses if the requested ID is
   * not found. Because less checking is required
   *
   * We:
   * 1. Call Read() at least once
   * 2. Read seeks ahead to the first non-empty block
   * 3. Read reads the current record
   * 4. If the current record's flags do not match the fieldmask, it
   *    continues to step 2
   * 5. If the current record's flags match, the function exits
   * 6. The returned ID is examined. If:
   *    - ID is smaller than requested, continue to step 1
   *    - ID is larger than requested, return NOTFOUND
   *    - ID is equal, return OK
   */

  if (decoder.seeker) {
    // // if needed - skip to the next block (skipping empty blocks that may appear here due to GC)
    while (BufferReader_AtEnd(&br)) {
      // We're at the end of the last block...
      if (currentBlock + 1 == idx->size) {
        goto eof;
      }
      AdvanceBlock();
    }

    // the seeker will return 1 only when it found a docid which is greater or equals the
    // searched docid and the field mask matches the searched fields mask. We need to continue
    // scanning only when we found such an id or we reached the end of the inverted index.
    while (! (decoder.*(decoder.seeker))(&br, this, docId, record)) {
      if (BufferReader_AtEnd(&br)) {
        if (currentBlock < idx->size - 1) {
          AdvanceBlock();
        } else {
          return INDEXREAD_EOF;
        }
      }
    }
    // Found a document that match the field mask and greater or equal the searched docid
    *hit = record;
    return (record->docId == docId) ? INDEXREAD_OK : INDEXREAD_NOTFOUND;
  } else {
    int rc;
    t_docId rid;
    while (INDEXREAD_EOF != (rc = Read(hit))) {
      rid = lastId;
      if (rid < docId) continue;
      if (rid == docId) return INDEXREAD_OK;
      return INDEXREAD_NOTFOUND;
    }
  }
eof:
  SetAtEnd(1);
  return INDEXREAD_EOF;
}

//---------------------------------------------------------------------------------------------

size_t IndexReader::NumDocs() const {
  // otherwise we use our counter
  return len;
}

//---------------------------------------------------------------------------------------------

IndexReader::IndexReader(const IndexSpec *sp, InvertedIndex *idx, IndexDecoder decoder,
    RSIndexResult *record, double weight) : IndexIterator(this), sp(sp), idx(idx), decoder(decoder), 
    record(record), weight(weight) {
  gcMarker = idx->gcMarker;
  lastId = CurrentBlock().firstId;
  br.Set(&CurrentBlock().buf);

  currentBlock = 0;
  len = 0;
  isValidP = NULL;
  SetAtEnd(0);
}

IndexReader::~IndexReader() {
  delete record;
}

//---------------------------------------------------------------------------------------------

static double calculateIDF(size_t totalDocs, size_t termDocs) {
  return logb(1.0F + totalDocs / (termDocs ? termDocs : (double)1));
}

//---------------------------------------------------------------------------------------------

static RSQueryTerm *termWithIDF(RSQueryTerm *term, InvertedIndex *idx, IndexSpec *sp) {
  if (term && sp) {
    // compute IDF based on num of docs in the header
    term->idf = calculateIDF(sp->docs.size, idx->numDocs);
  }
  return term;
}

//---------------------------------------------------------------------------------------------

TermIndexReader::TermIndexReader(InvertedIndex *idx, IndexSpec *sp, t_fieldMask fieldMask,
    RSQueryTerm *term, double weight) : IndexReader(sp, idx, 
      IndexDecoder((uint32_t)idx->flags & INDEX_STORAGE_MASK, fieldMask), 
      NewTermRecord(termWithIDF(term, idx, sp), weight),
      weight) {
}

//---------------------------------------------------------------------------------------------

void IndexReader::Abort() {
  SetAtEnd(1);
}

//---------------------------------------------------------------------------------------------

inline t_docId IndexReader::LastDocId() const {
  return lastId;
}

//---------------------------------------------------------------------------------------------

void IndexReader::Rewind() {
  SetAtEnd(0);
  currentBlock = 0;
  gcMarker = idx->gcMarker;
  br.Set(&CurrentBlock().buf);
  lastId = CurrentBlock().firstId;
}

//---------------------------------------------------------------------------------------------

IndexIterator *IndexReader::NewReadIterator() {
	auto ri = new IndexIterator(this);
	isValidP = &ri->isValid;
	return ri;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/* Repair an index block by removing garbage - records pointing at deleted documents.
 * Returns the number of records collected, and puts the number of bytes collected in the given
 * pointer. If an error occurred - returns -1
 */
int IndexBlock::Repair(DocTable *dt, IndexFlags flags, IndexRepairParams *params) {
  t_docId lastReadId = firstId;
  bool isFirstRes = true;

  t_docId oldFirstBlock = lastId;
  lastId = firstId = 0;
  Buffer repair = {0};
  BufferReader br(&buf);
  BufferWriter bw(&repair);

  RSIndexResult *res;
  if (flags == Index_StoreNumeric)
    res = new NumericResult();
  else
    res = new TermResult(NULL, 1);
  size_t frags = 0;
  int isLastValid = 0;

  uint32_t readFlags = flags & INDEX_STORAGE_MASK;
  IndexDecoder decoder(readFlags);
  IndexEncoder encoder(readFlags);

  if (!encoder || decoder.decoder == nullptr) {
    fprintf(stderr, "Could not get decoder/encoder for index\n");
    return -1;
  }

  while (!br.AtEnd()) {
    //static const IndexDecoderCtx empty = {0};
    const char *bufBegin = br.Current();
    (decoder.*(decoder.decoder))(&br, res);
    size_t sz = br.Current() - bufBegin;
    if (!(isFirstRes && res->docId != 0)) {
      // if we are entering this here
      // then its not the first entry or its
      // not an old rdb version
      // on an old rdb version, the first entry is the docid itself and not
      // the delta, so no need to increase by the lastReadId
      res->docId = (*(uint32_t *)&res->docId) + lastReadId;
    }
    isFirstRes = false;
    lastReadId = res->docId;
    int docExists = DocTable_Exists(dt, res->docId);

    // If we found a deleted document, we increment the number of found "frags",
    // and not write anything, so the reader will advance but the writer won't.
    // this will close the "hole" in the index
    if (!docExists) {
      if (params->RepairCallback) {
        params->RepairCallback(res, this, params->arg);
      }
      if (!frags++) {
        // First invalid doc; copy everything prior to this to the repair
        // buffer
        bw.Write(buf.data, bufBegin - buf.data);
      }
      params->bytesCollected += sz;
      isLastValid = 0;
    } else {
      // Valid document, but we're rewriting the block:
      if (frags) {

        // In this case we are already closing holes, so we need to write back the record at the
        // writer's position. We also calculate the delta again
        if (!lastId) {
          lastId = res->docId;
        }
        if (isLastValid) {
          bw.Write(bufBegin, sz);
        } else {
          encoder(&bw, res->docId - lastId, res);
        }
      }

      // Update these for every valid document, even for those which
      // are not repaired
      if (firstId == 0) {
        firstId = res->docId;
      }
      lastId = res->docId;
      isLastValid = 1;
    }
  }
  if (frags) {
    // If we deleted stuff from this block, we need to change the number of docs and the data
    // pointer
    numDocs -= frags;
    Buffer_Free(&buf);
    buf = repair;
    buf.ShrinkToSize();
  }
  if (numDocs == 0) {
    // if we left with no elements we do need to keep the
    // first id so the binary search on the block will still working.
    // The last_id will turn zero indicating there is no records in this block.
    // We will not save empty blocks in rdb and also we will not read empty block 
    // from rdb (in case we read a corrunpted rdb from older versions).
    firstId = oldFirstBlock;
  }
  IndexResult_Free(res);
  return frags;
}

//---------------------------------------------------------------------------------------------

int InvertedIndex::Repair(DocTable *dt, uint32_t startBlock, IndexRepairParams *params) {
  size_t limit = params->limit ? params->limit : SIZE_MAX;
  size_t blocksProcessed = 0;
  for (; startBlock < size && blocksProcessed < limit; ++startBlock, ++blocksProcessed) {
    IndexBlock *blk = blocks + startBlock;
    if (blk->lastId - blk->firstId > UINT32_MAX) {
      // Skip over blocks which have a wide variation. In the future we might
      // want to split a block into two (or more) on high-delta boundaries.
      continue;
    }
    int repaired = blocks[startBlock].Repair(dt, flags, params);
    // We couldn't repair the block - return 0
    if (repaired == -1) {
      return 0;
    }
    if (repaired > 0) {
      // Record the number of records removed for gc stats
      params->docsCollected += repaired;
      numDocs -= repaired;

      // Increase the GC marker so other queries can tell that we did something
      ++gcMarker;
    }
  }

  return startBlock < size ? startBlock : 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

IndexIterator::IndexIterator() : ir(NULL), mode(IndexIteratorMode::Sorted), isValid(false), 
  current(ir->record) {}

IndexIterator::IndexIterator(IndexReader *ir) : ir(ir), mode(IndexIteratorMode::Sorted), 
  isValid(!ir->atEnd), current(ir->record) {}

IndexIterator::~IndexIterator() {
  delete ir; //@@ ownership?
}

///////////////////////////////////////////////////////////////////////////////////////////////
