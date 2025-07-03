/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern "C" {
#include "hiredis/sds.h"
}

#include "src/buffer/buffer.h"
#include "src/index.h"
#include "src/forward_index.h"
#include "src/index_result.h"
#include "src/query_parser/tokenizer.h"
#include "src/spec.h"
#include "src/tokenize.h"
#include "varint.h"
#include "src/hybrid_reader.h"
#include "src/metric_iterator.h"
#include "src/util/arr.h"
#include "src/util/references.h"

#include "rmutil/alloc.h"

#include "gtest/gtest.h"

#include "common.h"
#include "index_utils.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <time.h>
#include <float.h>
#include <vector>
#include <cstdint>
#include <random>
#include <chrono>
#include <iostream>

class IndexTest : public ::testing::Test {};

static RSOffsetVector offsetsFromVVW(const VarintVectorWriter *vvw) {
  RSOffsetVector ret = {0};
  ret.data = (char *) VVW_GetByteData(vvw);
  ret.len = VVW_GetByteLength(vvw);
  return ret;
}

TEST_F(IndexTest, testVarint) {
  VarintVectorWriter *vw = NewVarintVectorWriter(8);
  uint32_t expected[5] = {10, 1000, 1020, 10000, 10020};
  for (int i = 0; i < 5; i++) {
    VVW_Write(vw, expected[i]);
  }

  // VVW_Write(vw, 100);
  VVW_Truncate(vw);

  RSOffsetVector vec = offsetsFromVVW(vw);
  RSOffsetIterator it = RSOffsetVector_Iterate(&vec, NULL);
  int x = 0;
  uint32_t n = 0;
  while (RS_OFFSETVECTOR_EOF != (n = it.Next(it.ctx, NULL))) {
    auto curexp = expected[x++];
    ASSERT_EQ(curexp, n) << "Wrong number decoded";
    // printf("%d %d\n", x, n);
  }
  it.Free(it.ctx);
  VVW_Free(vw);
}

TEST_F(IndexTest, testDistance) {
  VarintVectorWriter *vw = NewVarintVectorWriter(8);
  VarintVectorWriter *vw2 = NewVarintVectorWriter(8);
  VarintVectorWriter *vw3 = NewVarintVectorWriter(8);
  VVW_Write(vw, 1);
  VVW_Write(vw, 9);
  VVW_Write(vw, 13);
  VVW_Write(vw, 16);
  VVW_Write(vw, 22);

  VVW_Write(vw2, 4);
  VVW_Write(vw2, 7);
  VVW_Write(vw2, 32);

  VVW_Write(vw3, 20);
  VVW_Write(vw3, 25);

  VVW_Truncate(vw);
  VVW_Truncate(vw2);

  RSIndexResult *tr1 = NewTokenRecord(NULL, 1);
  tr1->docId = 1;
  tr1->data.term.offsets = offsetsFromVVW(vw);

  RSIndexResult *tr2 = NewTokenRecord(NULL, 1);
  tr2->docId = 1;
  tr2->data.term.offsets = offsetsFromVVW(vw2);

  RSIndexResult *res = NewIntersectResult(2, 1);
  AggregateResult_AddChild(res, tr1);
  AggregateResult_AddChild(res, tr2);

  int delta = IndexResult_MinOffsetDelta(res);
  ASSERT_EQ(2, delta);

  ASSERT_EQ(0, IndexResult_IsWithinRange(res, 0, 0));
  ASSERT_EQ(0, IndexResult_IsWithinRange(res, 0, 1));
  ASSERT_EQ(0, IndexResult_IsWithinRange(res, 1, 1));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 1, 0));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 2, 1));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 2, 0));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 3, 1));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 4, 0));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 4, 1));
  ASSERT_EQ(1, IndexResult_IsWithinRange(res, 5, 1));

  RSIndexResult *tr3 = NewTokenRecord(NULL, 1);
  tr3->docId = 1;
  tr3->data.term.offsets = offsetsFromVVW(vw3);
  AggregateResult_AddChild(res, tr3);

  delta = IndexResult_MinOffsetDelta(res);
  ASSERT_EQ(7, delta);

  // test merge iteration
  RSOffsetIterator it = RSIndexResult_IterateOffsets(res);
  uint32_t expected[] = {1, 4, 7, 9, 13, 16, 20, 22, 25, 32, RS_OFFSETVECTOR_EOF};

  uint32_t rc;
  int i = 0;
  do {
    rc = it.Next(it.ctx, NULL);
    ASSERT_EQ(rc, (expected[i++]));
  } while (rc != RS_OFFSETVECTOR_EOF);
  it.Free(it.ctx);

  IndexResult_Free(tr1);
  IndexResult_Free(tr2);
  IndexResult_Free(tr3);
  IndexResult_Free(res);
  VVW_Free(vw);
  VVW_Free(vw2);
  VVW_Free(vw3);
}

class IndexFlagsTest : public testing::TestWithParam<int> {};

TEST_P(IndexFlagsTest, testRWFlags) {
  IndexFlags indexFlags = (IndexFlags)GetParam();
  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(indexFlags, 1, &index_memsize);
  int useFieldMask = indexFlags & Index_StoreFieldFlags;

  size_t t_fiedlMask_memsize = sizeof(t_fieldMask);
  size_t exp_t_fieldMask_memsize = 16;
  ASSERT_EQ(exp_t_fieldMask_memsize, t_fiedlMask_memsize);

  // Details of the memory occupied by InvertedIndex in bytes (64-bit system):
  // IndexBlock *blocks         8
  // uint32_t size              4
  // IndexFlags                 4
  // t_docId lastId             8
  // uint32_t numDocs           4
  // uint32_t gcMarker          4
  // union {
  //   t_fieldMask fieldMask;
  //   uint64_t numEntries;
  // };                        16
  // ----------------------------
  // Total                     48
  size_t ividx_memsize = sizeof(InvertedIndex);
  size_t exp_ividx_memsize = 48;
  ASSERT_EQ(exp_ividx_memsize, ividx_memsize);

  size_t idx_no_block_memsize = sizeof_InvertedIndex(indexFlags);
  size_t exp_idx_no_block_memsize = useFieldMask ?
                                    exp_ividx_memsize :
                                    exp_ividx_memsize - exp_t_fieldMask_memsize;
  ASSERT_EQ(exp_idx_no_block_memsize, idx_no_block_memsize);

  size_t block_memsize = sizeof(IndexBlock);
  size_t exp_block_memsize = 48;
  ASSERT_EQ(exp_block_memsize, block_memsize);

  size_t expectedIndexSize = exp_idx_no_block_memsize + exp_block_memsize + INDEX_BLOCK_INITIAL_CAP;
  // The memory occupied by a new inverted index depends of its flags
  // see NewInvertedIndex() and sizeof_InvertedIndex() for details
  ASSERT_EQ(expectedIndexSize, index_memsize);

  IndexEncoder enc = InvertedIndex_GetEncoder(indexFlags);
  IndexEncoder docIdEnc = InvertedIndex_GetEncoder(Index_DocIdsOnly);

  ASSERT_TRUE(enc != NULL);
  ASSERT_TRUE(docIdEnc != NULL);

  for (size_t i = 0; i < 200; i++) {

    ForwardIndexEntry h;
    h.docId = i;
    h.fieldMask = 1;
    h.freq = (1 + i % 100) / (float)101;

    h.vw = NewVarintVectorWriter(8);
    for (int n = 0; n < i % 4; n++) {
      VVW_Write(h.vw, n);
    }
    VVW_Truncate(h.vw);

    InvertedIndex_WriteForwardIndexEntry(idx, enc, &h);

    // printf("doc %d, score %f offset %zd\n", h.docId, h.docScore, w->bw.buf->offset);
    VVW_Free(h.vw);
  }

  ASSERT_EQ(200, idx->numDocs);
  if (enc != docIdEnc) {
    ASSERT_EQ(2, idx->size);
  } else {
    ASSERT_EQ(1, idx->size);
  }
  ASSERT_EQ(199, idx->lastId);

  for (int xx = 0; xx < 1; xx++) {
    IndexReader *ir = NewTermIndexReader(idx);
    RSIndexResult *h = NULL;

    int n = 0;
    int rc;
    while (!ir->atEnd_) {
      if ((rc = IR_Read(ir, &h)) == INDEXREAD_EOF) {
        break;
      }
      ASSERT_EQ(INDEXREAD_OK, rc);
      ASSERT_EQ(h->docId, n);
      n++;
    }
    IR_Free(ir);
  }

  InvertedIndex_Free(idx);
}

INSTANTIATE_TEST_SUITE_P(IndexFlagsP, IndexFlagsTest, ::testing::Values(
    // 1. Full encoding - docId, freq, flags, offset
    int(Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags),
    int(Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_WideSchema),
    // 2. (Frequency, Field)
    int(Index_StoreFreqs | Index_StoreFieldFlags),
    int(Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema),
    // 3. Frequencies only
    int(Index_StoreFreqs),
    // 4. Field only
    int(Index_StoreFieldFlags),
    int(Index_StoreFieldFlags | Index_WideSchema),
    // 5. (field, offset)
    int(Index_StoreFieldFlags | Index_StoreTermOffsets),
    int(Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema),
    // 6. (offset)
    int(Index_StoreTermOffsets),
    // 7. (freq, offset) Store term offsets but not field flags
    int(Index_StoreFreqs | Index_StoreTermOffsets),
    // 0. docid only
    int(Index_DocIdsOnly)
));

// Test we only get the right encoder and decoder for the right flags
TEST_F(IndexTest, testGetEncoderAndDecoders) {
  for (int curFlags = 0; curFlags <= INDEX_STORAGE_MASK; curFlags++) {
    switch (curFlags & INDEX_STORAGE_MASK) {
    // 1. Full encoding - docId, freq, flags, offset
    case Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags:
    case Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_WideSchema:
    // 2. (Frequency, Field)
    case Index_StoreFreqs | Index_StoreFieldFlags:
    case Index_StoreFreqs | Index_StoreFieldFlags | Index_WideSchema:
    // 3. Frequencies only
    case Index_StoreFreqs:
    // 4. Field only
    case Index_StoreFieldFlags:
    case Index_StoreFieldFlags | Index_WideSchema:
    // 5. (field, offset)
    case Index_StoreFieldFlags | Index_StoreTermOffsets:
    case Index_StoreFieldFlags | Index_StoreTermOffsets | Index_WideSchema:
    // 6. (offset)
    case Index_StoreTermOffsets:
    // 7. (freq, offset) Store term offsets but not field flags
    case Index_StoreFreqs | Index_StoreTermOffsets:
    // 0. docid only
    case Index_DocIdsOnly:
    // 9. Numeric
    case Index_StoreNumeric:
      ASSERT_TRUE(InvertedIndex_GetDecoder(IndexFlags(curFlags)).decoder);
      ASSERT_TRUE(InvertedIndex_GetEncoder(IndexFlags(curFlags)));
      break;

    // invalid flags combination
    default:
      // TODO: We currently test only with sanitizer since the sanitizer is
      // running in debug mode always, while the regular tests are running in
      // release mode.
      #ifdef __SANITIZE_ADDRESS__
        ASSERT_ANY_THROW(InvertedIndex_GetDecoder(IndexFlags(curFlags)));
        ASSERT_ANY_THROW(InvertedIndex_GetEncoder(IndexFlags(curFlags)));
      #else
        continue;
      #endif
    }
  }
}

TEST_F(IndexTest, testReadIterator) {
  InvertedIndex *idx = createPopulateTermsInvIndex(10, 1);

  IndexReader *r1 = NewTermIndexReader(idx);

  RSIndexResult *h = NULL;

  IndexIterator *it = NewReadIterator(r1);
  int i = 1;
  while (IITER_HAS_NEXT(it)) {
    if (it->Read(it->ctx, &h) == INDEXREAD_EOF) {
      break;
    }

    // printf("Iter got %d\n", h.docId);
    ASSERT_EQ(h->docId, i);
    i++;
  }
  ASSERT_EQ(11, i);

  it->Free(it);

  // IndexResult_Free(&h);
  InvertedIndex_Free(idx);
}

TEST_F(IndexTest, testUnion) {
  int oldConfig = RSGlobalConfig.iteratorsConfigParams.minUnionIterHeap;
  for (int cfg = 0; cfg < 2; ++cfg) {
    InvertedIndex *w = createPopulateTermsInvIndex(10, 2);
    InvertedIndex *w2 = createPopulateTermsInvIndex(10, 3);
    IndexReader *r1 = NewTermIndexReader(w);   //
    IndexReader *r2 = NewTermIndexReader(w2);  //

    // printf("Reading!\n");
    IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
    irs[0] = NewReadIterator(r1);
    irs[1] = NewReadIterator(r2);
    IteratorsConfig config{};
    iteratorsConfig_init(&config);
    IndexIterator *ui = NewUnionIterator(irs, 2, 0, 1, QN_UNION, NULL, &config);
    RSIndexResult *h = NULL;
    int expected[] = {2, 3, 4, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 24, 27, 30};
    int i = 0;
    while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
      // printf("%d <=> %d\n", h.docId, expected[i]);
      ASSERT_EQ(expected[i], h->docId);
      i++;

      RSIndexResult *copy = IndexResult_DeepCopy(h);
      ASSERT_TRUE(copy != NULL);
      ASSERT_TRUE(copy != h);
      ASSERT_TRUE(copy->isCopy);

      ASSERT_EQ(copy->docId, h->docId);
      ASSERT_EQ(copy->type, h->type);

      IndexResult_Free(copy);

      // printf("%d, ", h.docId);
    }


    // test read after skip goes to next id
    ui->Rewind(ui->ctx);
    ASSERT_EQ(ui->SkipTo(ui->ctx, 6, &h), INDEXREAD_OK);
    ASSERT_EQ(h->docId, 6);
    ASSERT_EQ(ui->Read(ui->ctx, &h), INDEXREAD_OK);
    ASSERT_EQ(h->docId, 8);
    // test for last id
    ASSERT_EQ(ui->SkipTo(ui->ctx, 30, &h), INDEXREAD_OK);
    ASSERT_EQ(h->docId, 30);
    ASSERT_EQ(ui->Read(ui->ctx, &h), INDEXREAD_EOF);

    ui->Free(ui);
    // IndexResult_Free(&h);
    InvertedIndex_Free(w);
    InvertedIndex_Free(w2);

    // change config parameter to use UI_ReadHigh and UI_SkipToHigh
    RSGlobalConfig.iteratorsConfigParams.minUnionIterHeap = 1;
  }
  RSGlobalConfig.iteratorsConfigParams.minUnionIterHeap = oldConfig;
}

TEST_F(IndexTest, testWeight) {
  InvertedIndex *w = createPopulateTermsInvIndex(10, 1);
  InvertedIndex *w2 = createPopulateTermsInvIndex(10, 2);
  FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = { .index = RS_INVALID_FIELD_INDEX }};
  IndexReader *r1 = NewTermIndexReaderEx(w, NULL, fieldMaskOrIndex, NULL, 0.5);  //
  IndexReader *r2 = NewTermIndexReader(w2);   //

  // printf("Reading!\n");
  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewReadIterator(r2);
  IteratorsConfig config{};
  iteratorsConfig_init(&config);
  IndexIterator *ui = NewUnionIterator(irs, 2, 0, 0.8, QN_UNION, NULL, &config);
  RSIndexResult *h = NULL;
  int expected[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20};
  int i = 0;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h.docId, expected[i]);
    ASSERT_EQ(h->docId, expected[i++]);
    ASSERT_EQ(h->weight, 0.8);
    if (h->data.agg.numChildren == 2) {
      ASSERT_EQ(h->data.agg.children[0]->weight, 0.5);
      ASSERT_EQ(h->data.agg.children[1]->weight, 1);
    } else {
      if (i <= 10) {
        ASSERT_EQ(h->data.agg.children[0]->weight, 0.5);
      } else {
        ASSERT_EQ(h->data.agg.children[0]->weight, 1);
      }
    }
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testNot) {
  InvertedIndex *w = createPopulateTermsInvIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createPopulateTermsInvIndex(10, 3);
  IndexReader *r1 = NewTermIndexReader(w);   //
  IndexReader *r2 = NewTermIndexReader(w2);  //

  // printf("Reading!\n");
  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewNotIterator(NewReadIterator(r2), w2->lastId, 1, {0}, NULL);

  IndexIterator *ui = NewIntersectIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1);
  RSIndexResult *h = NULL;
  int expected[] = {1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16};
  int i = 0;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT_EQ(expected[i++], h->docId);
    // printf("%d, ", h.docId);
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testPureNot) {
  InvertedIndex *w = createPopulateTermsInvIndex(10, 3);

  IndexReader *r1 = NewTermIndexReader(w);  //
  printf("last id: %llu\n", (unsigned long long)w->lastId);

  IndexIterator *ir = NewNotIterator(NewReadIterator(r1), w->lastId + 5, 1, {0}, NULL);

  RSIndexResult *h = NULL;
  int expected[] = {1,  2,  4,  5,  7,  8,  10, 11, 13, 14, 16, 17, 19,
                    20, 22, 23, 25, 26, 28, 29, 31, 32, 33, 34, 35};
  int i = 0;
  while (ir->Read(ir->ctx, &h) != INDEXREAD_EOF) {

    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT_EQ(expected[i++], h->docId);
  }
  ir->Free(ir);
  InvertedIndex_Free(w);
}

TEST_F(IndexTest, testNumericInverted) {
  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1, &index_memsize);

  size_t sz = 0;
  size_t expected_sz = 0;
  size_t written_bytes = 0;
  size_t bytes_per_entry = 0;
  size_t buff_cap = INDEX_BLOCK_INITIAL_CAP;
  size_t available_size = INDEX_BLOCK_INITIAL_CAP;

  for (int i = 0; i < 75; i++) {
    sz = InvertedIndex_WriteNumericEntry(idx, i + 1, (double)(i + 1));
    ASSERT_TRUE(sz == expected_sz);

    // The buffer has an initial capacity: INDEX_BLOCK_INITIAL_CAP = 6
    // For values < 7 (tiny numbers) the header (H) and value (V) will occupy
    // only 1 byte.
    // For values >= 7, the header will occupy 1 byte, and the value 1 bytes.
    //
    // The delta will occupy 1 byte.
    // The first entry has zero delta, so it will not be written.
    //
    // For the first 3 entries, the buffer will not grow, and sz = 0,
    // after that, the sz be greater than zero when the buffer grows.
    // The buffer will grow when there is not enough space to write the entry
    //
    // The number of bytes added to the capacity is defined by the formula:
    // MIN(1 + buf->cap / 5, 1024 * 1024)  (see buffer.c Buffer_Grow())
    //
    //   | H + V | Delta | Bytes     | Written  | Buff cap | Available | sz
    // i | bytes | bytes | per Entry | bytes    |          | size      |
    // ----------------------------------------------------------------------
    // 0 | 1     | 0     | 1         |  1       |  6       | 5         | 0
    // 1 | 1     | 1     | 2         |  3       |  6       | 3         | 0
    // 2 | 1     | 1     | 2         |  5       |  6       | 1         | 0
    // 3 | 1     | 1     | 2         |  7       |  8       | 1         | 2
    // 4 | 1     | 1     | 2         |  9       | 10       | 1         | 2
    // 5 | 1     | 1     | 2         | 11       | 13       | 2         | 3
    // 6 | 1     | 1     | 2         | 13       | 16       | 3         | 0
    // 7 | 2     | 1     | 3         | 16       | 16       | 0         | 3
    // 8 | 2     | 1     | 3         | 19       | 20       | 1         | 4
    // 9 | 2     | 1     | 3         | 19       | 25       | 1         | 5

    if(i < 7) {
      bytes_per_entry = 1 + (i > 0);
    } else {
      bytes_per_entry = 3;
    }

    // Simulate the buffer growth to get the expected size
    written_bytes += bytes_per_entry;
    if(buff_cap < written_bytes || buff_cap - written_bytes < bytes_per_entry) {
      expected_sz = MIN(1 + buff_cap / 5, 1024 * 1024);
    } else {
      expected_sz = 0;
    }
    buff_cap += expected_sz;
  }
  ASSERT_EQ(75, idx->lastId);

  // printf("written %zd bytes\n", IndexBlock_DataLen(&idx->blocks[0]));

  IndexReader *ir = NewMinimalNumericReader(idx, false);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;
  t_docId i = 1;
  while (INDEXREAD_EOF != it->Read(it->ctx, &res)) {
    // printf("%d %f\n", res->docId, res->num.value);

    ASSERT_EQ(i++, res->docId);
    ASSERT_EQ(res->data.num.value, (float)res->docId);
  }
  InvertedIndex_Free(idx);
  it->Free(it);
}

TEST_F(IndexTest, testNumericVaried) {
  // For various numeric values, of different types (NUM_ENCODING_COMMON_TYPE_TINY,
  // NUM_ENCODING_COMMON_TYPE_FLOAT, etc..) check that the number of allocated
  // bytes in buffers is as expected.

  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1, &index_memsize);

  static const double nums[] = {0,          0.13,          0.001,     -0.1,     1.0,
                                5.0,        4.323,         65535,     65535.53, 32768.432,
                                1LLU << 32, -(1LLU << 32), 1LLU << 40};
  static const size_t numCount = sizeof(nums) / sizeof(double);

  for (size_t i = 0; i < numCount; i++) {
    size_t min_data_len;
    size_t cap = idx->blocks[idx->size-1].buf.cap;
    size_t offset = idx->blocks[idx->size-1].buf.offset;
    size_t sz = InvertedIndex_WriteNumericEntry(idx, i + 1, nums[i]);

    if(i == 0 || i == 4 || i == 5) {
      // For tests numbers 0, 1.0, and 5.0, two bytes are enough to store them.
      min_data_len = 2;
    } else {
      min_data_len = 10;
    }

    // if there was not enough space to store the entry,
    // the capacity of the block was increased and sz > 0
    if(cap - offset < min_data_len) {
      ASSERT_TRUE(sz > 0);
    } else {
      ASSERT_TRUE(sz == 0);
    }
    // printf("[%lu]: Stored %lf\n", i, nums[i]);
  }

  IndexReader *ir = NewMinimalNumericReader(idx, false);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;

  for (size_t i = 0; i < numCount; i++) {
    // printf("Checking i=%lu. Expected=%lf\n", i, nums[i]);
    int rv = it->Read(it->ctx, &res);
    ASSERT_NE(INDEXREAD_EOF, rv);
    ASSERT_LT(fabs(nums[i] - res->data.num.value), 0.01);
  }

  ASSERT_EQ(INDEXREAD_EOF, it->Read(it->ctx, &res));
  InvertedIndex_Free(idx);
  it->Free(it);
}

typedef struct {
  double value;
  size_t size;
} encodingInfo;
static const encodingInfo infos[] = {
    {0, 1},                    // 0
    {1, 2},                    // 1
    {63, 3},                   // 2
    {-1, 3},                   // 3
    {-63, 3},                  // 4
    {64, 3},                   // 5
    {-64, 3},                  // 6
    {255, 3},                  // 7
    {-255, 3},                 // 8
    {65535, 4},                // 9
    {-65535, 4},               // 10
    {16777215, 5},             // 11
    {-16777215, 5},            // 12
    {4294967295, 6},           // 13
    {-4294967295, 6},          // 14
    {4294967295 + 1, 7},       // 15
    {4294967295 + 2, 7},       // 16
    {549755813888.0, 7},       // 17
    {549755813888.0 + 2, 7},   // 18
    {549755813888.0 - 23, 7},  // 19
    {-549755813888.0, 7},      // 20
    {1503342028.957225, 10},   // 21
    {42.4345, 10},              // 22
    {(float)0.5, 6},           // 23
    {DBL_MAX, 10},             // 24
    {UINT64_MAX >> 12, 9},     // 25
    {INFINITY, 2},             // 26
    {-INFINITY, 2}             // 27
};

void testNumericEncodingHelper(bool isMulti) {
  static const size_t numInfos = sizeof(infos) / sizeof(infos[0]);
  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1, &index_memsize);

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\n[%lu]: Expecting Val=%lf, Sz=%lu\n", ii, infos[ii].value, infos[ii].size);
    size_t cap = idx->blocks[idx->size-1].buf.cap;
    size_t offset = idx->blocks[idx->size-1].buf.offset;
    size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);

    // if there was not enough space to store the entry, sz will be greater than zero
    if(cap - offset < infos[ii].size) {
      ASSERT_TRUE(sz > 0);
    } else {
      ASSERT_TRUE(sz == 0);
    }

    if (isMulti) {
      cap = idx->blocks[idx->size-1].buf.cap;
      offset = idx->blocks[idx->size-1].buf.offset;

      size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);

      // Delta is 0, so we don't store it.
      if(cap - offset < infos[ii].size - 1) {
        ASSERT_TRUE(sz > 0);
      } else {
        ASSERT_TRUE(sz == 0);
      }
    }
  }

  IndexReader *ir = NewMinimalNumericReader(idx, isMulti);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\nReading [%lu]\n", ii);

    int rc = it->Read(it->ctx, &res);
    ASSERT_NE(rc, INDEXREAD_EOF);
    // printf("%lf <-> %lf\n", infos[ii].value, res->num.value);
    if (fabs(infos[ii].value) == INFINITY) {
      ASSERT_EQ(infos[ii].value, res->data.num.value);
    } else {
      ASSERT_LT(fabs(infos[ii].value - res->data.num.value), 0.01);
    }
  }

  InvertedIndex_Free(idx);
  it->Free(it);
}

TEST_F(IndexTest, testNumericEncoding) {
  testNumericEncodingHelper(0);
}

TEST_F(IndexTest, testNumericEncodingMulti) {
  testNumericEncodingHelper(1);
}

TEST_F(IndexTest, testAbort) {

  InvertedIndex *w = createPopulateTermsInvIndex(1000, 1);
  IndexReader *r = NewTermIndexReader(w);  //

  IndexIterator *it = NewReadIterator(r);
  int n = 0;
  RSIndexResult *res;
  while (INDEXREAD_EOF != it->Read(it->ctx, &res)) {
    if (n == 50) {
      it->Abort(it->ctx);
    }
    n++;
  }
  ASSERT_EQ(51, n);
  it->Free(it);
  InvertedIndex_Free(w);
}

TEST_F(IndexTest, testIntersection) {

  InvertedIndex *w = createPopulateTermsInvIndex(100000, 4);
  InvertedIndex *w2 = createPopulateTermsInvIndex(100000, 2);
  IndexReader *r1 = NewTermIndexReader(w);   //
  IndexReader *r2 = NewTermIndexReader(w2);  //

  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewReadIterator(r2);

  int count = 0;
  IndexIterator *ii = NewIntersectIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1);

  RSIndexResult *h = NULL;

  uint32_t topFreq = 0;
  while (ii->Read(ii->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_Intersection);
    ASSERT_TRUE(RSIndexResult_IsAggregate(h));
    ASSERT_TRUE(RSIndexResult_HasOffsets(h));
    topFreq = topFreq > h->freq ? topFreq : h->freq;

    RSIndexResult *copy = IndexResult_DeepCopy(h);
    ASSERT_TRUE(copy != NULL);
    ASSERT_TRUE(copy != h);
    ASSERT_TRUE(copy->isCopy == 1);

    ASSERT_TRUE(copy->docId == h->docId);
    ASSERT_TRUE(copy->type == RSResultType_Intersection);
    ASSERT_EQ((count * 2 + 2) * 2, h->docId);
    ASSERT_EQ(count * 2 + 2, h->freq);
    IndexResult_Free(copy);
    ++count;
  }

  // int count = IR_Intersect(r1, r2, onIntersect, &ctx);

  // printf("%d intersections in %lldms, %.0fns per iteration\n", count,
  // TimeSampler_DurationMS(&ts),
  // 1000000 * TimeSampler_IterationMS(&ts));
  // printf("top freq: %f\n", topFreq);
  ASSERT_EQ(count, 50000);
  ASSERT_EQ(topFreq, 100000.0);

  // test read after skip goes to next id
  ii->Rewind(ii->ctx);
  ASSERT_EQ(ii->SkipTo(ii->ctx, 8, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, 8);
  ASSERT_EQ(ii->Read(ii->ctx, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, 12);
  // test for last id
  ASSERT_EQ(ii->SkipTo(ii->ctx, 200000, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, 200000);
  ASSERT_EQ(ii->Read(ii->ctx, &h), INDEXREAD_EOF);

  ii->Free(ii);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testHybridVector) {

  size_t n = 100;
  size_t step = 4;
  size_t max_id = n*step;
  size_t d = 4;
  size_t k = 10;
  VecSimMetric met = VecSimMetric_L2;
  VecSimType t = VecSimType_FLOAT32;
  InvertedIndex *w = createPopulateTermsInvIndex(n, step);
  IndexReader *r = NewTermIndexReader(w);

  // Create vector index
  VecSimLogCtx logCtx = { .index_field_name = "v" };
  VecSimParams params{.algo = VecSimAlgo_HNSWLIB,
                      .algoParams = {.hnswParams = HNSWParams{.type = t,
                                               .dim = d,
                                               .metric = met,
                                               .initialCapacity = max_id,
                                               .M = 16,
                                               .efConstruction = 100}},
                      .logCtx = &logCtx};
  VecSimIndex *index = VecSimIndex_New(&params);
  for (size_t i = 1; i <= max_id; i++) {
    float f[d];
    for (size_t j = 0; j < d; j++) {
      f[j] = (float)i;
    }
    VecSimIndex_AddVector(index, (const void *)f, (int)i);
  }
  ASSERT_EQ(VecSimIndex_IndexSize(index), max_id);

  float query[] = {(float)max_id, (float)max_id, (float)max_id, (float)max_id};
  KNNVectorQuery top_k_query = {.vector = query, .vecLen = d, .k = 10, .order = BY_SCORE};
  VecSimQueryParams queryParams = {0};
  queryParams.hnswRuntimeParams.efRuntime = max_id;
  FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
  FieldFilterContext filterCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
  // Run simple top k query.
  HybridIteratorParams hParams = {.sctx=NULL,
                                  .index = index,
                                  .dim = d,
                                  .elementType = t,
                                  .spaceMetric = met,
                                  .query = top_k_query,
                                  .qParams = queryParams,
                                  .vectorScoreField = (char *)"__v_score",
                                  .canTrimDeepResults = true,
                                  .childIt = NULL,
                                  .filterCtx = &filterCtx
  };
  QueryError err = {QUERY_OK};
  IndexIterator *vecIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

  RSIndexResult *h = NULL;
  size_t count = 0;

  // Expect to get top 10 results in reverse order of the distance that passes the filter: 364, 368, ..., 400.
  while (vecIt->Read(vecIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_Metric);
    ASSERT_EQ(h->docId, max_id - count);
    count++;
  }
  ASSERT_EQ(count, k);
  ASSERT_FALSE(vecIt->HasNext(vecIt->ctx));

  vecIt->Rewind(vecIt->ctx);
  ASSERT_TRUE(vecIt->HasNext(vecIt->ctx));
  ASSERT_EQ(vecIt->NumEstimated(vecIt->ctx), k);
  ASSERT_EQ(vecIt->Len(vecIt->ctx), k);
  // Read one result to verify that we get the one with best score after rewind.
  ASSERT_EQ(vecIt->Read(vecIt->ctx, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, max_id);
  vecIt->Free(vecIt);

  // Test in hybrid mode.
  IndexIterator *ir = NewReadIterator(r);
  hParams.childIt = ir;
  IndexIterator *hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

  HybridIterator *hr = (HybridIterator *)hybridIt->ctx;
  hr->searchMode = VECSIM_HYBRID_BATCHES;

  // Expect to get top 10 results in the right order of the distance that passes the filter: 400, 396, ..., 364.
  count = 0;
  while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_Metric);
    // since larger ids has lower distance, in every we get lower id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(h->docId, expected_id);
  }
  ASSERT_EQ(count, k);
  ASSERT_FALSE(hybridIt->HasNext(hybridIt->ctx));

  hybridIt->Rewind(hybridIt->ctx);
  ASSERT_TRUE(hybridIt->HasNext(hybridIt->ctx));
  ASSERT_EQ(hybridIt->NumEstimated(hybridIt->ctx), k);
  ASSERT_EQ(hybridIt->Len(hybridIt->ctx), k);

  // check rerun and abort (go over only half of the results)
  count = 0;
  for (size_t i = 0; i < k/2; i++) {
    ASSERT_EQ(hybridIt->Read(hybridIt->ctx, &h), INDEXREAD_OK);
    ASSERT_EQ(h->type, RSResultType_Metric);
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(h->docId, expected_id);
  }
  ASSERT_EQ(hybridIt->LastDocId(hybridIt->ctx), max_id - step*((k/2)-1));
  hybridIt->Abort(hybridIt->ctx);
  ASSERT_FALSE(hybridIt->HasNext(hybridIt->ctx));

  // Rerun in AD_HOC BF mode.
  hybridIt->Rewind(hybridIt->ctx);
  hr->searchMode = VECSIM_HYBRID_ADHOC_BF;
  count = 0;
  while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_Metric);
    // since larger ids has lower distance, in every we get higher id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(h->docId, expected_id);
  }
  hybridIt->Free(hybridIt);

  // Rerun without ignoring document scores.
  r = NewTermIndexReader(w);
  ir = NewReadIterator(r);
  hParams.canTrimDeepResults = false;
  hParams.childIt = ir;
  hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  hr = (HybridIterator *)hybridIt->ctx;
  hr->searchMode = VECSIM_HYBRID_BATCHES;

  // This time, result is a tree with 2 children: vector score and subtree of terms (for scoring).
  count = 0;
  while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_HybridMetric);
    ASSERT_TRUE(RSIndexResult_IsAggregate(h));
    ASSERT_EQ(h->data.agg.numChildren, 2);
    ASSERT_EQ(h->data.agg.children[0]->type, RSResultType_Metric);
    // since larger ids has lower distance, in every we get higher id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(h->docId, expected_id);
  }
  ASSERT_EQ(count, k);
  ASSERT_FALSE(hybridIt->HasNext(hybridIt->ctx));

  // Rerun in AD_HOC BF mode.
  hybridIt->Rewind(hybridIt->ctx);
  hr->searchMode = VECSIM_HYBRID_ADHOC_BF;
  count = 0;
  while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_HybridMetric);
    ASSERT_TRUE(RSIndexResult_IsAggregate(h));
    ASSERT_EQ(h->data.agg.numChildren, 2);
    ASSERT_EQ(h->data.agg.children[0]->type, RSResultType_Metric);
    // since larger ids has lower distance, in every we get higher id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(h->docId, expected_id);
  }
  hybridIt->Free(hybridIt);

  InvertedIndex_Free(w);
  VecSimIndex_Free(index);
}

TEST_F(IndexTest, testInvalidHybridVector) {

  size_t n = 1;
  size_t d = 4;
  InvertedIndex *w = createPopulateTermsInvIndex(n, 1);
  IndexReader *r = NewTermIndexReader(w);

  // Create vector index with a single vector.
  VecSimLogCtx logCtx = { .index_field_name = "v" };
  VecSimParams params{
      .algo = VecSimAlgo_HNSWLIB,
      .algoParams = {.hnswParams = HNSWParams{
          .type = VecSimType_FLOAT32, .dim = d, .metric = VecSimMetric_L2, .initialCapacity = n}},
      .logCtx = &logCtx};
  VecSimIndex *index = VecSimIndex_New(&params);

  float vec[] = {(float)n, (float)n, (float)n, (float)n};
  VecSimIndex_AddVector(index, (const void *)vec, n);
  ASSERT_EQ(VecSimIndex_IndexSize(index), n);

  KNNVectorQuery top_k_query = {.vector = vec, .vecLen = d, .k = 10, .order = BY_SCORE};
  VecSimQueryParams queryParams = {};

  // Create invalid intersection iterator (with a child iterator which is NULL).
  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r);
  irs[1] = NULL;
  // The iterator is initialized with inOrder=true, to test the case where the null
  // child isn't the first child (since inOrder=true will trigger sorting).
  IndexIterator *ii = NewIntersectIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 1, 1);

  FieldMaskOrIndex fieldMaskOrIndex = {.isFieldMask = false, .value = {.index = RS_INVALID_FIELD_INDEX}};
  FieldFilterContext filterCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
  // Create hybrid iterator - should return NULL.
  HybridIteratorParams hParams = {.sctx = NULL,
                                  .index = index,
                                  .query = top_k_query,
                                  .qParams = queryParams,
                                  .vectorScoreField = (char *)"__v_score",
                                  .canTrimDeepResults = true,
                                  .childIt = ii,
                                  .filterCtx = &filterCtx};
  QueryError err = {QUERY_OK};
  IndexIterator *hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_FALSE(hybridIt);

  ii->Free(ii);
  InvertedIndex_Free(w);
  VecSimIndex_Free(index);
}

TEST_F(IndexTest, testMetric_VectorRange) {

  size_t n = 100;
  size_t d = 4;
  size_t k = 10;
  VecSimMetric met = VecSimMetric_Cosine;
  VecSimType t = VecSimType_FLOAT32;

  // Create vector index
  VecSimLogCtx logCtx = { .index_field_name = "v" };
  VecSimParams params{.algo = VecSimAlgo_HNSWLIB,
                      .algoParams = {.hnswParams = HNSWParams{.type = t,
                                               .dim = d,
                                               .metric = met,
                                               .initialCapacity = n,
                                               .M = 16,
                                               .efConstruction = 100}},
                      .logCtx = &logCtx};
  VecSimIndex *index = VecSimIndex_New(&params);
  for (size_t i = 1; i <= n; i++) {
    float f[d];
    f[0] = 1.0f;
    for (size_t j = 1; j < d; j++) {
      f[j] = (float)i / n;
    }
    VecSimIndex_AddVector(index, (const void *)f, (int)i);
  }
  ASSERT_EQ(VecSimIndex_IndexSize(index), n);

  float query[] = {(float)n, (float)n, (float)n, (float)n};
  RangeVectorQuery range_query = {.vector = query, .vecLen = d, .radius = 0.2, .order = BY_ID};
  VecSimQueryParams queryParams = {0};
  queryParams.hnswRuntimeParams.efRuntime = n;
  VecSimQueryReply *results =
      VecSimIndex_RangeQuery(index, range_query.vector, range_query.radius, &queryParams, range_query.order);

  // Run simple range query.
  IndexIterator *vecIt = createMetricIteratorFromVectorQueryResults(results, true);
  RSIndexResult *h = NULL;
  size_t count = 0;
  size_t lowest_id = 25;
  size_t n_expected_res = n - lowest_id + 1;

  // Expect to get top 76 results that are within the range, with ids: 25, 26, ... , 100
  VecSim_Normalize(query, d, t);
  while (vecIt->Read(vecIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_Metric);
    ASSERT_EQ(h->docId, lowest_id + count);
    double exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, h->docId, query);
    ASSERT_EQ(h->data.num.value, exp_dist);
    ASSERT_EQ(h->metrics[0].value->numval, exp_dist);
    count++;
  }
  ASSERT_EQ(count, n_expected_res);
  ASSERT_FALSE(vecIt->HasNext(vecIt->ctx));

  vecIt->Rewind(vecIt->ctx);
  ASSERT_TRUE(vecIt->HasNext(vecIt->ctx));
  ASSERT_EQ(vecIt->NumEstimated(vecIt->ctx), n_expected_res);
  ASSERT_EQ(vecIt->Len(vecIt->ctx), n_expected_res);

  // Read one result to verify that we get the minimum id after rewind.
  ASSERT_EQ(vecIt->Read(vecIt->ctx, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, lowest_id);

  // Test valid combinations of SkipTo
  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, lowest_id + 10, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, lowest_id + 10);
  double exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, h->docId, query);
  ASSERT_EQ(h->data.num.value, exp_dist);
  ASSERT_EQ(h->metrics[0].value->numval, exp_dist);
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), lowest_id + 10);

  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, n-1, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, n-1);
  exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, h->docId, query);
  ASSERT_EQ(h->data.num.value, exp_dist);
  ASSERT_EQ(h->metrics[0].value->numval, exp_dist);
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), n-1);

  // Invalid SkipTo
  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, n+1, &h), INDEXREAD_EOF);
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), n);
  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, n, &h), INDEXREAD_EOF);
  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, lowest_id + 10, &h), INDEXREAD_EOF);

  // Rewind and test skipping to the first id.
  vecIt->Rewind(vecIt->ctx);
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), 0);
  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, lowest_id, &h), INDEXREAD_OK);
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), lowest_id);

  // check rerun and abort (go over only half of the results)
  count = 1;
  for (size_t i = 0; i < n_expected_res/2; i++) {
    ASSERT_EQ(vecIt->Read(vecIt->ctx, &h), INDEXREAD_OK);
    ASSERT_EQ(h->type, RSResultType_Metric);
    ASSERT_EQ(h->docId, lowest_id + count);
    count++;
  }
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), lowest_id + count - 1);
  ASSERT_TRUE(vecIt->HasNext(vecIt->ctx));
  vecIt->Abort(vecIt->ctx);
  ASSERT_FALSE(vecIt->HasNext(vecIt->ctx));

  vecIt->Free(vecIt);
  VecSimIndex_Free(index);
}

TEST_F(IndexTest, testMetric_SkipTo) {
  size_t results_num = 7;

  t_docId *ids_arr = array_new(t_docId, results_num);
  t_docId ids[] = {2, 4, 6, 8, 10, 15, 20};
  array_ensure_append_n(ids_arr, ids , results_num);

  double *metrics_arr = array_new(double, results_num);
  double metrics[7] = {1.0};
  array_ensure_append_n(metrics_arr, metrics, results_num);

  IndexIterator *metric_it = NewMetricIterator(ids_arr, metrics_arr, VECTOR_DISTANCE, false);
  RSIndexResult *h = NULL;

  // Copy the behaviour of READ_ITERATOR in terms of SkipTo. That is, the iterator will return the
  // next docId whose id is equal or greater than the given id, as if we call Read and returned
  // that id (hence the iterator will advance its pointer).
  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 1, &h), INDEXREAD_NOTFOUND);
  ASSERT_EQ(h->docId, 2);

  // This situation should not occur in practice, but this is READ_ITERATOR's behavior.
  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 2, &h), INDEXREAD_NOTFOUND);
  ASSERT_EQ(h->docId, 4);

  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 8, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, 8);

  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 9, &h), INDEXREAD_NOTFOUND);
  ASSERT_EQ(h->docId, 10);

  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 12, &h), INDEXREAD_NOTFOUND);
  ASSERT_EQ(h->docId, 15);

  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 20, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, 20);

  ASSERT_EQ(metric_it->SkipTo(metric_it->ctx, 21, &h), INDEXREAD_EOF);
  ASSERT_EQ(h->docId, 20);

  metric_it->Free(metric_it);
}

TEST_F(IndexTest, testBuffer) {
  // TEST_START();
  Buffer b = {0};
  Buffer_Init(&b, 2);
  BufferWriter w = NewBufferWriter(&b);
  ASSERT_TRUE(w.buf->cap == 2) << "Wrong capacity";
  ASSERT_TRUE(w.buf->data != NULL);
  ASSERT_TRUE(Buffer_Offset(w.buf) == 0);
  ASSERT_TRUE(w.buf->data == BufferWriter_Current(&w));

  const char *x = "helololoolo";
  size_t l = Buffer_Write(&w, (void *)x, strlen(x) + 1);

  ASSERT_TRUE(l == strlen(x) + 1);
  ASSERT_TRUE(Buffer_Offset(w.buf) == l);
  ASSERT_EQ(Buffer_Capacity(w.buf), 14);

  l = WriteVarint(1337654, &w);
  ASSERT_TRUE(l == 3);
  ASSERT_EQ(Buffer_Offset(w.buf), 15);
  ASSERT_EQ(Buffer_Capacity(w.buf), 17);

  Buffer_Truncate(w.buf, 0);

  ASSERT_TRUE(Buffer_Capacity(w.buf) == 15);

  BufferReader br = NewBufferReader(w.buf);
  ASSERT_TRUE(br.pos == 0);

  char *y = (char *)malloc(strlen(x) + 1);
  l = Buffer_Read(&br, y, strlen(x) + 1);
  ASSERT_TRUE(l == strlen(x) + 1);

  ASSERT_TRUE(strcmp(y, x) == 0);
  ASSERT_TRUE(BufferReader_Offset(&br) == l);

  free(y);

  int n = ReadVarint(&br);
  ASSERT_TRUE(n == 1337654);

  Buffer_Free(w.buf);
}

TEST_F(IndexTest, testIndexSpec) {
  const char *title = "title", *body = "body", *foo = "foo", *bar = "bar", *name = "name";
  const char *args[] = {"STOPWORDS", "2",      "hello", "world",    "SCHEMA", title,
                        "text",      "weight", "0.1",   body,       "text",   "weight",
                        "2.0",       foo,      "text",  "sortable", bar,      "numeric",
                        "sortable",  name,     "text",  "nostem"};
  QueryError err = {QUERY_OK};
  const char* spec_name = "idx";
  StrongRef ref = IndexSpec_ParseC(spec_name, args, sizeof(args) / sizeof(const char *), &err);
  IndexSpec *s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(s->numFields == 5);
  ASSERT_TRUE(s->stopwords != NULL);
  ASSERT_TRUE(s->stopwords != DefaultStopWordList());
  ASSERT_TRUE(s->flags & Index_StoreFieldFlags);
  ASSERT_TRUE(s->flags & Index_StoreTermOffsets);
  ASSERT_TRUE(s->flags & Index_HasCustomStopwords);

  ASSERT_TRUE(StopWordList_Contains(s->stopwords, "hello", 5));
  ASSERT_TRUE(StopWordList_Contains(s->stopwords, "world", 5));
  ASSERT_TRUE(!StopWordList_Contains(s->stopwords, "werld", 5));

  const char *realName = IndexSpec_FormatName(s, false);
  ASSERT_STREQ(realName, spec_name);

  const char *obfuscatedName = IndexSpec_FormatName(s, true);
  ASSERT_STREQ(obfuscatedName, "Index@4e7f626df794f6491574a236f22c100c34ed804f");

  const FieldSpec *f = IndexSpec_GetFieldWithLength(s, body, strlen(body));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_STREQ(RediSearch_HiddenStringGet(f->fieldName), body);
  ASSERT_EQ(f->ftWeight, 2.0);
  ASSERT_EQ(FIELD_BIT(f), 2);
  ASSERT_EQ(f->options, 0);
  ASSERT_EQ(f->sortIdx, -1);

  f = IndexSpec_GetFieldWithLength(s, title, strlen(title));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_STREQ(RediSearch_HiddenStringGet(f->fieldName), title);
  ASSERT_TRUE(f->ftWeight == 0.1);
  ASSERT_TRUE(FIELD_BIT(f) == 1);
  ASSERT_TRUE(f->options == 0);
  ASSERT_TRUE(f->sortIdx == -1);

  f = IndexSpec_GetFieldWithLength(s, foo, strlen(foo));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_STREQ(RediSearch_HiddenStringGet(f->fieldName), foo);
  ASSERT_TRUE(f->ftWeight == 1);
  ASSERT_TRUE(FIELD_BIT(f) == 4);
  ASSERT_TRUE(f->options == FieldSpec_Sortable);
  ASSERT_TRUE(f->sortIdx == 0);

  f = IndexSpec_GetFieldWithLength(s, bar, strlen(bar));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_NUMERIC));

  ASSERT_STREQ(RediSearch_HiddenStringGet(f->fieldName), bar);
  ASSERT_EQ(f->options, FieldSpec_Sortable | FieldSpec_UNF); // UNF is set implicitly for sortable numerics
  ASSERT_TRUE(f->sortIdx == 1);
  ASSERT_TRUE(IndexSpec_GetFieldWithLength(s, "fooz", 4) == NULL);

  f = IndexSpec_GetFieldWithLength(s, name, strlen(name));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_STREQ(RediSearch_HiddenStringGet(f->fieldName), name);
  ASSERT_TRUE(f->ftWeight == 1);
  ASSERT_TRUE(FIELD_BIT(f) == 8);
  ASSERT_TRUE(f->options == FieldSpec_NoStemming);
  ASSERT_TRUE(f->sortIdx == -1);
  ASSERT_TRUE(s->numSortableFields == 2);

  IndexSpec_RemoveFromGlobals(ref, false);

  QueryError_ClearError(&err);
  const char *args2[] = {
      "NOOFFSETS", "NOFIELDS", "SCHEMA", title, "text",
  };
  ref = IndexSpec_ParseC("idx", args2, sizeof(args2) / sizeof(const char *), &err);
  s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(s->numFields == 1);

  ASSERT_TRUE(!(s->flags & Index_StoreFieldFlags));
  ASSERT_TRUE(!(s->flags & Index_StoreTermOffsets));
  IndexSpec_RemoveFromGlobals(ref, false);

  // User-reported bug
  const char *args3[] = {"SCHEMA", "ha", "NUMERIC", "hb", "TEXT", "WEIGHT", "1", "NOSTEM"};
  QueryError_ClearError(&err);
  ref = IndexSpec_ParseC("idx", args3, sizeof(args3) / sizeof(args3[0]), &err);
  s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(FieldSpec_IsNoStem(s->fields + 1));
  IndexSpec_RemoveFromGlobals(ref, false);
}

static void fillSchema(std::vector<char *> &args, size_t nfields) {
  args.resize(1 + nfields * 3);
  args[0] = strdup("SCHEMA");
  size_t n = 1;
  for (unsigned i = 0; i < nfields; i++) {
    __ignore__(asprintf(&args[n++], "field%u", i));
    if (i % 2 == 0) {
      args[n++] = strdup("TEXT");
    } else {
      if (i < 40) {
        // odd fields under 40 are TEXT noINDEX
        args[n++] = strdup("TEXT");
        args[n++] = strdup("NOINDEX");
      } else {
        // the rest are numeric
        args[n++] = strdup("NUMERIC");
      }
    }
  }
  args.resize(n);

  // for (int i = 0; i < n; i++) {
  //   printf("%s ", args[i]);
  // }
  // printf("\n");
}

static void freeSchemaArgs(std::vector<char *> &args) {
  for (auto s : args) {
    free(s);
  }
  args.clear();
}

TEST_F(IndexTest, testHugeSpec) {
  int N = 64;
  std::vector<char *> args;
  fillSchema(args, N);

  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_ParseC("idx", (const char **)&args[0], args.size(), &err);
  IndexSpec *s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(s->numFields == N);
  IndexSpec_RemoveFromGlobals(ref, false);
  freeSchemaArgs(args);

  // test too big a schema
  N = 300;
  fillSchema(args, N);

  QueryError_ClearError(&err);
  ref = IndexSpec_ParseC("idx", (const char **)&args[0], args.size(), &err);
  s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_TRUE(s == NULL);
  ASSERT_TRUE(QueryError_HasError(&err));
#if (defined(__x86_64__) || defined(__aarch64__) || defined(__arm64__)) && !defined(RS_NO_U128)
  ASSERT_STREQ("Schema is limited to 128 TEXT fields", QueryError_GetUserError(&err));
#else
  ASSERT_STREQ("Schema is limited to 64 TEXT fields", QueryError_GetUserError(&err));
#endif
  freeSchemaArgs(args);
  QueryError_ClearError(&err);
}

TEST_F(IndexTest, testIndexFlags) {

  ForwardIndexEntry h;
  h.docId = 1234;
  h.fieldMask = 0x01;
  h.freq = 1;
  h.vw = NewVarintVectorWriter(8);
  for (int n = 0; n < 10; n++) {
    VVW_Write(h.vw, n);
  }
  VVW_Truncate(h.vw);

  uint32_t flags = INDEX_DEFAULT_FLAGS;
  size_t index_memsize;
  InvertedIndex *w = NewInvertedIndex(IndexFlags(flags), 1, &index_memsize);
  // The memory occupied by a empty inverted index
  // created with INDEX_DEFAULT_FLAGS is 102 bytes,
  // which is the sum of the following (See NewInvertedIndex()):
  // sizeof_InvertedIndex(index->flags)   48
  // sizeof(IndexBlock)                   48
  // INDEX_BLOCK_INITIAL_CAP               6
  ASSERT_EQ(102, index_memsize);
  IndexEncoder enc = InvertedIndex_GetEncoder(w->flags);
  ASSERT_TRUE(w->flags == flags);
  size_t sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(10, sz);
  InvertedIndex_Free(w);

  flags &= ~Index_StoreTermOffsets;
  w = NewInvertedIndex(IndexFlags(flags), 1, &index_memsize);
  ASSERT_EQ(102, index_memsize);
  ASSERT_TRUE(!(w->flags & Index_StoreTermOffsets));
  enc = InvertedIndex_GetEncoder(w->flags);
  size_t sz2 = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(sz2, 0);
  InvertedIndex_Free(w);

  flags = INDEX_DEFAULT_FLAGS | Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), 1, &index_memsize);
  ASSERT_EQ(102, index_memsize);
  ASSERT_TRUE((w->flags & Index_WideSchema));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  ASSERT_EQ(19, InvertedIndex_WriteForwardIndexEntry(w, enc, &h));
  InvertedIndex_Free(w);

  flags |= Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), 1, &index_memsize);
  ASSERT_EQ(102, index_memsize);
  ASSERT_TRUE((w->flags & Index_WideSchema));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(19, sz);
  InvertedIndex_Free(w);

  flags &= Index_StoreFreqs;
  w = NewInvertedIndex(IndexFlags(flags), 1, &index_memsize);
  // The memory occupied by a empty inverted index with
  // Index_StoreFieldFlags == 0 is 86 bytes
  // which is the sum of the following (See NewInvertedIndex()):
  // sizeof_InvertedIndex(index->flags)   32
  // sizeof(IndexBlock)                   48
  // INDEX_BLOCK_INITIAL_CAP               6
  ASSERT_EQ(86, index_memsize);
  ASSERT_TRUE(!(w->flags & Index_StoreTermOffsets));
  ASSERT_TRUE(!(w->flags & Index_StoreFieldFlags));
  enc = InvertedIndex_GetEncoder(w->flags);
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(0, sz);
  InvertedIndex_Free(w);

  flags |= Index_StoreFieldFlags | Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), 1, &index_memsize);
  ASSERT_EQ(102, index_memsize);
  ASSERT_TRUE((w->flags & Index_WideSchema));
  ASSERT_TRUE((w->flags & Index_StoreFieldFlags));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(4, sz);
  InvertedIndex_Free(w);

  VVW_Free(h.vw);
}

TEST_F(IndexTest, testDocTable) {
  char buf[16];
  DocTable dt = NewDocTable(10, 10);
  size_t doc_table_size = sizeof(DocTable) + (10 * sizeof(DMDChain));
  ASSERT_EQ(doc_table_size, (int)dt.memsize);
  t_docId did = 0;
  // N is set to 100 and the max cap of the doc table is 10 so we surely will
  // get overflow and check that everything works correctly
  int N = 100;
  for (int i = 0; i < N; i++) {
    size_t nkey = snprintf(buf, sizeof(buf), "doc_%d", i);
    RSDocumentMetadata *dmd = DocTable_Put(&dt, buf, nkey, (double)i, Document_DefaultFlags, buf, strlen(buf), DocumentType_Hash);
    t_docId nd = dmd->id;
    DMD_Return(dmd);
    ASSERT_EQ(did + 1, nd);
    did = nd;
  }

  ASSERT_EQ(N + 1, dt.size);
  ASSERT_EQ(N, dt.maxDocId);
#ifdef __x86_64__
  ASSERT_EQ(10180 + doc_table_size, (int)dt.memsize);
#endif
  for (int i = 0; i < N; i++) {
    snprintf(buf, sizeof(buf), "doc_%d", i);
    const sds key = DocTable_GetKey(&dt, i + 1, NULL);
    ASSERT_STREQ(key, buf);
    sdsfree(key);

    const RSDocumentMetadata *dmd = DocTable_Borrow(&dt, i + 1);
    ASSERT_TRUE(dmd != NULL);
    ASSERT_TRUE(dmd->flags & Document_HasPayload);
    ASSERT_STREQ(dmd->keyPtr, buf);
    char *pl = dmd->payload->data;
    ASSERT_TRUE(!(strncmp(pl, (char *)buf, dmd->payload->len)));

    ASSERT_EQ((int)dmd->score, i);
    ASSERT_EQ((int)dmd->flags, (int)(Document_DefaultFlags | Document_HasPayload));

    t_docId xid = DocIdMap_Get(&dt.dim, buf, strlen(buf));

    ASSERT_EQ((int)xid, i + 1);

    int rc = DocTable_Delete(&dt, dmd->keyPtr, sdslen(dmd->keyPtr));
    ASSERT_EQ(1, rc);
    ASSERT_TRUE((int)(dmd->flags & Document_Deleted));
    DMD_Return(dmd);
    dmd = DocTable_Borrow(&dt, i + 1);
    ASSERT_TRUE(!dmd);
  }

  ASSERT_FALSE(DocIdMap_Get(&dt.dim, "foo bar", strlen("foo bar")));
  ASSERT_FALSE(DocTable_Borrow(&dt, N + 2));

  RSDocumentMetadata *dmd = DocTable_Put(&dt, "Hello", 5, 1.0, Document_DefaultFlags, NULL, 0, DocumentType_Hash);
  t_docId strDocId = dmd->id;
  ASSERT_TRUE(0 != strDocId);
  ASSERT_EQ(71 + doc_table_size, (int)dt.memsize);

  // Test that binary keys also work here
  static const char binBuf[] = {"Hello\x00World"};
  const size_t binBufLen = 11;
  ASSERT_FALSE(DocIdMap_Get(&dt.dim, binBuf, binBufLen));
  DMD_Return(dmd);
  dmd = DocTable_Put(&dt, binBuf, binBufLen, 1.0, Document_DefaultFlags, NULL, 0, DocumentType_Hash);
  ASSERT_TRUE(dmd);
  ASSERT_EQ(148 + doc_table_size, (int)dt.memsize);
  ASSERT_NE(dmd->id, strDocId);
  ASSERT_EQ(dmd->id, DocIdMap_Get(&dt.dim, binBuf, binBufLen));
  ASSERT_EQ(strDocId, DocIdMap_Get(&dt.dim, "Hello", 5));
  DMD_Return(dmd);
  DocTable_Free(&dt);
}

TEST_F(IndexTest, testVarintFieldMask) {
  t_fieldMask x = 127;
  size_t expected[] = {0, 2, 1, 1, 2, 0, 2, 0, 2, 3, 0, 0, 3, 0, 0, 4};
  Buffer b = {0};
  Buffer_Init(&b, 1);
  BufferWriter bw = NewBufferWriter(&b);
  for (int i = 0; i < sizeof(t_fieldMask); i++, x |= x << 8) {
    size_t sz = WriteVarintFieldMask(x, &bw);
    ASSERT_EQ(expected[i], sz);
    BufferReader br = NewBufferReader(bw.buf);

    t_fieldMask y = ReadVarintFieldMask(&br);

    ASSERT_EQ(y, x);
    BufferWriter_Seek(&bw, 0);
  }
  Buffer_Free(&b);
}

TEST_F(IndexTest, testDeltaSplits) {
  size_t index_memsize = 0;
  InvertedIndex *idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1, &index_memsize);
  ForwardIndexEntry ent = {0};
  ent.docId = 1;
  ent.fieldMask = RS_FIELDMASK_ALL;

  IndexEncoder enc = InvertedIndex_GetEncoder(idx->flags);
  InvertedIndex_WriteForwardIndexEntry(idx, enc, &ent);
  ASSERT_EQ(idx->size, 1);

  ent.docId = 200;
  InvertedIndex_WriteForwardIndexEntry(idx, enc, &ent);
  ASSERT_EQ(idx->size, 1);

  ent.docId = 1LLU << 48;
  InvertedIndex_WriteForwardIndexEntry(idx, enc, &ent);
  ASSERT_EQ(idx->size, 2);
  ent.docId++;
  InvertedIndex_WriteForwardIndexEntry(idx, enc, &ent);
  ASSERT_EQ(idx->size, 2);

  IndexReader *ir = NewTermIndexReader(idx);
  RSIndexResult *h = NULL;
  ASSERT_EQ(INDEXREAD_OK, IR_Read(ir, &h));
  ASSERT_EQ(1, h->docId);

  ASSERT_EQ(INDEXREAD_OK, IR_Read(ir, &h));
  ASSERT_EQ(200, h->docId);

  ASSERT_EQ(INDEXREAD_OK, IR_Read(ir, &h));
  ASSERT_EQ((1LLU << 48), h->docId);

  ASSERT_EQ(INDEXREAD_OK, IR_Read(ir, &h));
  ASSERT_EQ((1LLU << 48) + 1, h->docId);

  ASSERT_EQ(INDEXREAD_EOF, IR_Read(ir, &h));

  IR_Free(ir);
  InvertedIndex_Free(idx);
}

TEST_F(IndexTest, testRawDocId) {
  const int previousConfig = RSGlobalConfig.invertedIndexRawDocidEncoding;
  RSGlobalConfig.invertedIndexRawDocidEncoding = true;
  size_t index_memsize = 0;
  InvertedIndex *idx = NewInvertedIndex(Index_DocIdsOnly, 1, &index_memsize);
  IndexEncoder enc = InvertedIndex_GetEncoder(idx->flags);

  // Add a few entries, all with an odd docId
  for (t_docId id = 1; id < INDEX_BLOCK_SIZE; id += 2) {
    InvertedIndex_WriteEntryGeneric(idx, enc, id, NULL);
  }

  // Test that we can read them back
  IndexReader *ir = NewTermIndexReader(idx);
  RSIndexResult *cur;
  for (t_docId id = 1; id < INDEX_BLOCK_SIZE; id += 2) {
    ASSERT_EQ(INDEXREAD_OK, IR_Read(ir, &cur));
    ASSERT_EQ(id, cur->docId);
  }
  ASSERT_EQ(INDEXREAD_EOF, IR_Read(ir, &cur));

  // Test that we can skip to all the ids
  for (t_docId id = 1; id < INDEX_BLOCK_SIZE; id++) {
    IR_Rewind(ir);
    int rc = IR_SkipTo(ir, id, &cur);
    if (id % 2 == 0) {
      ASSERT_EQ(INDEXREAD_NOTFOUND, rc);
      ASSERT_EQ(id + 1, ir->lastId);
      ASSERT_EQ(id + 1, cur->docId) << "Expected to skip to " << id + 1 << " but got " << cur->docId;
    } else {
      ASSERT_EQ(INDEXREAD_OK, rc);
      ASSERT_EQ(id, ir->lastId);
      ASSERT_EQ(id, cur->docId);
    }
  }

  // Clean up
  IR_Free(ir);
  InvertedIndex_Free(idx);
  RSGlobalConfig.invertedIndexRawDocidEncoding = previousConfig;
}
