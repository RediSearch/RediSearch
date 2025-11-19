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
#include "src/forward_index.h"
#include "src/index_result.h"
#include "src/query_parser/tokenizer.h"
#include "src/spec.h"
#include "src/tokenize.h"
#include "varint.h"
#include "src/iterators/inverted_index_iterator.h"
#include "src/iterators/hybrid_reader.h"
#include "src/iterators/idlist_iterator.h"
#include "src/iterators/union_iterator.h"
#include "src/iterators/intersection_iterator.h"
#include "src/iterators/not_iterator.h"
#include "src/iterators/empty_iterator.h"
#include "src/iterators/wildcard_iterator.h"
#include "src/util/arr.h"
#include "src/util/references.h"
#include "types_rs.h"

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
  char *data = (char *) VVW_GetByteData(vvw);
  uint32_t len = VVW_GetByteLength(vvw);
  RSOffsetVector_SetData(&ret, data, len);
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
  *IndexResult_TermOffsetsRefMut(tr1) = offsetsFromVVW(vw);

  RSIndexResult *tr2 = NewTokenRecord(NULL, 1);
  tr2->docId = 1;
  *IndexResult_TermOffsetsRefMut(tr2) = offsetsFromVVW(vw2);

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
  *IndexResult_TermOffsetsRefMut(tr3) = offsetsFromVVW(vw3);
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
  InvertedIndex *idx = NewInvertedIndex(indexFlags, &index_memsize);
  int useFieldMask = indexFlags & Index_StoreFieldFlags;

  size_t t_fiedlMask_memsize = sizeof(t_fieldMask);
  size_t exp_t_fieldMask_memsize = 16;
  ASSERT_EQ(exp_t_fieldMask_memsize, t_fiedlMask_memsize);

  // Details of the memory occupied by InvertedIndex in bytes (64-bit system):
  // Vec<IndexBlock> blocks    24
  // u32 n_uniqe_blocks         4
  // flags IndexFlags           4
  // u32 gc_marker              4
  // ----------------------------
  // Total                     36
  // After padding             40

  size_t exp_idx_no_block_memsize = 40;

  if (useFieldMask) {
    exp_idx_no_block_memsize += t_fiedlMask_memsize;
  }

  size_t expectedIndexSize = exp_idx_no_block_memsize;
  // The memory occupied by a new inverted index depends of its flags
  // see NewInvertedIndex() for details
  ASSERT_EQ(expectedIndexSize, index_memsize);

  for (size_t i = 0; i < 200; i++) {

    ForwardIndexEntry h;
    h.docId = i + 1; // docId starts from 1
    h.fieldMask = 1;
    h.freq = (1 + i % 100) / (float)101;

    h.vw = NewVarintVectorWriter(8);
    for (int n = 0; n < i % 4; n++) {
      VVW_Write(h.vw, n);
    }
    VVW_Truncate(h.vw);

    InvertedIndex_WriteForwardIndexEntry(idx, &h);

    // printf("doc %d, score %f offset %zd\n", h.docId, h.docScore, w->bw.buf->offset);
    VVW_Free(h.vw);
  }

  ASSERT_EQ(200, InvertedIndex_NumDocs(idx));
  if ((indexFlags & INDEX_STORAGE_MASK) != Index_DocIdsOnly) {
    ASSERT_EQ(2, InvertedIndex_NumBlocks(idx));
  } else {
    ASSERT_EQ(1, InvertedIndex_NumBlocks(idx));
  }
  ASSERT_EQ(200, InvertedIndex_LastId(idx));

  for (int xx = 0; xx < 1; xx++) {
    IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL};
    IndexReader *reader = NewIndexReader(idx, decoderCtx);
    RSIndexResult *res = NewTokenRecord(NULL, 1);
    res->freq = 1;
    res->fieldMask = RS_FIELDMASK_ALL;

    int n = 1;
    IteratorStatus rc;
    while (IndexReader_Next(reader, res)) {
      ASSERT_EQ(res->docId, n);
      n++;
    }
    IndexReader_Free(reader);
    IndexResult_Free(res);
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

TEST_F(IndexTest, testUnion) {
  int oldConfig = RSGlobalConfig.iteratorsConfigParams.minUnionIterHeap;
  for (int cfg = 0; cfg < 2; ++cfg) {
    InvertedIndex *w = createPopulateTermsInvIndex(10, 2);
    InvertedIndex *w2 = createPopulateTermsInvIndex(10, 3);

    // printf("Reading!\n");
    QueryIterator **irs = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
    FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
    irs[0] = NewInvIndIterator_TermQuery(w, nullptr, f, nullptr, 1);
    irs[1] = NewInvIndIterator_TermQuery(w2, nullptr, f, nullptr, 1);
    IteratorsConfig config{};
    iteratorsConfig_init(&config);
    QueryIterator *ui = NewUnionIterator(irs, 2, 0, 1, QN_UNION, NULL, &config);
    int expected[] = {2, 3, 4, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 24, 27, 30};
    int i = 0;
    while (ui->Read(ui) != ITERATOR_EOF) {
      // printf("%d <=> %d\n", h.docId, expected[i]);
      ASSERT_EQ(expected[i], ui->lastDocId);
      i++;

      RSIndexResult *copy = IndexResult_DeepCopy(ui->current);
      ASSERT_TRUE(copy != NULL);
      ASSERT_TRUE(copy != ui->current);
      ASSERT_EQ(copy->data.term.tag, RSTermRecord_Owned);

      ASSERT_EQ(copy->docId, ui->current->docId);
      ASSERT_EQ(copy->data.tag, ui->current->data.tag);

      IndexResult_Free(copy);

      // printf("%d, ", h.docId);
    }


    // test read after skip goes to next id
    ui->Rewind(ui);
    ASSERT_EQ(ui->SkipTo(ui, 6), ITERATOR_OK);
    ASSERT_EQ(ui->lastDocId, 6);
    ASSERT_EQ(ui->Read(ui), ITERATOR_OK);
    ASSERT_EQ(ui->lastDocId, 8);
    // test for last id
    ASSERT_EQ(ui->SkipTo(ui, 30), ITERATOR_OK);
    ASSERT_EQ(ui->lastDocId, 30);
    ASSERT_EQ(ui->Read(ui), ITERATOR_EOF);

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
  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX};
  QueryIterator **irs = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
  irs[0] = NewInvIndIterator_TermQuery(w, nullptr, fieldMaskOrIndex, nullptr, 0.5);
  FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
  irs[1] = NewInvIndIterator_TermQuery(w2, nullptr, f, nullptr, 1);
  IteratorsConfig config{};
  iteratorsConfig_init(&config);
  QueryIterator *ui = NewUnionIterator(irs, 2, 0, 0.8, QN_UNION, NULL, &config);
  int expected[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20};
  int i = 0;
  while (ui->Read(ui) != ITERATOR_EOF) {
    RSIndexResult *h = ui->current;
    // printf("%d <=> %d\n", h.docId, expected[i]);
    ASSERT_EQ(h->docId, expected[i++]);
    ASSERT_EQ(h->weight, 0.8);
    const RSAggregateResult *agg = IndexResult_AggregateRef(h);
    if (AggregateResult_NumChildren(agg) == 2) {
      ASSERT_EQ(AggregateResult_Get(agg, 0)->weight, 0.5);
      ASSERT_EQ(AggregateResult_Get(agg, 1)->weight, 1);
    } else {
      if (i <= 10) {
        ASSERT_EQ(AggregateResult_Get(agg, 0)->weight, 0.5);
      } else {
        ASSERT_EQ(AggregateResult_Get(agg, 0)->weight, 1);
      }
    }
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testNot) {
  auto ctx = std::make_unique<MockQueryEvalCtx>();
  InvertedIndex *w = createPopulateTermsInvIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createPopulateTermsInvIndex(10, 3);
  QueryIterator **irs = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
  FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
  irs[0] = NewInvIndIterator_TermQuery(w, nullptr, f, nullptr, 1);
  irs[1] = NewNotIterator(NewInvIndIterator_TermQuery(w2, nullptr, f, nullptr, 1), InvertedIndex_LastId(w2), 1, {0}, &ctx->qctx);

  QueryIterator *ui = NewIntersectionIterator(irs, 2, -1, 0, 1);
  int expected[] = {1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16};
  int i = 0;
  while (ui->Read(ui) != ITERATOR_EOF) {
    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT_EQ(expected[i++], ui->lastDocId);
    // printf("%d, ", h.docId);
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testPureNot) {
  InvertedIndex *w = createPopulateTermsInvIndex(10, 3);
  auto ctx = std::make_unique<MockQueryEvalCtx>();
  FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
  QueryIterator *ir = NewNotIterator(NewInvIndIterator_TermQuery(w, nullptr, f, nullptr, 1), InvertedIndex_LastId(w) + 5, 1, {0}, &ctx->qctx);

  RSIndexResult *h = NULL;
  int expected[] = {1,  2,  4,  5,  7,  8,  10, 11, 13, 14, 16, 17, 19,
                    20, 22, 23, 25, 26, 28, 29, 31, 32, 33, 34, 35};
  int i = 0;
  while (ir->Read(ir) != ITERATOR_EOF) {

    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT_EQ(expected[i++], ir->lastDocId);
  }
  ir->Free(ir);
  InvertedIndex_Free(w);
}

TEST_F(IndexTest, testNumericInverted) {
  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, &index_memsize);

  size_t sz = 0;
  size_t expected_sz = 0;
  size_t written_bytes = 0;
  size_t bytes_per_entry = 0;
  size_t buff_cap = 0; // Initial block capacity

  for (int i = 0; i < 75; i++) {
    // The buffer has an initial capacity of 0 bytes
    // For values < 7 (tiny numbers) the header (H) and value (V) will occupy
    // only 1 byte.
    // For values >= 7, the header will occupy 1 byte, and the value 1 bytes.
    //
    // The delta will occupy 1 byte.
    // The first entry has zero delta, so it will not be written.
    //
    // The buffer will grow when there is not enough space to write the entry
    //
    // The number of bytes added to the capacity is defined by the formula:
    // MIN(1 + buf.cap / 5, 1024 * 1024)  (see controlled_cursor.rs reserve_and_pad())
    //
    //   | H + V | Delta | Bytes     | Written  | Buff cap | Available | sz
    // i | bytes | bytes | per Entry | bytes    |          | size      |
    // ----------------------------------------------------------------------
    // 0 | 1     | 0     | 1         |  1       |  1       | 0         | 1
    // 1 | 1     | 1     | 2         |  3       |  3       | 0         | 2
    // 2 | 1     | 1     | 2         |  5       |  5       | 0         | 2
    // 3 | 1     | 1     | 2         |  7       |  7       | 0         | 2
    // 4 | 1     | 1     | 2         |  9       |  9       | 0         | 2
    // 5 | 1     | 1     | 2         | 11       | 11       | 0         | 2
    // 6 | 1     | 1     | 2         | 13       | 14       | 1         | 3
    // 7 | 2     | 1     | 3         | 16       | 17       | 1         | 3
    // 8 | 2     | 1     | 3         | 19       | 21       | 2         | 4
    // 9 | 2     | 1     | 3         | 22       | 26       | 4         | 5

    if (i < 1) {
      bytes_per_entry = 1;
    } else if (i < 7) {
      bytes_per_entry = 2;
    } else {
      bytes_per_entry = 3;
    }

    // Simulate the buffer growth to get the expected size
    written_bytes += bytes_per_entry;
    size_t target_cap = buff_cap;
    while (target_cap < written_bytes) {
      target_cap += MIN(1 + target_cap / 5, 1024 * 1024);
    }

    expected_sz = target_cap - buff_cap;
    buff_cap = target_cap;

    // The first write will make an index block of 48 bytes
    if (i < 1) {
      expected_sz += 48;
    }

    // Check if the write matches the simulation
    sz = InvertedIndex_WriteNumericEntry(idx, i + 1, (double)(i + 1));
    ASSERT_EQ(sz, expected_sz) << " at i=" << i;
  }
  ASSERT_EQ(75, InvertedIndex_LastId(idx));

  // printf("written %zd bytes\n", IndexBlock_DataLen(&idx->blocks[0]));

  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX};
  FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
  QueryIterator *it = NewInvIndIterator_NumericQuery(idx, nullptr, &fieldCtx, nullptr, nullptr, -INFINITY, INFINITY);
  t_docId i = 1;
  while (ITERATOR_EOF != it->Read(it)) {
    RSIndexResult *res = it->current;
    // printf("%d %f\n", res->docId, res->num.value);

    ASSERT_EQ(i++, res->docId);
    ASSERT_EQ(IndexResult_NumValue(res), (float)res->docId);
  }
  InvertedIndex_Free(idx);
  it->Free(it);
}

TEST_F(IndexTest, testNumericVaried) {
  // For various numeric values, of different types (NUM_ENCODING_COMMON_TYPE_TINY,
  // NUM_ENCODING_COMMON_TYPE_FLOAT, etc..) check that the number of allocated
  // bytes in buffers is as expected.

  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, &index_memsize);

  static const double nums[] = {0,          0.13,          0.001,     -0.1,     1.0,
                                5.0,        4.323,         65535,     65535.53, 32768.432,
                                1LLU << 32, -(1LLU << 32), 1LLU << 40};
  static const size_t numCount = sizeof(nums) / sizeof(double);

  for (size_t i = 0; i < numCount; i++) {
    size_t sz = InvertedIndex_WriteNumericEntry(idx, i + 1, nums[i]);
    // printf("[%lu]: Stored %lf\n", i, nums[i]);
  }

  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX};
  FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
  QueryIterator *it = NewInvIndIterator_NumericQuery(idx, nullptr, &fieldCtx, nullptr, nullptr, -INFINITY, INFINITY);

  for (size_t i = 0; i < numCount; i++) {
    // printf("Checking i=%lu. Expected=%lf\n", i, nums[i]);
    IteratorStatus rv = it->Read(it);
    ASSERT_NE(ITERATOR_EOF, rv);
    ASSERT_LT(fabs(nums[i] - IndexResult_NumValue(it->current)), 0.01);
  }

  ASSERT_EQ(ITERATOR_EOF, it->Read(it));
  InvertedIndex_Free(idx);
  it->Free(it);
}

typedef struct {
  double value;
} encodingInfo;
static const encodingInfo infos[] = {
    {0},                    // 0
    {1},                    // 1
    {63},                   // 2
    {-1},                   // 3
    {-63},                  // 4
    {64},                   // 5
    {-64},                  // 6
    {255},                  // 7
    {-255},                 // 8
    {65535},                // 9
    {-65535},               // 10
    {16777215},             // 11
    {-16777215},            // 12
    {4294967295},           // 13
    {-4294967295},          // 14
    {4294967295 + 1},       // 15
    {4294967295 + 2},       // 16
    {549755813888.0},       // 17
    {549755813888.0 + 2},   // 18
    {549755813888.0 - 23},  // 19
    {-549755813888.0},      // 20
    {1503342028.957225},   // 21
    {42.4345},              // 22
    {(float)0.5},           // 23
    {DBL_MAX},             // 24
    {UINT64_MAX >> 12},     // 25
    {INFINITY},             // 26
    {-INFINITY}             // 27
};

void testNumericEncodingHelper(bool isMulti) {
  static const size_t numInfos = sizeof(infos) / sizeof(infos[0]);
  size_t index_memsize;
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, &index_memsize);

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\n[%lu]: Expecting Val=%lf, Sz=%lu\n", ii, infos[ii].value, infos[ii].size);
    size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);

    if (isMulti) {
      size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);
    }
  }

  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX};
  FieldFilterContext fieldCtx = {.field = fieldMaskOrIndex, .predicate = FIELD_EXPIRATION_DEFAULT};
  QueryIterator *it = NewInvIndIterator_NumericQuery(idx, nullptr, &fieldCtx, nullptr, nullptr, -INFINITY, INFINITY);

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\nReading [%lu]\n", ii);

    IteratorStatus rc = it->Read(it);
    ASSERT_NE(rc, ITERATOR_EOF);
    // printf("%lf <-> %lf\n", infos[ii].value, res->num.value);
    if (fabs(infos[ii].value) == INFINITY) {
      ASSERT_EQ(infos[ii].value, IndexResult_NumValue(it->current));
    } else {
      ASSERT_LT(fabs(infos[ii].value - IndexResult_NumValue(it->current)), 0.01);
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

TEST_F(IndexTest, testIntersection) {

  InvertedIndex *w = createPopulateTermsInvIndex(100000, 4);
  InvertedIndex *w2 = createPopulateTermsInvIndex(100000, 2);

  QueryIterator **irs = (QueryIterator **)rm_calloc(2, sizeof(QueryIterator *));
  FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
  irs[0] = NewInvIndIterator_TermQuery(w, nullptr, f, nullptr, 1);
  irs[1] = NewInvIndIterator_TermQuery(w2, nullptr, f, nullptr, 1);

  int count = 0;
  QueryIterator *ii = NewIntersectionIterator(irs, 2, -1, 0, 1);

  uint32_t topFreq = 0;
  while (ii->Read(ii) != ITERATOR_EOF) {
    RSIndexResult *h = ii->current;
    ASSERT_EQ(h->data.tag, RSResultData_Intersection);
    ASSERT_TRUE(IndexResult_IsAggregate(h));
    ASSERT_TRUE(RSIndexResult_HasOffsets(h));
    topFreq = std::max(topFreq, h->freq);

    RSIndexResult *copy = IndexResult_DeepCopy(h);
    ASSERT_TRUE(copy != NULL);
    ASSERT_TRUE(copy != h);
    ASSERT_EQ(copy->data.term.tag, RSTermRecord_Owned);

    ASSERT_TRUE(copy->docId == h->docId);
    ASSERT_TRUE(copy->data.tag == RSResultData_Intersection);
    ASSERT_EQ((count * 2 + 2) * 2, h->docId);
    ASSERT_EQ(2, h->freq);
    IndexResult_Free(copy);
    ++count;
  }

  // int count = IR_Intersect(r1, r2, onIntersect, &ctx);

  // printf("%d intersections in %lldms, %.0fns per iteration\n", count,
  // TimeSampler_DurationMS(&ts),
  // 1000000 * TimeSampler_IterationMS(&ts));
  // printf("top freq: %f\n", topFreq);
  ASSERT_EQ(count, 50000);
  ASSERT_EQ(topFreq, 2);

  // test read after skip goes to next id
  ii->Rewind(ii);
  ASSERT_EQ(ii->SkipTo(ii, 8), ITERATOR_OK);
  ASSERT_EQ(ii->lastDocId, 8);
  ASSERT_EQ(ii->Read(ii), ITERATOR_OK);
  ASSERT_EQ(ii->lastDocId, 12);
  // test for last id
  ASSERT_EQ(ii->SkipTo(ii, 200000), ITERATOR_OK);
  ASSERT_EQ(ii->lastDocId, 200000);
  ASSERT_EQ(ii->Read(ii), ITERATOR_EOF);

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
  FieldMaskOrIndex fieldMaskOrIndex = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX};
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
  QueryError err = QueryError_Default();
  QueryIterator *vecIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

  size_t count = 0;

  // Expect to get top 10 results in reverse order of the distance that passes the filter: 364, 368, ..., 400.
  while (vecIt->Read(vecIt) != ITERATOR_EOF) {
    ASSERT_EQ(vecIt->current->data.tag, RSResultData_Metric);
    ASSERT_EQ(vecIt->current->docId, max_id - count);
    count++;
  }
  ASSERT_EQ(count, k);
  ASSERT_TRUE(vecIt->atEOF);

  vecIt->Rewind(vecIt);
  ASSERT_FALSE(vecIt->atEOF);
  ASSERT_EQ(vecIt->NumEstimated(vecIt), k);
  // Read one result to verify that we get the one with best score after rewind.
  ASSERT_EQ(vecIt->Read(vecIt), ITERATOR_OK);
  ASSERT_EQ(vecIt->current->docId, max_id);
  vecIt->Free(vecIt);

  // Test in hybrid mode.
  FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
  QueryIterator *ir = NewInvIndIterator_TermQuery(w, nullptr, f, nullptr, 1);
  hParams.childIt = ir;
  QueryIterator *hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

  HybridIterator *hr = (HybridIterator *)hybridIt;
  hr->searchMode = VECSIM_HYBRID_BATCHES;

  // Expect to get top 10 results in the right order of the distance that passes the filter: 400, 396, ..., 364.
  count = 0;
  while (hybridIt->Read(hybridIt) != ITERATOR_EOF) {
    ASSERT_EQ(hybridIt->current->data.tag, RSResultData_Metric);
    // since larger ids has lower distance, in every we get lower id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(hybridIt->lastDocId, expected_id);
  }
  ASSERT_EQ(count, k);
  ASSERT_TRUE(hybridIt->atEOF);

  hybridIt->Rewind(hybridIt);
  ASSERT_FALSE(hybridIt->atEOF);
  ASSERT_EQ(hybridIt->NumEstimated(hybridIt), k);

  // check rerun and abort (go over only half of the results)
  count = 0;
  for (size_t i = 0; i < k/2; i++) {
    ASSERT_EQ(hybridIt->Read(hybridIt), ITERATOR_OK);
    ASSERT_EQ(hybridIt->current->data.tag, RSResultData_Metric);
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(hybridIt->lastDocId, expected_id);
  }
  ASSERT_EQ(hybridIt->lastDocId, max_id - step*((k/2)-1));
  ASSERT_EQ(hybridIt->Revalidate(hybridIt), VALIDATE_OK);

  // Rerun in AD_HOC BF mode.
  hybridIt->Rewind(hybridIt);
  hr->searchMode = VECSIM_HYBRID_ADHOC_BF;
  count = 0;
  while (hybridIt->Read(hybridIt) != ITERATOR_EOF) {
    ASSERT_EQ(hybridIt->current->data.tag, RSResultData_Metric);
    // since larger ids has lower distance, in every we get higher id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(hybridIt->lastDocId, expected_id);
  }
  hybridIt->Free(hybridIt);

  // Rerun without ignoring document scores.
  ir = NewInvIndIterator_TermQuery(w, nullptr, f, nullptr, 1);
  hParams.canTrimDeepResults = false;
  hParams.childIt = ir;
  hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
  hr = (HybridIterator *)hybridIt;
  hr->searchMode = VECSIM_HYBRID_BATCHES;

  // This time, result is a tree with 2 children: vector score and subtree of terms (for scoring).
  count = 0;
  while (hybridIt->Read(hybridIt) != ITERATOR_EOF) {
    RSIndexResult *h = hybridIt->current;
    ASSERT_EQ(h->data.tag, RSResultData_HybridMetric);
    ASSERT_TRUE(IndexResult_IsAggregate(h));
    const RSAggregateResult *agg = IndexResult_AggregateRef(h);
    ASSERT_EQ(AggregateResult_NumChildren(agg), 2);
    ASSERT_EQ(AggregateResult_Get(agg, 0)->data.tag, RSResultData_Metric);
    // since larger ids has lower distance, in every we get higher id (where max id is the final result).
    size_t expected_id = max_id - step*(count++);
    ASSERT_EQ(h->docId, expected_id);
  }
  ASSERT_EQ(count, k);
  ASSERT_TRUE(hybridIt->atEOF);

  // Rerun in AD_HOC BF mode.
  hybridIt->Rewind(hybridIt);
  hr->searchMode = VECSIM_HYBRID_ADHOC_BF;
  count = 0;
  while (hybridIt->Read(hybridIt) != ITERATOR_EOF) {
    RSIndexResult *h = hybridIt->current;
    ASSERT_EQ(h->data.tag, RSResultData_HybridMetric);
    ASSERT_TRUE(IndexResult_IsAggregate(h));
    const RSAggregateResult *agg = IndexResult_AggregateRef(h);
    ASSERT_EQ(AggregateResult_NumChildren(agg), 2);
    ASSERT_EQ(AggregateResult_Get(agg, 0)->data.tag, RSResultData_Metric);
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
  QueryIterator *vecIt = createMetricIteratorFromVectorQueryResults(results, true);
  size_t count = 0;
  size_t lowest_id = 25;
  size_t n_expected_res = n - lowest_id + 1;

  // Expect to get top 76 results that are within the range, with ids: 25, 26, ... , 100
  VecSim_Normalize(query, d, t);
  while (vecIt->Read(vecIt) != ITERATOR_EOF) {
    RSIndexResult *h = vecIt->current;
    ASSERT_EQ(h->data.tag, RSResultData_Metric);
    ASSERT_EQ(h->docId, lowest_id + count);
    double exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, h->docId, query);
    ASSERT_EQ(IndexResult_NumValue(h), exp_dist);
    ASSERT_EQ(RSValue_Number_Get(h->metrics[0].value), exp_dist);
    count++;
  }
  ASSERT_EQ(count, n_expected_res);
  ASSERT_TRUE(vecIt->atEOF);

  vecIt->Rewind(vecIt);
  ASSERT_FALSE(vecIt->atEOF);
  ASSERT_EQ(vecIt->NumEstimated(vecIt), n_expected_res);

  // Read one result to verify that we get the minimum id after rewind.
  ASSERT_EQ(vecIt->Read(vecIt), ITERATOR_OK);
  ASSERT_EQ(vecIt->lastDocId, lowest_id);

  // Test valid combinations of SkipTo
  ASSERT_EQ(vecIt->SkipTo(vecIt, lowest_id + 10), ITERATOR_OK);
  ASSERT_EQ(vecIt->lastDocId, lowest_id + 10);
  double exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, vecIt->lastDocId, query);
  ASSERT_EQ(IndexResult_NumValue(vecIt->current), exp_dist);
  ASSERT_EQ(RSValue_Number_Get(vecIt->current->metrics[0].value), exp_dist);

  ASSERT_EQ(vecIt->SkipTo(vecIt, n-1), ITERATOR_OK);
  ASSERT_EQ(vecIt->lastDocId, n-1);
  exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, vecIt->lastDocId, query);
  ASSERT_EQ(IndexResult_NumValue(vecIt->current), exp_dist);
  ASSERT_EQ(RSValue_Number_Get(vecIt->current->metrics[0].value), exp_dist);

  // Invalid SkipTo
  ASSERT_EQ(vecIt->SkipTo(vecIt, n+1), ITERATOR_EOF);
  ASSERT_EQ(vecIt->lastDocId, n - 1);
  ASSERT_EQ(vecIt->SkipTo(vecIt, n), ITERATOR_EOF);
  ASSERT_EQ(vecIt->SkipTo(vecIt, lowest_id + 10), ITERATOR_EOF);

  // Rewind and test skipping to the first id.
  vecIt->Rewind(vecIt);
  ASSERT_EQ(vecIt->lastDocId, 0);
  ASSERT_EQ(vecIt->SkipTo(vecIt, lowest_id), ITERATOR_OK);
  ASSERT_EQ(vecIt->lastDocId, lowest_id);

  // check rerun and abort (go over only half of the results)
  count = 1;
  for (size_t i = 0; i < n_expected_res/2; i++) {
    ASSERT_EQ(vecIt->Read(vecIt), ITERATOR_OK);
    RSIndexResult *h = vecIt->current;
    ASSERT_EQ(h->data.tag, RSResultData_Metric);
    ASSERT_EQ(h->docId, lowest_id + count);
    count++;
  }
  ASSERT_EQ(vecIt->lastDocId, lowest_id + count - 1);
  ASSERT_FALSE(vecIt->atEOF);

  vecIt->Free(vecIt);
  VecSimIndex_Free(index);
}

TEST_F(IndexTest, testMetric_SkipTo) {
  size_t results_num = 7;

  t_docId *ids_arr = (t_docId *)rm_malloc(sizeof(t_docId) * results_num);
  t_docId ids[] = {2, 4, 6, 8, 10, 15, 20};
  memcpy(ids_arr, ids, sizeof(t_docId) * results_num);

  double *metrics_arr = (double *)rm_malloc(sizeof(double) * results_num);
  double metrics[7] = {1.0};
  memcpy(metrics_arr, metrics, sizeof(double) * results_num);

  QueryIterator *metric_it = NewMetricIterator(ids_arr, metrics_arr, results_num, VECTOR_DISTANCE);

  // Copy the behaviour of INV_IDX_ITERATOR in terms of SkipTo. That is, the iterator will return the
  // next docId whose id is equal or greater than the given id, as if we call Read and returned
  // that id (hence the iterator will advance its pointer).
  ASSERT_EQ(metric_it->SkipTo(metric_it, 1), ITERATOR_NOTFOUND);
  ASSERT_EQ(metric_it->lastDocId, 2);

  ASSERT_EQ(metric_it->SkipTo(metric_it, 3), ITERATOR_NOTFOUND);
  ASSERT_EQ(metric_it->lastDocId, 4);

  ASSERT_EQ(metric_it->SkipTo(metric_it, 8), ITERATOR_OK);
  ASSERT_EQ(metric_it->lastDocId, 8);

  ASSERT_EQ(metric_it->SkipTo(metric_it, 9), ITERATOR_NOTFOUND);
  ASSERT_EQ(metric_it->lastDocId, 10);

  ASSERT_EQ(metric_it->SkipTo(metric_it, 12), ITERATOR_NOTFOUND);
  ASSERT_EQ(metric_it->lastDocId, 15);

  ASSERT_EQ(metric_it->SkipTo(metric_it, 20), ITERATOR_OK);
  ASSERT_EQ(metric_it->lastDocId, 20);

  ASSERT_EQ(metric_it->SkipTo(metric_it, 21), ITERATOR_EOF);
  ASSERT_EQ(metric_it->lastDocId, 20);

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
  QueryError err = QueryError_Default();
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

  QueryError err = QueryError_Default();
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
  InvertedIndex *w = NewInvertedIndex(IndexFlags(flags), &index_memsize);
  // The memory occupied by a empty inverted index
  // created with INDEX_DEFAULT_FLAGS is 56 bytes,
  // which is the sum of the following (See NewInvertedIndex()):
  // sizeof InvertedIndex                 40
  // storing fieldmask on idx             16
  ASSERT_EQ(56, index_memsize);
  ASSERT_TRUE(InvertedIndex_Flags(w) == flags);
  size_t sz = InvertedIndex_WriteForwardIndexEntry(w, &h);
  ASSERT_EQ(65, sz);
  InvertedIndex_Free(w);

  flags &= ~Index_StoreTermOffsets;
  w = NewInvertedIndex(IndexFlags(flags), &index_memsize);
  ASSERT_EQ(56, index_memsize);
  ASSERT_TRUE(!(InvertedIndex_Flags(w) & Index_StoreTermOffsets));
  size_t sz2 = InvertedIndex_WriteForwardIndexEntry(w, &h);
  ASSERT_EQ(sz2, 52);
  InvertedIndex_Free(w);

  flags = INDEX_DEFAULT_FLAGS | Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), &index_memsize);
  ASSERT_EQ(56, index_memsize);
  ASSERT_TRUE((InvertedIndex_Flags(w) & Index_WideSchema));
  h.fieldMask = 0xffffffffffff;
  ASSERT_EQ(69, InvertedIndex_WriteForwardIndexEntry(w, &h));
  InvertedIndex_Free(w);

  flags &= Index_StoreFreqs;
  w = NewInvertedIndex(IndexFlags(flags), &index_memsize);
  // The memory occupied by a empty inverted index with
  // Index_StoreFieldFlags == 0 is 40 bytes
  // which is the sum of the following (See NewInvertedIndex()):
  // sizeof InvertedIndex                 40
  ASSERT_EQ(40, index_memsize);
  ASSERT_TRUE(!(InvertedIndex_Flags(w) & Index_StoreTermOffsets));
  ASSERT_TRUE(!(InvertedIndex_Flags(w) & Index_StoreFieldFlags));
  sz = InvertedIndex_WriteForwardIndexEntry(w, &h);
  ASSERT_EQ(51, sz);
  InvertedIndex_Free(w);

  flags |= Index_StoreFieldFlags | Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), &index_memsize);
  ASSERT_EQ(56, index_memsize);
  ASSERT_TRUE((InvertedIndex_Flags(w) & Index_WideSchema));
  ASSERT_TRUE((InvertedIndex_Flags(w) & Index_StoreFieldFlags));
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, &h);
  ASSERT_EQ(59, sz);
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

    ASSERT_TRUE(DocTable_Pop(&dt, dmd->keyPtr, sdslen(dmd->keyPtr)) != NULL);
    DMD_Return(dmd);

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
  InvertedIndex *idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS | Index_WideSchema), &index_memsize);
  ForwardIndexEntry ent = {0};
  ent.docId = 1;
  ent.fieldMask = RS_FIELDMASK_ALL;

  InvertedIndex_WriteForwardIndexEntry(idx, &ent);
  ASSERT_EQ(InvertedIndex_NumBlocks(idx), 1);

  ent.docId = 200;
  InvertedIndex_WriteForwardIndexEntry(idx, &ent);
  ASSERT_EQ(InvertedIndex_NumBlocks(idx), 1);

  ent.docId = 1LLU << 48;
  InvertedIndex_WriteForwardIndexEntry(idx, &ent);
  ASSERT_EQ(InvertedIndex_NumBlocks(idx), 2);
  ent.docId++;
  InvertedIndex_WriteForwardIndexEntry(idx, &ent);
  ASSERT_EQ(InvertedIndex_NumBlocks(idx), 2);

  IndexDecoderCtx decoderCtx = {.field_mask_tag = IndexDecoderCtx_FieldMask, .field_mask = RS_FIELDMASK_ALL};
  IndexReader *reader = NewIndexReader(idx, decoderCtx);
  RSIndexResult *res = NewTokenRecord(NULL, 1);
  res->freq = 1;
  res->fieldMask = RS_FIELDMASK_ALL;

  ASSERT_TRUE(IndexReader_Next (reader, res));
  ASSERT_EQ(1, res->docId);

  ASSERT_TRUE(IndexReader_Next (reader, res));
  ASSERT_EQ(200, res->docId);

  ASSERT_TRUE(IndexReader_Next (reader, res));
  ASSERT_EQ((1LLU << 48), res->docId);

  ASSERT_TRUE(IndexReader_Next (reader, res));
  ASSERT_EQ((1LLU << 48) + 1, res->docId);

  ASSERT_FALSE(IndexReader_Next (reader, res));

  IndexReader_Free(reader);
  IndexResult_Free(res);
  InvertedIndex_Free(idx);
}

TEST_F(IndexTest, testRawDocId) {
  const int previousConfig = RSGlobalConfig.invertedIndexRawDocidEncoding;
  RSGlobalConfig.invertedIndexRawDocidEncoding = true;
  size_t index_memsize = 0;
  InvertedIndex *idx = NewInvertedIndex(Index_DocIdsOnly, &index_memsize);

  // Add a few entries, all with an odd docId
  for (t_docId id = 1; id < 100; id += 2) {
    RSIndexResult rec = {.docId = id, .data = {.tag = RSResultData_Virtual}};
    InvertedIndex_WriteEntryGeneric(idx, &rec);
  }

  // Test that we can read them back
  FieldMaskOrIndex f = {.mask_tag = FieldMaskOrIndex_Mask, .mask = RS_FIELDMASK_ALL};
  QueryIterator *ir = NewInvIndIterator_TermQuery(idx, nullptr, f, nullptr, 1);
  for (t_docId id = 1; id < 100; id += 2) {
    ASSERT_EQ(ITERATOR_OK, ir->Read(ir));
    ASSERT_EQ(id, ir->lastDocId);
  }
  ASSERT_EQ(ITERATOR_EOF, ir->Read(ir));

  // Test that we can skip to all the ids
  for (t_docId id = 1; id < 100; id++) {
    ir->Rewind(ir);
    IteratorStatus rc = ir->SkipTo(ir, id);
    RSIndexResult *cur = ir->current;
    if (id % 2 == 0) {
      ASSERT_EQ(ITERATOR_NOTFOUND, rc);
      ASSERT_EQ(id + 1, ir->lastDocId);
      ASSERT_EQ(id + 1, cur->docId) << "Expected to skip to " << id + 1 << " but got " << cur->docId;
    } else {
      ASSERT_EQ(ITERATOR_OK, rc);
      ASSERT_EQ(id, ir->lastDocId);
      ASSERT_EQ(id, cur->docId);
    }
  }

  // Clean up
  ir->Free(ir);
  InvertedIndex_Free(idx);
  RSGlobalConfig.invertedIndexRawDocidEncoding = previousConfig;
}

// Test HybridIteratorReducer optimization with NULL child iterator
TEST_F(IndexTest, testHybridIteratorReducerWithEmptyChild) {
  // Create hybrid params with NULL child iterator
  size_t n = 100;
  size_t d = 4;
  size_t step = 4;
  size_t max_id = n*step;
  size_t k = 10;

  VecSimQueryParams queryParams = {0};
  KNNVectorQuery top_k_query = {.vector = NULL, .vecLen = d, .k = k, .order = BY_SCORE};

  HybridIteratorParams hParams = {
    .sctx = NULL,
    .index = NULL,
    .dim = d,
    .elementType = VecSimType_FLOAT32,
    .spaceMetric = VecSimMetric_L2,
    .query = top_k_query,
    .qParams = queryParams,
    .vectorScoreField = (char *)"__v_score",
    .canTrimDeepResults = true,
    .childIt = NewEmptyIterator(),  // Empty child iterator
    .filterCtx = NULL
  };

  QueryError err = QueryError_Default();
  QueryIterator *hybridIt = NewHybridVectorIterator(hParams, &err);

  // Verify the iterator was not created due to NULL child
  ASSERT_FALSE(QueryError_HasError(&err));
  ASSERT_TRUE(hybridIt == hParams.childIt);
  ASSERT_EQ(hybridIt->type, EMPTY_ITERATOR);
  hybridIt->Free(hybridIt);
}

// Test HybridIteratorReducer optimization with invalid child iterator
TEST_F(IndexTest, testHybridIteratorReducerWithWildcardChild) {
  size_t n = 100;
  size_t d = 4;
  size_t step = 4;
  size_t max_id = n*step;
  size_t k = 10;

  VecSimQueryParams queryParams = {0};
  KNNVectorQuery top_k_query = {.vector = NULL, .vecLen = d, .k = k, .order = BY_SCORE};
  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = RS_INVALID_FIELD_INDEX}, .predicate = FIELD_EXPIRATION_DEFAULT};

  // Mock the WILDCARD_ITERATOR consideration
  QueryIterator* wildcardIt = NewWildcardIterator_NonOptimized(max_id, n, 1.0);

  HybridIteratorParams hParams = {
    .sctx = NULL,
    .index = NULL,
    .dim = d,
    .elementType = VecSimType_FLOAT32,
    .spaceMetric = VecSimMetric_L2,
    .query = top_k_query,
    .qParams = queryParams,
    .vectorScoreField = (char *)"__v_score",
    .canTrimDeepResults = true,
    .childIt = wildcardIt,
    .filterCtx = &filterCtx
  };

  QueryError err = QueryError_Default();
  QueryIterator *hybridIt = NewHybridVectorIterator(hParams, &err);

  // Verify the iterator was not created due to NULL child
  ASSERT_FALSE(QueryError_HasError(&err));
  ASSERT_EQ(hybridIt->type, HYBRID_ITERATOR);
  HybridIterator* hi = (HybridIterator *)hybridIt;
  ASSERT_EQ(hi->searchMode, VECSIM_STANDARD_KNN);
  hybridIt->Free(hybridIt);
}
