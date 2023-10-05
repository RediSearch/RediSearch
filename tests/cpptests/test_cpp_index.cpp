#include "src/buffer.h"
#include "src/index.h"
#include "src/inverted_index.h"
#include "src/index_result.h"
#include "src/query_parser/tokenizer.h"
#include "src/spec.h"
#include "src/tokenize.h"
#include "src/varint.h"
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
  ret.data = VVW_GetByteData(vvw);
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
  // printf("%ld %ld\n", BufferLen(vw->bw.buf), vw->bw.buf->cap);
  VVW_Truncate(vw);

  RSOffsetVector vec = offsetsFromVVW(vw);
  // Buffer_Seek(vw->bw.buf, 0);
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
  tr1->term.offsets = offsetsFromVVW(vw);

  RSIndexResult *tr2 = NewTokenRecord(NULL, 1);
  tr2->docId = 1;
  tr2->term.offsets = offsetsFromVVW(vw2);

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
  tr3->term.offsets = offsetsFromVVW(vw3);
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
  InvertedIndex *idx = NewInvertedIndex(indexFlags, 1);

  IndexEncoder enc = InvertedIndex_GetEncoder(indexFlags);
  IndexEncoder docIdEnc = InvertedIndex_GetEncoder(Index_DocIdsOnly);

  ASSERT_TRUE(enc != NULL);
  ASSERT_TRUE(docIdEnc != NULL);

  for (size_t i = 0; i < 200; i++) {
    // if (i % 10000 == 1) {
    //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
    //     w->ndocs);
    // }

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

  // IW_MakeSkipIndex(w, NewMemoryBuffer(8, BUFFER_WRITE));

  //   for (int x = 0; x < w->skipIdx.len; x++) {
  //     printf("Skip entry %d: %d, %d\n", x, w->skipIdx.entries[x].docId,
  //     w->skipIdx.entries[x].offset);
  //   }
  // printf("iw cap: %ld, iw size: %ld, numdocs: %d\n", w->bw.buf->cap, IW_Len(w), w->ndocs);

  for (int xx = 0; xx < 1; xx++) {
    // printf("si: %d\n", si->len);
    IndexReader *ir = NewTermIndexReader(idx, NULL, RS_FIELDMASK_ALL, NULL, 1);  //
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
    // for (int z= 0; z < 10; z++) {
    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_time);

    // IR_SkipTo(ir, 900001, &h);

    // clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_time);
    // long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;

    // printf("Time elapsed: %ldnano\n", diffInNanos);
    // //IR_Free(ir);
    // }
    // IndexResult_Free(&h);
    IR_Free(ir);
  }

  // IW_Free(w);
  // // overriding the regular IW_Free because we already deleted the buffer
  InvertedIndex_Free(idx);
}

INSTANTIATE_TEST_SUITE_P(IndexFlagsP, IndexFlagsTest, ::testing::Range(1, 32));

int printIntersect(void *ctx, RSIndexResult *hits, int argc) {
  printf("intersect: %llu\n", (unsigned long long)hits[0].docId);
  return 0;
}

TEST_F(IndexTest, testReadIterator) {
  InvertedIndex *idx = createIndex(10, 1);

  IndexReader *r1 = NewTermIndexReader(idx, NULL, RS_FIELDMASK_ALL, NULL, 1);  //

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
    InvertedIndex *w = createIndex(10, 2);
    InvertedIndex *w2 = createIndex(10, 3);
    IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);   //
    IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL, 1);  //

    // printf("Reading!\n");
    IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
    irs[0] = NewReadIterator(r1);
    irs[1] = NewReadIterator(r2);
    IteratorsConfig config{};
    iteratorsConfig_init(&config);
    IndexIterator *ui = NewUnionIterator(irs, 2, NULL, 0, 1, QN_UNION, NULL, &config);
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
  InvertedIndex *w = createIndex(10, 1);
  InvertedIndex *w2 = createIndex(10, 2);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 0.5);  //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL, 1);   //

  // printf("Reading!\n");
  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewReadIterator(r2);
  IteratorsConfig config{};
  iteratorsConfig_init(&config);
  IndexIterator *ui = NewUnionIterator(irs, 2, NULL, 0, 0.8, QN_UNION, NULL, &config);
  RSIndexResult *h = NULL;
  int expected[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20};
  int i = 0;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h.docId, expected[i]);
    ASSERT_EQ(h->docId, expected[i++]);
    ASSERT_EQ(h->weight, 0.8);
    if (h->agg.numChildren == 2) {
      ASSERT_EQ(h->agg.children[0]->weight, 0.5);
      ASSERT_EQ(h->agg.children[1]->weight, 1);
    } else {
      if (i <= 10) {
        ASSERT_EQ(h->agg.children[0]->weight, 0.5);
      } else {
        ASSERT_EQ(h->agg.children[0]->weight, 1);
      }
    }
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testNot) {
  InvertedIndex *w = createIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createIndex(10, 3);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL, 1);  //

  // printf("Reading!\n");
  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewNotIterator(NewReadIterator(r2), w2->lastId, 1);

  IndexIterator *ui = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1);
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
  InvertedIndex *w = createIndex(10, 3);

  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);  //
  printf("last id: %llu\n", (unsigned long long)w->lastId);

  IndexIterator *ir = NewNotIterator(NewReadIterator(r1), w->lastId + 5, 1);

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

// Note -- in test_index.c, this test was never actually run!
TEST_F(IndexTest, DISABLED_testOptional) {
  InvertedIndex *w = createIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createIndex(10, 3);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL, 1);  //

  // printf("Reading!\n");
  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewOptionalIterator(NewReadIterator(r2), w2->lastId, 1);

  IndexIterator *ui = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1);
  RSIndexResult *h = NULL;

  int i = 1;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h->docId, i);
    ASSERT_EQ(i, h->docId);
    if (i > 0 && i % 3 == 0) {
      ASSERT_EQ(1, h->agg.children[1]->freq);
    } else {
      ASSERT_EQ(0, h->agg.children[1]->freq);
    }
    // printf("%d, ", h.docId);
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
}

TEST_F(IndexTest, testNumericInverted) {

  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1);

  for (int i = 0; i < 75; i++) {
    size_t sz = InvertedIndex_WriteNumericEntry(idx, i + 1, (double)(i + 1));
    // printf("written %zd bytes\n", sz);

    ASSERT_TRUE(sz > (i ? 1 : 0)); // first doc has zero delta (not written)
  }
  ASSERT_EQ(75, idx->lastId);

  // printf("written %zd bytes\n", IndexBlock_DataLen(&idx->blocks[0]));

  IndexReader *ir = NewNumericReader(NULL, idx, NULL, 0, 0, false);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;
  t_docId i = 1;
  while (INDEXREAD_EOF != it->Read(it->ctx, &res)) {
    // printf("%d %f\n", res->docId, res->num.value);

    ASSERT_EQ(i++, res->docId);
    ASSERT_EQ(res->num.value, (float)res->docId);
  }
  InvertedIndex_Free(idx);
  it->Free(it);
}

TEST_F(IndexTest, testNumericVaried) {
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1);

  static const double nums[] = {0,          0.13,          0.001,     -0.1,     1.0,
                                5.0,        4.323,         65535,     65535.53, 32768.432,
                                1LLU << 32, -(1LLU << 32), 1LLU << 40};
  static const size_t numCount = sizeof(nums) / sizeof(double);

  for (size_t i = 0; i < numCount; i++) {
    size_t sz = InvertedIndex_WriteNumericEntry(idx, i + 1, nums[i]);
    ASSERT_GT(sz, (i ? 1 : 0)); // first doc has zero delta (not written)
    // printf("[%lu]: Stored %lf\n", i, nums[i]);
  }

  IndexReader *ir = NewNumericReader(NULL, idx, NULL, 0, 0, false);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;

  for (size_t i = 0; i < numCount; i++) {
    // printf("Checking i=%lu. Expected=%lf\n", i, nums[i]);
    int rv = it->Read(it->ctx, &res);
    ASSERT_NE(INDEXREAD_EOF, rv);
    ASSERT_LT(fabs(nums[i] - res->num.value), 0.01);
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
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1);

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\n[%lu]: Expecting Val=%lf, Sz=%lu\n", ii, infos[ii].value, infos[ii].size);
    size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);
    ASSERT_EQ(infos[ii].size, sz);
    if (isMulti) {
      size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);
      // in multi mode we do not write the zero delta
      // (first entry has zero delta also for single mode)
      ASSERT_EQ(infos[ii].size - (ii ? 1 : 0), sz);
    }
  }

  IndexReader *ir = NewNumericReader(NULL, idx, NULL, 0, 0, isMulti);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\nReading [%lu]\n", ii);

    int rc = it->Read(it->ctx, &res);
    ASSERT_NE(rc, INDEXREAD_EOF);
    // printf("%lf <-> %lf\n", infos[ii].value, res->num.value);
    if (fabs(infos[ii].value) == INFINITY) {
      ASSERT_EQ(infos[ii].value, res->num.value);
    } else {
      ASSERT_LT(fabs(infos[ii].value - res->num.value), 0.01);
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

  InvertedIndex *w = createIndex(1000, 1);
  IndexReader *r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);  //

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

  InvertedIndex *w = createIndex(100000, 4);
  InvertedIndex *w2 = createIndex(100000, 2);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL, 1);  //

  IndexIterator **irs = (IndexIterator **)calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewReadIterator(r2);

  int count = 0;
  IndexIterator *ii = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0, 1);

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
  InvertedIndex *w = createIndex(n, step);
  IndexReader *r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);

  // Create vector index
  VecSimParams params{.algo = VecSimAlgo_HNSWLIB,
                      .algoParams = {.hnswParams = HNSWParams{.type = t,
                                               .dim = d,
                                               .metric = met,
                                               .initialCapacity = max_id,
                                               .M = 16,
                                               .efConstruction = 100}}};
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

  // Run simple top k query.
  HybridIteratorParams hParams = {.index = index,
                                  .dim = d,
                                  .elementType = t,
                                  .spaceMetric = met,
                                  .query = top_k_query,
                                  .qParams = queryParams,
                                  .vectorScoreField = (char *)"__v_score",
                                  .ignoreDocScore = true,
                                  .childIt = NULL
  };
  QueryError err = {QUERY_OK};
  IndexIterator *vecIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);

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
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);

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
  r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);
  ir = NewReadIterator(r);
  hParams.ignoreDocScore = false;
  hParams.childIt = ir;
  hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
  hr = (HybridIterator *)hybridIt->ctx;
  hr->searchMode = VECSIM_HYBRID_BATCHES;

  // This time, result is a tree with 2 children: vector score and subtree of terms (for scoring).
  count = 0;
  while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
    ASSERT_EQ(h->type, RSResultType_HybridMetric);
    ASSERT_TRUE(RSIndexResult_IsAggregate(h));
    ASSERT_EQ(h->agg.numChildren, 2);
    ASSERT_EQ(h->agg.children[0]->type, RSResultType_Metric);
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
    ASSERT_EQ(h->agg.numChildren, 2);
    ASSERT_EQ(h->agg.children[0]->type, RSResultType_Metric);
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
  InvertedIndex *w = createIndex(n, 1);
  IndexReader *r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);

  // Create vector index with a single vector.
  VecSimParams params{
      .algo = VecSimAlgo_HNSWLIB,
      .algoParams = {.hnswParams = HNSWParams{
          .type = VecSimType_FLOAT32, .dim = d, .metric = VecSimMetric_L2, .initialCapacity = n}}};
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
  IndexIterator *ii = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 1, 1);

  // Create hybrid iterator - should return NULL.
  HybridIteratorParams hParams = {.index = index,
                                  .query = top_k_query,
                                  .qParams = queryParams,
                                  .vectorScoreField = (char *)"__v_score",
                                  .ignoreDocScore = true,
                                  .childIt = ii};
  QueryError err = {QUERY_OK};
  IndexIterator *hybridIt = NewHybridVectorIterator(hParams, &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
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
  VecSimParams params{.algo = VecSimAlgo_HNSWLIB,
                      .algoParams = {.hnswParams = HNSWParams{.type = t,
                                               .dim = d,
                                               .metric = met,
                                               .initialCapacity = n,
                                               .M = 16,
                                               .efConstruction = 100}}};
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
  VecSimQueryParams queryParams;
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
    ASSERT_EQ(h->num.value, exp_dist);
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
  ASSERT_EQ(h->num.value, exp_dist);
  ASSERT_EQ(h->metrics[0].value->numval, exp_dist);
  ASSERT_EQ(vecIt->LastDocId(vecIt->ctx), lowest_id + 10);

  ASSERT_EQ(vecIt->SkipTo(vecIt->ctx, n-1, &h), INDEXREAD_OK);
  ASSERT_EQ(h->docId, n-1);
  exp_dist = VecSimIndex_GetDistanceFrom_Unsafe(index, h->docId, query);
  ASSERT_EQ(h->num.value, exp_dist);
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
  ASSERT_TRUE(w.buf->data == w.pos);

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

typedef struct {
  int num;
  char **expected;

} tokenContext;

int tokenFunc(void *ctx, const Token *t) {
  tokenContext *tx = (tokenContext *)ctx;
  int ret = strncmp(t->tok, tx->expected[tx->num++], t->tokLen);
  EXPECT_TRUE(ret == 0);
  EXPECT_TRUE(t->pos > 0);
  return 0;
}

// int testTokenize() {
//   char *txt = strdup("Hello? world...   ? -WAZZ@UP? שלום");
//   tokenContext ctx = {0};
//   const char *expected[] = {"hello", "world", "wazz", "up", "שלום"};
//   ctx.expected = (char **)expected;

//   tokenize(txt, &ctx, tokenFunc, NULL, 0, DefaultStopWordList(), 0);
//   ASSERT_TRUE(ctx.num == 5);

//   free(txt);

//   return 0;
// }

// int testForwardIndex() {

//   Document doc = NewDocument(NULL, 1, 1, "english");
//   doc.docId = 1;
//   doc.fields[0] = N
//   ForwardIndex *idx = NewForwardIndex(doc);
//   char *txt = strdup("Hello? world...  hello hello ? __WAZZ@UP? שלום");
//   tokenize(txt, 1, 1, idx, forwardIndexTokenFunc);

//   return 0;
// }

TEST_F(IndexTest, testIndexSpec) {
  const char *title = "title", *body = "body", *foo = "foo", *bar = "bar", *name = "name";
  const char *args[] = {"STOPWORDS", "2",      "hello", "world",    "SCHEMA", title,
                        "text",      "weight", "0.1",   body,       "text",   "weight",
                        "2.0",       foo,      "text",  "sortable", bar,      "numeric",
                        "sortable",  name,     "text",  "nostem"};
  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  IndexSpec *s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
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

  const FieldSpec *f = IndexSpec_GetField(s, body, strlen(body));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_STREQ(f->name, body);
  ASSERT_EQ(f->ftWeight, 2.0);
  ASSERT_EQ(FIELD_BIT(f), 2);
  ASSERT_EQ(f->options, 0);
  ASSERT_EQ(f->sortIdx, -1);

  f = IndexSpec_GetField(s, title, strlen(title));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_TRUE(strcmp(f->name, title) == 0);
  ASSERT_TRUE(f->ftWeight == 0.1);
  ASSERT_TRUE(FIELD_BIT(f) == 1);
  ASSERT_TRUE(f->options == 0);
  ASSERT_TRUE(f->sortIdx == -1);

  f = IndexSpec_GetField(s, foo, strlen(foo));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_TRUE(strcmp(f->name, foo) == 0);
  ASSERT_TRUE(f->ftWeight == 1);
  ASSERT_TRUE(FIELD_BIT(f) == 4);
  ASSERT_TRUE(f->options == FieldSpec_Sortable);
  ASSERT_TRUE(f->sortIdx == 0);

  f = IndexSpec_GetField(s, bar, strlen(bar));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_NUMERIC));

  ASSERT_TRUE(strcmp(f->name, bar) == 0);
  ASSERT_EQ(f->options, FieldSpec_Sortable | FieldSpec_UNF); // UNF is set implicitly for sortable numerics
  ASSERT_TRUE(f->sortIdx == 1);
  ASSERT_TRUE(IndexSpec_GetField(s, "fooz", 4) == NULL);

  f = IndexSpec_GetField(s, name, strlen(name));
  ASSERT_TRUE(f != NULL);
  ASSERT_TRUE(FIELD_IS(f, INDEXFLD_T_FULLTEXT));
  ASSERT_TRUE(strcmp(f->name, name) == 0);
  ASSERT_TRUE(f->ftWeight == 1);
  ASSERT_TRUE(FIELD_BIT(f) == 8);
  ASSERT_TRUE(f->options == FieldSpec_NoStemming);
  ASSERT_TRUE(f->sortIdx == -1);

  ASSERT_TRUE(s->sortables != NULL);
  ASSERT_TRUE(s->sortables->len == 2);
  int rc = RSSortingTable_GetFieldIdx(s->sortables, foo);
  ASSERT_EQ(0, rc);
  rc = RSSortingTable_GetFieldIdx(s->sortables, bar);
  ASSERT_EQ(1, rc);
  rc = RSSortingTable_GetFieldIdx(s->sortables, title);
  ASSERT_EQ(-1, rc);

  StrongRef_Release(ref);

  QueryError_ClearError(&err);
  const char *args2[] = {
      "NOOFFSETS", "NOFIELDS", "SCHEMA", title, "text",
  };
  ref = IndexSpec_Parse("idx", args2, sizeof(args2) / sizeof(const char *), &err);
  s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(s->numFields == 1);

  ASSERT_TRUE(!(s->flags & Index_StoreFieldFlags));
  ASSERT_TRUE(!(s->flags & Index_StoreTermOffsets));
  StrongRef_Release(ref);

  // User-reported bug
  const char *args3[] = {"SCHEMA", "ha", "NUMERIC", "hb", "TEXT", "WEIGHT", "1", "NOSTEM"};
  QueryError_ClearError(&err);
  ref = IndexSpec_Parse("idx", args3, sizeof(args3) / sizeof(args3[0]), &err);
  s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(FieldSpec_IsNoStem(s->fields + 1));
  StrongRef_Release(ref);
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
  StrongRef ref = IndexSpec_Parse("idx", (const char **)&args[0], args.size(), &err);
  IndexSpec *s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
  ASSERT_TRUE(s);
  ASSERT_TRUE(s->numFields == N);
  StrongRef_Release(ref);
  freeSchemaArgs(args);

  // test too big a schema
  N = 300;
  fillSchema(args, N);

  QueryError_ClearError(&err);
  ref = IndexSpec_Parse("idx", (const char **)&args[0], args.size(), &err);
  s = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_TRUE(s == NULL);
  ASSERT_TRUE(QueryError_HasError(&err));
#if !defined(__arm__) && !defined(__aarch64__)
  ASSERT_STREQ("Schema is limited to 128 TEXT fields", QueryError_GetError(&err));
#else
  ASSERT_STREQ("Schema is limited to 64 TEXT fields", QueryError_GetError(&err));
#endif
  freeSchemaArgs(args);
  QueryError_ClearError(&err);
}

typedef union {

  int i;
  float f;
} u;

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
  InvertedIndex *w = NewInvertedIndex(IndexFlags(flags), 1);
  IndexEncoder enc = InvertedIndex_GetEncoder(w->flags);
  ASSERT_TRUE(w->flags == flags);
  size_t sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  // printf("written %zd bytes. Offset=%zd\n", sz, h.vw->buf.offset);
  ASSERT_EQ(15, sz);
  InvertedIndex_Free(w);

  flags &= ~Index_StoreTermOffsets;
  w = NewInvertedIndex(IndexFlags(flags), 1);
  ASSERT_TRUE(!(w->flags & Index_StoreTermOffsets));
  enc = InvertedIndex_GetEncoder(w->flags);
  size_t sz2 = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  // printf("Wrote %zd bytes. Offset=%zd\n", sz2, h.vw->buf.offset);
  ASSERT_EQ(sz2, sz - Buffer_Offset(&h.vw->buf) - 1);
  InvertedIndex_Free(w);

  flags = INDEX_DEFAULT_FLAGS | Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), 1);
  ASSERT_TRUE((w->flags & Index_WideSchema));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  ASSERT_EQ(21, InvertedIndex_WriteForwardIndexEntry(w, enc, &h));
  InvertedIndex_Free(w);

  flags |= Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), 1);
  ASSERT_TRUE((w->flags & Index_WideSchema));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(21, sz);
  InvertedIndex_Free(w);

  flags &= Index_StoreFreqs;
  w = NewInvertedIndex(IndexFlags(flags), 1);
  ASSERT_TRUE(!(w->flags & Index_StoreTermOffsets));
  ASSERT_TRUE(!(w->flags & Index_StoreFieldFlags));
  enc = InvertedIndex_GetEncoder(w->flags);
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(3, sz);
  InvertedIndex_Free(w);

  flags |= Index_StoreFieldFlags | Index_WideSchema;
  w = NewInvertedIndex(IndexFlags(flags), 1);
  ASSERT_TRUE((w->flags & Index_WideSchema));
  ASSERT_TRUE((w->flags & Index_StoreFieldFlags));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQ(10, sz);
  InvertedIndex_Free(w);

  VVW_Free(h.vw);
}

TEST_F(IndexTest, testDocTable) {
  char buf[16];
  DocTable dt = NewDocTable(10, 10);
  t_docId did = 0;
  // N is set to 100 and the max cap of the doc table is 10 so we surely will
  // get overflow and check that everything works correctly
  int N = 100;
  for (int i = 0; i < N; i++) {
    size_t nkey = sprintf(buf, "doc_%d", i);
    RSDocumentMetadata *dmd = DocTable_Put(&dt, buf, nkey, (double)i, Document_DefaultFlags, buf, strlen(buf), DocumentType_Hash);
    t_docId nd = dmd->id;
    DMD_Return(dmd);
    ASSERT_EQ(did + 1, nd);
    did = nd;
  }

  ASSERT_EQ(N + 1, dt.size);
  ASSERT_EQ(N, dt.maxDocId);
#ifdef __x86_64__
  ASSERT_EQ(10180, (int)dt.memsize);
#endif
  for (int i = 0; i < N; i++) {
    sprintf(buf, "doc_%d", i);
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
  ASSERT_EQ(71, (int)dt.memsize);

  // Test that binary keys also work here
  static const char binBuf[] = {"Hello\x00World"};
  const size_t binBufLen = 11;
  ASSERT_FALSE(DocIdMap_Get(&dt.dim, binBuf, binBufLen));
  DMD_Return(dmd);
  dmd = DocTable_Put(&dt, binBuf, binBufLen, 1.0, Document_DefaultFlags, NULL, 0, DocumentType_Hash);
  ASSERT_TRUE(dmd);
  ASSERT_EQ(148, (int)dt.memsize);
  ASSERT_NE(dmd->id, strDocId);
  ASSERT_EQ(dmd->id, DocIdMap_Get(&dt.dim, binBuf, binBufLen));
  ASSERT_EQ(strDocId, DocIdMap_Get(&dt.dim, "Hello", 5));
  DMD_Return(dmd);
  DocTable_Free(&dt);
}

TEST_F(IndexTest, testSortable) {
  RSSortingTable *tbl = NewSortingTable();
  RSSortingTable_Add(&tbl, "foo", RSValue_String);
  RSSortingTable_Add(&tbl, "bar", RSValue_String);
  RSSortingTable_Add(&tbl, "baz", RSValue_String);
  ASSERT_EQ(3, tbl->len);

  ASSERT_STREQ("foo", tbl->fields[0].name);
  ASSERT_EQ(RSValue_String, tbl->fields[0].type);
  ASSERT_STREQ("bar", tbl->fields[1].name);
  ASSERT_STREQ("baz", tbl->fields[2].name);
  ASSERT_EQ(0, RSSortingTable_GetFieldIdx(tbl, "foo"));
  ASSERT_EQ(0, RSSortingTable_GetFieldIdx(tbl, "FoO"));
  ASSERT_EQ(-1, RSSortingTable_GetFieldIdx(NULL, "FoO"));

  ASSERT_EQ(1, RSSortingTable_GetFieldIdx(tbl, "bar"));
  ASSERT_EQ(-1, RSSortingTable_GetFieldIdx(tbl, "barbar"));

  RSSortingVector *v = NewSortingVector(tbl->len);
  ASSERT_EQ(v->len, tbl->len);

  const char *str = "hello";
  const char *masse = "Maße";
  double num = 3.141;
  ASSERT_TRUE(RSValue_IsNull(v->values[0]));
  RSSortingVector_Put(v, 0, str, RS_SORTABLE_STR, 0);
  ASSERT_EQ(v->values[0]->t, RSValue_String);
  ASSERT_EQ(v->values[0]->strval.stype, RSString_RMAlloc);

  ASSERT_TRUE(RSValue_IsNull(v->values[1]));
  ASSERT_TRUE(RSValue_IsNull(v->values[2]));
  RSSortingVector_Put(v, 1, &num, RSValue_Number, 0);
  ASSERT_EQ(v->values[1]->t, RS_SORTABLE_NUM);

  RSSortingVector *v2 = NewSortingVector(tbl->len);
  RSSortingVector_Put(v2, 0, masse, RS_SORTABLE_STR, 0);

  /// test string unicode lowercase normalization
  ASSERT_STREQ("masse", v2->values[0]->strval.str);

  double s2 = 4.444;
  RSSortingVector_Put(v2, 1, &s2, RS_SORTABLE_NUM, 0);

  RSSortingKey sk = {.index = 0, .ascending = 0};

  QueryError qerr;
  QueryError_Init(&qerr);

  int rc = RSSortingVector_Cmp(v, v2, &sk, &qerr);
  ASSERT_LT(0, rc);
  ASSERT_EQ(QUERY_OK, qerr.code);
  sk.ascending = 1;
  rc = RSSortingVector_Cmp(v, v2, &sk, &qerr);
  ASSERT_GT(0, rc);
  ASSERT_EQ(QUERY_OK, qerr.code);
  rc = RSSortingVector_Cmp(v, v, &sk, &qerr);
  ASSERT_EQ(0, rc);
  ASSERT_EQ(QUERY_OK, qerr.code);

  sk.index = 1;

  rc = RSSortingVector_Cmp(v, v2, &sk, &qerr);
  ASSERT_TRUE(-1 == rc && qerr.code == QUERY_OK);
  sk.ascending = 0;
  rc = RSSortingVector_Cmp(v, v2, &sk, &qerr);
  ASSERT_TRUE(1 == rc && qerr.code == QUERY_OK);

  SortingTable_Free(tbl);
  SortingVector_Free(v);
  SortingVector_Free(v2);
}

TEST_F(IndexTest, testVarintFieldMask) {
  t_fieldMask x = 127;
  size_t expected[] = {1, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 19};
  Buffer b = {0};
  Buffer_Init(&b, 1);
  BufferWriter bw = NewBufferWriter(&b);
  for (int i = 0; i < sizeof(t_fieldMask); i++, x |= x << 8) {
    size_t sz = WriteVarintFieldMask(x, &bw);
    ASSERT_EQ(expected[i], sz);
    BufferWriter_Seek(&bw, 0);
    BufferReader br = NewBufferReader(bw.buf);

    t_fieldMask y = ReadVarintFieldMask(&br);

    ASSERT_EQ(y, x);
  }
  Buffer_Free(&b);
}

TEST_F(IndexTest, testDeltaSplits) {
  InvertedIndex *idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1);
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

  IndexReader *ir = NewTermIndexReader(idx, NULL, RS_FIELDMASK_ALL, NULL, 1);
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
