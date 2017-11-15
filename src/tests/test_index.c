#include "../buffer.h"
#include "../index.h"
#include "../inverted_index.h"
#include "../index_result.h"
#include "../query_parser/tokenizer.h"
#include "../rmutil/alloc.h"
#include "../spec.h"
#include "../tokenize.h"
#include "../varint.h"
#include "test_util.h"
#include "time_sample.h"
#include "../rmutil/alloc.h"
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <time.h>
#include <float.h>

RSOffsetIterator _offsetVector_iterate(RSOffsetVector *v);
int testVarint() {
  VarintVectorWriter *vw = NewVarintVectorWriter(8);
  uint32_t expected[5] = {10, 1000, 1020, 10000, 10020};
  for (int i = 0; i < 5; i++) {
    VVW_Write(vw, expected[i]);
  }

  // VVW_Write(vw, 100);
  // printf("%ld %ld\n", BufferLen(vw->bw.buf), vw->bw.buf->cap);
  VVW_Truncate(vw);

  RSOffsetVector vec = (RSOffsetVector)VVW_OFFSETVECTOR_INIT(vw);
  // Buffer_Seek(vw->bw.buf, 0);
  RSOffsetIterator it = _offsetVector_iterate(&vec);
  int x = 0;
  uint32_t n = 0;
  while (RS_OFFSETVECTOR_EOF != (n = it.Next(it.ctx, NULL))) {

    ASSERTM(n == expected[x++], "Wrong number decoded");
    // printf("%d %d\n", x, n);
  }
  it.Free(it.ctx);
  VVW_Free(vw);
  return 0;
}

int testDistance() {
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

  RSIndexResult *tr1 = NewTokenRecord(NULL);
  tr1->docId = 1;
  tr1->term.offsets = (RSOffsetVector)(RSOffsetVector)VVW_OFFSETVECTOR_INIT(vw);

  RSIndexResult *tr2 = NewTokenRecord(NULL);
  tr2->docId = 1;
  tr2->term.offsets = (RSOffsetVector)(RSOffsetVector)VVW_OFFSETVECTOR_INIT(vw2);

  RSIndexResult *res = NewIntersectResult(2);
  AggregateResult_AddChild(res, tr1);
  AggregateResult_AddChild(res, tr2);

  int delta = IndexResult_MinOffsetDelta(res);
  ASSERT_EQUAL(2, delta);

  ASSERT_EQUAL(0, IndexResult_IsWithinRange(res, 0, 0));
  ASSERT_EQUAL(0, IndexResult_IsWithinRange(res, 0, 1));
  ASSERT_EQUAL(0, IndexResult_IsWithinRange(res, 1, 1));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 1, 0));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 2, 1));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 2, 0));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 3, 1));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 4, 0));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 4, 1));
  ASSERT_EQUAL(1, IndexResult_IsWithinRange(res, 5, 1));

  RSIndexResult *tr3 = NewTokenRecord(NULL);
  tr3->docId = 1;
  tr3->term.offsets = (RSOffsetVector)VVW_OFFSETVECTOR_INIT(vw3);
  AggregateResult_AddChild(res, tr3);

  delta = IndexResult_MinOffsetDelta(res);
  ASSERT_EQUAL(7, delta);

  // test merge iteration
  RSOffsetIterator it = RSIndexResult_IterateOffsets(res);
  uint32_t expected[] = {1, 4, 7, 9, 13, 16, 20, 22, 25, 32, RS_OFFSETVECTOR_EOF};

  uint32_t rc;
  int i = 0;
  do {
    rc = it.Next(it.ctx, NULL);
    ASSERT_EQUAL(rc, (expected[i++]));
  } while (rc != RS_OFFSETVECTOR_EOF);
  it.Free(it.ctx);

  IndexResult_Free(tr1);
  IndexResult_Free(tr2);
  IndexResult_Free(tr3);
  IndexResult_Free(res);
  VVW_Free(vw);
  VVW_Free(vw2);
  VVW_Free(vw3);

  return 0;
}

int testIndexReadWriteFlags(uint32_t indexFlags) {

  InvertedIndex *idx = NewInvertedIndex(indexFlags, 1);

  IndexEncoder enc = InvertedIndex_GetEncoder(indexFlags);
  ASSERT(enc != NULL);

  for (int i = 0; i < 200; i++) {
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

  ASSERT_EQUAL(200, idx->numDocs);
  ASSERT_EQUAL(2, idx->size);
  ASSERT_EQUAL(199, idx->lastId);

  // IW_MakeSkipIndex(w, NewMemoryBuffer(8, BUFFER_WRITE));

  //   for (int x = 0; x < w->skipIdx.len; x++) {
  //     printf("Skip entry %d: %d, %d\n", x, w->skipIdx.entries[x].docId,
  //     w->skipIdx.entries[x].offset);
  //   }
  // printf("iw cap: %ld, iw size: %ld, numdocs: %d\n", w->bw.buf->cap, IW_Len(w), w->ndocs);

  for (int xx = 0; xx < 1; xx++) {
    // printf("si: %d\n", si->len);
    IndexReader *ir = NewTermIndexReader(idx, NULL, RS_FIELDMASK_ALL, NULL);  //
    RSIndexResult *h = NULL;

    int n = 0;
    int rc;
    while (IR_HasNext(ir)) {
      if ((rc = IR_Read(ir, &h)) == INDEXREAD_EOF) {
        break;
      }
      ASSERT_EQUAL(INDEXREAD_OK, rc);
      ASSERT_EQUAL(h->docId, n);
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
  return 0;
}

int testIndexReadWrite() {
  for (uint32_t i = 0; i < 32; i++) {
    // printf("Testing %u BEGIN\n", i);
    int rv = testIndexReadWriteFlags(i);
    // printf("Testing %u END\n", i);
    if (rv != 0) {
      return -1;
    }
  }
  return 0;
}

InvertedIndex *createIndex(int size, int idStep) {
  InvertedIndex *idx = NewInvertedIndex(INDEX_DEFAULT_FLAGS, 1);

  IndexEncoder enc = InvertedIndex_GetEncoder(idx->flags);
  t_docId id = idStep;
  for (int i = 0; i < size; i++) {
    // if (i % 10000 == 1) {
    //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
    //     w->ndocs);
    // }
    ForwardIndexEntry h;
    h.docId = id;
    h.fieldMask = 1;
    h.freq = 1;
    h.term = "hello";
    h.len = 5;

    h.vw = NewVarintVectorWriter(8);
    for (int n = idStep; n < idStep + i % 4; n++) {
      VVW_Write(h.vw, n);
    }

    InvertedIndex_WriteForwardIndexEntry(idx, enc, &h);
    VVW_Free(h.vw);

    id += idStep;
  }

  // printf("BEFORE: iw cap: %ld, iw size: %zd, numdocs: %d\n", w->bw.buf->cap,
  //        IW_Len(w), w->ndocs);

  return idx;
}

int printIntersect(void *ctx, RSIndexResult *hits, int argc) {
  printf("intersect: %d\n", hits[0].docId);
  return 0;
}

int testReadIterator() {
  InvertedIndex *idx = createIndex(10, 1);

  IndexReader *r1 = NewTermIndexReader(idx, NULL, RS_FIELDMASK_ALL, NULL);  //

  RSIndexResult *h = NULL;

  IndexIterator *it = NewReadIterator(r1);
  int i = 1;
  while (it->HasNext(it->ctx)) {
    if (it->Read(it->ctx, &h) == INDEXREAD_EOF) {
      break;
    }

    // printf("Iter got %d\n", h.docId);
    ASSERT(h->docId == i++);
  }
  ASSERT(i == 11);

  it->Free(it);

  // IndexResult_Free(&h);
  InvertedIndex_Free(idx);
  return 0;
}

int testUnion() {
  InvertedIndex *w = createIndex(10, 2);
  InvertedIndex *w2 = createIndex(10, 3);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL);  //

  // printf("Reading!\n");
  IndexIterator **irs = calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewReadIterator(r2);

  IndexIterator *ui = NewUnionIterator(irs, 2, NULL, 0);
  RSIndexResult *h = NULL;
  int expected[] = {2, 3, 4, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 24, 27, 30};
  int i = 0;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h.docId, expected[i]);
    ASSERT(h->docId == expected[i++]);

    RSIndexResult *copy = IndexResult_DeepCopy(h);
    ASSERT(copy != NULL);
    ASSERT(copy != h);
    ASSERT(copy->isCopy == 1);

    ASSERT(copy->docId == h->docId);
    ASSERT(copy->type == h->type);

    IndexResult_Free(copy);

    // printf("%d, ", h.docId);
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
  return 0;
}

int testNot() {
  InvertedIndex *w = createIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createIndex(10, 3);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL);  //

  // printf("Reading!\n");
  IndexIterator **irs = calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewNotIterator(NewReadIterator(r2), w2->lastId);

  IndexIterator *ui = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0);
  RSIndexResult *h = NULL;
  int expected[] = {1, 2, 4, 5, 7, 8, 10, 11, 13, 14, 16};
  int i = 0;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT(h->docId == expected[i++]);
    // printf("%d, ", h.docId);
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
  return 0;
}

int testPureNot() {
  InvertedIndex *w = createIndex(10, 3);

  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL);  //
  printf("last id: %d\n", w->lastId);

  IndexIterator *ir = NewNotIterator(NewReadIterator(r1), w->lastId + 5);

  RSIndexResult *h = NULL;
  int expected[] = {1,  2,  4,  5,  7,  8,  10, 11, 13, 14, 16, 17, 19,
                    20, 22, 23, 25, 26, 28, 29, 31, 32, 33, 34, 35};
  int i = 0;
  while (ir->Read(ir->ctx, &h) != INDEXREAD_EOF) {

    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT(h->docId == expected[i++]);
  }

  ir->Free(ir);
  InvertedIndex_Free(w);
  return 0;
}

int testOptional() {
  InvertedIndex *w = createIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createIndex(10, 3);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL);  //

  // printf("Reading!\n");
  IndexIterator **irs = calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewOptionalIterator(NewReadIterator(r2), w2->lastId);

  IndexIterator *ui = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0);
  RSIndexResult *h = NULL;

  int i = 1;
  while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    // printf("%d <=> %d\n", h->docId, expected[i]);
    ASSERT(h->docId == i);
    if (i > 0 && i % 3 == 0) {
      ASSERT(h->agg.children[1]->freq == 1)
    } else {
      ASSERT(h->agg.children[1]->freq == 0)
    }
    // printf("%d, ", h.docId);
  }

  ui->Free(ui);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);
  return 0;
}

int testNumericInverted() {

  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1);

  for (int i = 0; i < 75; i++) {
    size_t sz = InvertedIndex_WriteNumericEntry(idx, i + 1, (double)(i + 1));
    // printf("written %zd bytes\n", sz);

    ASSERT(sz > 1);
  }
  ASSERT_EQUAL(75, idx->lastId);

  printf("written %zd bytes\n", idx->blocks[0].data->offset);

  IndexReader *ir = NewNumericReader(idx, NULL);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;
  t_docId i = 1;
  while (INDEXREAD_EOF != it->Read(it->ctx, &res)) {
    // printf("%d %f\n", res->docId, res->num.value);

    ASSERT_EQUAL(i++, res->docId);
    ASSERT_EQUAL(res->num.value, (float)res->docId);
  }
  InvertedIndex_Free(idx);
  it->Free(it);
  return 0;
}

int testNumericVaried() {
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1);

  static const double nums[] = {0,          0.13,          0.001,     -0.1,     1.0,
                                5.0,        4.323,         65535,     65535.53, 32768.432,
                                1LLU << 32, -(1LLU << 32), 1LLU << 40};
  static const size_t numCount = sizeof(nums) / sizeof(double);

  for (size_t i = 0; i < numCount; i++) {
    size_t sz = InvertedIndex_WriteNumericEntry(idx, i + 1, nums[i]);
    ASSERT(sz > 1);
    // printf("[%lu]: Stored %lf\n", i, nums[i]);
  }

  IndexReader *ir = NewNumericReader(idx, NULL);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;

  for (size_t i = 0; i < numCount; i++) {
    printf("Checking i=%lu. Expected=%lf\n", i, nums[i]);
    int rv = it->Read(it->ctx, &res);
    ASSERT(INDEXREAD_EOF != rv);
    ASSERT(fabs(nums[i] - res->num.value) < 0.01);
  }

  ASSERT_EQUAL(INDEXREAD_EOF, it->Read(it->ctx, &res));
  InvertedIndex_Free(idx);
  it->Free(it);

  return 0;
}

typedef struct {
  double value;
  size_t size;
} encodingInfo;
static const encodingInfo infos[] = {
    {0, 2},                    // 0
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
    {42.4345, 6},              // 22
    {(float)0.5, 6},           // 23
    {DBL_MAX, 10},             // 24
    {UINT64_MAX >> 12, 9},     // 25
    {INFINITY, 2},             // 26
    {-INFINITY, 2}             // 27
};

int testNumericEncoding() {
  static const size_t numInfos = sizeof(infos) / sizeof(infos[0]);
  InvertedIndex *idx = NewInvertedIndex(Index_StoreNumeric, 1);
  // printf("TestNumericEncoding\n");

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\n[%lu]: Expecting Val=%lf, Sz=%lu\n", ii, infos[ii].value, infos[ii].size);
    size_t sz = InvertedIndex_WriteNumericEntry(idx, ii + 1, infos[ii].value);
    ASSERT_EQUAL(infos[ii].size, sz);
  }

  IndexReader *ir = NewNumericReader(idx, NULL);
  IndexIterator *it = NewReadIterator(ir);
  RSIndexResult *res;

  for (size_t ii = 0; ii < numInfos; ii++) {
    // printf("\nReading [%lu]\n", ii);

    int rc = it->Read(it->ctx, &res);
    ASSERT(rc != INDEXREAD_EOF);
    // printf("%lf <-> %lf\n", infos[ii].value, res->num.value);
    if (fabs(infos[ii].value) == INFINITY) {
      ASSERT(infos[ii].value == res->num.value);
    } else {
      ASSERT(fabs(infos[ii].value - res->num.value) < 0.01);
    }
  }

  InvertedIndex_Free(idx);
  it->Free(it);

  return 0;
}

int testAbort() {

  InvertedIndex *w = createIndex(1000, 1);
  IndexReader *r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL);  //

  IndexIterator *it = NewReadIterator(r);
  int n = 0;
  RSIndexResult *res;
  while (INDEXREAD_EOF != it->Read(it->ctx, &res)) {
    if (n == 50) {
      it->Abort(it->ctx);
    }
    n++;
  }
  ASSERT_EQUAL(51, n);
  it->Free(it);
  InvertedIndex_Free(w);
  return 0;
}

int testIntersection() {

  InvertedIndex *w = createIndex(100000, 4);
  InvertedIndex *w2 = createIndex(100000, 2);
  IndexReader *r1 = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL);   //
  IndexReader *r2 = NewTermIndexReader(w2, NULL, RS_FIELDMASK_ALL, NULL);  //

  IndexIterator **irs = calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewReadIterator(r2);

  int count = 0;
  IndexIterator *ii = NewIntersecIterator(irs, 2, NULL, RS_FIELDMASK_ALL, -1, 0);

  RSIndexResult *h = NULL;

  TimeSample ts;
  TimeSampler_Start(&ts);
  float topFreq = 0;
  while (ii->Read(ii->ctx, &h) != INDEXREAD_EOF) {
    ASSERT(h->type == RSResultType_Intersection);
    ASSERT(RSIndexResult_IsAggregate(h));
    ASSERT(RSIndexResult_HasOffsets(h));
    topFreq = topFreq > h->freq ? topFreq : h->freq;

    RSIndexResult *copy = IndexResult_DeepCopy(h);
    ASSERT(copy != NULL);
    ASSERT(copy != h);
    ASSERT(copy->isCopy == 1);

    ASSERT(copy->docId == h->docId);
    ASSERT(copy->type == RSResultType_Intersection);

    IndexResult_Free(copy);

    // printf("%d\n", h.docId);
    TimeSampler_Tick(&ts);
    ++count;
  }
  TimeSampler_End(&ts);

  // int count = IR_Intersect(r1, r2, onIntersect, &ctx);

  // printf("%d intersections in %lldms, %.0fns per iteration\n", count,
  // TimeSampler_DurationMS(&ts),
  // 1000000 * TimeSampler_IterationMS(&ts));
  // printf("top freq: %f\n", topFreq);
  ASSERT(count == 50000)
  ASSERT(topFreq == 100000.0);

  ii->Free(ii);
  // IndexResult_Free(&h);
  InvertedIndex_Free(w);
  InvertedIndex_Free(w2);

  return 0;
}

int testBuffer() {
  // TEST_START();

  BufferWriter w = NewBufferWriter(NewBuffer(2));
  ASSERTM(w.buf->cap == 2, "Wrong capacity");
  ASSERT(w.buf->data != NULL);
  ASSERT(Buffer_Offset(w.buf) == 0);
  ASSERT(w.buf->data == w.pos);

  const char *x = "helololoolo";
  size_t l = Buffer_Write(&w, (void *)x, strlen(x) + 1);

  ASSERT(l == strlen(x) + 1);
  ASSERT(Buffer_Offset(w.buf) == l);
  ASSERT_EQUAL(Buffer_Capacity(w.buf), 14);

  l = WriteVarint(1337654, &w);
  ASSERT(l == 3);
  ASSERT_EQUAL(Buffer_Offset(w.buf), 15);
  ASSERT_EQUAL(Buffer_Capacity(w.buf), 17);

  Buffer_Truncate(w.buf, 0);

  ASSERT(Buffer_Capacity(w.buf) == 15);

  BufferReader br = NewBufferReader(w.buf);
  ASSERT(br.pos == 0);

  char *y = malloc(strlen(x) + 1);
  l = Buffer_Read(&br, y, strlen(x) + 1);
  ASSERT(l == strlen(x) + 1);

  ASSERT(strcmp(y, x) == 0);
  ASSERT(BufferReader_Offset(&br) == l);

  free(y);

  int n = ReadVarint(&br);
  ASSERT(n == 1337654);

  Buffer_Free(w.buf);
  free(w.buf);

  return 0;
}

typedef struct {
  int num;
  char **expected;

} tokenContext;

int tokenFunc(void *ctx, const Token *t) {
  tokenContext *tx = ctx;
  int ret = strncmp(t->tok, tx->expected[tx->num++], t->tokLen);
  assert(ret == 0);
  assert(t->pos > 0);
  return 0;
}

// int testTokenize() {
//   char *txt = strdup("Hello? world...   ? -WAZZ@UP? שלום");
//   tokenContext ctx = {0};
//   const char *expected[] = {"hello", "world", "wazz", "up", "שלום"};
//   ctx.expected = (char **)expected;

//   tokenize(txt, &ctx, tokenFunc, NULL, 0, DefaultStopWordList(), 0);
//   ASSERT(ctx.num == 5);

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

int testIndexSpec() {

  const char *title = "title", *body = "body", *foo = "foo", *bar = "bar", *name = "name";
  const char *args[] = {"STOPWORDS", "2",      "hello", "world",    "SCHEMA", title,
                        "text",      "weight", "0.1",   body,       "text",   "weight",
                        "2.0",       foo,      "text",  "sortable", bar,      "numeric",
                        "sortable",  name,     "text",  "nostem"};

  char *err = NULL;

  IndexSpec *s = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  if (err != NULL) {
    FAIL("Error parsing spec: %s", err);
  }
  ASSERT(s != NULL);
  ASSERT(err == NULL);
  ASSERT(s->numFields == 5)

  ASSERT(s->stopwords != NULL);
  ASSERT(s->stopwords != DefaultStopWordList());
  ASSERT(s->flags & Index_StoreFieldFlags);
  ASSERT(s->flags & Index_StoreTermOffsets);
  ASSERT(s->flags & Index_HasCustomStopwords);

  ASSERT(IndexSpec_IsStopWord(s, "hello", 5));
  ASSERT(IndexSpec_IsStopWord(s, "world", 5));
  ASSERT(!IndexSpec_IsStopWord(s, "werld", 5));

  FieldSpec *f = IndexSpec_GetField(s, body, strlen(body));
  ASSERT(f != NULL);
  ASSERT(f->type == FIELD_FULLTEXT);
  ASSERT(strcmp(f->name, body) == 0);
  ASSERT(f->textOpts.weight == 2.0);
  ASSERT_EQUAL(FIELD_BIT(f), 2);
  ASSERT(f->options == 0);
  ASSERT(f->sortIdx == -1);

  f = IndexSpec_GetField(s, title, strlen(title));
  ASSERT(f != NULL);
  ASSERT(f->type == FIELD_FULLTEXT);
  ASSERT(strcmp(f->name, title) == 0);
  ASSERT(f->textOpts.weight == 0.1);
  ASSERT(FIELD_BIT(f) == 1);
  ASSERT(f->options == 0);
  ASSERT(f->sortIdx == -1);

  f = IndexSpec_GetField(s, foo, strlen(foo));
  ASSERT(f != NULL);
  ASSERT(f->type == FIELD_FULLTEXT);
  ASSERT(strcmp(f->name, foo) == 0);
  ASSERT(f->textOpts.weight == 1);
  ASSERT(FIELD_BIT(f) == 4);
  ASSERT(f->options == FieldSpec_Sortable);
  ASSERT(f->sortIdx == 0);

  f = IndexSpec_GetField(s, bar, strlen(bar));
  ASSERT(f != NULL);
  ASSERT(f->type == FIELD_NUMERIC);

  ASSERT(strcmp(f->name, bar) == 0);
  ASSERT(f->textOpts.weight == 0);
  ASSERT(FIELD_BIT(f) == 1);
  ASSERT(f->options == FieldSpec_Sortable);
  ASSERT(f->sortIdx == 1);
  ASSERT(IndexSpec_GetField(s, "fooz", 4) == NULL)

  f = IndexSpec_GetField(s, name, strlen(name));
  ASSERT(f != NULL);
  ASSERT(f->type == FIELD_FULLTEXT);
  ASSERT(strcmp(f->name, name) == 0);
  ASSERT(f->textOpts.weight == 1);
  ASSERT(FIELD_BIT(f) == 8);
  ASSERT(f->options == FieldSpec_NoStemming);
  ASSERT(f->sortIdx == -1);

  ASSERT(s->sortables != NULL);
  ASSERT(s->sortables->len == 2);
  int rc = IndexSpec_GetFieldSortingIndex(s, foo, strlen(foo));
  ASSERT_EQUAL(0, rc);
  rc = IndexSpec_GetFieldSortingIndex(s, bar, strlen(bar));
  ASSERT_EQUAL(1, rc);
  rc = IndexSpec_GetFieldSortingIndex(s, title, strlen(title));
  ASSERT_EQUAL(-1, rc);

  IndexSpec_Free(s);

  const char *args2[] = {
      "NOOFFSETS", "NOFIELDS", "SCHEMA", title, "text",
  };
  s = IndexSpec_Parse("idx", args2, sizeof(args2) / sizeof(const char *), &err);
  if (err != NULL) {
    FAIL("Error parsing spec: %s", err);
  }
  ASSERT(s != NULL);
  ASSERT(err == NULL);
  ASSERT(s->numFields == 1);

  ASSERT(!(s->flags & Index_StoreFieldFlags));
  ASSERT(!(s->flags & Index_StoreTermOffsets));
  IndexSpec_Free(s);

  // User-reported bug
  const char *args3[] = {"mySpec", "SCHEMA", "ha", "NUMERIC", "hb",
                         "TEXT",   "WEIGHT", "1",  "NOSTEM"};
  s = IndexSpec_Parse("idx", args3, sizeof(args3) / sizeof(args3[0]), &err);
  if (err != NULL) {
    FAIL("Error parsing field spec: %s", err);
  }
  ASSERT(FieldSpec_IsNoStem(s->fields + 1));
  IndexSpec_Free(s);

  return 0;
}

void fillSchema(char **args, int N, int *sz) {
  args[0] = "mySpec";
  args[1] = "SCHEMA";
  int n = 2;
  for (int i = 0; i < N; i++) {
    asprintf(&args[n++], "field%d", i);
    if (i % 2 == 0) {
      args[n++] = "TEXT";
    } else {
      if (i < 40) {
        // odd fields under 40 are TEXT noINDEX
        args[n++] = ("TEXT");
        args[n++] = ("NOINDEX");
      } else {
        // the rest are numeric
        args[n++] = ("NUMERIC");
      }
    }
  }
  *sz = n;

  for (int i = 0; i < n; i++) {
    printf("%s ", args[i]);
  }
  printf("\n");
}

int testHugeSpec() {
  int N = 64;
  int n = 2;
  char *args[n + N * 3];
  fillSchema(args, N, &n);

  char *err = NULL;

  IndexSpec *s = IndexSpec_Parse("idx", (const char **)args, n, &err);
  if (err != NULL) {
    FAIL("Error parsing spec: %s", err);
  }
  ASSERT(s != NULL);

  ASSERT(err == NULL);
  ASSERT(s->numFields == N);
  IndexSpec_Free(s);

  // test too big a schema
  N = 300;
  n = 2;
  char *args2[n + N * 3];

  fillSchema(args2, N, &n);

  err = NULL;

  s = IndexSpec_Parse("idx", (const char **)args2, n, &err);
  ASSERT(s == NULL);
  ASSERT(err != NULL);
  ASSERT_STRING_EQ("Too many TEXT fields in schema", err);
  return 0;
}

typedef union {

  int i;
  float f;
} u;

int testIndexFlags() {

  ForwardIndexEntry h;
  h.docId = 1234;
  h.fieldMask = 0x01;
  h.freq = 1;
  h.vw = NewVarintVectorWriter(8);
  for (int n = 0; n < 10; n++) {
    VVW_Write(h.vw, n);
  }
  VVW_Truncate(h.vw);

  u_char flags = INDEX_DEFAULT_FLAGS;
  InvertedIndex *w = NewInvertedIndex(flags, 1);
  IndexEncoder enc = InvertedIndex_GetEncoder(w->flags);
  ASSERT(w->flags == flags);
  size_t sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  // printf("written %zd bytes. Offset=%zd\n", sz, h.vw->buf.offset);
  ASSERT_EQUAL(16, sz);
  InvertedIndex_Free(w);

  flags &= ~Index_StoreTermOffsets;
  w = NewInvertedIndex(flags, 1);
  ASSERT(!(w->flags & Index_StoreTermOffsets));
  enc = InvertedIndex_GetEncoder(w->flags);
  size_t sz2 = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  // printf("Wrote %zd bytes. Offset=%zd\n", sz2, h.vw->buf.offset);
  ASSERT_EQUAL(sz2, sz - Buffer_Offset(&h.vw->buf) - 1);
  InvertedIndex_Free(w);

  flags = INDEX_DEFAULT_FLAGS | Index_WideSchema;
  w = NewInvertedIndex(flags, 1);
  ASSERT((w->flags & Index_WideSchema));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;

  ASSERT_EQUAL(22, InvertedIndex_WriteForwardIndexEntry(w, enc, &h));
  InvertedIndex_Free(w);

  flags |= Index_WideSchema;
  w = NewInvertedIndex(flags, 1);
  ASSERT((w->flags & Index_WideSchema));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQUAL(22, sz);
  InvertedIndex_Free(w);

  flags &= Index_StoreFreqs;
  w = NewInvertedIndex(flags, 1);
  ASSERT(!(w->flags & Index_StoreTermOffsets));
  ASSERT(!(w->flags & Index_StoreFieldFlags));
  enc = InvertedIndex_GetEncoder(w->flags);
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQUAL(4, sz);
  InvertedIndex_Free(w);

  flags |= Index_StoreFieldFlags | Index_WideSchema;
  w = NewInvertedIndex(flags, 1);
  ASSERT((w->flags & Index_WideSchema));
  ASSERT((w->flags & Index_StoreFieldFlags));
  enc = InvertedIndex_GetEncoder(w->flags);
  h.fieldMask = 0xffffffffffff;
  sz = InvertedIndex_WriteForwardIndexEntry(w, enc, &h);
  ASSERT_EQUAL(11, sz);
  InvertedIndex_Free(w);

  VVW_Free(h.vw);

  return 0;
}

int testDocTable() {

  char buf[16];
  DocTable dt = NewDocTable(10);
  t_docId did = 0;
  int N = 100;
  for (int i = 0; i < N; i++) {
    sprintf(buf, "doc_%d", i);
    t_docId nd = DocTable_Put(&dt, buf, (double)i, Document_DefaultFlags, buf, strlen(buf));
    ASSERT_EQUAL(did + 1, nd);
    did = nd;
  }

  ASSERT_EQUAL(N + 1, dt.size);
  ASSERT_EQUAL(N, dt.maxDocId);
  ASSERT(dt.cap > dt.size);
#ifdef __x86_64__
  ASSERT_EQUAL(7580, (int)dt.memsize);
#endif
  for (int i = 0; i < N; i++) {
    sprintf(buf, "doc_%d", i);

    const char *k = DocTable_GetKey(&dt, i + 1);
    ASSERT_STRING_EQ(k, buf);

    float score = DocTable_GetScore(&dt, i + 1);
    ASSERT_EQUAL((int)score, i);

    RSDocumentMetadata *dmd = DocTable_Get(&dt, i + 1);
    ASSERT(dmd != NULL);
    ASSERT(dmd->flags & Document_HasPayload);
    ASSERT_STRING_EQ((char *)dmd->key, (char *)buf);
    char *pl = dmd->payload->data;
    ASSERT(!(strncmp(pl, (char *)buf, dmd->payload->len)));

    ASSERT_EQUAL((int)dmd->score, i);
    ASSERT_EQUAL((int)dmd->flags, (int)(Document_DefaultFlags | Document_HasPayload));

    t_docId xid = DocIdMap_Get(&dt.dim, buf);

    ASSERT_EQUAL((int)xid, i + 1);

    int rc = DocTable_Delete(&dt, dmd->key);
    ASSERT_EQUAL(1, rc);
    ASSERT((int)(dmd->flags & Document_Deleted));
  }

  ASSERT(0 == DocIdMap_Get(&dt.dim, "foo bar"));

  ASSERT(NULL == DocTable_Get(&dt, N + 2));
  DocTable_Free(&dt);
  return 0;
}

int testSortable() {
  RSSortingTable *tbl = NewSortingTable(3);
  ASSERT_EQUAL(3, tbl->len);
  SortingTable_SetFieldName(tbl, 0, "foo");
  SortingTable_SetFieldName(tbl, 1, "bar");
  SortingTable_SetFieldName(tbl, 2, "baz");
  SortingTable_SetFieldName(NULL, 2, "baz");

  ASSERT_STRING_EQ("foo", tbl->fields[0]);
  ASSERT_STRING_EQ("bar", tbl->fields[1]);
  ASSERT_STRING_EQ("baz", tbl->fields[2]);
  ASSERT_EQUAL(0, RSSortingTable_GetFieldIdx(tbl, "foo"));
  ASSERT_EQUAL(0, RSSortingTable_GetFieldIdx(tbl, "FoO"));
  ASSERT_EQUAL(-1, RSSortingTable_GetFieldIdx(NULL, "FoO"));

  ASSERT_EQUAL(1, RSSortingTable_GetFieldIdx(tbl, "bar"));
  ASSERT_EQUAL(-1, RSSortingTable_GetFieldIdx(tbl, "barbar"));

  RSSortingVector *v = NewSortingVector(tbl->len);
  ASSERT_EQUAL(v->len, tbl->len);
  char *str = "hello";
  char *masse = "Maße";

  double num = 3.141;
  ASSERT_EQUAL(v->values[0].type, RS_SORTABLE_NIL);
  RSSortingVector_Put(v, 0, str, RS_SORTABLE_STR);
  ASSERT_EQUAL(v->values[0].type, RS_SORTABLE_STR);
  ASSERT_EQUAL(v->values[1].type, RS_SORTABLE_NIL);
  ASSERT_EQUAL(v->values[2].type, RS_SORTABLE_NIL);
  RSSortingVector_Put(v, 1, &num, RS_SORTABLE_NUM);
  ASSERT_EQUAL(v->values[1].type, RS_SORTABLE_NUM);

  RSSortingVector *v2 = NewSortingVector(tbl->len);
  RSSortingVector_Put(v2, 0, masse, RS_SORTABLE_STR);

  /// test string unicode lowercase normalization
  ASSERT_STRING_EQ("masse", v2->values[0].str);

  double s2 = 4.444;
  RSSortingVector_Put(v2, 1, &s2, RS_SORTABLE_NUM);

  RSSortingKey sk = {.index = 0, .ascending = 0};

  int rc = RSSortingVector_Cmp(v, v2, &sk);
  ASSERT(rc > 0);
  sk.ascending = 1;
  rc = RSSortingVector_Cmp(v, v2, &sk);
  ASSERT(rc < 0);
  rc = RSSortingVector_Cmp(v, v, &sk);
  ASSERT_EQUAL(0, rc);

  sk.index = 1;

  rc = RSSortingVector_Cmp(v, v2, &sk);
  ASSERT_EQUAL(-1, rc);
  sk.ascending = 0;
  rc = RSSortingVector_Cmp(v, v2, &sk);
  ASSERT_EQUAL(1, rc);

  SortingTable_Free(tbl);
  SortingVector_Free(v);
  SortingVector_Free(v2);
  return 0;
}

int testVarintFieldMask() {

  t_fieldMask x = 127;
  size_t expected[] = {1, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 19};
  BufferWriter bw = NewBufferWriter(NewBuffer(1));
  for (int i = 0; i < sizeof(t_fieldMask); i++, x |= x << 8) {
    size_t sz = WriteVarintFieldMask(x, &bw);
    ASSERT_EQUAL(expected[i], sz);
    BufferWriter_Seek(&bw, 0);
    BufferReader br = NewBufferReader(bw.buf);

    t_fieldMask y = ReadVarintFieldMask(&br);

    ASSERT(y == x);
  }
  RETURN_TEST_SUCCESS;
}
TEST_MAIN({

  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();
  TESTFUNC(testVarintFieldMask);

  TESTFUNC(testPureNot);
  TESTFUNC(testHugeSpec);

  TESTFUNC(testAbort)
  TESTFUNC(testNumericInverted);
  TESTFUNC(testNumericVaried);
  TESTFUNC(testNumericEncoding);

  TESTFUNC(testVarint);
  TESTFUNC(testDistance);
  TESTFUNC(testIndexReadWrite);

  TESTFUNC(testReadIterator);
  TESTFUNC(testIntersection);
  TESTFUNC(testNot);
  TESTFUNC(testUnion);

  TESTFUNC(testBuffer);
  // TESTFUNC(testTokenize);
  TESTFUNC(testIndexSpec);
  TESTFUNC(testIndexFlags);
  TESTFUNC(testDocTable);
  TESTFUNC(testSortable);
});
