#include "../buffer.h"
#include "../index.h"
#include "../inverted_index.h"
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

  RSOffsetVector vec = (RSOffsetVector){.data = vw->bw.buf->data, .len = vw->bw.buf->offset};
  // Buffer_Seek(vw->bw.buf, 0);
  RSOffsetIterator it = _offsetVector_iterate(&vec);
  int x = 0;
  uint32_t n = 0;
  while (RS_OFFSETVECTOR_EOF != (n = it.Next(it.ctx))) {

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
  tr1->term.offsets = (RSOffsetVector){.data = vw->bw.buf->data, .len = vw->bw.buf->offset};

  RSIndexResult *tr2 = NewTokenRecord(NULL);
  tr2->docId = 1;
  tr2->term.offsets = (RSOffsetVector){.data = vw2->bw.buf->data, .len = vw2->bw.buf->offset};

  RSIndexResult *res = NewIntersectResult(2);
  AggregateResult_AddChild(res, tr1);
  AggregateResult_AddChild(res, tr2);

  int delta = IndexResult_MinOffsetDelta(res);
  ASSERT_EQUAL(4, delta);

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
  tr3->term.offsets = (RSOffsetVector){.data = vw3->bw.buf->data, .len = vw3->bw.buf->offset};
  AggregateResult_AddChild(res, tr3);

  delta = IndexResult_MinOffsetDelta(res);
  ASSERT_EQUAL(53, delta);

  // test merge iteration
  RSOffsetIterator it = RSIndexResult_IterateOffsets(res);
  uint32_t expected[] = {1, 4, 7, 9, 13, 16, 20, 22, 25, 32, RS_OFFSETVECTOR_EOF};

  uint32_t rc;
  int i = 0;
  do {
    rc = it.Next(it.ctx);
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

int testIndexReadWrite() {

  InvertedIndex *idx = NewInvertedIndex(INDEX_DEFAULT_FLAGS, 1);

  for (int i = 0; i < 200; i++) {
    // if (i % 10000 == 1) {
    //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
    //     w->ndocs);
    // }

    ForwardIndexEntry h;
    h.docId = i;
    h.fieldMask = 1;
    h.freq = (1 + i % 100) / (float)101;
    h.docScore = (1 + (i + 2) % 30) / (float)31;

    h.vw = NewVarintVectorWriter(8);
    for (int n = 0; n < i % 4; n++) {
      VVW_Write(h.vw, n);
    }
    VVW_Truncate(h.vw);

    InvertedIndex_WriteEntry(idx, &h);

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
    IndexReader *ir = NewIndexReader(idx, NULL, RS_FIELDMASK_ALL, INDEX_DEFAULT_FLAGS, NULL, 1);  //
    RSIndexResult *h = NULL;

    int n = 0;
    while (IR_HasNext(ir)) {
      if (IR_Read(ir, &h) == INDEXREAD_EOF) {
        break;
      }
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

InvertedIndex *createIndex(int size, int idStep) {
  InvertedIndex *idx = NewInvertedIndex(INDEX_DEFAULT_FLAGS, 1);

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
    h.docScore = 1;
    h.stringFreeable = 0;
    h.term = "hello";
    h.len = 5;

    h.vw = NewVarintVectorWriter(8);
    for (int n = idStep; n < idStep + i % 4; n++) {
      VVW_Write(h.vw, n);
    }
    InvertedIndex_WriteEntry(idx, &h);
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

  IndexReader *r1 = NewIndexReader(idx, NULL, RS_FIELDMASK_ALL, INDEX_DEFAULT_FLAGS, NULL, 0);
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
  IndexReader *r1 = NewIndexReader(w, NULL, RS_FIELDMASK_ALL, w->flags, NULL, 0);
  IndexReader *r2 = NewIndexReader(w2, NULL, RS_FIELDMASK_ALL, w2->flags, NULL, 0);

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
  IndexReader *r1 = NewIndexReader(w, NULL, RS_FIELDMASK_ALL, w->flags, NULL, 0);
  IndexReader *r2 = NewIndexReader(w2, NULL, RS_FIELDMASK_ALL, w2->flags, NULL, 0);

  // printf("Reading!\n");
  IndexIterator **irs = calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewNotIterator(NewReadIterator(r2));

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

int testOptional() {
  InvertedIndex *w = createIndex(16, 1);
  // not all numbers that divide by 3
  InvertedIndex *w2 = createIndex(10, 3);
  IndexReader *r1 = NewIndexReader(w, NULL, RS_FIELDMASK_ALL, w->flags, NULL, 0);
  IndexReader *r2 = NewIndexReader(w2, NULL, RS_FIELDMASK_ALL, w2->flags, NULL, 0);

  // printf("Reading!\n");
  IndexIterator **irs = calloc(2, sizeof(IndexIterator *));
  irs[0] = NewReadIterator(r1);
  irs[1] = NewOptionalIterator(NewReadIterator(r2));

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

int testIntersection() {

  InvertedIndex *w = createIndex(100000, 4);
  InvertedIndex *w2 = createIndex(100000, 2);
  IndexReader *r1 = NewIndexReader(w, NULL, RS_FIELDMASK_ALL, w->flags, NULL, 0);
  IndexReader *r2 = NewIndexReader(w2, NULL, RS_FIELDMASK_ALL, w2->flags, NULL, 0);

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

int tokenFunc(void *ctx, Token t) {
  tokenContext *tx = ctx;
  int ret = strcmp(t.s, tx->expected[tx->num++]);
  assert(ret == 0);
  assert(t.len == strlen(t.s));
  assert(t.fieldId == 1);
  assert(t.pos > 0);
  assert(t.score == 1);
  return 0;
}

int testTokenize() {
  char *txt = strdup("Hello? world...   ? -WAZZ@UP? שלום");
  tokenContext ctx = {0};
  const char *expected[] = {"hello", "world", "wazz", "up", "שלום"};
  ctx.expected = (char **)expected;

  tokenize(txt, 1, 1, &ctx, tokenFunc, NULL, 0, DefaultStopWordList());
  ASSERT(ctx.num == 5);

  free(txt);

  return 0;
}

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

  const char *title = "title", *body = "body", *foo = "foo", *bar = "bar";
  const char *args[] = {"STOPWORDS", "2",        "hello", "world",   "SCHEMA",  title, "text",
                        "weight",    "0.1",      body,    "text",    "weight",  "2.0", foo,
                        "text",      "sortable", bar,     "numeric", "sortable"};

  char *err = NULL;

  IndexSpec *s = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  if (err != NULL) {
    FAIL("Error parsing spec: %s", err);
  }
  ASSERT(s != NULL);
  ASSERT(err == NULL);
  ASSERT(s->numFields == 4)

  ASSERT(s->stopwords != NULL);
  ASSERT(s->stopwords != DefaultStopWordList());
  ASSERT(s->flags & Index_StoreScoreIndexes);
  ASSERT(s->flags & Index_StoreFieldFlags);
  ASSERT(s->flags & Index_StoreTermOffsets);
  ASSERT(s->flags & Index_HasCustomStopwords);

  ASSERT(IndexSpec_IsStopWord(s, "hello", 5));
  ASSERT(IndexSpec_IsStopWord(s, "world", 5));
  ASSERT(!IndexSpec_IsStopWord(s, "werld", 5));

  FieldSpec *f = IndexSpec_GetField(s, body, strlen(body));
  ASSERT(f != NULL);
  ASSERT(f->type == F_FULLTEXT);
  ASSERT(strcmp(f->name, body) == 0);
  ASSERT(f->weight == 2.0);
  ASSERT(f->id == 2);
  ASSERT(f->sortable == 0);
  ASSERT(f->sortIdx == -1);

  f = IndexSpec_GetField(s, title, strlen(title));
  ASSERT(f != NULL);
  ASSERT(f->type == F_FULLTEXT);
  ASSERT(strcmp(f->name, title) == 0);
  ASSERT(f->weight == 0.1);
  ASSERT(f->id == 1);
  ASSERT(f->sortable == 0);
  ASSERT(f->sortIdx == -1);

  f = IndexSpec_GetField(s, foo, strlen(foo));
  ASSERT(f != NULL);
  ASSERT(f->type == F_FULLTEXT);
  ASSERT(strcmp(f->name, foo) == 0);
  ASSERT(f->weight == 1);
  ASSERT(f->id == 4);
  ASSERT(f->sortable == 1);
  ASSERT(f->sortIdx == 0);

  f = IndexSpec_GetField(s, bar, strlen(bar));
  ASSERT(f != NULL);
  ASSERT(f->type == F_NUMERIC);
  ASSERT(strcmp(f->name, bar) == 0);
  ASSERT(f->weight == 0);
  ASSERT(f->id == 0);
  ASSERT(f->sortable == 1);
  ASSERT(f->sortIdx == 1);
  ASSERT(IndexSpec_GetField(s, "fooz", 4) == NULL)

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
      "NOOFFSETS", "NOFIELDS", "NOSCOREIDX", "SCHEMA", title, "text",
  };
  s = IndexSpec_Parse("idx", args2, sizeof(args2) / sizeof(const char *), &err);
  if (err != NULL) {
    FAIL("Error parsing spec: %s", err);
  }
  ASSERT(s != NULL);
  ASSERT(err == NULL);
  ASSERT(s->numFields == 1);

  ASSERT(!(s->flags & Index_StoreScoreIndexes));
  ASSERT(!(s->flags & Index_StoreFieldFlags));
  ASSERT(!(s->flags & Index_StoreTermOffsets));
  IndexSpec_Free(s);

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
  h.docScore = 100;
  h.vw = NewVarintVectorWriter(8);
  for (int n = 0; n < 10; n++) {
    VVW_Write(h.vw, n);
  }
  VVW_Truncate(h.vw);

  u_char flags = INDEX_DEFAULT_FLAGS;
  InvertedIndex *w = NewInvertedIndex(flags, 1);

  ASSERT(w->flags == flags);
  size_t sz = InvertedIndex_WriteEntry(w, &h);
  // printf("written %d bytes\n", sz);
  ASSERT_EQUAL(16, sz);
  InvertedIndex_Free(w);

  flags &= ~Index_StoreTermOffsets;
  w = NewInvertedIndex(flags, 1);
  ASSERT(!(w->flags & Index_StoreTermOffsets));
  size_t sz2 = InvertedIndex_WriteEntry(w, &h);
  ASSERT_EQUAL(sz2, sz - Buffer_Offset(h.vw->bw.buf) - 1);
  InvertedIndex_Free(w);

  flags &= ~Index_StoreFieldFlags;
  w = NewInvertedIndex(flags, 1);
  ASSERT(!(w->flags & Index_StoreTermOffsets));
  ASSERT(!(w->flags & Index_StoreFieldFlags));
  sz = InvertedIndex_WriteEntry(w, &h);
  ASSERT_EQUAL(4, sz);
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
  ASSERT_EQUAL(6780, (int)dt.memsize);

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
  ASSERT_STRING_EQ("foo", tbl->fields[0]);
  ASSERT_STRING_EQ("bar", tbl->fields[1]);
  ASSERT_STRING_EQ("baz", tbl->fields[2]);
  ASSERT_EQUAL(0, RSSortingTable_GetFieldIdx(tbl, "foo"));
  ASSERT_EQUAL(0, RSSortingTable_GetFieldIdx(tbl, "FoO"));

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

TEST_MAIN({

  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();

  TESTFUNC(testVarint);
  TESTFUNC(testDistance);
  TESTFUNC(testIndexReadWrite);

  TESTFUNC(testReadIterator);
  TESTFUNC(testIntersection);
  TESTFUNC(testNot);
  TESTFUNC(testUnion);

  TESTFUNC(testBuffer);
  TESTFUNC(testTokenize);
  TESTFUNC(testIndexSpec);
  TESTFUNC(testIndexFlags);
  TESTFUNC(testDocTable);
  TESTFUNC(testSortable);
});