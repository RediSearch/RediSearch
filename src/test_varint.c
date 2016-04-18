#include <stdio.h>
#include "varint.h"
#include "index.h"
#include "buffer.h"
#include <assert.h>
#include <math.h>
#include <time.h>

int testVarint() {
  VarintVectorWriter *vw = NewVarintVectorWriter(8);
  VVW_Write(vw, 100000);
  VVW_Write(vw, 100020);
  VVW_Write(vw, 100100);
  // VVW_Write(vw, 100);
  printf("%ld %ld\n", BufferLen(vw->bw.buf), vw->bw.buf->cap);
  VVW_Truncate(vw);

  VarintVectorIterator i = VarIntVector_iter(vw->v);
  int x = 0;
  while (VV_HasNext(&i)) {
    printf("%d %d\n", x++, VV_Next(&i));
  }


  VVW_Free(vw);
  return 0;
}

int testDistance() {
    VarintVectorWriter *vw = NewVarintVectorWriter(8);
     VarintVectorWriter *vw2 = NewVarintVectorWriter(8);
    VVW_Write(vw, 1);
    VVW_Write(vw2, 4);
    
    //VVW_Write(vw, 9);
    VVW_Write(vw2, 7);
    
    VVW_Write(vw, 9);
    VVW_Write(vw, 13);
    
    VVW_Write(vw, 16);
    VVW_Write(vw, 22);
    VVW_Truncate(vw);
    VVW_Truncate(vw2);
    
    VarintVector *v[2] = {vw->v, vw2->v};
    int delta = VV_MinDistance(v, 2);
    printf("%d\n", delta);
    
    VVW_Free(vw);
    VVW_Free(vw2);
    
    
    return 0;
    
    
}



void testIndexReadWrite() {
  IndexWriter *w = NewIndexWriter(10000);

  for (int i = 0; i < 1000000; i++) {
    // if (i % 10000 == 1) {
    //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
    //     w->ndocs);
    // }
    IndexHit h;
    h.docId = i;
    h.flags = 0;
    h.freq = i % 10;

    VarintVectorWriter *vw = NewVarintVectorWriter(8);
    for (int n = 0; n < i % 4; n++) {
      VVW_Write(vw, n);
    }
    VVW_Truncate(vw);
    h.offsets = *vw->v;

    IW_Write(w, &h);
    VVW_Free(vw);
  }

  printf("iw cap: %ld, iw size: %ld, numdocs: %d\n", w->bw.buf->cap, IW_Len(w),
         w->ndocs);
  IW_Close(w);
  //IW_MakeSkipIndex(w, 10);
  
//   for (int x = 0; x < w->skipIdx.len; x++) {
//     printf("Skip entry %d: %d, %d\n", x, w->skipIdx.entries[x].docId, w->skipIdx.entries[x].offset);
//   }
  printf("iw cap: %ld, iw size: %ld, numdocs: %d\n", w->bw.buf->cap, IW_Len(w),
         w->ndocs);

  
  IndexHit h;
  int n = 0;

  for (int xx = 0; xx < 1; xx++) {
    IndexReader *ir = NewIndexReader(w->bw.buf->data, w->bw.buf->cap, &w->skipIdx);
    IndexHit h;
    int n = 0;
    

    struct timespec start_time, end_time;
    for (int z= 0; z < 10; z++) {
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_time);

    IR_SkipTo(ir, 900001, &h);
    
    
    
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_time);
    long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;
    

    printf("Time elapsed: %ldnano\n", diffInNanos);
    }
  }
  IW_Free(w);
}

IndexWriter *createIndex(int size, int idStep) {
   IndexWriter *w = NewIndexWriter(100);
   
  t_docId id = idStep;
  for (int i = 0; i < size; i++) {
    // if (i % 10000 == 1) {
    //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
    //     w->ndocs);
    // }
    IndexHit h;
    h.docId = id;
    h.flags = 0;
    h.freq = i % 10;

    VarintVectorWriter *vw = NewVarintVectorWriter(8);
    for (int n = idStep; n < idStep + i % 4; n++) {
      VVW_Write(vw, n);
    }
    VVW_Truncate(vw);
    h.offsets = *vw->bw.buf;

    IW_Write(w, &h);
    VVW_Free(vw);
    id += idStep;
  }


printf("BEFORE: iw cap: %ld, iw size: %d, numdocs: %d\n", w->bw.buf->cap, IW_Len(w),
         w->ndocs);
  IW_Close(w);
  printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->bw.buf->cap, IW_Len(w),
         w->ndocs);
  return w;
}


typedef struct {
    int maxFreq;
    int counter;
} IterationContext;

int onIntersect(void *ctx, IndexHit *hits, int argc) {
    
    //printf("%d\n", hits[0].docId);
    IterationContext *ic = ctx;
    ++ic->counter;
    VarintVector *viv[argc];
    double score = 0;
    for (int i =0; i < argc; i++) {
        viv[i] = &hits[i].offsets;
        score += log((double)hits[i].freq+2);
        
        
        // if (hits[i].freq > ic->maxFreq) 
        //     ic->maxFreq = hits[i].freq;
    }
    
    
    
    int dist = VV_MinDistance(viv, argc);
    //score /= pow ((double)(dist+1), 2.0);
    //printf("%lf %d %lf\n", score, dist, score/pow ((double)(dist+1), 2.0) );
    return 0;
}

int printIntersect(void *ctx, IndexHit *hits, int argc) {
    
    printf("%d\n", hits[0].docId);
    return 0;
}


void testUnion() {
    IndexWriter *w = createIndex(20, 1);
    IndexReader *r1 = NewIndexReader(w->bw.buf->data,  IW_Len(w), &w->skipIdx);
    IndexWriter *w2 = createIndex(10, 2);
    IndexReader *r2 = NewIndexReader(w2->bw.buf->data , IW_Len(w2), &w2->skipIdx);
    printf("Reading!\n");
    IndexIterator *irs[] = {NewIndexTerator(r1), NewIndexTerator(r2)};
    IndexIterator *ui = NewUnionIterator(irs, 2);
    
    IndexWriter *w3 = createIndex(30, 5);
    IndexReader *r3 = NewIndexReader(w3->bw.buf->data,  IW_Len(w3), &w3->skipIdx);
    IndexHit h;
    
    IndexIterator *irs2[] = {ui, NewIndexTerator(r3)};
    // while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
    //     printf("Read %d\n", h.docId);
    // }
     IterationContext ctx = {0,0};
    int count = IR_Intersect2(irs2, 2, printIntersect, &ctx);
    IndexIterator_Free(ui);
    
}

void testIntersection() {
    
    IndexWriter *w = createIndex(1000000, 2);
    IndexReader *r1 = NewIndexReader(w->bw.buf->data,  IW_Len(w), &w->skipIdx);
    IndexWriter *w2 = createIndex(1000000, 4);
    IndexReader *r2 = NewIndexReader(w2->bw.buf->data,  IW_Len(w2), &w2->skipIdx);
    
    // IndexWriter *w3 = createIndex(10000, 3);
    // IndexReader *r3 = NewIndexReader(w3->bw.buf->data,  IW_Len(w3), &w3->skipIdx);
    
    IterationContext ctx = {0,0};
    
    
    IndexIterator *irs[] = {NewIndexTerator(r1), NewIndexTerator(r2)};//,NewIndexTerator(r2)};
    
    printf ("Intersecting...\n");
    
    struct timespec start_time, end_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    
    int count = IR_Intersect2(irs, 2, onIntersect, &ctx);    
    
    //int count = IR_Intersect(r1, r2, onIntersect, &ctx);
    clock_gettime(CLOCK_REALTIME, &end_time);
    long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;

    printf("%d intersections in %ldns\n", ctx.counter, diffInNanos); 
    printf("top freq: %d\n", ctx.maxFreq);
}

#define TESTFUNC(f) printf("Testing %s ...\t", __STRING(f)); fflush(stdout); printf("%s\n\n", f() == 0 ? "PASS" : "FAIL")
#define ASSERT(expr, ...) if (!(expr)) { fprintf (stderr, "Assertion '%s' Failed: " __VA_ARGS__ "\n", __STRING(expr)); return -1; }
#define ASSERT_EQUAL_INT(x,y,...) if (x!=y) { fprintf (stderr, "%d != %d: " __VA_ARGS__ "\n", x, y); return -1; }
                    
#define TEST_START() printf("Testing %s... ",  __FUNCTION__); fflush(stdout);
#define TEST_END() printf ("PASS!");

int testMemBuffer() {
    //TEST_START();
    
    BufferWriter w = NewBufferWriter(2);
    ASSERT( w.buf->cap == 2, "Wrong capacity");
    ASSERT (w.buf->data != NULL);
    ASSERT( BufferLen(w.buf) == 0);
    ASSERT( w.buf->data == w.buf->pos );
    return 0;
    
    const char *x = "helo";
    size_t l = w.Write(w.buf, (void*)x, strlen(x)+1);
    
    ASSERT( l == strlen(x)+1);
    ASSERT( BufferLen(w.buf) == l);
    ASSERT (w.buf->cap == 8);
    
    l = WriteVarint(1337, &w);
    ASSERT (l == 2);
    ASSERT( BufferLen(w.buf) == 7);
    ASSERT( w.buf->cap == 8);
    
    w.Truncate(w.buf, 0);
    ASSERT( w.buf->cap == 7);
    
    BufferReader r = NewBufferReader(w.buf->data, w.buf->cap);
    ASSERT(r.buf->cap = w.buf->cap);
    ASSERT(r.buf->data = w.buf->data);
    ASSERT(r.buf->pos == r.buf->data);
    
    
    char *y = malloc(5);
    l = r.Read(r.buf, y, 5);
    ASSERT(l == l);
    
    ASSERT( strcmp(y, "helo") == 0 );
    ASSERT( BufferLen(r.buf) == 5);
    
    free(y);
    
    int n = ReadVarint(&r);
    ASSERT (n == 1337);
    
    w.Release(w.buf);
    //TEST_END();
    return 0;
}


int main(int argc, char **argv) {
  
   //TESTFUNC(testVarint);
  // TESTFUNC(testDistance);
  //testIndexReadWrite();
  testIntersection();
  //testUnion();
  
  //TESTFUNC(testMemBuffer);
  
  return 0;
}