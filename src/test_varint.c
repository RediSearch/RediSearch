#include <stdio.h>
#include "varint.h"
#include "index.h"
#include <time.h>

void testVarint() {
  VarintVectorWriter *vw = NewVarintVectorWriter(8);
  VVW_Write(vw, 100000);
  VVW_Write(vw, 100020);
  VVW_Write(vw, 100100);
  // VVW_Write(vw, 100);
  printf("%ld %ld\n", vw->pos - vw->v, vw->cap);
  VVW_Truncate(vw);

  VarintVectorIterator i = VarIntVector_iter(vw->v);
  int x = 0;
  while (VV_HasNext(&i)) {
    printf("%d %d\n", x++, VV_Next(&i));
  }


  VVW_Free(vw);
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
    h.offsets = vw->v;

    IW_Write(w, &h);
    VVW_Free(vw);
  }

  printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
         w->ndocs);
  IW_Close(w);
  //IW_MakeSkipIndex(w, 10);
  
//   for (int x = 0; x < w->skipIdx.len; x++) {
//     printf("Skip entry %d: %d, %d\n", x, w->skipIdx.entries[x].docId, w->skipIdx.entries[x].offset);
//   }
  printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
         w->ndocs);

  
  IndexHit h;
  int n = 0;

  for (int xx = 0; xx < 1; xx++) {
    IndexReader *ir = NewIndexReader(w->buf, w->cap, &w->skipIdx);
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
    for (int n = 0; n < i % 4; n++) {
      VVW_Write(vw, n);
    }
    VVW_Truncate(vw);
    h.offsets = vw->v;

    IW_Write(w, &h);
    VVW_Free(vw);
    id += idStep;
  }


  IW_Close(w);
  printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w),
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
    for (int i =0; i < argc; i++) {
        if (hits[i].freq > ic->maxFreq) 
            ic->maxFreq = hits[i].freq;
    }
    
    
    return 0;
}

void testUnion() {
    IndexWriter *w = createIndex(10, 2);
    IndexReader *r1 = NewIndexReader(w->buf,  IW_Len(w), &w->skipIdx);
    IndexWriter *w2 = createIndex(10, 3);
    IndexReader *r2 = NewIndexReader(w2->buf,  IW_Len(w2), &w2->skipIdx);
    printf("Reading!\n");
    IndexIterator *irs[] = {NewIndexTerator(r1), NewIndexTerator(r2)};
    IndexIterator *ui = NewUnionIterator(irs, 2);
    
    
    IndexHit h;
    
    while (ui->Read(ui->ctx, &h) != INDEXREAD_EOF) {
        printf("Read %d\n", h.docId);
    }
    IndexIterator_Free(ui);
    
}

void testIntersection() {
    
    IndexWriter *w = createIndex(1000000, 1);
    IndexReader *r1 = NewIndexReader(w->buf,  IW_Len(w), &w->skipIdx);
    IndexWriter *w2 = createIndex(5000, 2);
    IndexReader *r2 = NewIndexReader(w2->buf,  IW_Len(w2), &w2->skipIdx);
    
    IndexWriter *w3 = createIndex(10000, 3);
    IndexReader *r3 = NewIndexReader(w3->buf,  IW_Len(w3), &w3->skipIdx);
    
    IterationContext ctx = {0,0};
    printf ("Intersecting...\n");
    
    
    IndexIterator *irs[] = {NewIndexTerator(r1), NewIndexTerator(r2),NewIndexTerator(r2)};
    
    struct timespec start_time, end_time;
    
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start_time);
    
    int count = IR_Intersect2(irs, 3, onIntersect, &ctx);    
    
    //count = IR_Intersect(r1, r2, onIntersect, &ctx);
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_time);
    long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;

    printf("%d intersections in %ldns\n", ctx.counter, diffInNanos); 
    printf("top freq: %d\n", ctx.maxFreq);
}
int main(int argc, char **argv) {
  
   //testVarint();
  //testIndexReadWrite();
  //testIntersection();
  testUnion();
}