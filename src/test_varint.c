#include <stdio.h>
#include "varint.h"
#include "index.h"
#include <sys/time.h>


void testVarint() {
    VarintVectorWriter *vw = NewVarintVectorWriter(8);
    VVW_Write(vw, 1);
    VVW_Write(vw, 2);
    VVW_Write(vw, 3);
    //VVW_Write(vw, 100);
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
    
    IndexWriter *w = NewIndexWriter(1000);
    
    for (int i =0 ; i < 1000000; i++) {
        // if (i % 10000 == 1) {
        //     printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w), w->ndocs);
        // }
        IndexHit h;
        h.docId = i;
        h.flags = 0;
        h.freq = i % 10;
        
        VarintVectorWriter *vw = NewVarintVectorWriter(8);
        for (int n =0; n < i % 4; n++) {
            VVW_Write(vw, n);
        }
        VVW_Truncate(vw);
        h.offsets = vw->v;
        
        IW_Write(w, &h);
        VVW_Free(vw);
    }
    
    printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w), w->ndocs);
    IW_Close(w);
    printf("iw cap: %ld, iw size: %d, numdocs: %d\n", w->cap, IW_Len(w), w->ndocs);
    
    IndexReader *ir = NewIndexReader(w->buf, w->cap);
    IndexHit h;
    int n = 0;
    
    
    struct timeval tval_before, tval_after, tval_result;

    gettimeofday(&tval_before, NULL);
    
    while (IR_Next(ir) != INDEXREAD_EOF) {
        n++;
       
        // if (n++ % 10000 == 0) {
        //     printf("%d\n", h.docId);
        // }
    }
    gettimeofday(&tval_after, NULL);

    timersub(&tval_after, &tval_before, &tval_result);

    printf("Time elapsed: %ld.%06ld\n", (long int)tval_result.tv_sec, (long int)tval_result.tv_usec);
    
    
    
    
    
}
int main(int argc, char **argv) {
    //testVarint();
    testIndexReadWrite();

}