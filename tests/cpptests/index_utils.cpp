#include "index_utils.h"
#include "src/index.h"
#include "src/inverted_index.h"

InvertedIndex *createIndex(int size, int idStep, int start_with) {
    size_t sz;
    InvertedIndex *idx = NewInvertedIndex((IndexFlags)(INDEX_DEFAULT_FLAGS), 1, &sz);

    IndexEncoder enc = InvertedIndex_GetEncoder(idx->flags);
    t_docId id = start_with > 0 ? start_with : idStep;
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
