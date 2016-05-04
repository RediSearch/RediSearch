#include "score_index.h"


static inline int ScoreEntry_cmp(const void *e1,  const void *e2, const void *udata) {
    const ScoreIndexEntry *h1 = e1, *h2 = e2;
    if (h1->score < h2->score) {
        return -1;
    } else if (h1->score > h2->score) {
        return 1;
    }
    return 0;
} 


ScoreIndexWriter NewScoreIndexWriter(BufferWriter bw) {
    Buffer *b = bw.buf;
    ScoreIndexWriter w;
    w.bw = bw;
    if (b->cap > sizeof(ScoreIndexHeader)) {
        size_t bo = BufferOffset(b);
       
        BufferSeek(b, 0);
        BufferRead(b, &w.header, sizeof(ScoreIndexHeader));
        if (bo > 0) {
            BufferSeek(b, bo);
        }     
    } else {
        w.header.lowestIndex = 0;
        w.header.lowestScore = 0;
        w.header.numEntries = 0;
    }
    
    return w;
}

ScoreIndex NewScoreIndex(Buffer *b) {
    
    BufferSeek(b, 0);
    ScoreIndex si;
    BufferRead(b, &si.header, sizeof(ScoreIndexHeader));
    si.entries = (ScoreIndexEntry*)b->pos;
    
    return si;
}

int ScoreIndexWriter_AddEntry(ScoreIndexWriter *w, float score, t_offset offset) {
    Buffer *b = w->bw.buf;
    //printf("Adding %f. lowest score :%f, lowest index: %d\n", score, w->header.lowestScore, w->header.lowestIndex);
    if (w->header.numEntries < MAX_SCOREINDEX_SIZE) {
        if (w->header.numEntries == 0 || score < w->header.lowestScore) {
            w->header.lowestScore = score;
            w->header.lowestIndex = 0;
            //printf("Added. lowest score :%f, lowest index: %d\n", w->header.lowestScore, w->header.lowestIndex);
        }
        w->header.numEntries++;
        
        size_t bo = BufferOffset(b);
        BufferSeek(b, 0);
        w->bw.Write(b, &w->header, sizeof(w->header));
        
        if (bo > 0) {
            BufferSeek(b, bo);
        }
        
        ScoreIndexEntry ent = {offset, score};
        
        w->bw.Write(w->bw.buf, &ent, sizeof(ent));
        
        return 1;
        
    } else if (w->header.lowestScore < score) {
        
        ScoreIndexEntry *entries = (void*)b->data + sizeof(w->header);
        printf("Replacing. lowest score :%f, lowest index: %d\n", w->header.lowestScore, w->header.lowestIndex);
        entries[w->header.lowestIndex].offset = offset;
        entries[w->header.lowestIndex].score = score;
        
        w->header.lowestScore = score;
        
        // find the new lowest entry and mark it in the header
        for (int i = 0; i < w->header.numEntries; i++) {
            if (entries[i].score < w->header.lowestScore) {
                w->header.lowestScore = entries[i].score;
                w->header.lowestIndex = i;
            }
        }
        
        size_t bo = BufferOffset(b);
        BufferSeek(b, 0);
        w->bw.Write(b, &w->header, sizeof(w->header));
        
        if (bo > 0) {
            BufferSeek(b, bo);
        }
        return 1;
    }
    
    return 0;
}


