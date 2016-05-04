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
        
        BufferRead(b, &w.header, sizeof(ScoreIndexHeader));
        BufferSeek(b, sizeof(ScoreIndexHeader) + w.header.numEntries*sizeof(ScoreIndexEntry));     
    } else {
        w.header.lowestIndex = 0;
        w.header.lowestScore = 0;
        w.header.numEntries = 0;
        
        bw.Write(b, &w.header, sizeof(ScoreIndexHeader));
    }
    
    return w;
}

void ScoreIndexWriter_Terminate(ScoreIndexWriter w) {
    w.bw.Release(w.bw.buf);
}

ScoreIndexEntry *ScoreIndex_Next(ScoreIndex *si) {
    if (si == NULL) {
        return NULL;
    }
    
    if (si->offset < si->header.numEntries) {
        return &si->entries[si->offset++];
    }
    return NULL;
}

ScoreIndex *NewScoreIndex(Buffer *b) {
    
    BufferSeek(b, 0);
    ScoreIndex *si = malloc(sizeof(ScoreIndex));
    BufferRead(b, &si->header, sizeof(ScoreIndexHeader));
    si->entries = (ScoreIndexEntry*)b->pos;
    si->offset = 0;
    return si;
}

void ScoreIndex_Free(ScoreIndex *si) {
    free(si);
}

int ScoreIndexWriter_AddEntry(ScoreIndexWriter *w, float score, t_offset offset, t_docId docId) {
    Buffer *b = w->bw.buf;
    //printf("Adding size %d buffer cap %zd. lowest score :%f, lowest index: %d\n", w->header.numEntries, b->cap, w->header.lowestScore, w->header.lowestIndex);
    // If the index is not at full capacity - we just append to it
    if (w->header.numEntries < MAX_SCOREINDEX_SIZE) {
        if (w->header.numEntries == 0 || score < w->header.lowestScore) {
            w->header.lowestScore = score;
            w->header.lowestIndex = 0;
            
        }
        w->header.numEntries++;
        
        size_t bo = BufferOffset(b);
        BufferSeek(b, 0);
        w->bw.Write(b, &w->header, sizeof(ScoreIndexHeader));
        
        if (bo > 0) {
            BufferSeek(b, bo);
        }
        
        ScoreIndexEntry ent = {offset, score, docId};
        
        w->bw.Write(w->bw.buf, &ent, sizeof(ScoreIndexEntry));
        
        return 1;
        
    } else if (w->header.lowestScore < score) {
        
        // If the index is already at full capacity, we know the lowest scored element
        // so replacing it is  O(1). Then we just need to mark the new lowest element
        // which is O(n), but since n is small and constant, this can be viewed as O(1)
        
        ScoreIndexEntry *entries = (void*)b->data + sizeof(ScoreIndexHeader);
        entries[w->header.lowestIndex].offset = offset;
        entries[w->header.lowestIndex].score = score;
        entries[w->header.lowestIndex].docId = docId;
        
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
        w->bw.Write(b, &w->header, sizeof(ScoreIndexHeader));
        
        if (bo > 0) {
            BufferSeek(b, bo);
        }
        return 1;
    }
    
    return 0;
}


