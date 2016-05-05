#include <stdio.h>
#include <sys/param.h>
#include "forward_index.h"
#include "tokenize.h"
#include "util/logging.h"


ForwardIndex *NewForwardIndex(t_docId docId, float docScore) {
    
    ForwardIndex *idx = malloc(sizeof(ForwardIndex));
    
    idx->hits = kh_init(32);
    idx->docScore = docScore;
    idx->docId = docId;
    idx->totalFreq = 0;
    idx->maxFreq = 0;
    
    return idx;
}

void ForwardIndexFree(ForwardIndex *idx) {
   khiter_t k;
   for (k = kh_begin(idx->hits); k != kh_end(idx->hits); ++k) {
      if (kh_exist(idx->hits, k)) {
         ForwardIndexEntry *ent = kh_value(idx->hits , k);
         kh_del(32, idx->hits, k);
         VVW_Free(ent->vw);
         free(ent);
      }
   }
    kh_destroy(32, idx->hits);
    free(idx);
    //TODO: check if we need to free each entry separately
}


void ForwardIndex_NormalizeFreq(ForwardIndex *idx, ForwardIndexEntry *e) {
    e->freq = e->freq/idx->maxFreq;
}

int forwardIndexTokenFunc(void *ctx, Token t) {
    ForwardIndex *idx = ctx;
    
    ForwardIndexEntry *h = NULL;
    // Retrieve the value for key "apple"
    khiter_t k = kh_get(32, idx->hits, t.s);  // first have to get ieter
    if (k == kh_end(idx->hits)) {  // k will be equal to kh_end if key not present
        h = calloc(1, sizeof(ForwardIndexEntry));
        h->docId = idx->docId;
        h->term = t.s;
        h->vw = NewVarintVectorWriter(4);
        h->docScore = idx->docScore;

        int ret;
        k = kh_put(32, idx->hits, t.s, &ret);
        kh_value(idx->hits, k) = h;
    } else {
        h = kh_val(idx->hits, k);
    }

    h->flags |= t.fieldId;
    h->freq += (float)t.score;
    idx->totalFreq += (float)t.score;

    idx->maxFreq = MAX(h->freq, idx->maxFreq);
    VVW_Write(h->vw, t.pos);
    LG_DEBUG("%d) %s, token freq: %f total freq: %f\n", t.pos,t.s, h->freq, idx->totalFreq);
    return 0;
    
}


ForwardIndexIterator ForwardIndex_Iterate(ForwardIndex *i) {
    ForwardIndexIterator iter;
    iter.idx = i;
    iter.k = kh_begin(i->hits);
    
    return iter;
}

ForwardIndexEntry *ForwardIndexIterator_Next(ForwardIndexIterator *iter) {
     
   if (iter->k == kh_end(iter->idx->hits)) {
       return NULL;
   }
   if (kh_exist(iter->idx->hits, iter->k)) {
         ForwardIndexEntry *entry = kh_value(iter->idx->hits, iter->k);
         iter->k++;
         return entry;
      
   } else {
       iter->k++;
       return ForwardIndexIterator_Next(iter);
   }
   return NULL;
}