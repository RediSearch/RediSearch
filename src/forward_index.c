#include <stdio.h>
#include <sys/param.h>
#include "forward_index.h"
#include "tokenize.h"
#include "util/logging.h"

ForwardIndex *NewForwardIndex(Document doc) {
    ForwardIndex *idx = malloc(sizeof(ForwardIndex));

    idx->hits = kh_init(32);
    idx->docScore = doc.score;
    idx->docId = doc.docId;
    idx->totalFreq = 0;
    idx->maxFreq = 0;
    idx->stemmer = NewStemmer(SnowballStemmer, doc.language);

    return idx;
}

void ForwardIndexFree(ForwardIndex *idx) {
    khiter_t k;
    for (k = kh_begin(idx->hits); k != kh_end(idx->hits); ++k) {
        if (kh_exist(idx->hits, k)) {
            ForwardIndexEntry *ent = kh_value(idx->hits, k);
            // free((void *)ent->term);
            kh_del(32, idx->hits, k);
            VVW_Free(ent->vw);
            free(ent);
        }
    }
    kh_destroy(32, idx->hits);
    free(idx);
    // TODO: check if we need to free each entry separately
}

void ForwardIndex_NormalizeFreq(ForwardIndex *idx, ForwardIndexEntry *e) {
    e->freq = e->freq / idx->maxFreq;
}

int forwardIndexTokenFunc(void *ctx, Token t) {
    ForwardIndex *idx = ctx;

    static char buf[1024];
    // char *p = buf;
    // char *s = (char *)t.s;
    // strncpy(buf, t.s, MIN(1024, t.len));
    // memccpy(buf, t.s, t.len);
    for (size_t i = 0; i < t.len && i < 1024; ++i) {
        buf[i] = t.s[i];
    }
    buf[t.len] = 0;

    // char *buf = strndup(t.s, t.len);  //
    // snprintf(buf, 1024, "%.*s", (int)t.len, t.s);

    // we need to ndup the string because it's not null terminated

    ForwardIndexEntry *h = NULL;
    khiter_t k = kh_get(32, idx->hits, buf);  // first have to get ieter
    if (k == kh_end(idx->hits)) {             // k will be equal to kh_end if key not present

        h = calloc(1, sizeof(ForwardIndexEntry));
        h->docId = idx->docId;
        h->flags = 0;
        h->term = t.s;
        h->len = t.len;

        h->vw = NewVarintVectorWriter(4);
        h->docScore = idx->docScore;

        int ret;
        k = kh_put(32, idx->hits, buf, &ret);
        kh_value(idx->hits, k) = h;
    } else {
        h = kh_val(idx->hits, k);
        // free(buf);
    }

    h->flags |= (t.fieldId & 0xff);
    float score = (float)t.score;

    // stem tokens get lower score
    if (t.type == DT_STEM) {
        score *= STEM_TOKEN_FACTOR;
    }
    h->freq += score;
    idx->totalFreq += (float)t.score;

    idx->maxFreq = MAX(h->freq, idx->maxFreq);
    VVW_Write(h->vw, t.pos);

    // LG_DEBUG("%d) %s, token freq: %f total freq: %f\n", t.pos, t.s, h->freq, idx->totalFreq);
    return 0;
}

ForwardIndexIterator ForwardIndex_Iterate(ForwardIndex *i) {
    ForwardIndexIterator iter;
    iter.idx = i;
    iter.k = kh_begin(i->hits);

    return iter;
}

ForwardIndexEntry *ForwardIndexIterator_Next(ForwardIndexIterator *iter) {
    // advance the iterator while it's empty
    while (iter->k != kh_end(iter->idx->hits) && !kh_exist(iter->idx->hits, iter->k)) {
        ++iter->k;
    }

    // if we haven't reached the end, return the current iterator's entry
    if (iter->k != kh_end(iter->idx->hits)) {
        ForwardIndexEntry *entry = kh_value(iter->idx->hits, iter->k);
        ++iter->k;
        return entry;
    }

    return NULL;
}