#include "forward_index.h"
#include "tokenize.h"
#include "util/fnv.h"
#include "util/logging.h"
#include <stdio.h>
#include <sys/param.h>
#include <assert.h>
#include "rmalloc.h"

typedef struct {
  ForwardIndexEntry ent;
  VarintVectorWriter vw;
} khIdxEntry;

ForwardIndex *NewForwardIndex(Document *doc, uint32_t idxFlags) {
  ForwardIndex *idx = rm_malloc(sizeof(ForwardIndex));

  idx->hits = kh_init(32);
  idx->docScore = doc->score;
  idx->docId = doc->docId;
  idx->totalFreq = 0;
  idx->idxFlags = idxFlags;
  idx->uniqueTokens = 0;
  idx->maxFreq = 0;
  idx->stemmer = NewStemmer(SnowballStemmer, doc->language);
  memset(&idx->entries, 0, sizeof idx->entries);
  memset(&idx->terms, 0, sizeof idx->terms);
  return idx;
}

static void clearEntry(void *p) {
  ForwardIndexEntry *fwEnt = p;
  if (fwEnt->vw) {
    VVW_Cleanup(fwEnt->vw);
  }
}

static inline int hasOffsets(const ForwardIndex *idx) {
  return (idx->idxFlags & Index_StoreTermOffsets);
}

void ForwardIndexFree(ForwardIndex *idx) {
  size_t elemSize = hasOffsets(idx) ? sizeof(khIdxEntry) : sizeof(ForwardIndexEntry);

  BlkAlloc_FreeAll(&idx->entries, clearEntry, sizeof(khIdxEntry));
  BlkAlloc_FreeAll(&idx->terms, NULL, 0);

  kh_destroy(32, idx->hits);

  if (idx->stemmer) {
    idx->stemmer->Free(idx->stemmer);
  }
  rm_free(idx);
}

#define ENTRIES_PER_BLOCK 32
#define TERM_BLOCK_SIZE 128

static khIdxEntry *allocIdxEntry(ForwardIndex *idx) {
  return BlkAlloc_Alloc(&idx->entries, sizeof(khIdxEntry), ENTRIES_PER_BLOCK * sizeof(khIdxEntry));
}

static char *copyTempString(ForwardIndex *idx, const char *s, size_t n) {
  char *dst = BlkAlloc_Alloc(&idx->terms, n, MAX(n, TERM_BLOCK_SIZE));
  memcpy(dst, s, n);
  return dst;
}

// void ForwardIndex_NormalizeFreq(ForwardIndex *idx, ForwardIndexEntry *e) {
//   e->freq = e->freq / idx->maxFreq;
// }
int forwardIndexTokenFunc(void *ctx, const Token *t) {
  ForwardIndex *idx = ctx;

  // we hash the string ourselves because khash suckz azz
  uint32_t hval = fnv_32a_buf((void *)t->s, t->len, 0);
  // LG_DEBUG("token %.*s, hval %d\n", t.len, t.s, hval);
  ForwardIndexEntry *h = NULL;
  khiter_t k = kh_get(32, idx->hits, hval);  // first have to get ieter
  if (k == kh_end(idx->hits)) {              // k will be equal to kh_end if key not present
    /// LG_DEBUG("new entry %.*s\n", t.len, t.s);
    khIdxEntry *kh = allocIdxEntry(idx);
    h = &kh->ent;
    h->docId = idx->docId;
    h->fieldMask = 0;
    if (t->stringFreeable) {
      h->term = copyTempString(idx, t->s, t->len);
    } else {
      h->term = t->s;
    }
    h->len = t->len;
    h->freq = 0;

    if (hasOffsets(idx)) {
      h->vw = &kh->vw;
      VVW_Init(h->vw, 64);
    }
    h->docScore = idx->docScore;

    int ret;
    k = kh_put(32, idx->hits, hval, &ret);
    kh_value(idx->hits, k) = h;
  } else {
    h = kh_val(idx->hits, k);
  }

  h->fieldMask |= (t->fieldId & RS_FIELDMASK_ALL);
  float score = (float)t->score;

  // stem tokens get lower score
  if (t->type == DT_STEM) {
    score *= STEM_TOKEN_FACTOR;
  }
  h->freq += MAX(1, (uint32_t)score);
  idx->totalFreq += h->freq;
  idx->uniqueTokens++;
  idx->maxFreq = MAX(h->freq, idx->maxFreq);
  if (h->vw) {
    VVW_Write(h->vw, t->pos);
  }

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
  if (iter->k != kh_end(iter->idx->hits) && kh_exist(iter->idx->hits, iter->k)) {
    ForwardIndexEntry *entry = kh_value(iter->idx->hits, iter->k);
    ++iter->k;
    return entry;
  }

  return NULL;
}