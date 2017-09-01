#include "forward_index.h"
#include "tokenize.h"
#include "util/fnv.h"
#include "util/logging.h"
#include <stdio.h>
#include <sys/param.h>
#include <assert.h>
#include "rmalloc.h"

typedef struct {
  KHTableEntry khBase;
  ForwardIndexEntry ent;
} khIdxEntry;

#define ENTRIES_PER_BLOCK 32
#define TERM_BLOCK_SIZE 128

static int khtCompare(const KHTableEntry *entBase, const void *s, size_t n, uint32_t h) {
  khIdxEntry *ee = (khIdxEntry *)entBase;
  ForwardIndexEntry *ent = &ee->ent;
  if (ent->hash != h) {
    return 1;
  }
  if (ent->len != n) {
    return 1;
  }
  return memcmp(ent->term, s, n);
}

static uint32_t khtHash(const KHTableEntry *entBase) {
  return ((khIdxEntry *)entBase)->ent.hash;
}

static KHTableEntry *allocBucketEntry(void *ptr) {
  BlkAlloc *alloc = ptr;
  void *p = BlkAlloc_Alloc(alloc, sizeof(khIdxEntry), ENTRIES_PER_BLOCK * sizeof(khIdxEntry));
  return p;
}

static uint32_t hashKey(const void *s, size_t n) {
  return fnv_32a_buf((void *)s, n, 0);
}

#define CHARS_PER_TERM 5
static size_t estimtateTermCount(const Document *doc) {
  size_t nChars = 0;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    size_t n;
    RedisModule_StringPtrLen(doc->fields[ii].text, &n);
    nChars += n;
  }
  return nChars / CHARS_PER_TERM;
}

static void *vvwAlloc(void) {
  VarintVectorWriter *vvw = calloc(1, sizeof(*vvw));
  VVW_Init(vvw, 64);
  return vvw;
}

static void vvwFree(void *p) {
  // printf("Releasing VVW=%p\n", p);
  VVW_Cleanup(p);
  free(p);
}

static void ForwardIndex_InitCommon(ForwardIndex *idx, Document *doc, uint32_t idxFlags) {
  idx->idxFlags = idxFlags;
  idx->maxFreq = 0;
  if (idx->stemmer) {
    idx->stemmer->Free(idx->stemmer);
  }
  idx->stemmer = NewStemmer(SnowballStemmer, doc->language);
}

ForwardIndex *NewForwardIndex(Document *doc, uint32_t idxFlags) {
  ForwardIndex *idx = rm_malloc(sizeof(ForwardIndex));

  BlkAlloc_Init(&idx->terms);
  BlkAlloc_Init(&idx->entries);

  static const KHTableProcs procs = {
      .Alloc = allocBucketEntry, .Compare = khtCompare, .Hash = khtHash,
  };

  size_t termCount = estimtateTermCount(doc);
  idx->hits = calloc(1, sizeof(*idx->hits));
  idx->stemmer = NULL;

  KHTable_Init(idx->hits, &procs, &idx->entries, termCount);
  idx->vvwPool = mempool_new(termCount, vvwAlloc, vvwFree);

  ForwardIndex_InitCommon(idx, doc, idxFlags);
  return idx;
}

static void clearEntry(void *elem, void *pool) {
  khIdxEntry *ent = elem;
  ForwardIndexEntry *fwEnt = &ent->ent;
  if (fwEnt->vw) {
    mempool_release(pool, fwEnt->vw);
    fwEnt->vw = NULL;
  }
}

void ForwardIndex_Reset(ForwardIndex *idx, Document *doc, uint32_t idxFlags) {
  BlkAlloc_Clear(&idx->terms, NULL, NULL, 0);
  BlkAlloc_Clear(&idx->entries, clearEntry, idx->vvwPool, sizeof(khIdxEntry));
  KHTable_Clear(idx->hits);
  ForwardIndex_InitCommon(idx, doc, idxFlags);
}

static inline int hasOffsets(const ForwardIndex *idx) {
  return (idx->idxFlags & Index_StoreTermOffsets);
}

void ForwardIndexFree(ForwardIndex *idx) {
  size_t elemSize = sizeof(khIdxEntry);

  BlkAlloc_FreeAll(&idx->entries, clearEntry, idx->vvwPool, sizeof(khIdxEntry));
  BlkAlloc_FreeAll(&idx->terms, NULL, NULL, 0);
  KHTable_Free(idx->hits);
  free(idx->hits);
  mempool_destroy(idx->vvwPool);

  if (idx->stemmer) {
    idx->stemmer->Free(idx->stemmer);
  }

  rm_free(idx);
}

static char *copyTempString(ForwardIndex *idx, const char *s, size_t n) {
  char *dst = BlkAlloc_Alloc(&idx->terms, n + 1, MAX(n + 1, TERM_BLOCK_SIZE));
  memcpy(dst, s, n);
  dst[n] = '\0';
  return dst;
}

static khIdxEntry *makeEntry(ForwardIndex *idx, const char *s, size_t n, uint32_t h, int *isNew) {
  KHTableEntry *bb = KHTable_GetEntry(idx->hits, s, n, h, isNew);
  return (khIdxEntry *)bb;
}

static void ForwardIndex_HandleToken(ForwardIndex *idx, const char *tok, size_t tokLen,
                                     uint32_t pos, float fieldScore, t_fieldMask fieldId,
                                     int isStem) {
  // LG_DEBUG("token %.*s, hval %d\n", t.len, t.s, hval);
  ForwardIndexEntry *h = NULL;
  int isNew = 0;
  uint32_t hash = hashKey(tok, tokLen);
  khIdxEntry *kh = makeEntry(idx, tok, tokLen, hash, &isNew);
  h = &kh->ent;

  if (isNew) {
    // printf("New token %.*s\n", (int)t->len, t->s);
    h->fieldMask = 0;
    h->hash = hash;
    h->next = NULL;

    if (isStem) {
      h->term = copyTempString(idx, tok, tokLen);
    } else {
      h->term = tok;
    }
    h->len = tokLen;
    h->freq = 0;

    if (hasOffsets(idx)) {
      h->vw = mempool_get(idx->vvwPool);
      // printf("Got VVW=%p\n", h->vw);
      VVW_Reset(h->vw);
    } else {
      h->vw = NULL;
    }

  } else {
    // printf("Existing token %.*s\n", (int)t->len, t->s);
  }

  h->fieldMask |= (fieldId & RS_FIELDMASK_ALL);
  float score = (float)fieldScore;

  // stem tokens get lower score
  if (isStem) {
    score *= STEM_TOKEN_FACTOR;
  }
  h->freq += MAX(1, (uint32_t)score);
  idx->maxFreq = MAX(h->freq, idx->maxFreq);
  if (h->vw) {
    VVW_Write(h->vw, pos);
  }

  // LG_DEBUG("%d) %s, token freq: %f total freq: %f\n", t.pos, t.s, h->freq, idx->totalFreq);
}

// void ForwardIndex_NormalizeFreq(ForwardIndex *idx, ForwardIndexEntry *e) {
//   e->freq = e->freq / idx->maxFreq;
// }
int forwardIndexTokenFunc(void *ctx, const Token *tokInfo) {
  const ForwardIndexTokenizerCtx *tokCtx = ctx;
  ForwardIndex_HandleToken(tokCtx->idx, tokInfo->tok, tokInfo->tokLen, tokInfo->pos,
                           tokCtx->fieldScore, tokCtx->fieldId, 0);
  if (tokInfo->stem) {
    ForwardIndex_HandleToken(tokCtx->idx, tokInfo->stem, tokInfo->stemLen, tokInfo->pos,
                             tokCtx->fieldScore, tokCtx->fieldId, 1);
  }
  return 0;
}

ForwardIndexEntry *ForwardIndex_Find(ForwardIndex *i, const char *s, size_t n, uint32_t hash) {
  int dummy;
  KHTableEntry *baseEnt = KHTable_GetEntry(i->hits, s, n, hash, NULL);
  if (!baseEnt) {
    return NULL;
  } else {
    khIdxEntry *bEnt = (khIdxEntry *)baseEnt;
    return &bEnt->ent;
  }
}

ForwardIndexIterator ForwardIndex_Iterate(ForwardIndex *i) {
  ForwardIndexIterator iter;
  iter.hits = i->hits;
  iter.curBucketIdx = 0;
  iter.curEnt = NULL;
  // khTable_Dump(iter.hits);
  return iter;
}

ForwardIndexEntry *ForwardIndexIterator_Next(ForwardIndexIterator *iter) {
  KHTable *table = iter->hits;

  while (iter->curEnt == NULL && iter->curBucketIdx < table->numBuckets) {
    iter->curEnt = table->buckets[iter->curBucketIdx++];
  }

  if (iter->curEnt == NULL) {
    return NULL;
  }

  KHTableEntry *ret = iter->curEnt;
  iter->curEnt = ret->next;
  // printf("Yielding entry: %.*s. Next=%p -- (%p)\n", (int)ret->self.ent.len, ret->self.ent.term,
  //  ret->next, iter->curEnt);
  khIdxEntry *bEnt = (khIdxEntry *)ret;
  return &bEnt->ent;
}