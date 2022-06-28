#include "forward_index.h"
#include "tokenize.h"

#include "rmalloc.h"
#include "util/fnv.h"
#include "util/logging.h"

#include <stdio.h>
#include <sys/param.h>

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
  return rs_fnv_32a_buf((void *)s, n, 0);
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
  return new VarintVectorWriter(64);
}

static void vvwFree(void *p) {
  // printf("Releasing VVW=%p\n", p);
  VVW_Cleanup(p);
  rm_free(p);
}

void ForwardIndex::InitCommon(Document *doc, uint32_t idxFlags_) {
  idxFlags = idxFlags_;
  maxFreq = 0;
  totalFreq = 0;

  if (stemmer && !stemmer->ResetStemmer(SnowballStemmer, doc->language)) {
    delete stemmer;
  }

  if (!stemmer) {
    stemmer = new Stemmer(SnowballStemmer, doc->language);
  }
}

ForwardIndex::ForwardIndex(Document *doc, uint32_t idxFlags_) {
  BlkAlloc_Init(&terms);
  BlkAlloc_Init(&entries);

  static const KHTableProcs procs = {
      Compare: khtCompare,
      Hash: khtHash,
      Alloc: allocBucketEntry
  };

  size_t termCount = estimtateTermCount(doc);
  hits = rm_calloc(1, sizeof(*hits));
  stemmer = NULL;
  totalFreq = 0;

  KHTable_Init(hits, &procs, &entries, termCount);
  mempool_options options = { alloc: vvwAlloc, free: vvwFree, initialCap: termCount};
  vvwPool = mempool_new(&options);

  InitCommon(doc, idxFlags_);
}

static void clearEntry(void *elem, void *pool) {
  khIdxEntry *ent = elem;
  ForwardIndexEntry *fwEnt = &ent->ent;
  if (fwEnt->vw) {
    mempool_release(pool, fwEnt->vw);
    fwEnt->vw = NULL;
  }
}

void ForwardIndex::Reset(Document *doc, uint32_t idxFlags_) {
  BlkAlloc_Clear(&terms, NULL, NULL, 0);
  BlkAlloc_Clear(&entries, clearEntry, vvwPool, sizeof(khIdxEntry));
  KHTable_Clear(hits);
  if (smap) {
    delete smap;
  }

  ForwardIndex_InitCommon(idx, doc, idxFlags_);
}

inline int ForwardIndex::hasOffsets() const {
  return (idxFlags & Index_StoreTermOffsets);
}

void ForwardIndex::~ForwardIndex() {
  BlkAlloc_FreeAll(&entries, clearEntry, vvwPool, sizeof(khIdxEntry));
  BlkAlloc_FreeAll(&terms, NULL, NULL, 0);
  KHTable_Free(hits);
  rm_free(hits);
  mempool_destroy(vvwPool);

  if (stemmer) {
    stemmer->Free(stemmer);
  }

  if (smap) {
    delete smap;
  }

  smap = NULL;
}

char *ForwardIndex::copyTempString(const char *s, size_t n) {
  char *dst = BlkAlloc_Alloc(&terms, n + 1, MAX(n + 1, TERM_BLOCK_SIZE));
  memcpy(dst, s, n);
  dst[n] = '\0';
  return dst;
}

khIdxEntry *ForwardIndex::makeEntry(const char *s, size_t n, uint32_t h, int *isNew) {
  KHTableEntry *bb = KHTable_GetEntry(hits, s, n, h, isNew);
  return (khIdxEntry *)bb;
}

#define TOKOPT_F_STEM 0x01
#define TOKOPT_F_COPYSTR 0x02

void ForwardIndex::HandleToken(const char *tok, size_t tokLen, uint32_t pos,
                               float fieldScore, t_fieldId fieldId, int options) {
  // LG_DEBUG("token %.*s, hval %d\n", t.len, t.s, hval);
  ForwardIndexEntry *h;
  int isNew = 0;
  uint32_t hash = hashKey(tok, tokLen);
  khIdxEntry *kh = makeEntry(tok, tokLen, hash, &isNew);
  h = &kh->ent;

  if (isNew) {
    // printf("New token %.*s\n", (int)t->len, t->s);
    h->fieldMask = 0;
    h->hash = hash;
    h->next = NULL;
    if (options & TOKOPT_F_COPYSTR) {
      h->term = copyTempString(tok, tokLen);
    } else {
      h->term = tok;
    }

    h->len = tokLen;
    h->freq = 0;

    if (hasOffsets()) {
      h->vw = mempool_get(vvwPool);
      // printf("Got VVW=%p\n", h->vw);
      VVW_Reset(h->vw);
    } else {
      h->vw = NULL;
    }
  } else {
    // printf("Existing token %.*s\n", (int)t->len, t->s);
  }

  h->fieldMask |= ((t_fieldMask)1) << fieldId;
  float score = (float)fieldScore;

  // stem tokens get lower score
  if (options & TOKOPT_F_STEM) {
    score *= STEM_TOKEN_FACTOR;
  }
  h->freq += MAX(1, (uint32_t)score);
  maxFreq = MAX(h->freq, maxFreq);
  totalFreq += h->freq;
  if (h->vw) {
    VVW_Write(h->vw, pos);
  }

  // LG_DEBUG("%d) %s, token freq: %f total freq: %f\n", t.pos, t.s, h->freq, idx->totalFreq);
}

// void ForwardIndex::NormalizeFreq(ForwardIndexEntry *e) {
//   e->freq = e->freq / maxFreq;
// }
static int ForwardIndexTokenizerCtx::TokenFunc(const Token *tokInfo) {
#define SYNONYM_BUFF_LEN 100
  int options = 0;
  if (tokInfo->flags & Token_CopyRaw) {
    options |= TOKOPT_F_COPYSTR;
  }
  idx->HandleToken(tokInfo->tok, tokInfo->tokLen, tokInfo->pos,
                   fieldScore, fieldId, options);

  if (allOffsets) {
    VVW_Write(allOffsets, tokInfo->raw - doc);
  }

  if (tokInfo->stem) {
    int stemopts = TOKOPT_F_STEM;
    if (tokInfo->flags & Token_CopyStem) {
      stemopts |= TOKOPT_F_COPYSTR;
    }
    idx->HandleToken(tokInfo->stem, tokInfo->stemLen, tokInfo->pos,
                     fieldScore, fieldId, stemopts);
  }

  if (idx->smap) {
    TermData *t_data = idx->smap->GetIdsBySynonym(tokInfo->tok, tokInfo->tokLen);
    if (t_data) {
      char synonym_buff[SYNONYM_BUFF_LEN];
      size_t synonym_len;
      for (int i = 0; i < array_len(t_data->ids); ++i) {
        synonym_len = SynonymMap::IdToStr(t_data->ids[i], synonym_buff, SYNONYM_BUFF_LEN);
        idx->HandleToken(synonym_buff, synonym_len, tokInfo->pos,
                         fieldScore, fieldId, TOKOPT_F_COPYSTR);
      }
    }
  }

  if (tokInfo->phoneticsPrimary) {
    idx->HandleToken(tokInfo->phoneticsPrimary, strlen(tokInfo->phoneticsPrimary),
                     tokInfo->pos, fieldScore, fieldId, TOKOPT_F_COPYSTR);
  }

  return 0;
}

// Find an existing entry within the index
ForwardIndexEntry *ForwardIndex::Find(const char *s, size_t n, uint32_t hash) {
  KHTableEntry *baseEnt = KHTable_GetEntry(hits, s, n, hash, NULL);
  if (!baseEnt) {
    return NULL;
  } else {
    khIdxEntry *bEnt = (khIdxEntry *)baseEnt;
    return &bEnt->ent;
  }
}

ForwardIndexIterator ForwardIndex::Iterate() {
  ForwardIndexIterator iter;
  iter.hits = hits;
  iter.curBucketIdx = 0;
  iter.curEnt = NULL;
  // khTable_Dump(iter.hits);
  return iter;
}

ForwardIndexEntry *ForwardIndexIterator::Next() {
  KHTable *table = hits;

  while (curEnt == NULL && curBucketIdx < table->numBuckets) {
    curEnt = table->buckets[curBucketIdx++];
  }

  if (curEnt == NULL) {
    return NULL;
  }

  KHTableEntry *ret = curEnt;
  curEnt = ret->next;
  // printf("Yielding entry: %.*s. Next=%p -- (%p)\n", (int)ret->self.ent.len, ret->self.ent.term,
  //  ret->next, curEnt);
  khIdxEntry *bEnt = (khIdxEntry *)ret;
  return &bEnt->ent;
}
