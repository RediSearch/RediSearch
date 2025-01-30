/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "forward_index.h"
#include "tokenize.h"
#include "util/fnv.h"
#include "util/logging.h"
#include <stdio.h>
#include <sys/param.h>
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
  return rs_fnv_32a_buf((void *)s, n, 0);
}

#define CHARS_PER_TERM 5
static size_t estimtateTermCount(const Document *doc) {
  size_t nChars = 0;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    DocumentField *field = doc->fields + ii;
    if (field->unionType == FLD_VAR_T_CSTR || field->unionType == FLD_VAR_T_RMS) {
      size_t n;
      DocumentField_GetValueCStr(field, &n);
      nChars += n;
    } else if (field->unionType == FLD_VAR_T_ARRAY) {
        nChars += DocumentField_GetArrayValueCStrTotalLen(field);
    }
  }
  return nChars / CHARS_PER_TERM;
}

static void *vvwAlloc(void) {
  VarintVectorWriter *vvw = rm_calloc(1, sizeof(*vvw));
  VVW_Init(vvw, 64);
  return vvw;
}

static void vvwFree(void *p) {
  // printf("Releasing VVW=%p\n", p);
  VVW_Cleanup(p);
  rm_free(p);
}

static void ForwardIndex_InitCommon(ForwardIndex *idx, Document *doc, uint32_t idxFlags) {
  idx->idxFlags = idxFlags;
  idx->maxFreq = 0;
  idx->totalFreq = 0;

  if (idx->stemmer && !ResetStemmer(idx->stemmer, SnowballStemmer, doc->language)) {
    idx->stemmer->Free(idx->stemmer);
    idx->stemmer = NULL;
  }

  if (!idx->stemmer) {
    idx->stemmer = NewStemmer(SnowballStemmer, doc->language);
  }
}

ForwardIndex *NewForwardIndex(Document *doc, uint32_t idxFlags) {
  ForwardIndex *idx = rm_malloc(sizeof(ForwardIndex));

  BlkAlloc_Init(&idx->terms);
  BlkAlloc_Init(&idx->entries);

  static const KHTableProcs procs = {
      .Alloc = allocBucketEntry,
      .Compare = khtCompare,
      .Hash = khtHash,
  };

  size_t termCount = estimtateTermCount(doc);
  idx->hits = rm_calloc(1, sizeof(*idx->hits));
  idx->stemmer = NULL;
  idx->totalFreq = 0;

  KHTable_Init(idx->hits, &procs, &idx->entries, termCount);
  mempool_options options = {.initialCap = termCount, .alloc = vvwAlloc, .free = vvwFree};
  idx->vvwPool = mempool_new(&options);

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
  if (idx->smap) {
    SynonymMap_Free(idx->smap);
    idx->smap = NULL;
  }

  ForwardIndex_InitCommon(idx, doc, idxFlags);
}

static inline int hasOffsets(const ForwardIndex *idx) {
  return (idx->idxFlags & Index_StoreTermOffsets);
}

void ForwardIndexFree(ForwardIndex *idx) {
  BlkAlloc_FreeAll(&idx->entries, clearEntry, idx->vvwPool, sizeof(khIdxEntry));
  BlkAlloc_FreeAll(&idx->terms, NULL, NULL, 0);
  KHTable_Free(idx->hits);
  rm_free(idx->hits);
  mempool_destroy(idx->vvwPool);

  if (idx->stemmer) {
    idx->stemmer->Free(idx->stemmer);
  }

  if (idx->smap) {
    SynonymMap_Free(idx->smap);
  }

  idx->smap = NULL;

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

#define TOKOPT_F_STEM 0x01
#define TOKOPT_F_COPYSTR 0x02
#define TOKOPT_F_SUFFIX_TRIE 0x04
#define TOKOPT_F_RAW 0x08

static void ForwardIndex_HandleToken(ForwardIndex *idx, const char *tok, size_t tokLen,
                                     uint32_t pos, float fieldScore, t_fieldId fieldId,
                                     int options) {
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
    if (options & TOKOPT_F_COPYSTR) {
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

  h->fieldMask |= ((t_fieldMask)1) << fieldId;
  float score = (float)fieldScore;

  // stem tokens get lower score
  if (options & TOKOPT_F_STEM) {
    score *= STEM_TOKEN_FACTOR;
  }
  h->freq += MAX(1, (uint32_t)score);
  idx->maxFreq = MAX(h->freq, idx->maxFreq);
  if (options & TOKOPT_F_RAW) {
    // Account for this term as part of the document's length.
    idx->totalFreq += MAX(1, (uint32_t)score);
  }
  if (h->vw) {
    VVW_Write(h->vw, pos);
  }

  // LG_DEBUG("%d) %s, token freq: %f total freq: %f\n", t.pos, t.s, h->freq, idx->totalFreq);
}

// void ForwardIndex_NormalizeFreq(ForwardIndex *idx, ForwardIndexEntry *e) {
//   e->freq = e->freq / idx->maxFreq;
// }
int forwardIndexTokenFunc(void *ctx, const Token *tokInfo) {
#define SYNONYM_BUFF_LEN 100
  const ForwardIndexTokenizerCtx *tokCtx = ctx;
  int options = TOKOPT_F_RAW;  // this is the actual word given in the query
  if (tokInfo->flags & Token_CopyRaw) {
    options |= TOKOPT_F_COPYSTR;
    options |= TOKOPT_F_SUFFIX_TRIE;
  }
  ForwardIndex_HandleToken(tokCtx->idx, tokInfo->tok, tokInfo->tokLen, tokInfo->pos,
                           tokCtx->fieldScore, tokCtx->fieldId, options);

  if (tokCtx->allOffsets) {
    VVW_Write(tokCtx->allOffsets, tokInfo->raw - tokCtx->doc);
  }

  if (tokInfo->stem) {
    int stemopts = TOKOPT_F_STEM;
    if (tokInfo->flags & Token_CopyStem) {
      stemopts |= TOKOPT_F_COPYSTR;
    }
    ForwardIndex_HandleToken(tokCtx->idx, tokInfo->stem, tokInfo->stemLen, tokInfo->pos,
                             tokCtx->fieldScore, tokCtx->fieldId, stemopts);
  }

  if (tokCtx->idx->smap) {
    TermData *t_data = SynonymMap_GetIdsBySynonym(tokCtx->idx->smap, tokInfo->tok, tokInfo->tokLen);
    if (t_data) {
      char synonym_buff[SYNONYM_BUFF_LEN];
      size_t synonym_len;
      for (int i = 0; i < array_len(t_data->groupIds); ++i) {
        ForwardIndex_HandleToken(tokCtx->idx, t_data->groupIds[i], strlen(t_data->groupIds[i]), tokInfo->pos,
                                 tokCtx->fieldScore, tokCtx->fieldId, TOKOPT_F_COPYSTR);
      }
    }
  }

  if (tokInfo->phoneticsPrimary) {
    ForwardIndex_HandleToken(tokCtx->idx, tokInfo->phoneticsPrimary,
                             strlen(tokInfo->phoneticsPrimary), tokInfo->pos, tokCtx->fieldScore,
                             tokCtx->fieldId, TOKOPT_F_COPYSTR);
  }

  return 0;
}

ForwardIndexEntry *ForwardIndex_Find(ForwardIndex *i, const char *s, size_t n, uint32_t hash) {
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
