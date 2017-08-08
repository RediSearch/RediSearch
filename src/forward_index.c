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

typedef struct bucketEntry {
  struct bucketEntry *next;
  khIdxEntry self;
} bucketEntry;

typedef struct khTable {
  // Allocation for members
  BlkAlloc alloc;

  // Buckets for the table
  bucketEntry **buckets;
  size_t numBuckets;
  size_t numItems;
} khTable;

static uint32_t primes[] = {5ul,         11ul,        23ul,      47ul,       97ul,       199ul,
                            409ul,       823ul,       1741ul,    3469ul,     6949ul,     14033ul,
                            28411ul,     57557ul,     116731ul,  236897ul,   480881ul,   976369ul,
                            1982627ul,   4026031ul,   8175383ul, 16601593ul, 33712729ul, 68460391ul,
                            139022417ul, 282312799ul, 0};

static void khTable_Init(khTable *table, size_t estSize) {
  BlkAlloc_Init(&table->alloc);
  // Traverse a list of primes until we find one suitable
  uint32_t *p;
  for (p = primes; *p; p++) {
    if (*p > estSize) {
      table->numBuckets = *p;
      break;
    }
  }
  if (*p == 0) {
    p--;
    table->numBuckets = *p;
  }

  table->buckets = calloc(sizeof(*table->buckets), table->numBuckets);
  table->numItems = 0;
}

#define ENTRIES_PER_BLOCK 32
#define TERM_BLOCK_SIZE 128
#define DEFAULT_TABLE_SIZE 4096

static bucketEntry *allocBucketEntry(khTable *table) {
  return BlkAlloc_Alloc(&table->alloc, sizeof(bucketEntry),
                        ENTRIES_PER_BLOCK * sizeof(bucketEntry));
}

static int khTable_Rehash(khTable *table) {
  // Find new capacity
  size_t newCapacity = 0;
  for (uint32_t *p = primes; *p; p++) {
    if (*p > table->numItems) {
      newCapacity = *p;
      break;
    }
  }

  if (!newCapacity) {
    return 0;
  }

  // printf("Rehashing %lu -> %lu\n", table->numBuckets, newCapacity);

  bucketEntry **newEntries = calloc(newCapacity, sizeof(*table->buckets));
  for (size_t ii = 0; ii < table->numBuckets; ++ii) {

    bucketEntry *cur = table->buckets[ii];
    while (cur) {
      uint32_t hash = fnv_32a_buf((void *)cur->self.ent.term, cur->self.ent.len, 0);
      bucketEntry **newBucket = newEntries + (hash % newCapacity);
      bucketEntry *next = cur->next;
      if (*newBucket) {
        cur->next = *newBucket;
      } else {
        cur->next = NULL;
      }
      *newBucket = cur;
      cur = next;
    }
  }

  free(table->buckets);
  table->buckets = newEntries;
  table->numBuckets = newCapacity;

  return 1;
}

static khIdxEntry *khTable_InsertNewEntry(khTable *table, uint32_t hash, bucketEntry **bucketHead) {
  if (++table->numItems == table->numBuckets) {
    khTable_Rehash(table);
    bucketHead = table->buckets + (hash % table->numBuckets);
  }
  bucketEntry *entry = allocBucketEntry(table);
  if (*bucketHead) {
    entry->next = *bucketHead;
  } else {
    entry->next = NULL;
  }
  *bucketHead = entry;
  return &entry->self;
}

/**
 * Return an entry for the given key, creating one if it does not already
 * exist.
 */
static khIdxEntry *khTable_GetEntry(khTable *table, const char *s, size_t n, int create,
                                    int *isNew) {
  uint32_t hash = fnv_32a_buf((void *)s, n, 0);
  // Find the bucket
  uint32_t ix = hash % table->numBuckets;
  bucketEntry **bucket = table->buckets + ix;

  if (*bucket == NULL) {
    if (!create) {
      return NULL;
    }
    *isNew = 1;
    // Most likely case - no need for rehashing
    if (++table->numItems != table->numBuckets) {
      *bucket = allocBucketEntry(table);
      (*bucket)->next = NULL;
      return &(*bucket)->self;
    } else {
      khTable_Rehash(table);
      khIdxEntry *ret =
          khTable_InsertNewEntry(table, hash, table->buckets + (hash % table->numBuckets));
      // Decrement the count again
      table->numItems--;
      return ret;
    }
  }

  for (bucketEntry *cur = *bucket; cur; cur = cur->next) {
    if (cur->self.ent.len == n && memcmp(cur->self.ent.term, s, n) == 0) {
      *isNew = 0;
      return &cur->self;
    }
  }

  if (!create) {
    return NULL;
  }

  *isNew = 1;
  return khTable_InsertNewEntry(table, hash, bucket);
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
  // return 3;
}

ForwardIndex *NewForwardIndex(Document *doc, uint32_t idxFlags) {
  ForwardIndex *idx = rm_malloc(sizeof(ForwardIndex));
  idx->hits = calloc(1, sizeof(*idx->hits));
  khTable_Init(idx->hits, estimtateTermCount(doc));
  idx->docScore = doc->score;
  idx->docId = doc->docId;
  idx->totalFreq = 0;
  idx->idxFlags = idxFlags;
  idx->uniqueTokens = 0;
  idx->maxFreq = 0;
  idx->stemmer = NewStemmer(SnowballStemmer, doc->language);
  memset(&idx->terms, 0, sizeof idx->terms);
  return idx;
}

static void clearEntry(void *p) {
  bucketEntry *ent = p;
  ForwardIndexEntry *fwEnt = &ent->self.ent;
  if (fwEnt->vw) {
    VVW_Cleanup(fwEnt->vw);
  }
}

static inline int hasOffsets(const ForwardIndex *idx) {
  return (idx->idxFlags & Index_StoreTermOffsets);
}

void ForwardIndexFree(ForwardIndex *idx) {
  size_t elemSize = sizeof(khIdxEntry);

  BlkAlloc_FreeAll(&idx->hits->alloc, clearEntry, sizeof(bucketEntry));
  BlkAlloc_FreeAll(&idx->terms, NULL, 0);

  if (idx->stemmer) {
    idx->stemmer->Free(idx->stemmer);
  }

  free(idx->hits->buckets);
  free(idx->hits);
  rm_free(idx);
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

  // LG_DEBUG("token %.*s, hval %d\n", t.len, t.s, hval);
  ForwardIndexEntry *h = NULL;
  int isNew = 0;
  khIdxEntry *kh = khTable_GetEntry(idx->hits, t->s, t->len, 1, &isNew);
  h = &kh->ent;

  if (isNew) {
    // printf("New token %.*s\n", (int)t->len, t->s);
    h->docId = idx->docId;
    h->fieldMask = 0;
    h->indexerState = 0;
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
  } else {
    // printf("Existing token %.*s\n", (int)t->len, t->s);
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

static void khTable_Dump(const khTable *table) {
  printf("Table=%p\n", table);
  printf("NumEntries: %lu\n", table->numItems);
  printf("NumBuckets: %lu\n", table->numBuckets);
  for (size_t ii = 0; ii < table->numBuckets; ++ii) {
    bucketEntry *ent = table->buckets[ii];
    if (!ent) {
      continue;
    }
    printf("Bucket[%lu]\n", ii);
    for (; ent; ent = ent->next) {
      printf("   => %.*s\n", (int)ent->self.ent.len, ent->self.ent.term);
    }
  }
}

ForwardIndexEntry *ForwardIndex_Find(ForwardIndex *i, const char *s, size_t n) {
  int dummy;
  khIdxEntry *ent = khTable_GetEntry(i->hits, s, n, 0, &dummy);
  if (ent) {
    return &ent->ent;
  } else {
    return NULL;
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
  khTable *table = iter->hits;
  while (iter->curEnt == NULL && iter->curBucketIdx < table->numBuckets) {
    iter->curEnt = table->buckets[iter->curBucketIdx++];
  }

  if (iter->curEnt == NULL) {
    return NULL;
  }

  bucketEntry *ret = iter->curEnt;
  iter->curEnt = ret->next;
  // printf("Yielding entry: %.*s. Next=%p -- (%p)\n", (int)ret->self.ent.len, ret->self.ent.term,
  //  ret->next, iter->curEnt);
  return &ret->self.ent;
}