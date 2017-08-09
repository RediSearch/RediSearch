#include <string.h>
#include <assert.h>

#include "document.h"
#include "forward_index.h"
#include "geo_index.h"
#include "index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "redis_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "tokenize.h"
#include "util/logging.h"
#include "search_request.h"
#include "rmalloc.h"
#include "concurrent_ctx.h"

// Preprocessors can store field data to this location
typedef union FieldData {
  double numeric;
  struct {
    char *slon;
    char *slat;
  } geo;
} fieldData;

typedef struct DocumentIndexer {
  RSAddDocumentCtx *head;
  RSAddDocumentCtx *tail;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  size_t size;
  ConcurrentSearchCtx concCtx;
} DocumentIndexer;

// Process a single context
static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *ctx);

// Main loop
static void *DocumentIndexer_Run(void *arg);

// Enqueue the context to be written, and return when the context has been written
// (or an error occurred)
static int DocumentIndexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx);

static int makeDocumentId(Document *doc, RedisSearchCtx *ctx, int replace,
                          const char **errorString) {
  const char *keystr = RedisModule_StringPtrLen(doc->docKey, NULL);
  DocTable *table = &ctx->spec->docs;
  if (replace) {
    DocTable_Delete(table, keystr);
  }
  doc->docId = DocTable_Put(table, keystr, doc->score, 0, doc->payload, doc->payloadSize);
  if (doc->docId == 0) {
    *errorString = "Document already exists";
    return -1;
  }
  return 0;
}

static void reopenCb(RedisModuleKey *k, void *arg) {
  // Index Key
  RSAddDocumentCtx *aCtx = arg;
  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
    aCtx->rsCtx.spec = NULL;
    return;
  }

  aCtx->rsCtx.spec = RedisModule_ModuleTypeGetValue(k);
}

static void sendReply(RSAddDocumentCtx *aCtx) {
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(aCtx->thCtx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(aCtx->thCtx, "OK");
  }
  RedisModule_UnblockClient(aCtx->bc, aCtx);
}

RSAddDocumentCtx *NewAddDocumentCtx(RedisModuleBlockedClient *client, IndexSpec *sp,
                                    Document *base) {
  // Block the client and create the context!
  RSAddDocumentCtx *aCtx = calloc(1, sizeof(*aCtx));
  aCtx->bc = client;
  aCtx->thCtx = RedisModule_GetThreadSafeContext(aCtx->bc);
  aCtx->doc = *base;

  RedisModule_AutoMemory(aCtx->thCtx);
  aCtx->rsCtx.redisCtx = aCtx->thCtx;
  aCtx->rsCtx.spec = sp;
  aCtx->next = NULL;
  aCtx->rsCtx.keyName = RedisModule_CreateStringPrintf(aCtx->thCtx, INDEX_SPEC_KEY_FMT, sp->name);
  aCtx->fwIdx = NewForwardIndex(&aCtx->doc, sp->flags);
  aCtx->specFlags = sp->flags;

  // Also, get the field specs. We cache this here because the context is unlocked
  // during the actual tokenization
  Document *doc = &aCtx->doc;

  aCtx->fspecs = malloc(sizeof(*aCtx->fspecs) * doc->numFields);
  aCtx->fdatas = malloc(sizeof(*aCtx->fdatas) * doc->numFields);

  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(aCtx->rsCtx.spec, f->name, strlen(f->name));
    if (fs) {
      aCtx->fspecs[i] = *fs;
    } else {
      aCtx->fspecs[i].name = NULL;
    }
  }

  ConcurrentSearchCtx_Init(aCtx->thCtx, &aCtx->conc);

  ConcurrentSearch_AddKey(&aCtx->conc, NULL, REDISMODULE_READ | REDISMODULE_WRITE,
                          aCtx->rsCtx.keyName, reopenCb, aCtx, NULL);
  return aCtx;
}

void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx) {
  Document_FreeDetached(&aCtx->doc, aCtx->thCtx);
  ConcurrentSearchCtx_Free(&aCtx->conc);
  RedisModule_FreeThreadSafeContext(aCtx->thCtx);

  if (aCtx->fwIdx) {
    ForwardIndexFree(aCtx->fwIdx);
  }

  free(aCtx->fspecs);
  free(aCtx->fdatas);
  free(aCtx);
}

#define ACTX_SPEC(actx) (actx)->rsCtx.spec

static void ensureSortingVector(RedisSearchCtx *sctx, RSAddDocumentCtx *aCtx) {
  if (!aCtx->sv) {
    aCtx->sv = NewSortingVector(sctx->spec->sortables->len);
  }
}

#define FIELD_HANDLER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs, \
                  fieldData *fdata, const char **errorString)

#define FIELD_INDEXER FIELD_HANDLER
#define FIELD_PREPROCESSOR FIELD_HANDLER

typedef int (*FieldFunc)(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs,
                         fieldData *fdata, const char **errorString);

typedef FieldFunc PreprocessorFunc;
typedef FieldFunc IndexerFunc;

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  if (fs->sortable) {
    ensureSortingVector(ctx, aCtx);
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  aCtx->totalTokens = tokenize(c, fs->weight, fs->id, aCtx->fwIdx, forwardIndexTokenFunc,
                               aCtx->fwIdx->stemmer, aCtx->totalTokens, ctx->spec->stopwords);
  return 0;
}

FIELD_PREPROCESSOR(numericPreprocessor) {
  if (RedisModule_StringToDouble(field->text, &fdata->numeric) == REDISMODULE_ERR) {
    *errorString = "Could not parse numeric index value";
    return -1;
  }

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (fs->sortable) {
    ensureSortingVector(&aCtx->rsCtx, aCtx);
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM);
  }
  return 0;
}

FIELD_INDEXER(numericIndexer) {
  NumericRangeTree *rt = OpenNumericIndex(&aCtx->rsCtx, fs->name);
  NumericRangeTree_Add(rt, aCtx->doc.docId, fdata->numeric);
  return 0;
}

FIELD_PREPROCESSOR(geoPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  char *pos = strpbrk(c, " ,");
  if (!pos) {
    *errorString = "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"";
    return -1;
  }
  *pos = '\0';
  pos++;
  fdata->geo.slon = (char *)c;
  fdata->geo.slat = (char *)pos;
  return 0;
}

FIELD_INDEXER(geoIndexer) {

  GeoIndex gi = {.ctx = &aCtx->rsCtx, .sp = fs};
  int rv = GeoIndex_AddStrings(&gi, aCtx->doc.docId, fdata->geo.slon, fdata->geo.slat);

  if (rv == REDISMODULE_ERR) {
    *errorString = "Could not index geo value";
    return -1;
  }
  return 0;
}

static PreprocessorFunc getPreprocessor(const FieldType ft) {
  switch (ft) {
    case F_FULLTEXT:
      return fulltextPreprocessor;
    case F_NUMERIC:
      return numericPreprocessor;
    case F_GEO:
      return geoPreprocessor;
    default:
      return NULL;
  }
}

static IndexerFunc getIndexer(const FieldType ft) {
  switch (ft) {
    case F_NUMERIC:
      return numericIndexer;
    case F_GEO:
      return geoIndexer;
    case F_FULLTEXT:
    default:
      return NULL;
  }
}

/* Add a parsed document to the index. If replace is set, we will add it be deleting an older
 * version of it first */
int Document_AddToIndexes(RSAddDocumentCtx *aCtx) {
  Document *doc = &aCtx->doc;
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  int ourRv = REDISMODULE_OK;

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    // Get handler
    PreprocessorFunc pp = getPreprocessor(fs->type);
    if (pp == NULL) {
      continue;
    }

    if (pp(aCtx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  if (DocumentIndexer_Add(Indexer_g, aCtx) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (ourRv != REDISMODULE_OK) {
    if (aCtx->errorString == NULL) {
      aCtx->errorString = "ERR couldn't index document";
    }
    sendReply(aCtx);
  }
  return ourRv;
}

static void writeIndexEntry(IndexSpec *spec, InvertedIndex *idx, IndexEncoder encoder,
                            ForwardIndexEntry *entry) {
  size_t sz = InvertedIndex_WriteForwardIndexEntry(idx, encoder, entry);

  /*******************************************
  * update stats for the index
  ********************************************/
  /* record the actual size consumption change */
  spec->stats.invertedSize += sz;

  spec->stats.numRecords++;
  /* increment the number of terms if this is a new term*/

  /* Record the space saved for offset vectors */
  if (spec->flags & Index_StoreTermOffsets) {
    spec->stats.offsetVecsSize += VVW_GetByteLength(entry->vw);
    spec->stats.offsetVecRecords += VVW_GetCount(entry->vw);
  }
  entry->indexerState = 1;
}

#define TERMS_PER_BLOCK 128

typedef struct mergedEntry {
  KHTableEntry base;
  // size_t count;
  ForwardIndexEntry *head;
  ForwardIndexEntry *tail;
} mergedEntry;

static int mergedCompare(const KHTableEntry *ent, const void *s, size_t n, uint32_t h) {
  mergedEntry *e = (mergedEntry *)ent;
  if (e->head->hash != h) {
    return 1;
  }
  if (e->head->len != n) {
    return 1;
  }
  return memcmp(e->head->term, s, n);
}

static uint32_t mergedHash(const KHTableEntry *ent) {
  mergedEntry *e = (mergedEntry *)ent;
  return e->head->hash;
}

static KHTableEntry *mergedAlloc(void *ctx) {
  BlkAlloc *alloc = ctx;
  return BlkAlloc_Alloc(alloc, sizeof(mergedEntry), sizeof(mergedEntry) * TERMS_PER_BLOCK);
}

static size_t countMerged(mergedEntry *ent) {
  size_t n = 0;
  for (ForwardIndexEntry *cur = ent->head; cur; cur = cur->next) {
    n++;
  }
  return n;
}

static void doMerge(RSAddDocumentCtx *aCtx, KHTable *ht) {
  RSAddDocumentCtx *last, *cur;
  size_t counter = 0;
  cur = aCtx;

merge_loop:
  last = cur;
  for (; last->next; last = last->next) {
  }

  // Traverse *all* the entries of *all* the tables
  for (; cur != last->next; cur = cur->next) {

    ForwardIndexIterator it = ForwardIndex_Iterate(cur->fwIdx);
    ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);

    while (entry) {
      // Set the document ID
      entry->docId = cur->doc.docId;
      // Get the entry for it.
      int isNew = 0;
      mergedEntry *mergedEnt =
          (mergedEntry *)KHTable_GetEntry(ht, entry->term, entry->len, entry->hash, &isNew);

      if (isNew) {
        mergedEnt->head = mergedEnt->tail = entry;

      } else {
        mergedEnt->tail->next = entry;
        mergedEnt->tail = entry;
      }

      entry->next = NULL;
      entry = ForwardIndexIterator_Next(&it);
    }
    cur->stateFlags |= ACTX_F_MERGED;
  }

  assert(cur == last->next);
  // Check again to see if we have more items in the queue, but if we have a
  // high workload, don't overfill the hashtable
  if (cur && ++counter < 100) {
    goto merge_loop;
  }
}

static int writeMergedEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx, KHTable *ht) {
  IndexEncoder encoder = InvertedIndex_GetEncoder(ACTX_SPEC(aCtx)->flags);

  // Iterate over all the entries
  for (uint32_t curBucketIdx = 0; curBucketIdx < ht->numBuckets; curBucketIdx++) {
    for (KHTableEntry *entp = ht->buckets[curBucketIdx]; entp; entp = entp->next) {
      mergedEntry *merged = (mergedEntry *)entp;

      // Open the inverted index:
      ForwardIndexEntry *fwent = merged->head;
      IndexSpec_AddTerm(aCtx->rsCtx.spec, fwent->term, fwent->len);

      RedisModuleKey *idxKey = NULL;
      InvertedIndex *invidx =
          Redis_OpenInvertedIndexEx(&aCtx->rsCtx, fwent->term, fwent->len, 1, &idxKey);

      if (invidx == NULL) {
        continue;
      }

      size_t curNumMerged = 0;
      for (; fwent != NULL; fwent = fwent->next) {
        writeIndexEntry(ACTX_SPEC(aCtx), invidx, encoder, fwent);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ACTX_SPEC(aCtx) == NULL) {
        printf("Spec is NULL!?\n");
        aCtx->errorString = "ERR Index is no longer valid!";
        return -1;
      }
    }
  }
  return 0;
}

static void writeCurEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->specFlags);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  while (entry != NULL) {
    RedisModuleKey *idxKey = NULL;

    if (entry->indexerState) {
      goto next_entry;
    }

    IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, entry->term, entry->len, 1, &idxKey);
    if (invidx) {
      entry->docId = aCtx->doc.docId;
      writeIndexEntry(ctx->spec, invidx, encoder, entry);
      // // Determine if this term is even common
      // const IndexStats *stats = &ctx->spec->stats;
      // double avgFrequency = (double)stats->numRecords / (double)stats->numTerms;

      // if (invidx->numDocs < (avgFrequency * 10)) {
      //   goto next_entry;
      // }

      // for (RSAddDocumentCtx *cur = aCtx->next; cur; cur = cur->next) {
      //   // Find more tables
      //   ForwardIndexEntry *curEnt =
      //       ForwardIndex_Find(cur->fwIdx, entry->term, entry->len, entry->hash);
      //   if (curEnt == NULL || curEnt->indexerState) {
      //     continue;
      //   }

      //   writeIndexEntry(ctx->spec, invidx, encoder, curEnt);
      // }
    }

  next_entry:
    entry = ForwardIndexIterator_Next(&it);
    if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ACTX_SPEC(aCtx) == NULL) {
      aCtx->errorString = "ERR Index is no longer valid!";
      return;
    }
  }
}

static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  // printf("totaltokens :%d\n", totalTokens);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  KHTable mergedEntries;
  BlkAlloc entriesAlloc;
  BlkAlloc_Init(&entriesAlloc);

  const int needsFtIndex = !(aCtx->stateFlags & ACTX_F_MERGED);
  int useHt = 0;
  static const KHTableProcs procs = {
      .Alloc = mergedAlloc, .Compare = mergedCompare, .Hash = mergedHash};

  if (indexer->size > 1 && needsFtIndex) {
    useHt = 1;
    KHTable_Init(&mergedEntries, &procs, &entriesAlloc, aCtx->fwIdx->hits->numItems);
    doMerge(aCtx, &mergedEntries);
  }

  indexer->concCtx.numOpenKeys = 0;
  indexer->concCtx.ctx = aCtx->rsCtx.redisCtx;
  ConcurrentSearch_AddKey(&indexer->concCtx, NULL, REDISMODULE_READ | REDISMODULE_WRITE,
                          ctx->keyName, reopenCb, aCtx, NULL);

  ConcurrentSearchCtx_ResetClock(&indexer->concCtx);
  ConcurrentSearchCtx_Lock(&indexer->concCtx);

  if (!ACTX_SPEC(aCtx)) {
    aCtx->errorString = "ERR Index no longer valid";
    goto cleanup;
  }

  Document *doc = &aCtx->doc;
  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL) {
      continue;
    }

    IndexerFunc ifx = getIndexer(fs->type);
    if (ifx == NULL) {
      continue;
    }

    if (ifx(aCtx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      goto cleanup;
    }
  }

  RSDocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc->docId);
  md->maxFreq = aCtx->fwIdx->maxFreq;
  if (aCtx->sv) {
    DocTable_SetSortingVector(&ctx->spec->docs, doc->docId, aCtx->sv);
  }

  if (needsFtIndex) {
    if (useHt) {
      // printf("Going fast! :)\n");
      writeMergedEntries(indexer, aCtx, &mergedEntries);
    } else {
      // printf("Going slow... :'(\n");
      writeCurEntries(indexer, aCtx);
    }
  }

cleanup:
  ConcurrentSearchCtx_Unlock(&indexer->concCtx);
  if (useHt) {
    BlkAlloc_FreeAll(&entriesAlloc, NULL, 0);
    KHTable_Free(&mergedEntries);
  }
}

static void *DocumentIndexer_Run(void *p) {
  DocumentIndexer *indexer = p;

  while (1) {
    pthread_mutex_lock(&indexer->lock);
    while (indexer->head == NULL) {
      pthread_cond_wait(&indexer->cond, &indexer->lock);
    }

    RSAddDocumentCtx *cur = indexer->head;
    indexer->size--;

    if ((indexer->head = cur->next) == NULL) {
      indexer->tail = NULL;
    }
    pthread_mutex_unlock(&indexer->lock);
    DocumentIndexer_Process(indexer, cur);
    sendReply(cur);
  }
  return NULL;
}

struct DocumentIndexer *Indexer_g = NULL;

void StartDocumentIndexer() {
  Indexer_g = calloc(1, sizeof(*Indexer_g));
  Indexer_g->head = Indexer_g->tail = NULL;
  ConcurrentSearchCtx_Init(NULL, &Indexer_g->concCtx);
  pthread_cond_init(&Indexer_g->cond, NULL);
  pthread_mutex_init(&Indexer_g->lock, NULL);
  static pthread_t dummyThr;
  pthread_create(&dummyThr, NULL, DocumentIndexer_Run, Indexer_g);
}

static int tryMakeDocId(RSAddDocumentCtx *aCtx) {
  int rv = 0;

  ConcurrentSearchCtx_Lock(&aCtx->conc);
  if (!ACTX_SPEC(aCtx)) {
    aCtx->errorString = "ERR index was deleted";
    rv = -1;
    goto cleanup;
  }

  if (makeDocumentId(&aCtx->doc, &aCtx->rsCtx, aCtx->options & DOCUMENT_ADD_REPLACE,
                     &aCtx->errorString) != 0) {
    rv = -1;
  }

cleanup:
  ConcurrentSearchCtx_Unlock(&aCtx->conc);
  return rv;
}

static int DocumentIndexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {

  pthread_mutex_lock(&indexer->lock);
  if (tryMakeDocId(aCtx) != 0) {
    pthread_mutex_unlock(&indexer->lock);
    return -1;
  }

  if (indexer->tail) {
    indexer->tail->next = aCtx;
    indexer->tail = aCtx;
  } else {
    indexer->head = indexer->tail = aCtx;
  }

  pthread_cond_signal(&indexer->cond);
  pthread_mutex_unlock(&indexer->lock);

  indexer->size++;
  return 0;
}