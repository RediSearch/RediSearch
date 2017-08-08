#include <string.h>

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
typedef union {
  double numeric;
  struct {
    char *slon;
    char *slat;
  } geo;
} fieldData;

typedef struct {
  ForwardIndex *idx;
  RSSortingVector *sv;
  Document *doc;
  FieldSpec *specs;
  fieldData *fdatas;
  size_t totalTokens;
} indexingContext;

typedef struct DocumentIndexer {
  RSAddDocumentCtx *head;
  RSAddDocumentCtx *tail;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  ConcurrentSearchCtx concCtx;
} DocumentIndexer;

// Process a single context
static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *ctx);

// Main loop
static void *DocumentIndexer_Run(void *arg);

// Enqueue the context to be written, and return when the context has been written
// (or an error occurred)
static int DocumentIndexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx,
                               indexingContext *ictx, const char **errorString);

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

RSAddDocumentCtx *NewAddDocumentCtx(RedisModuleBlockedClient *client, IndexSpec *sp) {
  // Block the client and create the context!
  RSAddDocumentCtx *aCtx = calloc(1, sizeof(*aCtx));
  aCtx->bc = client;
  aCtx->thCtx = RedisModule_GetThreadSafeContext(aCtx->bc);
  RedisModule_AutoMemory(aCtx->thCtx);
  aCtx->rsCtx.redisCtx = aCtx->thCtx;
  aCtx->rsCtx.spec = sp;
  aCtx->next = NULL;
  aCtx->rsCtx.keyName = RedisModule_CreateStringPrintf(aCtx->thCtx, INDEX_SPEC_KEY_FMT, sp->name);

  pthread_cond_init(&aCtx->cond, NULL);

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
  pthread_cond_destroy(&aCtx->cond);
  free(aCtx);
}

#define ACTX_SPEC(actx) (actx)->rsCtx.spec

static void ensureSortingVector(RedisSearchCtx *sctx, indexingContext *ictx) {
  if (!ictx->sv) {
    ictx->sv = NewSortingVector(sctx->spec->sortables->len);
  }
}

#define FIELD_HANDLER(name)                                                                  \
  static int name(RSAddDocumentCtx *aCtx, indexingContext *ictx, const DocumentField *field, \
                  const FieldSpec *fs, fieldData *fdata, const char **errorString)

#define FIELD_INDEXER FIELD_HANDLER
#define FIELD_PREPROCESSOR FIELD_HANDLER

typedef int (*FieldFunc)(RSAddDocumentCtx *aCtx, indexingContext *ictx, const DocumentField *field,
                         const FieldSpec *fs, fieldData *fdata, const char **errorString);

typedef FieldFunc PreprocessorFunc;
typedef FieldFunc IndexerFunc;

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  if (fs->sortable) {
    ensureSortingVector(ctx, ictx);
    RSSortingVector_Put(ictx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  ictx->totalTokens = tokenize(c, fs->weight, fs->id, ictx->idx, forwardIndexTokenFunc,
                               ictx->idx->stemmer, ictx->totalTokens, ctx->spec->stopwords);
  return 0;
}

FIELD_PREPROCESSOR(numericPreprocessor) {
  if (RedisModule_StringToDouble(field->text, &fdata->numeric) == REDISMODULE_ERR) {
    *errorString = "Could not parse numeric index value";
    return -1;
  }

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (fs->sortable) {
    ensureSortingVector(&aCtx->rsCtx, ictx);
    RSSortingVector_Put(ictx->sv, fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM);
  }
  return 0;
}

FIELD_INDEXER(numericIndexer) {
  NumericRangeTree *rt = OpenNumericIndex(&aCtx->rsCtx, fs->name);
  NumericRangeTree_Add(rt, ictx->doc->docId, fdata->numeric);
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
  int rv = GeoIndex_AddStrings(&gi, ictx->doc->docId, fdata->geo.slon, fdata->geo.slat);

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

void addTokensToIndex(indexingContext *ictx, RSAddDocumentCtx *aCtx) {
}

/* Add a parsed document to the index. If replace is set, we will add it be deleting an older
 * version of it first */
int Document_AddToIndexes(RSAddDocumentCtx *aCtx, const char **errorString) {
  Document *doc = &aCtx->doc;
  RedisSearchCtx *ctx = &aCtx->rsCtx;
  indexingContext ictx = {0};
  ictx.specs = malloc(aCtx->doc.numFields * sizeof(*ictx.specs));
  ictx.fdatas = malloc(aCtx->doc.numFields * sizeof(*ictx.fdatas));

  int ourRv = REDISMODULE_OK;
  int isLocked = 0;

#define ENSURE_SPEC()                                        \
  if (ACTX_SPEC(aCtx) == NULL) {                             \
    LG_DEBUG("Someone deleted the index while processing!"); \
    ourRv = REDISMODULE_ERR;                                 \
    goto cleanup;                                            \
  }

#define DO_LOCK()                        \
  ConcurrentSearchCtx_Lock(&aCtx->conc); \
  isLocked = 1;
#define DO_UNLOCK()                        \
  ConcurrentSearchCtx_Unlock(&aCtx->conc); \
  isLocked = 0;

  DO_LOCK();
  ENSURE_SPEC();

  if ((aCtx->options & DOCUMENT_ADD_NOSAVE) == 0 &&
      (ourRv = Redis_SaveDocument(ctx, doc)) != REDISMODULE_OK) {
    *errorString = "Couldn't save document";
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }
  uint32_t specFlags = ACTX_SPEC(aCtx)->flags;

  // Also, get the field specs. We cache this here because the context is unlocked
  // during the actual tokenization
  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(aCtx->rsCtx.spec, f->name, strlen(f->name));
    if (fs) {
      ictx.specs[i] = *fs;
    } else {
      ictx.specs[i].name = NULL;
    }
  }

  DO_UNLOCK();

  ictx.idx = aCtx->fwIdx = NewForwardIndex(doc, specFlags);
  ictx.doc = doc;
  ictx.totalTokens = 0;

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = ictx.specs + i;
    fieldData *fdata = ictx.fdatas + i;
    if (fs->name == NULL) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    // Get handler
    PreprocessorFunc pp = getPreprocessor(fs->type);
    if (pp == NULL) {
      continue;
    }

    if (pp(aCtx, &ictx, &doc->fields[i], fs, fdata, errorString) != 0) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  if (DocumentIndexer_Add(Indexer_g, aCtx, &ictx, errorString) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (isLocked) {
    DO_UNLOCK();
  }
  free(ictx.specs);
  free(ictx.fdatas);
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
}

static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  // printf("totaltokens :%d\n", totalTokens);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);

  indexer->concCtx.numOpenKeys = 0;
  indexer->concCtx.ctx = aCtx->rsCtx.redisCtx;
  ConcurrentSearch_AddKey(&indexer->concCtx, NULL, REDISMODULE_READ | REDISMODULE_WRITE,
                          ctx->keyName, reopenCb, aCtx, NULL);

  ConcurrentSearchCtx_ResetClock(&aCtx->conc);
  ConcurrentSearchCtx_Lock(&indexer->concCtx);

  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->rsCtx.spec->flags);

  while (entry != NULL) {
    entry->docId = aCtx->doc.docId;

    IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
    RedisModuleKey *idxKey;
    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, entry->term, entry->len, 1, &idxKey);
    if (invidx) {
      writeIndexEntry(ctx->spec, invidx, encoder, entry);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }

    entry = ForwardIndexIterator_Next(&it);

    if (CONCURRENT_CTX_TICK(&indexer->concCtx)) {
      if (ACTX_SPEC(aCtx) == NULL) {
        // Spec is gone!
        return;
      }
    }
  }
  ConcurrentSearchCtx_Unlock(&indexer->concCtx);
}

static void *DocumentIndexer_Run(void *p) {
  DocumentIndexer *indexer = p;
  while (1) {
    pthread_mutex_lock(&indexer->lock);
    while (indexer->head == NULL) {
      pthread_cond_wait(&indexer->cond, &indexer->lock);
    }

    RSAddDocumentCtx *cur = indexer->head;

    if ((indexer->head = cur->next) == NULL) {
      indexer->tail = NULL;
    }
    pthread_mutex_unlock(&indexer->lock);

    DocumentIndexer_Process(indexer, cur);
    cur->done = 1;
    pthread_cond_signal(&cur->cond);
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

static int DocumentIndexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx,
                               indexingContext *ictx, const char **errorString) {
  Document *doc = &aCtx->doc;
  RedisSearchCtx *ctx = &aCtx->rsCtx;
  int isLocked = 0, hasGil = 0;
  int rv = 0;

  pthread_mutex_lock(&indexer->lock);
  ConcurrentSearchCtx_Lock(&aCtx->conc);
  isLocked = hasGil = 1;

  if (makeDocumentId(doc, ctx, aCtx->options & DOCUMENT_ADD_REPLACE, errorString) != 0) {
    rv = -1;
    goto cleanup;
  }

  RSDocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc->docId);
  md->maxFreq = aCtx->fwIdx->maxFreq;
  if (ictx->sv) {
    DocTable_SetSortingVector(&ctx->spec->docs, doc->docId, ictx->sv);
    // Sorting vector is now shared - so we shouldn't touch this afterwards
    ictx->sv = NULL;
  }

  // Write the 'bare' indexes
  // Index all the other fields
  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = ictx->specs + i;
    fieldData *fdata = ictx->fdatas + i;
    if (fs->name == NULL) {
      continue;
    }

    IndexerFunc ifx = getIndexer(fs->type);
    if (ifx == NULL) {
      continue;
    }

    if (ifx(aCtx, ictx, &doc->fields[i], fs, fdata, errorString) != 0) {
      rv = -1;
      goto cleanup;
    }
  }

  ConcurrentSearchCtx_Unlock(&aCtx->conc);
  hasGil = 0;

  if (ictx->totalTokens > 0) {
    if (indexer->tail) {
      indexer->tail->next = aCtx;
      indexer->tail = aCtx;
    } else {
      indexer->head = indexer->tail = aCtx;
    }

    // Unlock and allow an item to be popped off the queue
    pthread_mutex_unlock(&indexer->lock);
    isLocked = 0;

    pthread_cond_signal(&indexer->cond);

    // Lock it again (so we can unlock via cond_wait)
    pthread_mutex_lock(&indexer->lock);
    isLocked = 1;

    while (!aCtx->done) {
      pthread_cond_wait(&aCtx->cond, &indexer->lock);
    }

    pthread_mutex_unlock(&indexer->lock);
    isLocked = 0;
  }

cleanup:
  if (hasGil) {
    ConcurrentSearchCtx_Unlock(&aCtx->conc);
  }
  if (isLocked) {
    pthread_mutex_unlock(&indexer->lock);
  }
  return rv;
}