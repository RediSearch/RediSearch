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

static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  // printf("totaltokens :%d\n", totalTokens);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->specFlags);

  size_t idxNameLen;
  const char *idxName = RedisModule_StringPtrLen(ctx->keyName, &idxNameLen);
  idxName += sizeof(INDEX_SPEC_KEY_PREFIX) - 1;
  idxNameLen -= sizeof(INDEX_SPEC_KEY_PREFIX) - 1;

  indexer->concCtx.numOpenKeys = 0;
  indexer->concCtx.ctx = aCtx->rsCtx.redisCtx;
  ConcurrentSearch_AddKey(&indexer->concCtx, NULL, REDISMODULE_READ | REDISMODULE_WRITE,
                          ctx->keyName, reopenCb, aCtx, NULL);

  // Build a table of terms

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

      // Determine if this term is even common
      const IndexStats *stats = &ctx->spec->stats;
      double avgFrequency = (double)stats->numRecords / (double)stats->numTerms;

      if (invidx->numDocs < (avgFrequency * 10)) {
        goto next_entry;
      }

      for (RSAddDocumentCtx *cur = aCtx->next; cur; cur = cur->next) {
        // Find more tables
        ForwardIndexEntry *curEnt =
            ForwardIndex_Find(cur->fwIdx, entry->term, entry->len, entry->hash);
        if (curEnt == NULL || curEnt->indexerState) {
          continue;
        }

        // Compare spec names
        size_t curNameLen;
        const char *curName = RedisModule_StringPtrLen(cur->rsCtx.keyName, &curNameLen);
        curName += sizeof(INDEX_SPEC_KEY_PREFIX) - 1;
        curNameLen -= sizeof(INDEX_SPEC_KEY_PREFIX) - 1;
        if (curNameLen != idxNameLen || strncmp(curName, idxName, curNameLen)) {
          continue;
        }

        writeIndexEntry(ctx->spec, invidx, encoder, curEnt);
      }
    }

  next_entry:
    entry = ForwardIndexIterator_Next(&it);
    if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ACTX_SPEC(aCtx) == NULL) {
      aCtx->errorString = "ERR Index is no longer valid!";
      goto cleanup;
    }
  }

cleanup:
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

  indexer->size++;
  pthread_cond_signal(&indexer->cond);
  pthread_mutex_unlock(&indexer->lock);
  return 0;
}