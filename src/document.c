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
  size_t totalTokens;
} indexingContext;

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
  // printf("totaltokens :%d\n", totalTokens);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  ForwardIndexIterator it = ForwardIndex_Iterate(ictx->idx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->rsCtx.spec->flags);

  ConcurrentSearchCtx_ResetClock(&aCtx->conc);

  while (entry != NULL) {
    IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
    RedisModuleKey *idxKey;
    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, entry->term, entry->len, 1, &idxKey);
    size_t sz = InvertedIndex_WriteForwardIndexEntry(invidx, encoder, entry);

    /*******************************************
    * update stats for the index
    ********************************************/
    /* record the actual size consumption change */
    ctx->spec->stats.invertedSize += sz;

    ctx->spec->stats.numRecords++;
    /* increment the number of terms if this is a new term*/

    /* Record the space saved for offset vectors */
    if (ctx->spec->flags & Index_StoreTermOffsets) {
      ctx->spec->stats.offsetVecsSize += VVW_GetByteLength(entry->vw);
      ctx->spec->stats.offsetVecRecords += VVW_GetCount(entry->vw);
    }

    // Redis_CloseWriter(w);
    RedisModule_CloseKey(idxKey);

    entry = ForwardIndexIterator_Next(&it);

    if (CONCURRENT_CTX_TICK(&aCtx->conc)) {
      if (ACTX_SPEC(aCtx) == NULL) {
        // Spec is gone!
        return;
      }
    }
  }
}

/* Add a parsed document to the index. If replace is set, we will add it be deleting an older
 * version of it first */
int Document_AddToIndexes(RSAddDocumentCtx *aCtx, const char **errorString) {
  Document *doc = &aCtx->doc;
  RedisSearchCtx *ctx = &aCtx->rsCtx;
  FieldSpec *fieldSpecs = malloc(aCtx->doc.numFields * sizeof(*fieldSpecs));
  fieldData *fieldDatas = malloc(aCtx->doc.numFields * sizeof(*fieldDatas));
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

  if (makeDocumentId(doc, ctx, aCtx->options & DOCUMENT_ADD_REPLACE, errorString) != 0) {
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
      fieldSpecs[i] = *fs;
    } else {
      fieldSpecs[i].name = NULL;
    }
  }

  DO_UNLOCK();
  aCtx->fwIdx = NewForwardIndex(doc, specFlags);
  indexingContext ictx = {.idx = aCtx->fwIdx, .doc = doc, .totalTokens = 0};

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = fieldSpecs + i;
    fieldData *fdata = fieldDatas + i;
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

  DO_LOCK();
  ENSURE_SPEC();

  RSDocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc->docId);
  md->maxFreq = aCtx->fwIdx->maxFreq;
  if (ictx.sv) {
    DocTable_SetSortingVector(&ctx->spec->docs, doc->docId, ictx.sv);
    // Sorting vector is now shared - so we shouldn't touch this afterwards
    ictx.sv = NULL;
  }

  // Index all the other fields
  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = fieldSpecs + i;
    fieldData *fdata = fieldDatas + i;
    if (fs->name == NULL) {
      continue;
    }
    IndexerFunc ifx = getIndexer(fs->type);
    if (ifx == NULL) {
      continue;
    }

    if (ifx(aCtx, &ictx, &doc->fields[i], fs, fdata, errorString) != 0) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  if (ictx.totalTokens > 0) {
    addTokensToIndex(&ictx, aCtx);
  }

  DO_UNLOCK();

cleanup:
  if (isLocked) {
    DO_UNLOCK();
  }
  free(fieldSpecs);
  free(fieldDatas);
  return ourRv;
}
