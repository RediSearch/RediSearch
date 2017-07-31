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

void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize) {
  doc->docKey = docKey;
  doc->score = (float)score;
  doc->numFields = numFields;
  doc->fields = calloc(doc->numFields, sizeof(DocumentField));
  doc->language = lang;
  doc->payload = payload;
  doc->payloadSize = payloadSize;
}

void Document_Detach(Document *doc, RedisModuleCtx *srcCtx) {
  RedisModule_RetainString(srcCtx, doc->docKey);
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    DocumentField *f = doc->fields + ii;
    RedisModule_RetainString(srcCtx, f->text);
    f->name = strdup(f->name);
  }
  doc->stringOwner = 1;
  if (doc->payload) {
    doc->payload = strndup(doc->payload, doc->payloadSize);
  }
  if (doc->language) {
    doc->language = strdup(doc->language);
  }
}

void Document_Free(Document *doc) {
  free(doc->fields);
}

void Document_FreeDetached(Document *doc, RedisModuleCtx *anyCtx) {
  RedisModule_FreeString(anyCtx, doc->docKey);

  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    RedisModule_FreeString(anyCtx, doc->fields[ii].text);
    free((void *)doc->fields[ii].name);
  }

  free((char *)doc->payload);
  free((char *)doc->language);

  Document_Free(doc);
}

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc) {

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, doc->docKey, REDISMODULE_WRITE | REDISMODULE_READ);
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < doc->numFields; i++) {
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, doc->fields[i].name, doc->fields[i].text,
                        NULL);
  }
  return REDISMODULE_OK;
}

static int Document_Store(Document *doc, RedisSearchCtx *ctx, int options,
                          const char **errorString) {
  const char *keystr = RedisModule_StringPtrLen(doc->docKey, NULL);
  DocTable *table = &ctx->spec->docs;

  // if we're in replace mode, first we need to try and delete the older version of the document
  if (options & DOCUMENT_ADD_REPLACE) {
    DocTable_Delete(table, keystr);
  }
  doc->docId = DocTable_Put(table, keystr, doc->score, 0, doc->payload, doc->payloadSize);

  // Make sure the document is not already in the index - it needs to be
  // incremental!
  if (doc->docId == 0) {
    *errorString = "Couldn't allocate new document table entry";
    return -1;
  }

  if (!(options & DOCUMENT_ADD_NOSAVE)) {
    // first save the document as hash
    if (REDISMODULE_ERR == Redis_SaveDocument(ctx, doc)) {
      *errorString = "Couldn't save document";
      goto fail;
    }
  }

  return 0;

fail:
  DocTable_Delete(table, keystr);
  return -1;
}

typedef struct {
  ForwardIndex *idx;
  RSSortingVector *sv;
  Document *doc;
  size_t totalTokens;
} indexingContext;

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

  RedisModuleString *indexName =
      RedisModule_CreateStringPrintf(aCtx->thCtx, INDEX_SPEC_KEY_FMT, sp->name);

  ConcurrentSearchCtx_Init(aCtx->thCtx, &aCtx->conc);

  ConcurrentSearch_AddKey(&aCtx->conc, NULL, REDISMODULE_READ | REDISMODULE_WRITE, indexName,
                          reopenCb, aCtx, NULL);
  return aCtx;
}

void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx) {
  Document_FreeDetached(&aCtx->doc, aCtx->thCtx);
  ConcurrentSearchCtx_Free(&aCtx->conc);
  RedisModule_FreeThreadSafeContext(aCtx->thCtx);
  free(aCtx);
}

#define ACTX_SPEC(actx) (actx)->rsCtx.spec

static void ensureSortingVector(RedisSearchCtx *sctx, indexingContext *ictx) {
  if (!ictx->sv) {
    ictx->sv = NewSortingVector(sctx->spec->sortables->len);
  }
}

static int handleFulltextField(RSAddDocumentCtx *aCtx, indexingContext *ictx,
                               const DocumentField *field, const FieldSpec *fs,
                               const char **errorString) {
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

static int handleNumericField(RSAddDocumentCtx *aCtx, indexingContext *ictx,
                              const DocumentField *field, const FieldSpec *fs,
                              const char **errorString) {
  double score;
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  if (RedisModule_StringToDouble(field->text, &score) == REDISMODULE_ERR) {
    *errorString = "Could not parse numeric index value";
    return -1;
  }

  ConcurrentSearchCtx_Lock(&aCtx->conc);
  if (ACTX_SPEC(aCtx) != NULL) {
    NumericRangeTree *rt = OpenNumericIndex(&aCtx->rsCtx, fs->name);
    NumericRangeTree_Add(rt, ictx->doc->docId, score);
  }
  ConcurrentSearchCtx_Unlock(&aCtx->conc);

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (fs->sortable) {
    ensureSortingVector(ctx, ictx);
    RSSortingVector_Put(ictx->sv, fs->sortIdx, &score, RS_SORTABLE_NUM);
  }
  return 0;
}

static int handleGeoField(RSAddDocumentCtx *aCtx, indexingContext *ictx, const DocumentField *field,
                          const FieldSpec *fs, const char **errorString) {

  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  char *pos = strpbrk(c, " ,");
  if (!pos) {
    *errorString = "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"";
    return -1;
  }
  *pos = '\0';
  pos++;
  char *slon = (char *)c, *slat = (char *)pos;

  GeoIndex gi = {.ctx = ctx, .sp = fs};

  ConcurrentSearchCtx_Lock(&aCtx->conc);
  int rv = GeoIndex_AddStrings(&gi, ictx->doc->docId, slon, slat);
  ConcurrentSearchCtx_Unlock(&aCtx->conc);

  if (rv == REDISMODULE_ERR) {
    *errorString = "Could not index geo value";
    return -1;
  }
  return 0;
}

static int indexField(RSAddDocumentCtx *aCtx, const DocumentField *field, const char **errorString,
                      indexingContext *ictx, const FieldSpec *fs) {

  switch (fs->type) {
    case F_FULLTEXT:
      return handleFulltextField(aCtx, ictx, field, fs, errorString);
    case F_NUMERIC:
      return handleNumericField(aCtx, ictx, field, fs, errorString);
    case F_GEO:
      return handleGeoField(aCtx, ictx, field, fs, errorString);
    default:
      break;
  }
  return 0;
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
  int ourRv = REDISMODULE_OK;
  int isLocked = 0;
  ForwardIndex *idx = NULL;

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

  int rv = Document_Store(doc, ctx, aCtx->options, errorString);
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

  if (rv != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

  idx = NewForwardIndex(doc, specFlags);
  indexingContext ictx = {.idx = idx, .doc = doc, .totalTokens = 0};

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = fieldSpecs + i;
    if (fs->name == NULL) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    if (indexField(aCtx, &doc->fields[i], errorString, &ictx, fs) == -1) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  DO_LOCK();
  ENSURE_SPEC();

  RSDocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc->docId);
  md->maxFreq = idx->maxFreq;
  if (ictx.sv) {
    DocTable_SetSortingVector(&ctx->spec->docs, doc->docId, ictx.sv);
    // Sorting vector is now shared - so we shouldn't touch this afterwards
    ictx.sv = NULL;
  }
  if (ictx.totalTokens > 0) {
    addTokensToIndex(&ictx, aCtx);
  }

  if (ctx->spec != NULL) {
    ctx->spec->stats.numDocuments += 1;
  }
  DO_UNLOCK();

cleanup:
  if (isLocked) {
    DO_UNLOCK();
  }
  if (idx != NULL) {
    ForwardIndexFree(idx);
  }
  free(fieldSpecs);
  return ourRv;
}
