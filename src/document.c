#include <string.h>
#include <assert.h>

#include "document.h"
#include "forward_index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/mempool.h"
#include "spec.h"
#include "tokenize.h"
#include "util/logging.h"
#include "search_request.h"
#include "rmalloc.h"
#include "indexer.h"
#include "tag_index.h"
#include "aggregate/expr/expression.h"

// Memory pool for RSAddDocumentContext contexts
static mempool_t *actxPool_g = NULL;

// For documentation, see these functions' definitions
static void *allocDocumentContext(void) {
  // See if there's one in the pool?
  RSAddDocumentCtx *aCtx = calloc(1, sizeof(*aCtx));
  return aCtx;
}

static void freeDocumentContext(void *p) {
  RSAddDocumentCtx *aCtx = p;
  if (aCtx->fwIdx) {
    ForwardIndexFree(aCtx->fwIdx);
  }

  free(aCtx->fspecs);
  free(aCtx->fdatas);
  free(aCtx);
}

#define DUP_FIELD_ERRSTR "Requested to index field twice"

static int AddDocumentCtx_SetDocument(RSAddDocumentCtx *aCtx, IndexSpec *sp, Document *base,
                                      size_t oldFieldCount) {
  aCtx->doc = *base;
  Document *doc = &aCtx->doc;

  if (oldFieldCount < doc->numFields) {
    // Pre-allocate the field specs
    aCtx->fspecs = realloc(aCtx->fspecs, sizeof(*aCtx->fspecs) * doc->numFields);
    aCtx->fdatas = realloc(aCtx->fdatas, sizeof(*aCtx->fdatas) * doc->numFields);
  }

  size_t numIndexable = 0;

  // size: uint16_t * SPEC_MAX_FIELDS
  FieldSpecDedupeArray dedupe = {0};
  int hasTextFields = 0;
  int hasOtherFields = 0;

  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(sp, f->name, strlen(f->name));
    if (fs && f->text) {

      aCtx->fspecs[i] = *fs;
      if (dedupe[fs->index]) {
        aCtx->errorString = DUP_FIELD_ERRSTR;
        return -1;
      }

      dedupe[fs->index] = 1;

      if (FieldSpec_IsSortable(fs)) {
        // mark sortable fields to be updated in the state flags
        aCtx->stateFlags |= ACTX_F_SORTABLES;
      }

      if (FieldSpec_IsIndexable(fs)) {
        if (fs->type == FIELD_FULLTEXT) {
          numIndexable++;
          hasTextFields = 1;
        } else {
          hasOtherFields = 1;
        }
      }
    } else {
      // Field is not in schema, or is duplicate
      aCtx->fspecs[i].name = NULL;
    }
  }

  if (hasTextFields || hasOtherFields) {
    aCtx->stateFlags |= ACTX_F_INDEXABLES;
  }
  if (!hasTextFields) {
    aCtx->stateFlags |= ACTX_F_TEXTINDEXED;
  }
  if (!hasOtherFields) {
    aCtx->stateFlags |= ACTX_F_OTHERINDEXED;
  }

  if ((aCtx->stateFlags & ACTX_F_SORTABLES) && aCtx->sv == NULL) {
    aCtx->sv = NewSortingVector(sp->sortables->len);
  }

  if ((aCtx->options & DOCUMENT_ADD_NOSAVE) == 0 && numIndexable &&
      (sp->flags & Index_StoreByteOffsets)) {
    if (!aCtx->byteOffsets) {
      aCtx->byteOffsets = NewByteOffsets();
      ByteOffsetWriter_Init(&aCtx->offsetsWriter);
    }
    RSByteOffsets_ReserveFields(aCtx->byteOffsets, numIndexable);
  }
  return 0;
}

RSAddDocumentCtx *NewAddDocumentCtx(IndexSpec *sp, Document *b, const char **err) {

  if (!actxPool_g) {
    actxPool_g = mempool_new(16, allocDocumentContext, freeDocumentContext);
  }

  // Get a new context
  RSAddDocumentCtx *aCtx = mempool_get(actxPool_g);
  aCtx->stateFlags = 0;
  aCtx->errorString = NULL;
  aCtx->totalTokens = 0;
  aCtx->client.bc = NULL;
  aCtx->next = NULL;
  aCtx->specFlags = sp->flags;
  aCtx->indexer = GetDocumentIndexer(sp->name);

  // Assign the document:
  if (AddDocumentCtx_SetDocument(aCtx, sp, b, aCtx->doc.numFields) != 0) {
    *err = aCtx->errorString;
    mempool_release(actxPool_g, aCtx);
    return NULL;
  }

  // try to reuse the forward index on recycled contexts
  if (aCtx->fwIdx) {
    ForwardIndex_Reset(aCtx->fwIdx, &aCtx->doc, sp->flags);
  } else {
    aCtx->fwIdx = NewForwardIndex(&aCtx->doc, sp->flags);
  }

  if (sp->smap) {
    // we get a read only copy of the synonym map for accessing in the index thread with out worring
    // about thready safe issues
    aCtx->fwIdx->smap = SynonymMap_GetReadOnlyCopy(sp->smap);
  } else {
    aCtx->fwIdx->smap = NULL;
  }

  aCtx->tokenizer = GetTokenizer(b->language, aCtx->fwIdx->stemmer, sp->stopwords);
  StopWordList_Ref(sp->stopwords);

  aCtx->doc.docId = 0;
  return aCtx;
}

static void doReplyFinish(RSAddDocumentCtx *aCtx, RedisModuleCtx *ctx) {
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(ctx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  AddDocumentCtx_Free(aCtx);
}

static int replyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RSAddDocumentCtx *aCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  doReplyFinish(aCtx, ctx);
  return REDISMODULE_OK;
}

static void threadCallback(void *p) {
  Document_AddToIndexes(p);
}

void AddDocumentCtx_Finish(RSAddDocumentCtx *aCtx) {
  if (aCtx->stateFlags & ACTX_F_NOBLOCK) {
    doReplyFinish(aCtx, aCtx->client.sctx->redisCtx);
  } else {
    RedisModule_UnblockClient(aCtx->client.bc, aCtx);
  }
}

// How many bytes in a document to warrant it being tokenized in a separate thread
#define SELF_EXEC_THRESHOLD 1024

void Document_Dump(const Document *doc) {
  printf("Document Key: %s. ID=%llu\n", RedisModule_StringPtrLen(doc->docKey, NULL), doc->docId);
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    printf("  [%lu]: %s => %s\n", ii, doc->fields[ii].name,
           RedisModule_StringPtrLen(doc->fields[ii].text, NULL));
  }
}

static void AddDocumentCtx_UpdateNoIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx);

static int AddDocumentCtx_ReplaceMerge(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  /**
   * The REPLACE operation contains fields which must be reindexed. This means
   * that a new document ID needs to be assigned, and as a consequence, all
   * fields must be reindexed.
   */
  // Free the old field data
  size_t oldFieldCount = aCtx->doc.numFields;
  Document_ClearDetachedFields(&aCtx->doc, sctx->redisCtx);

  // Get a list of fields needed to be loaded for reindexing
  const char **toLoad = array_new(const char *, 8);

  for (size_t ii = 0; ii < sctx->spec->numFields; ++ii) {
    // TODO: We should only need to reload fields that are actually being reindexed!
    toLoad = array_append(toLoad, sctx->spec->fields[ii].name);
  }

  // for (size_t ii = 0; ii < array_len(toLoad); ++ii) {
  //   printf("Loading: %s\n", toLoad[ii]);
  // }

  int rv =
      Redis_LoadDocumentEx(sctx, aCtx->doc.docKey, toLoad, array_len(toLoad), &aCtx->doc, NULL);
  // int rv = Redis_LoadDocument(sctx, aCtx->doc.docKey, &aCtx->doc);
  array_free(toLoad);

  if (rv != REDISMODULE_OK) {
    RedisModule_ReplyWithError(sctx->redisCtx, "Error updating document");
    AddDocumentCtx_Free(aCtx);
    return 1;
  }

  // Keep hold of the new fields.
  Document_DetachFields(&aCtx->doc, sctx->redisCtx);
  AddDocumentCtx_SetDocument(aCtx, sctx->spec, &aCtx->doc, oldFieldCount);

  return 0;
}

static int handlePartialUpdate(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  // Handle partial update of fields
  if (aCtx->stateFlags & ACTX_F_INDEXABLES) {
    return AddDocumentCtx_ReplaceMerge(aCtx, sctx);
  } else {
    // No indexable fields are updated, we can just update the metadata.
    // Quick update just updates the score, payload and sortable fields of the document.
    // Thus full-reindexing of the document is not required
    AddDocumentCtx_UpdateNoIndex(aCtx, sctx);
    return 1;
  }
}

void AddDocumentCtx_Submit(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, uint32_t options) {
  aCtx->options = options;
  if ((aCtx->options & DOCUMENT_ADD_PARTIAL) && handlePartialUpdate(aCtx, sctx)) {
    return;
  }
  if (AddDocumentCtx_IsBlockable(aCtx)) {
    aCtx->client.bc = RedisModule_BlockClient(sctx->redisCtx, replyCallback, NULL, NULL, 0);
  } else {
    aCtx->client.sctx = sctx;
  }
  assert(aCtx->client.bc);
  size_t totalSize = 0;
  for (size_t ii = 0; ii < aCtx->doc.numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    if (fs->name && (fs->type == FIELD_FULLTEXT || fs->type == FIELD_TAG)) {
      size_t n;
      RedisModule_StringPtrLen(aCtx->doc.fields[ii].text, &n);
      totalSize += n;
    }
  }

  if (totalSize >= SELF_EXEC_THRESHOLD && AddDocumentCtx_IsBlockable(aCtx)) {
    ConcurrentSearch_ThreadPoolRun(threadCallback, aCtx, CONCURRENT_POOL_INDEX);
  } else {
    Document_AddToIndexes(aCtx);
  }
}

void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx) {
  // Destroy the common fields:
  Document_FreeDetached(&aCtx->doc, aCtx->indexer->redisCtx);

  if (aCtx->sv) {
    SortingVector_Free(aCtx->sv);
    aCtx->sv = NULL;
  }

  if (aCtx->byteOffsets) {
    RSByteOffsets_Free(aCtx->byteOffsets);
    aCtx->byteOffsets = NULL;
  }

  if (aCtx->tokenizer) {
    // aCtx->tokenizer->Free(aCtx->tokenizer);
    Tokenizer_Release(aCtx->tokenizer);
    aCtx->tokenizer = NULL;
  }

  if (aCtx->oldMd) {
    DMD_Decref(aCtx->oldMd);
    aCtx->oldMd = NULL;
  }

  ByteOffsetWriter_Cleanup(&aCtx->offsetsWriter);

  mempool_release(actxPool_g, aCtx);
}

#define FIELD_HANDLER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs, \
                  fieldData *fdata, const char **errorString)

#define FIELD_BULK_INDEXER(name)                                                    \
  static int name(IndexBulkData *bulk, RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, \
                  DocumentField *field, const FieldSpec *fs, fieldData *fdata,      \
                  const char **errorString)

#define FIELD_BULK_CTOR(name) \
  static void name(IndexBulkData *bulk, const FieldSpec *fs, RedisSearchCtx *ctx)
#define FIELD_BULK_FINALIZER(name) static void name(IndexBulkData *bulk, RedisSearchCtx *ctx)

#define FIELD_PREPROCESSOR FIELD_HANDLER

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  size_t fl;
  const char *c = RedisModule_StringPtrLen(field->text, &fl);
  if (FieldSpec_IsSortable(fs)) {
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  if (FieldSpec_IsIndexable(fs)) {
    ForwardIndexTokenizerCtx tokCtx;
    VarintVectorWriter *curOffsetWriter = NULL;
    RSByteOffsetField *curOffsetField = NULL;
    if (aCtx->byteOffsets) {
      curOffsetField =
          RSByteOffsets_AddField(aCtx->byteOffsets, fs->textOpts.id, aCtx->totalTokens + 1);
      curOffsetWriter = &aCtx->offsetsWriter;
    }

    ForwardIndexTokenizerCtx_Init(&tokCtx, aCtx->fwIdx, c, curOffsetWriter, fs->textOpts.id,
                                  fs->textOpts.weight);

    uint32_t options = TOKENIZE_DEFAULT_OPTIONS;
    if (FieldSpec_IsNoStem(fs)) {
      options |= TOKENIZE_NOSTEM;
    }
    if (FieldSpec_IsPhonetics(fs)) {
      options |= TOKENIZE_PHONETICS;
    }
    aCtx->tokenizer->Start(aCtx->tokenizer, (char *)c, fl, options);

    Token tok;
    uint32_t lastTokPos = 0;
    uint32_t newTokPos;
    while (0 != (newTokPos = aCtx->tokenizer->Next(aCtx->tokenizer, &tok))) {
      forwardIndexTokenFunc(&tokCtx, &tok);
      lastTokPos = newTokPos;
    }

    if (curOffsetField) {
      curOffsetField->lastTokPos = lastTokPos;
    }
    aCtx->totalTokens = lastTokPos;
  }
  return 0;
}

FIELD_PREPROCESSOR(numericPreprocessor) {
  if (RedisModule_StringToDouble(field->text, &fdata->numeric) == REDISMODULE_ERR) {
    *errorString = "Could not parse numeric index value";
    return -1;
  }

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (FieldSpec_IsSortable(fs)) {
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM);
  }
  return 0;
}

FIELD_BULK_INDEXER(numericIndexer) {
  NumericRangeTree *rt = bulk->indexData;
  // NumericRangeTree *rt = OpenNumericIndex(ctx, fs->name, &idxKey);
  NumericRangeTree_Add(rt, aCtx->doc.docId, fdata->numeric);
  // RedisModule_CloseKey(idxKey);
  return 0;
}

FIELD_BULK_CTOR(numericCtor) {
  RedisModuleString *keyName = IndexSpec_GetFormattedKey(ctx->spec, fs);
  bulk->indexData = OpenNumericIndex(ctx, keyName, &bulk->indexKey);
}

FIELD_PREPROCESSOR(geoPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
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

FIELD_BULK_INDEXER(geoIndexer) {
  GeoIndex gi = {.ctx = ctx, .sp = fs};
  int rv = GeoIndex_AddStrings(&gi, aCtx->doc.docId, fdata->geo.slon, fdata->geo.slat);

  if (rv == REDISMODULE_ERR) {
    *errorString = "Could not index geo value";
    return -1;
  }
  return 0;
}

FIELD_PREPROCESSOR(tagPreprocessor) {

  fdata->tags = TagIndex_Preprocess(&fs->tagOpts, field);

  if (fdata->tags == NULL) {
    return 0;
  }
  if (FieldSpec_IsSortable(fs)) {
    size_t fl;
    const char *c = RedisModule_StringPtrLen(field->text, &fl);
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }
  return 0;
}

FIELD_BULK_CTOR(tagCtor) {
  RedisModuleString *kname = IndexSpec_GetFormattedKey(ctx->spec, fs);
  bulk->indexData = TagIndex_Open(ctx->redisCtx, kname, 1, &bulk->indexKey);
}

FIELD_BULK_INDEXER(tagIndexer) {
  int rc = 0;
  if (!bulk->indexData) {
    *errorString = "Could not open tag index for indexing";
    rc = -1;
  } else {
    TagIndex_Index(bulk->indexData, fdata->tags, aCtx->doc.docId);
  }
  if (fdata->tags) {
    TagIndex_FreePreprocessedData(fdata->tags);
  }
  return rc;
}

PreprocessorFunc GetIndexPreprocessor(const FieldType ft) {
  switch (ft) {
    case FIELD_FULLTEXT:
      return fulltextPreprocessor;
    case FIELD_NUMERIC:
      return numericPreprocessor;
    case FIELD_GEO:
      return geoPreprocessor;
    case FIELD_TAG:
      return tagPreprocessor;
    default:
      return NULL;
  }
}

static BulkIndexer geoBulkProcs = {.BulkAdd = geoIndexer};
static BulkIndexer numBulkProcs = {.BulkInit = numericCtor, .BulkAdd = numericIndexer};
static BulkIndexer tagBulkProcs = {.BulkInit = tagCtor, .BulkAdd = tagIndexer};

const BulkIndexer *GetBulkIndexer(const FieldType ft) {
  switch (ft) {
    case FIELD_NUMERIC:
      return &numBulkProcs;
    case FIELD_TAG:
      return &tagBulkProcs;
    case FIELD_GEO:
      return &geoBulkProcs;
    default:
      abort();
      return NULL;
  }
}

int Document_AddToIndexes(RSAddDocumentCtx *aCtx) {
  Document *doc = &aCtx->doc;
  int ourRv = REDISMODULE_OK;

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    // Get handler
    PreprocessorFunc pp = GetIndexPreprocessor(fs->type);
    if (pp == NULL) {
      continue;
    }

    if (pp(aCtx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  if (Indexer_Add(aCtx->indexer, aCtx) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (ourRv != REDISMODULE_OK) {
    if (aCtx->errorString == NULL) {
      aCtx->errorString = "ERR couldn't index document";
    }
    AddDocumentCtx_Finish(aCtx);
  }
  return ourRv;
}

/* Evaluate an IF expression (e.g. IF "@foo == 'bar'") against a document, by getting the properties
 * from the sorting table or from the hash representation of the document.
 *
 * NOTE: This is disconnected from the document indexing flow, and loads the document and discards
 * of it internally
 *
 * Returns  REDISMODULE_ERR on failure, OK otherwise*/
int Document_EvalExpression(RedisSearchCtx *sctx, RedisModuleString *key, const char *expr,
                            int *result, char **err) {

  // Try to parser the expression first, fail if we can't
  RSExpr *e = RSExpr_Parse(expr, strlen(expr), err);
  if (!e) {
    return REDISMODULE_ERR;
  }

  // Get the fields needed to evaluate the expression, so we'll know what to load (if any)
  const char **fields = Expr_GetRequiredFields(e);

  Document doc = {.docKey = key};
  // Get the metadata, which should include sortables
  RSDocumentMetadata *md = DocTable_GetByKeyR(&sctx->spec->docs, doc.docKey);

  // Make sure the field list only includes fields which are not already in the sorting vector
  size_t loadFields = 0;
  if (md && md->sortVector) {
    // If a field is in the sorting vector, we simply skip it. n is the total fields not in the
    // sorting vector
    for (size_t i = 0; i < array_len(fields); i++) {
      if (RSSortingTable_GetFieldIdx(sctx->spec->sortables, fields[i]) == -1) {
        fields[loadFields++] = fields[i];
      }
    }
  } else {
    // If we don't have a sorting vector - we need to load all the fields.
    loadFields = array_len(fields);
  }

  // loadFields > 0 means that some fields needed are not sortable and should be loaded from hash
  if (loadFields > 0) {
    if (Redis_LoadDocumentEx(sctx, key, fields, loadFields, &doc, NULL) == REDISMODULE_ERR) {
      SET_ERR(err, "Could not load document");
      array_free(fields);
      return REDISMODULE_ERR;
    }
  }

  // Create a field map from the document fields
  RSFieldMap *fm = RS_NewFieldMap(doc.numFields);
  for (int i = 0; i < doc.numFields; i++) {
    RSFieldMap_Add(&fm, doc.fields[i].name, RS_RedisStringVal(doc.fields[i].text));
  }

  // create a mock search result
  SearchResult res = (SearchResult){
      .docId = doc.docId,
      .fields = fm,
      .scorerPrivateData = md,
  };
  // All this is needed to eval the expression
  RSFunctionEvalCtx *fctx = RS_NewFunctionEvalCtx();
  fctx->res = &res;
  RSExprEvalCtx evctx = (RSExprEvalCtx){
      .r = &res,
      .sortables = sctx ? (sctx->spec ? sctx->spec->sortables : NULL) : NULL,
      .fctx = fctx,
  };
  RSValue out = RSVALUE_STATIC;
  int rc = REDISMODULE_OK;

  // Now actually eval the expression
  if (EXPR_EVAL_ERR == RSExpr_Eval(&evctx, e, &out, err)) {
    rc = REDISMODULE_ERR;
  } else {
    // The result is the boolean value of the expression's output
    *result = RSValue_BoolTest(&out);
  }

  // Cleanup
  array_free(fields);
  RSFunctionEvalCtx_Free(fctx);
  RSFieldMap_Free(fm);
  RSExpr_Free(e);
  Document_Free(&doc);
  return rc;
}

static void AddDocumentCtx_UpdateNoIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
#define BAIL(s)            \
  do {                     \
    aCtx->errorString = s; \
    goto done;             \
  } while (0);

  Document *doc = &aCtx->doc;
  t_docId docId = DocTable_GetId(&sctx->spec->docs, MakeDocKeyR(doc->docKey));
  if (docId == 0) {
    BAIL("Couldn't load old document");
  }
  RSDocumentMetadata *md = DocTable_Get(&sctx->spec->docs, docId);
  if (!md) {
    BAIL("Couldn't load document metadata");
  }

  // Update the score
  md->score = doc->score;
  // Set the payload if needed
  if (doc->payload) {
    DocTable_SetPayload(&sctx->spec->docs, docId, doc->payload, doc->payloadSize);
  }

  if (aCtx->stateFlags & ACTX_F_SORTABLES) {
    FieldSpecDedupeArray dedupes = {0};
    // Update sortables if needed
    for (int i = 0; i < doc->numFields; i++) {
      DocumentField *f = &doc->fields[i];
      FieldSpec *fs = IndexSpec_GetField(sctx->spec, f->name, strlen(f->name));
      if (fs == NULL || !FieldSpec_IsSortable(fs)) {
        continue;
      }

      if (dedupes[fs->index]) {
        BAIL(DUP_FIELD_ERRSTR);
      }

      dedupes[fs->index] = 1;

      int idx = IndexSpec_GetFieldSortingIndex(sctx->spec, f->name, strlen(f->name));
      if (idx < 0) continue;

      if (!md->sortVector) {
        md->sortVector = NewSortingVector(sctx->spec->sortables->len);
      }

      switch (fs->type) {
        case FIELD_FULLTEXT:
          RSSortingVector_Put(md->sortVector, idx, (void *)RedisModule_StringPtrLen(f->text, NULL),
                              RS_SORTABLE_STR);
          break;
        case FIELD_NUMERIC: {
          double numval;
          if (RedisModule_StringToDouble(f->text, &numval) == REDISMODULE_ERR) {
            BAIL("Could not parse numeric index value");
          }
          RSSortingVector_Put(md->sortVector, idx, &numval, RS_SORTABLE_NUM);
          break;
        }
        default:
          BAIL("Unsupported sortable type");
          break;
      }
    }
  }

done:
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(sctx->redisCtx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(sctx->redisCtx, "OK");
  }
  AddDocumentCtx_Free(aCtx);
}

DocumentField *Document_GetField(Document *d, const char *fieldName) {
  if (!d || !fieldName) return NULL;

  for (int i = 0; i < d->numFields; i++) {
    if (!strcasecmp(d->fields[i].name, fieldName)) {
      return &d->fields[i];
    }
  }
  return NULL;
}
