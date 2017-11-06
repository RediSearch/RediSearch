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

static void AddDocumentCtx_SetDocument(RSAddDocumentCtx *aCtx, IndexSpec *sp, Document *base,
                                       size_t oldFieldCount) {
  aCtx->doc = *base;
  Document *doc = &aCtx->doc;

  if (oldFieldCount < doc->numFields) {
    // Pre-allocate the field specs
    aCtx->fspecs = realloc(aCtx->fspecs, sizeof(*aCtx->fspecs) * doc->numFields);
    aCtx->fdatas = realloc(aCtx->fdatas, sizeof(*aCtx->fdatas) * doc->numFields);
  }

  size_t numIndexable = 0;
  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(sp, f->name, strlen(f->name));
    if (fs) {
      aCtx->fspecs[i] = *fs;
      if (FieldSpec_IsSortable(fs)) {
        // mark sortable fields to be updated in the state flags
        aCtx->stateFlags |= ACTX_F_SORTABLES;
      }
      if (fs->type == FIELD_FULLTEXT && FieldSpec_IsIndexable(fs)) {
        numIndexable++;
      }
      // mark non text fields in the state flags
      if (fs->type != FIELD_FULLTEXT) {
        aCtx->stateFlags |= ACTX_F_NONTXTFLDS;
      }

      // mark indexable fields in the state flags
      if (FieldSpec_IsIndexable(fs)) {
        aCtx->stateFlags |= ACTX_F_INDEXABLES;
      }
    } else {
      aCtx->fspecs[i].name = NULL;
    }
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
}

RSAddDocumentCtx *NewAddDocumentCtx(IndexSpec *sp, Document *b) {

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
  AddDocumentCtx_SetDocument(aCtx, sp, b, aCtx->doc.numFields);

  // try to reuse the forward index on recycled contexts
  if (aCtx->fwIdx) {
    ForwardIndex_Reset(aCtx->fwIdx, &aCtx->doc, sp->flags);
  } else {
    aCtx->fwIdx = NewForwardIndex(&aCtx->doc, sp->flags);
  }

  if (!strcasecmp(b->language, "CHINESE")) {
    aCtx->tokenizer = NewChineseTokenizer(aCtx->fwIdx->stemmer, sp->stopwords, 0);
  } else {
    aCtx->tokenizer = NewSimpleTokenizer(aCtx->fwIdx->stemmer, sp->stopwords, 0);
  }

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

static void AddDocumentCtx_UpdateNoIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx);
static int handlePartialUpdate(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  // Handle partial update of fields
  if (aCtx->stateFlags & ACTX_F_INDEXABLES) {

    // The document update is partial - but it contains indexable fields. We need to load the
    // current version of it, and then do a normal indexing flow

    // Free the old field data
    size_t oldFieldCount = aCtx->doc.numFields;
    Document_ClearDetachedFields(&aCtx->doc, sctx->redisCtx);
    if (Redis_LoadDocument(sctx, aCtx->doc.docKey, &aCtx->doc) != REDISMODULE_OK) {
      RedisModule_ReplyWithError(sctx->redisCtx, "Error updating document");
      AddDocumentCtx_Free(aCtx);
      return 1;
    }

    // Keep hold of the new fields.
    Document_DetachFields(&aCtx->doc, sctx->redisCtx);
    AddDocumentCtx_SetDocument(aCtx, sctx->spec, &aCtx->doc, oldFieldCount);

    return 0;
  }

  // No indexable fields are updated, we can just update the metadata.
  // Quick update just updates the score, payload and sortable fields of the document.
  // Thus full-reindexing of the document is not required
  AddDocumentCtx_UpdateNoIndex(aCtx, sctx);
  return 1;
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

  ByteOffsetWriter_Cleanup(&aCtx->offsetsWriter);

  mempool_release(actxPool_g, aCtx);
}

#define FIELD_HANDLER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs, \
                  fieldData *fdata, const char **errorString)

#define FIELD_INDEXER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField *field, \
                  const FieldSpec *fs, fieldData *fdata, const char **errorString)

#define FIELD_PREPROCESSOR FIELD_HANDLER

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  size_t fl;
  const char *c = RedisModule_StringPtrLen(field->text, &fl);
  if (FieldSpec_IsSortable(fs)) {
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  if (FieldSpec_IsIndexable(fs)) {
    Stemmer *stemmer = FieldSpec_IsNoStem(fs) ? NULL : aCtx->fwIdx->stemmer;
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
    aCtx->tokenizer->Start(aCtx->tokenizer, (char *)c, fl,
                           FieldSpec_IsNoStem(fs) ? TOKENIZE_NOSTEM : TOKENIZE_DEFAULT_OPTIONS);
    Token tok;
    uint32_t newTokPos;
    while (0 != (newTokPos = aCtx->tokenizer->Next(&aCtx->tokenizer->ctx, &tok))) {
      forwardIndexTokenFunc(&tokCtx, &tok);
    }

    if (curOffsetField) {
      curOffsetField->lastTokPos = newTokPos;
    }
    aCtx->totalTokens = newTokPos;
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

FIELD_INDEXER(numericIndexer) {
  RedisModuleKey *idxKey;
  NumericRangeTree *rt = OpenNumericIndex(ctx, fs->name, &idxKey);
  NumericRangeTree_Add(rt, aCtx->doc.docId, fdata->numeric);
  RedisModule_CloseKey(idxKey);
  return 0;
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

FIELD_INDEXER(geoIndexer) {
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
    *errorString = "Could not index tag field";
    return -1;
  }
  return 0;
}

FIELD_INDEXER(tagIndexer) {
  RedisModuleKey *idxKey;
  RedisModuleString *kname = TagIndex_FormatName(ctx, fs->name);
  int rc = 0;
  TagIndex *ti = TagIndex_Open(ctx->redisCtx, kname, 1, &idxKey);
  if (!ti) {
    *errorString = "Could not open tag index for indexing";
    rc = -1;
    goto cleanup;
  }

  TagIndex_Index(ti, fdata->tags, aCtx->doc.docId);
cleanup:
  RedisModule_CloseKey(idxKey);
  RedisModule_FreeString(ctx->redisCtx, kname);
  if (fdata->tags) {
    for (size_t i = 0; i < Vector_Size(fdata->tags); i++) {
      char *tok = NULL;
      Vector_Get(fdata->tags, i, &tok);
      free(tok);
    }
    Vector_Free(fdata->tags);
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

IndexerFunc GetIndexIndexer(const FieldType ft) {
  switch (ft) {
    case FIELD_NUMERIC:
      return numericIndexer;
    case FIELD_GEO:
      return geoIndexer;
    case FIELD_TAG:
      return tagIndexer;
    case FIELD_FULLTEXT:
    default:
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

static void AddDocumentCtx_UpdateNoIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
#define BAIL(s)            \
  do {                     \
    aCtx->errorString = s; \
    goto done;             \
  } while (0);

  Document *doc = &aCtx->doc;
  t_docId docId = DocTable_GetId(&sctx->spec->docs, RedisModule_StringPtrLen(doc->docKey, NULL));
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
    // Update sortables if needed
    for (int i = 0; i < doc->numFields; i++) {
      DocumentField *f = &doc->fields[i];
      FieldSpec *fs = IndexSpec_GetField(sctx->spec, f->name, strlen(f->name));
      if (!FieldSpec_IsSortable(fs)) continue;

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

int Document_CanAdd(Document *doc, IndexSpec *sp, int replace) {
  if (!replace && DocTable_GetId(&sp->docs, RedisModule_StringPtrLen(doc->docKey, NULL)) != 0) {
    return 0;
  }
  return 1;
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