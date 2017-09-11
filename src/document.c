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

  // Also, get the field specs. We cache this here because the
  // context is unlocked
  // during the actual tokenization
  Document *doc = &aCtx->doc;

  // We might be able to use the old block of fields. But, if it's too small,
  // just free and call malloc again; that's basically what realloc does
  // anyway.
  if (oldFieldCount != 0 && oldFieldCount < doc->numFields) {
    free(aCtx->fspecs);
    free(aCtx->fdatas);
    oldFieldCount = 0;
  }

  if (oldFieldCount == 0) {
    aCtx->fspecs = malloc(sizeof(*aCtx->fspecs) * doc->numFields);
    aCtx->fdatas = malloc(sizeof(*aCtx->fdatas) * doc->numFields);
  }

  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(sp, f->name, strlen(f->name));
    if (fs) {
      aCtx->fspecs[i] = *fs;
      if (FieldSpec_IsSortable(fs) && aCtx->sv == NULL) {
        aCtx->sv = NewSortingVector(sp->sortables->len);
        // mark sortable fields to be updated in the state flags
        aCtx->stateFlags |= ACTX_F_SORTABLES;
      }
      // mark non text fields in the state flags
      if (fs->type != F_FULLTEXT) {
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
}

RSAddDocumentCtx *NewAddDocumentCtx(IndexSpec *sp, Document *b) {

  if (!actxPool_g) {
    actxPool_g = mempool_new(16, allocDocumentContext, freeDocumentContext);
  }

  // Get a new context
  RSAddDocumentCtx *aCtx = mempool_get(actxPool_g);

  // Assign the document:
  AddDocumentCtx_SetDocument(aCtx, sp, b, aCtx->doc.numFields);

  // Per-client fields; these must always be recreated
  aCtx->bc = NULL;
  aCtx->next = NULL;

  if (aCtx->fwIdx) {
    ForwardIndex_Reset(aCtx->fwIdx, &aCtx->doc, sp->flags);
    aCtx->stateFlags = 0;
    aCtx->errorString = NULL;
    aCtx->totalTokens = 0;
  } else {
    aCtx->fwIdx = NewForwardIndex(&aCtx->doc, sp->flags);
  }

  aCtx->specFlags = sp->flags;
  aCtx->stopwords = sp->stopwords;
  aCtx->indexer = GetDocumentIndexer(sp->name);

  StopWordList_Ref(sp->stopwords);

  aCtx->doc.docId = 0;
  return aCtx;
}

static int replyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RSAddDocumentCtx *aCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(ctx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  AddDocumentCtx_Free(aCtx);
  return REDISMODULE_OK;
}

static void threadCallback(void *p) {
  Document_AddToIndexes(p);
}

void AddDocumentCtx_Finish(RSAddDocumentCtx *aCtx) {
  RedisModule_UnblockClient(aCtx->bc, aCtx);
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
  aCtx->bc = RedisModule_BlockClient(sctx->redisCtx, replyCallback, NULL, NULL, 0);
  assert(aCtx->bc);
  size_t totalSize = 0;
  for (size_t ii = 0; ii < aCtx->doc.numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    if (fs->name && fs->type == F_FULLTEXT) {
      size_t n;
      RedisModule_StringPtrLen(aCtx->doc.fields[ii].text, &n);
      totalSize += n;
    }
  }

  if (totalSize >= SELF_EXEC_THRESHOLD) {
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

  if (aCtx->stopwords) {
    StopWordList_Unref(aCtx->stopwords);
    aCtx->stopwords = NULL;
  }

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
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  if (FieldSpec_IsSortable(fs)) {
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  Stemmer *stemmer = FieldSpec_IsNoStem(fs) ? NULL : aCtx->fwIdx->stemmer;
  aCtx->totalTokens = tokenize(c, fs->weight, fs->id, aCtx->fwIdx, forwardIndexTokenFunc, stemmer,
                               aCtx->totalTokens, aCtx->stopwords);
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

PreprocessorFunc GetIndexPreprocessor(const FieldType ft) {
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

IndexerFunc GetIndexIndexer(const FieldType ft) {
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

    if (!FieldSpec_IsIndexable(fs)) {
      printf("Skipping non indexable field %s\n", fs->name);
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
#define BAIL(s)                   \
  do {                            \
    aCtx->errorString = "ERR " s; \
    goto done;                    \
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
      case F_FULLTEXT:
        RSSortingVector_Put(md->sortVector, idx, (void *)RedisModule_StringPtrLen(f->text, NULL),
                            RS_SORTABLE_STR);
        break;
      case F_NUMERIC: {
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

done:
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(sctx->redisCtx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(sctx->redisCtx, "OK");
  }
  AddDocumentCtx_Free(aCtx);
}
