/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <string.h>
#include <inttypes.h>

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
#include "rmalloc.h"
#include "indexer.h"
#include "tag_index.h"
#include "geometry/geometry_api.h"
#include "aggregate/expr/expression.h"
#include "rmutil/rm_assert.h"

// Memory pool for RSAddDocumentContext contexts
static mempool_t *actxPool_g = NULL;
extern RedisModuleCtx *RSDummyContext;
// For documentation, see these functions' definitions
static void *allocDocumentContext(void) {
  // See if there's one in the pool?
  RSAddDocumentCtx *aCtx = rm_calloc(1, sizeof(*aCtx));
  return aCtx;
}

static void freeDocumentContext(void *p) {
  RSAddDocumentCtx *aCtx = p;
  if (aCtx->fwIdx) {
    ForwardIndexFree(aCtx->fwIdx);
  }

  rm_free(aCtx->fspecs);
  rm_free(aCtx->fdatas);
  rm_free(aCtx->specName);
  rm_free(aCtx);
}

#define DUP_FIELD_ERRSTR "Requested to index field twice"

#define FIELD_IS_VALID(aCtx, ix) ((aCtx)->fspecs[ix].name != NULL)
#define FIELD_IS_NULL(aCtx, ix) ((aCtx)->fdatas[ix].isNull)

static int AddDocumentCtx_SetDocument(RSAddDocumentCtx *aCtx, IndexSpec *sp) {
  Document *doc = aCtx->doc;
  aCtx->stateFlags &= ~ACTX_F_INDEXABLES;
  aCtx->stateFlags &= ~ACTX_F_TEXTINDEXED;
  aCtx->stateFlags &= ~ACTX_F_OTHERINDEXED;

  aCtx->fspecs = rm_realloc(aCtx->fspecs, sizeof(*aCtx->fspecs) * doc->numFields);
  aCtx->fdatas = rm_realloc(aCtx->fdatas, sizeof(*aCtx->fdatas) * doc->numFields);

  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    // zero out field data. We check at the destructor to see if there is any
    // left-over tag data here; if we've realloc'd, then this contains
    // garbage
    aCtx->fdatas[ii].tags = NULL;
    aCtx->fdatas[ii].isNull = 0;
  }

  size_t numTextIndexable = 0;

  // size: uint16_t * SPEC_MAX_FIELDS
  FieldSpecDedupeArray dedupe = {0};
  int hasTextFields = 0;
  int hasOtherFields = 0;

  for (size_t i = 0; i < doc->numFields; i++) {
    DocumentField *f = doc->fields + i;
    const FieldSpec *fs = IndexSpec_GetField(sp, f->name, strlen(f->name));
    if (!fs || (isSpecHash(sp) && !f->text)) {
      aCtx->fspecs[i].name = NULL;
      aCtx->fspecs[i].path = NULL;
      aCtx->fspecs[i].types = 0;
      continue;
    }

    aCtx->fspecs[i] = *fs;
    if (dedupe[fs->index]) {
      QueryError_SetErrorFmt(&aCtx->status, QUERY_EDUPFIELD, "Tried to insert `%s` twice",
                             fs->name);
      return -1;
    }

    dedupe[fs->index] = 1;

    if (FieldSpec_IsSortable(fs)) {
      // mark sortable fields to be updated in the state flags
      aCtx->stateFlags |= ACTX_F_SORTABLES;
    }

    // See what we want the given field indexed as:
    if (!f->indexAs) {
      f->indexAs = fs->types;
    } else {
      // Verify the flags:
      if ((f->indexAs & fs->types) != f->indexAs) {
        QueryError_SetErrorFmt(&aCtx->status, QUERY_EUNSUPPTYPE,
                               "Tried to index field %s as type not specified in schema", fs->name);
        return -1;
      }
    }

    if (FieldSpec_IsIndexable(fs)) {
      if (f->indexAs & INDEXFLD_T_FULLTEXT) {
        numTextIndexable++;
        hasTextFields = 1;
      }

      if (f->indexAs != INDEXFLD_T_FULLTEXT) {
        // has non-text but indexable fields
        hasOtherFields = 1;
      }
    }
  }

  if (hasTextFields || hasOtherFields) {
    aCtx->stateFlags |= ACTX_F_INDEXABLES;
  } else {
    aCtx->stateFlags &= ~ACTX_F_INDEXABLES;
  }

  if (!hasTextFields) {
    aCtx->stateFlags |= ACTX_F_TEXTINDEXED;
  } else {
    aCtx->stateFlags &= ~ACTX_F_TEXTINDEXED;
  }

  if (!hasOtherFields) {
    aCtx->stateFlags |= ACTX_F_OTHERINDEXED;
  } else {
    aCtx->stateFlags &= ~ACTX_F_OTHERINDEXED;
  }

  if ((aCtx->stateFlags & ACTX_F_SORTABLES) && aCtx->sv == NULL) {
    aCtx->sv = NewSortingVector(sp->sortables->len);
  }

  int empty = (aCtx->sv == NULL) && !hasTextFields && !hasOtherFields;
  if (empty) {
    aCtx->stateFlags |= ACTX_F_EMPTY;
  }

  if ((aCtx->options & DOCUMENT_ADD_NOSAVE) == 0 && numTextIndexable &&
      (sp->flags & Index_StoreByteOffsets)) {
    if (!aCtx->byteOffsets) {
      aCtx->byteOffsets = NewByteOffsets();
      ByteOffsetWriter_Init(&aCtx->offsetsWriter);
    }
    RSByteOffsets_ReserveFields(aCtx->byteOffsets, numTextIndexable);
  }
  return 0;
}

RSAddDocumentCtx *NewAddDocumentCtx(IndexSpec *sp, Document *doc, QueryError *status) {

  if (!actxPool_g) {
    mempool_options mopts = {.initialCap = 16,
                             .alloc = allocDocumentContext,
                             .free = freeDocumentContext};
    mempool_test_set_global(&actxPool_g, &mopts);
  }

  // Get a new context
  RSAddDocumentCtx *aCtx = mempool_get(actxPool_g);
  aCtx->stateFlags = 0;
  QueryError_ClearError(&aCtx->status);
  aCtx->totalTokens = 0;
  aCtx->docFlags = 0;
  aCtx->client.bc = NULL;
  aCtx->next = NULL;
  aCtx->specFlags = sp->flags;
  aCtx->indexer = sp->indexer;
  aCtx->spec = sp;
  aCtx->oldMd = NULL;
  if (aCtx->specFlags & Index_Async) {
    size_t len = sp->nameLen + 1;
    if (aCtx->specName == NULL) {
      aCtx->specName = rm_malloc(len);
    } else if (len > aCtx->specNameLen) {
      aCtx->specName = rm_realloc(aCtx->specName, len);
      aCtx->specNameLen = len;
    }
    strncpy(aCtx->specName, sp->name, len);
    aCtx->specId = sp->uniqueId;
  }
  RS_LOG_ASSERT(sp->indexer, "No indexer");
  Indexer_Incref(aCtx->indexer);

  // Assign the document:
  aCtx->doc = doc;
  if (AddDocumentCtx_SetDocument(aCtx, sp) != 0) {
    *status = aCtx->status;
    aCtx->status.detail = NULL;
    mempool_release(actxPool_g, aCtx);
    return NULL;
  }

  // try to reuse the forward index on recycled contexts
  if (aCtx->fwIdx) {
    ForwardIndex_Reset(aCtx->fwIdx, aCtx->doc, sp->flags);
  } else {
    aCtx->fwIdx = NewForwardIndex(aCtx->doc, sp->flags);
  }

  if (sp->smap) {
    // we get a read only copy of the synonym map for accessing in the index thread with out worring
    // about thready safe issues
    aCtx->fwIdx->smap = SynonymMap_GetReadOnlyCopy(sp->smap);
  } else {
    aCtx->fwIdx->smap = NULL;
  }

  aCtx->tokenizer = GetTokenizer(doc->language, aCtx->fwIdx->stemmer,
                                 sp->stopwords, sp->delimiters);
//  aCtx->doc->docId = 0;
  return aCtx;
}

static void doReplyFinish(RSAddDocumentCtx *aCtx, RedisModuleCtx *ctx) {
  if (aCtx->donecb) {
    aCtx->donecb(aCtx, ctx, aCtx->donecbData);
  }
  Indexer_Decref(aCtx->indexer);
  AddDocumentCtx_Free(aCtx);
}

static int replyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RSAddDocumentCtx *aCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  doReplyFinish(aCtx, ctx);
  return REDISMODULE_OK;
}

typedef struct DocumentAddCtx {
  RSAddDocumentCtx *aCtx;
  RedisSearchCtx *sctx;
} DocumentAddCtx;

static void threadCallback(void *p) {
  DocumentAddCtx *ctx = p;
  Document_AddToIndexes(ctx->aCtx, ctx->sctx);
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

// LCOV_EXCL_START debug
void Document_Dump(const Document *doc) {
  printf("Document Key: %s. ID=%" PRIu64 "\n", RedisModule_StringPtrLen(doc->docKey, NULL),
         doc->docId);
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    printf("  [%lu]: %s => %s\n", ii, doc->fields[ii].name,
           RedisModule_StringPtrLen(doc->fields[ii].text, NULL));
  }
}
// LCOV_EXCL_STOP

static void AddDocumentCtx_UpdateNoIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx);
int Document_LoadSchemaFieldJson(Document *doc, RedisSearchCtx *sctx);

static int AddDocumentCtx_ReplaceMerge(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  /**
   * The REPLACE operation contains fields which must be reindexed. This means
   * that a new document ID needs to be assigned, and as a consequence, all
   * fields must be reindexed.
   */
  int rv = REDISMODULE_ERR;

  Document_Clear(aCtx->doc);

  // Path is not covered and is not relevant

  DocumentType ruleType = sctx->spec->rule->type;
  if (ruleType == DocumentType_Hash) {
    rv = Document_LoadSchemaFieldHash(aCtx->doc, sctx);
  } else if (ruleType == DocumentType_Json) {
    rv = Document_LoadSchemaFieldJson(aCtx->doc, sctx);
  }
  if (rv != REDISMODULE_OK) {
    QueryError_SetError(&aCtx->status, QUERY_ENODOC, "Could not load existing document");
    aCtx->donecb(aCtx, sctx->redisCtx, aCtx->donecbData);
    AddDocumentCtx_Free(aCtx);
    return 1;
  }

  // Keep hold of the new fields.
  Document_MakeStringsOwner(aCtx->doc);
  AddDocumentCtx_SetDocument(aCtx, sctx->spec);
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

  // We actually modify (!) the strings in the document, so we always require
  // ownership
  Document_MakeStringsOwner(aCtx->doc);

  if (AddDocumentCtx_IsBlockable(aCtx)) {
    aCtx->client.bc = RedisModule_BlockClient(sctx->redisCtx, replyCallback, NULL, NULL, 0);
  } else {
    aCtx->client.sctx = sctx;
  }

  RS_LOG_ASSERT(aCtx->client.bc, "No blocked client");

  bool concurrentSearch = false;
  if (AddDocumentCtx_IsBlockable(aCtx)) {
    size_t totalSize = 0;
    for (size_t ii = 0; ii < aCtx->doc->numFields; ++ii) {
      const DocumentField *ff = aCtx->doc->fields + ii;
      if ((ff->indexAs & (INDEXFLD_T_FULLTEXT | INDEXFLD_T_TAG))) {
        // TODO: GEOMETRY - handle geometry fields?
        size_t n;
        if (ff->unionType == FLD_VAR_T_CSTR || ff->unionType == FLD_VAR_T_RMS) {
          DocumentField_GetValueCStr(&aCtx->doc->fields[ii], &n);
          totalSize += n;
        } else if (ff->unionType == FLD_VAR_T_ARRAY) {
          for (size_t jj = 0; jj < ff->arrayLen; ++jj) {
            DocumentField_GetArrayValueCStr(&aCtx->doc->fields[ii], &n, jj);
            totalSize += n;
          }
        }
      }
    }
    concurrentSearch = (totalSize >= SELF_EXEC_THRESHOLD);
  }

  if (!concurrentSearch) {
    Document_AddToIndexes(aCtx, sctx);
  } else {
    // Deprecated and broken - should pass `DocumentAddCtx` and not `RSAddDocumentCtx`
    // also, we have to pass a weak ref to the spec, and handle it in the callback.
    ConcurrentSearch_ThreadPoolRun(threadCallback, aCtx, CONCURRENT_POOL_INDEX);
  }
}

void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx) {
  /**
   * Free preprocessed data; this is the only reliable place
   * to do it
   */
  for (size_t ii = 0; ii < aCtx->doc->numFields; ++ii) {
    if (FIELD_IS_VALID(aCtx, ii)) {
      if (FIELD_IS(aCtx->fspecs + ii, INDEXFLD_T_TAG) && aCtx->fdatas[ii].tags) {
        TagIndex_FreePreprocessedData(aCtx->fdatas[ii].tags);
        aCtx->fdatas[ii].tags = NULL;
      } else if (FIELD_IS(aCtx->fspecs + ii, INDEXFLD_T_GEO) && aCtx->fdatas[ii].isMulti &&
                 aCtx->fdatas[ii].arrNumeric && !FIELD_IS_NULL(aCtx, ii)) {
        array_free(aCtx->fdatas[ii].arrNumeric);
        aCtx->fdatas[ii].arrNumeric = NULL;
      }
    }
  }

  // Destroy the common fields:
  if (!(aCtx->stateFlags & ACTX_F_NOFREEDOC)) {
    Document_Free(aCtx->doc);
  }

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
    DMD_Return(aCtx->oldMd);
    aCtx->oldMd = NULL;
  }

  ByteOffsetWriter_Cleanup(&aCtx->offsetsWriter);
  QueryError_ClearError(&aCtx->status);

  mempool_release(actxPool_g, aCtx);
}

#define FIELD_HANDLER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, DocumentField *field, const FieldSpec *fs, \
                  FieldIndexerData *fdata, QueryError *status)

#define FIELD_BULK_INDEXER(name)                                                            \
  static int name(IndexBulkData *bulk, RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx,         \
                  const DocumentField *field, const FieldSpec *fs, FieldIndexerData *fdata, \
                  QueryError *status)

#define FIELD_BULK_CTOR(name) \
  static void name(IndexBulkData *bulk, const FieldSpec *fs, RedisSearchCtx *ctx)
#define FIELD_BULK_FINALIZER(name) static void name(IndexBulkData *bulk, RedisSearchCtx *ctx)

#define FIELD_PREPROCESSOR FIELD_HANDLER

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  switch (field->unionType) {
    // JSON NULL value is ignored
    case FLD_VAR_T_NULL:
      return 0;
    // Unsupported type retrun an error
    case FLD_VAR_T_BLOB_ARRAY:
    case FLD_VAR_T_NUM:
    case FLD_VAR_T_GEO:
    case FLD_VAR_T_GEOMETRY:
      return -1;
    case FLD_VAR_T_ARRAY:
    case FLD_VAR_T_CSTR:
    case FLD_VAR_T_RMS:
      /*continue*/;
  }

  size_t fl;
  const char *c = DocumentField_GetValueCStr(field, &fl);
  size_t valueCount = (field->unionType != FLD_VAR_T_ARRAY ? 1 : field->arrayLen);

  if (FieldSpec_IsSortable(fs)) {
    if (field->unionType != FLD_VAR_T_ARRAY) {
      RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR, fs->options & FieldSpec_UNF);
    } else if (field->multisv) {
      RSSortingVector_Put(aCtx->sv, fs->sortIdx, field->multisv, RS_SORTABLE_RSVAL, 0);
      field->multisv = NULL;
    }
  }

  if (FieldSpec_IsIndexable(fs)) {
    ForwardIndexTokenizerCtx tokCtx;
    VarintVectorWriter *curOffsetWriter = NULL;
    RSByteOffsetField *curOffsetField = NULL;
    if (aCtx->byteOffsets) {
      curOffsetField = RSByteOffsets_AddField(aCtx->byteOffsets, fs->ftId, aCtx->totalTokens + 1);
      curOffsetWriter = &aCtx->offsetsWriter;
    }

    uint32_t options = TOKENIZE_DEFAULT_OPTIONS;
    if (FieldSpec_IsNoStem(fs)) {
      options |= TOKENIZE_NOSTEM;
    }
    if (FieldSpec_IsPhonetics(fs)) {
      options |= TOKENIZE_PHONETICS;
    }

    unsigned int multiTextOffsetDelta;
    if (valueCount > 1 && RSGlobalConfig.multiTextOffsetDelta > 0) {
      multiTextOffsetDelta = RSGlobalConfig.multiTextOffsetDelta - 1;
    } else {
      multiTextOffsetDelta = 0;
    }

    for (size_t i = 0; i < valueCount; ++i) {

      // Already got the first value
      if (i) {
        c = DocumentField_GetArrayValueCStr(field, &fl, i);
      }
      ForwardIndexTokenizerCtx_Init(&tokCtx, aCtx->fwIdx, c, curOffsetWriter, fs->ftId, fs->ftWeight);
      aCtx->tokenizer->Start(aCtx->tokenizer, (char *)c, fl, options,
        fs->delimiters);

      Token tok = {0};
      uint32_t newTokPos;
      while (0 != (newTokPos = aCtx->tokenizer->Next(aCtx->tokenizer, &tok))) {
        forwardIndexTokenFunc(&tokCtx, &tok);
      }
      uint32_t lastTokPos = aCtx->tokenizer->ctx.lastOffset;

      if (curOffsetField) {
        curOffsetField->lastTokPos = lastTokPos;
      }
      aCtx->totalTokens = lastTokPos;
      Token_Destroy(&tok);

      aCtx->tokenizer->ctx.lastOffset += multiTextOffsetDelta;
    }
    // Decrease the last increment
    aCtx->tokenizer->ctx.lastOffset -= multiTextOffsetDelta;
  }
  return 0;
}

FIELD_PREPROCESSOR(numericPreprocessor) {
  switch (field->unionType) {
    case FLD_VAR_T_RMS:
      fdata->isMulti = 0;
      if (RedisModule_StringToDouble(field->text, &fdata->numeric) == REDISMODULE_ERR) {
        QueryError_SetCode(status, QUERY_ENOTNUMERIC);
        return -1;
      }
      break;
    case FLD_VAR_T_CSTR:
      {
        char *end;
        fdata->isMulti = 0;
        fdata->numeric = strtod(field->strval, &end);
        if (*end) {
          QueryError_SetCode(status, QUERY_ENOTNUMERIC);
          return -1;
        }
      }
      break;
    case FLD_VAR_T_NUM:
      fdata->isMulti = 0;
      fdata->numeric = field->numval;
      break;
    case FLD_VAR_T_NULL:
      fdata->isNull = 1;
      return 0;
    case FLD_VAR_T_ARRAY:
      fdata->isMulti = 1;
      // Borrow values
      fdata->arrNumeric = field->arrNumval;
      break;
    default:
      return -1;
  }

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (FieldSpec_IsSortable(fs)) {
    if (field->unionType != FLD_VAR_T_ARRAY) {
      RSSortingVector_Put(aCtx->sv, fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM, 0);
    } else if (field->multisv) {
      RSSortingVector_Put(aCtx->sv, fs->sortIdx, field->multisv, RS_SORTABLE_RSVAL, 0);
      field->multisv = NULL;
    }
  }
  return 0;
}


FIELD_PREPROCESSOR(geometryPreprocessor) {
  switch (field->unionType) {
    case FLD_VAR_T_RMS:
    {
      // From WKT RMS
      fdata->isMulti = 0;
      size_t len;
      const char *str = RedisModule_StringPtrLen(field->text, &len);
      fdata->str = str;
      fdata->strlen = len;
      fdata->format = GEOMETRY_FORMAT_WKT;
      break;
    }
    case FLD_VAR_T_CSTR:
      // From WKT string
      fdata->isMulti = 0;
      fdata->str = field->strval;
      fdata->strlen = field->strlen;
      fdata->format = GEOMETRY_FORMAT_WKT;
      break;
    case FLD_VAR_T_NUM:
    case FLD_VAR_T_NULL:
      return 0;
    case FLD_VAR_T_ARRAY:
      fdata->isMulti = 1;
      // TODO: GEOMETRY - parse geometries from string
      //fdata->arrGeometry = ...
      break;
    default:
      return -1;
  }

  // TODO: GEOMETRY
  // If this is a sortable geomtry value - copy the value to the sorting vector


  return 0;
}

FIELD_BULK_INDEXER(geometryIndexer) {
  GeometryIndex *rt = bulk->indexDatas[IXFLDPOS_GEOMETRY];
  if (!rt) {
    rt = bulk->indexDatas[IXFLDPOS_GEOMETRY] =
        OpenGeometryIndex(ctx->redisCtx, ctx->spec, &bulk->indexKeys[IXFLDPOS_GEOMETRY], fs);
    if (!rt) {
      QueryError_SetError(status, QUERY_EGENERIC, "Could not open geoshape index for indexing");
      return -1;
    }
  }

  const GeometryApi *api = GeometryApi_Get(rt);
  RedisModuleString *errMsg;
  if (!fdata->isMulti) {
    if (!api->addGeomStr(rt, fdata->format, fdata->str, fdata->strlen, aCtx->doc->docId, &errMsg)) {
      ++ctx->spec->stats.indexingFailures;
      // QueryError_SetErrorFmt(status, QUERY_EBADVAL, "Error indexing geoshape: %s",
      //                        RedisModule_StringPtrLen(errMsg, NULL));
      RedisModule_FreeString(NULL, errMsg);
      return -1;
    }
  } else {
    // for (uint32_t i = 0; i < array_len(fdata->arrGeometry); ++i) {
    //   //TODO: GEOMETRY
    // }
  }
  return 0;
}



FIELD_BULK_INDEXER(numericIndexer) {
  NumericRangeTree *rt = bulk->indexDatas[IXFLDPOS_NUMERIC];
  if (!rt) {
    RedisModuleString *keyName = IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_NUMERIC);
    rt = bulk->indexDatas[IXFLDPOS_NUMERIC] =
        OpenNumericIndex(ctx, keyName, &bulk->indexKeys[IXFLDPOS_NUMERIC]);
    if (!rt) {
      QueryError_SetError(status, QUERY_EGENERIC, "Could not open numeric index for indexing");
      return -1;
    }
  }

  if (!fdata->isMulti) {
    NRN_AddRv rv = NumericRangeTree_Add(rt, aCtx->doc->docId, fdata->numeric, false);
    ctx->spec->stats.invertedSize += rv.sz;
    ctx->spec->stats.numRecords += rv.numRecords;
  } else {
    for (uint32_t i = 0; i < array_len(fdata->arrNumeric); ++i) {
      double numval = fdata->arrNumeric[i];
      NRN_AddRv rv = NumericRangeTree_Add(rt, aCtx->doc->docId, numval, true);
      ctx->spec->stats.invertedSize += rv.sz;
      ctx->spec->stats.numRecords += rv.numRecords;
    }
  }
  return 0;
}

FIELD_PREPROCESSOR(vectorPreprocessor) {
  fdata->numVec = 0;
  if (field->unionType == FLD_VAR_T_RMS) {
    fdata->vector = RedisModule_StringPtrLen(field->text, &fdata->vecLen);
    fdata->numVec = 1; // In this case we can only have a single value
  } else if (field->unionType == FLD_VAR_T_CSTR) {
    fdata->vector = field->strval;
    fdata->vecLen = field->strlen;
    fdata->numVec = 1; // In this case we can only have a single value
  } else if (field->unionType == FLD_VAR_T_BLOB_ARRAY) {
    fdata->vector = field->blobArr;
    fdata->vecLen = field->blobSize;
    fdata->numVec = field->blobArrLen;
  } else if (field->unionType == FLD_VAR_T_NULL) {
    fdata->isNull = 1;
    return 0; // Skipping indexing missing vector
  }
  if (fdata->vecLen != fs->vectorOpts.expBlobSize) {
    // "Could not add vector with blob size %zu (expected size %zu)", len, fs->vectorOpts.expBlobSize
    QueryError_SetCode(status, QUERY_EBADATTR);
    return -1;
  }
  aCtx->fwIdx->maxFreq++;
  return 0;
}

FIELD_BULK_INDEXER(vectorIndexer) {
  IndexSpec *sp = ctx->spec;
  VecSimIndex *rt = bulk->indexDatas[IXFLDPOS_VECTOR];
  if (!rt) {
    RedisModuleString *keyName = IndexSpec_GetFormattedKey(sp, fs, INDEXFLD_T_VECTOR);
    rt = bulk->indexDatas[IXFLDPOS_VECTOR] =
        OpenVectorIndex(sp, keyName/*, &bulk->indexKeys[IXFLDPOS_VECTOR]*/);
    if (!rt) {
      QueryError_SetError(status, QUERY_EGENERIC, "Could not open vector for indexing");
      return -1;
    }
  }
  char *curr_vec = (char *)fdata->vector;
  for (size_t i = 0; i < fdata->numVec; i++) {
    VecSimIndex_AddVector(rt, curr_vec, aCtx->doc->docId);
    curr_vec += fdata->vecLen;
  }
  sp->stats.numRecords += fdata->numVec;
  return 0;
}

FIELD_PREPROCESSOR(geoPreprocessor) {
  size_t len;
  double lon, lat;
  double geohash;
  int str_count = 0;

  switch (field->unionType) {
    case FLD_VAR_T_GEO:
      fdata->isMulti = 0;
      geohash = calcGeoHash(field->lon, field->lat);
      if (geohash == INVALID_GEOHASH) {
        return REDISMODULE_ERR;
      }
      fdata->numeric = geohash;
      if (FieldSpec_IsSortable(fs)) {
        RSSortingVector_Put(aCtx->sv, fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM, 0);
      }
      return REDISMODULE_OK;
    case FLD_VAR_T_CSTR:
    case FLD_VAR_T_RMS:
      str_count = 1;
      break;
    case FLD_VAR_T_NULL:
      fdata->isNull = 1;
      return REDISMODULE_OK;
    case FLD_VAR_T_ARRAY:
      str_count = field->arrayLen;
      break;
    case FLD_VAR_T_BLOB_ARRAY:
    case FLD_VAR_T_NUM:
    case FLD_VAR_T_GEOMETRY:
      RS_LOG_ASSERT(0, "Oops");
  }

  const char *str = NULL;
  fdata->isMulti = 0;
  if (str_count == 1) {
    str = DocumentField_GetValueCStr(field, &len);
    if (parseGeo(str, len, &lon, &lat) != REDISMODULE_OK) {
      return REDISMODULE_ERR;
    }
    geohash = calcGeoHash(lon, lat);
    if (geohash == INVALID_GEOHASH) {
      return REDISMODULE_ERR;
    }
    fdata->numeric = geohash;
  } else if (str_count > 1) {
    fdata->isMulti = 1;
    arrayof(double) arr = array_new(double, str_count);
    for (size_t i = 0; i < str_count; ++i) {
      const char *cur_str = DocumentField_GetArrayValueCStr(field, &len, i);
      if ((parseGeo(cur_str, len, &lon, &lat) != REDISMODULE_OK) || ((geohash = calcGeoHash(lon, lat)) == INVALID_GEOHASH)) {
        array_free(arr);
        fdata->arrNumeric = NULL;
        return REDISMODULE_ERR;
      }
      array_ensure_append_1(arr, geohash);
    }
    str = DocumentField_GetArrayValueCStr(field, &len, 0);
    fdata->arrNumeric = arr;
  }

  if (str && FieldSpec_IsSortable(fs)) {
    if (field->unionType != FLD_VAR_T_ARRAY) {
      RSSortingVector_Put(aCtx->sv, fs->sortIdx, str, RS_SORTABLE_STR, fs->options & FieldSpec_UNF);
    } else if (field->multisv) {
      RSSortingVector_Put(aCtx->sv, fs->sortIdx, field->multisv, RS_SORTABLE_RSVAL, 0);
      field->multisv = NULL;
    }
  }

  return REDISMODULE_OK;
}

FIELD_PREPROCESSOR(tagPreprocessor) {
  if (TagIndex_Preprocess(fs, field, fdata)) {
    if (FieldSpec_IsSortable(fs)) {
      if (field->unionType != FLD_VAR_T_ARRAY) {
        size_t fl;
        const char *str = DocumentField_GetValueCStr(field, &fl);
        RSSortingVector_Put(aCtx->sv, fs->sortIdx, str, RS_SORTABLE_STR, fs->options & FieldSpec_UNF);
      } else if (field->multisv) {
        RSSortingVector_Put(aCtx->sv, fs->sortIdx, field->multisv, RS_SORTABLE_RSVAL, 0);
        field->multisv = NULL;
      }
    }
  }
  return 0;
}

FIELD_BULK_INDEXER(tagIndexer) {
  TagIndex *tidx = bulk->indexDatas[IXFLDPOS_TAG];
  if (!tidx) {
    RedisModuleString *kname = IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_TAG);
    tidx = bulk->indexDatas[IXFLDPOS_TAG] =
        TagIndex_Open(ctx, kname, 1, &bulk->indexKeys[IXFLDPOS_TAG]);
    if (!tidx) {
      QueryError_SetError(status, QUERY_EGENERIC, "Could not open tag index for indexing");
      return -1;
    }
    if (FieldSpec_HasSuffixTrie(fs) && !tidx->suffix) {
      tidx->suffix = NewTrieMap();
    }
  }

  ctx->spec->stats.invertedSize +=
      TagIndex_Index(tidx, (const char **)fdata->tags, array_len(fdata->tags), aCtx->doc->docId);
  ctx->spec->stats.numRecords++;
  return 0;
}

static PreprocessorFunc preprocessorMap[] = {
    // nl break
    [IXFLDPOS_FULLTEXT] = fulltextPreprocessor,
    [IXFLDPOS_NUMERIC] = numericPreprocessor,
    [IXFLDPOS_GEO] = geoPreprocessor,
    [IXFLDPOS_TAG] = tagPreprocessor,
    [IXFLDPOS_VECTOR] = vectorPreprocessor,
    [IXFLDPOS_GEOMETRY] = geometryPreprocessor,
    };

int IndexerBulkAdd(IndexBulkData *bulk, RSAddDocumentCtx *cur, RedisSearchCtx *sctx,
                   const DocumentField *field, const FieldSpec *fs, FieldIndexerData *fdata,
                   QueryError *status) {
  int rc = 0;
  for (size_t ii = 0; ii < INDEXFLD_NUM_TYPES && rc == 0; ++ii) {
    // see which types are supported in the current field...
    if (field->indexAs & INDEXTYPE_FROM_POS(ii)) {
      switch (ii) {
        case IXFLDPOS_TAG:
          rc = tagIndexer(bulk, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_NUMERIC:
        case IXFLDPOS_GEO:
          rc = numericIndexer(bulk, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_VECTOR:
          rc = vectorIndexer(bulk, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_GEOMETRY:
          rc = geometryIndexer(bulk, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_FULLTEXT:
          break;
        default:
          rc = -1;
          QueryError_SetError(status, QUERY_EINVAL, "BUG: invalid index type");
          break;
      }
    }
  }
  return rc;
}

void IndexerBulkCleanup(IndexBulkData *cur, RedisSearchCtx *sctx) {
  for (size_t ii = 0; ii < INDEXFLD_NUM_TYPES; ++ii) {
    if (cur->indexKeys[ii]) {
      RedisModule_CloseKey(cur->indexKeys[ii]);
    }
  }
}

int Document_AddToIndexes(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  Document *doc = aCtx->doc;
  int ourRv = REDISMODULE_OK;

  for (size_t i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    const DocumentField *ff = doc->fields + i;
    FieldIndexerData *fdata = aCtx->fdatas + i;

    for (size_t ii = 0; ii < INDEXFLD_NUM_TYPES; ++ii) {
      if (!FIELD_CHKIDX(ff->indexAs, INDEXTYPE_FROM_POS(ii))) {
        continue;
      }

      PreprocessorFunc pp = preprocessorMap[ii];
      if (pp(aCtx, sctx, &doc->fields[i], fs, fdata, &aCtx->status) != 0) {
        ++aCtx->spec->stats.indexingFailures;
        ourRv = REDISMODULE_ERR;
        goto cleanup;
      }
      if (!(fs->options & FieldSpec_Dynamic)) {
        // Non-dynamic fields are only indexed as a single type.
        // Only dynamic fields may be indexed as multiple index types.
        break;
      }
    }
  }

  if (Indexer_Add(aCtx->indexer, aCtx) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (ourRv != REDISMODULE_OK) {
    // if a document did not load properly, it is deleted
    // to prevent mismatch of index and hash
    t_docId docId = DocTable_GetIdR(&aCtx->spec->docs, doc->docKey);
    if (docId)
      IndexSpec_DeleteDoc_Unsafe(aCtx->spec, RSDummyContext, doc->docKey, docId);

    QueryError_SetCode(&aCtx->status, QUERY_EGENERIC);
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
                            int *result, QueryError *status) {

  int rc = REDISMODULE_ERR;
  RSExpr *e = NULL;
  RedisSearchCtx_LockSpecRead(sctx);
  const RSDocumentMetadata *dmd = DocTable_BorrowByKeyR(&sctx->spec->docs, key);
  if (!dmd) {
    // We don't know the document...
    QueryError_SetError(status, QUERY_ENODOC, "");
    goto done;
  }

  // Try to parser the expression first, fail if we can't
  if (!(e = ExprAST_Parse(expr, strlen(expr), status)) || QueryError_HasError(status)) {
    goto done;
  }

  RLookup lookup_s;
  RLookupRow row = {0};
  IndexSpecCache *spcache = IndexSpec_GetSpecCache(sctx->spec);
  RLookup_Init(&lookup_s, spcache);
  lookup_s.options |= RLOOKUP_OPT_ALL_LOADED; // Setting this option will cause creating keys of non-sortable fields possible
  if (ExprAST_GetLookupKeys(e, &lookup_s, status) == EXPR_EVAL_ERR) {
    goto CleanUp;
  }

  RLookupLoadOptions loadopts = {.sctx = sctx, .dmd = dmd, .status = status};
  if (RLookup_LoadDocument(&lookup_s, &row, &loadopts) != REDISMODULE_OK) {
    goto CleanUp;
  }

  ExprEval evaluator = {.err = status, .lookup = &lookup_s, .res = NULL, .srcrow = &row, .root = e};
  RSValue rv = RSVALUE_STATIC;
  if (ExprEval_Eval(&evaluator, &rv) != EXPR_EVAL_OK) {
    goto CleanUp;
  }

  *result = RSValue_BoolTest(&rv);
  RSValue_Clear(&rv);
  rc = REDISMODULE_OK;

CleanUp:
  RLookupRow_Cleanup(&row);
  RLookup_Cleanup(&lookup_s);
done:
  ExprAST_Free(e);
  DMD_Return(dmd);
  RedisSearchCtx_UnlockSpec(sctx);
  return rc;
}

static void AddDocumentCtx_UpdateNoIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
#define BAIL(s)                                            \
  do {                                                     \
    QueryError_SetError(&aCtx->status, QUERY_EGENERIC, s); \
    goto done;                                             \
  } while (0)

  RSDocumentMetadata *md = NULL;
  Document *doc = aCtx->doc;
  t_docId docId = DocTable_GetIdR(&sctx->spec->docs, doc->docKey);
  if (docId == 0) {
    BAIL("Couldn't load old document");
  }
  // Assumes we are under write lock
  md = (RSDocumentMetadata *)DocTable_Borrow(&sctx->spec->docs, docId);
  if (!md) {
    BAIL("Couldn't load document metadata");
  }

  // Update the score
  md->score = doc->score;
  // Set the payload if needed
  if (doc->payload) {
    DocTable_SetPayload(&sctx->spec->docs, md, doc->payload, doc->payloadSize);
  }

  if (aCtx->stateFlags & ACTX_F_SORTABLES) {
    FieldSpecDedupeArray dedupes = {0};
    // Update sortables if needed
    for (int i = 0; i < doc->numFields; i++) {
      DocumentField *f = &doc->fields[i];
      const FieldSpec *fs = IndexSpec_GetField(sctx->spec, f->name, strlen(f->name));
      if (fs == NULL || !FieldSpec_IsSortable(fs)) {
        continue;
      }

      if (dedupes[fs->index]) {
        BAIL(DUP_FIELD_ERRSTR);
      }

      dedupes[fs->index] = 1;

      int idx = RSSortingTable_GetFieldIdx(sctx->spec->sortables, f->name);
      if (idx < 0) continue;

      if (!md->sortVector) {
        md->sortVector = NewSortingVector(sctx->spec->sortables->len);
      }

      RS_LOG_ASSERT((fs->options & FieldSpec_Dynamic) == 0, "Dynamic field cannot use PARTIAL");

      switch (fs->types) {
        case INDEXFLD_T_FULLTEXT:
        case INDEXFLD_T_TAG:
        case INDEXFLD_T_GEO:
          RSSortingVector_Put(md->sortVector, idx, (void *)RedisModule_StringPtrLen(f->text, NULL),
                              RS_SORTABLE_STR, fs->options & FieldSpec_UNF);
          break;
        case INDEXFLD_T_NUMERIC: {
          double numval;
          if (RedisModule_StringToDouble(f->text, &numval) == REDISMODULE_ERR) {
            BAIL("Could not parse numeric index value");
          }
          RSSortingVector_Put(md->sortVector, idx, &numval, RS_SORTABLE_NUM, 0);
          break;
        }
        default:
          BAIL("Unsupported sortable type");
          break;
      }
    }
  }

done:
  DMD_Return(md);
  if (aCtx->donecb) {
    aCtx->donecb(aCtx, sctx->redisCtx, aCtx->donecbData);
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

const char *DocumentField_GetValueCStr(const DocumentField *df, size_t *len) {
  *len = 0;
  switch (df->unionType) {
    case FLD_VAR_T_RMS:
      return RedisModule_StringPtrLen(df->text, len);
    case FLD_VAR_T_CSTR:
      *len = df->strlen;
      return df->strval;
    case FLD_VAR_T_ARRAY:
      if (df->arrayLen > 0) {
        // Return the first entry
        *len = strlen(df->multiVal[0]);
        return df->multiVal[0];
      }
      break;
    case FLD_VAR_T_NULL:
      break;
    case FLD_VAR_T_BLOB_ARRAY:
    case FLD_VAR_T_NUM:
    case FLD_VAR_T_GEO:
    case FLD_VAR_T_GEOMETRY:
      RS_LOG_ASSERT(0, "invalid types");
  }
  return NULL;
}

const char *DocumentField_GetArrayValueCStr(const DocumentField *df, size_t *len, size_t index) {
  if (df->unionType == FLD_VAR_T_ARRAY && index < df->arrayLen) {
    *len = strlen(df->multiVal[index]);
    return df->multiVal[index];
  }
  *len = 0;
  return NULL;
}

size_t DocumentField_GetArrayValueCStrTotalLen(const DocumentField *df) {
  RS_LOG_ASSERT(df->unionType == FLD_VAR_T_ARRAY, "must be array");
  size_t len = 0;
  for (size_t i = 0; i < df->arrayLen; ++i) {
    len += strlen(df->multiVal[i]);
  }
  return len;
}
