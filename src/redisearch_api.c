#include "spec.h"
#include "field_spec.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "util/dict.h"
#include "query_node.h"
#include "search_options.h"
#include "query_internal.h"
#include "numeric_filter.h"
#include "query.h"
#include "indexer.h"
#include "extension.h"
#include "ext/default.h"
#include <float.h>
#include "rwlock.h"
#include "fork_gc.h"
#include "module.h"

int RediSearch_GetCApiVersion() {
  return REDISEARCH_CAPI_VERSION;
}

IndexSpec* RediSearch_CreateIndex(const char* name, const RSIndexOptions* options) {
  RSIndexOptions opts_s = {.gcPolicy = GC_POLICY_FORK, .stopwordsLen = -1};
  if (!options) {
    options = &opts_s;
  }
  IndexSpec* spec = NewIndexSpec(name);
  IndexSpec_MakeKeyless(spec);
  spec->flags |= Index_Temporary;  // temporary is so that we will not use threads!!
  if (!spec->indexer) {
    spec->indexer = NewIndexer(spec);
  }

  spec->getValue = options->gvcb;
  spec->getValueCtx = options->gvcbData;
  if (options->flags & RSIDXOPT_DOCTBLSIZE_UNLIMITED) {
    spec->docs.maxSize = DOCID_MAX;
  }
  if (options->gcPolicy != GC_POLICY_NONE) {
    IndexSpec_StartGCFromSpec(spec, GC_DEFAULT_HZ, options->gcPolicy);
  }
  if (options->stopwordsLen != -1) {
    // replace default list which is a global so no need to free anything.
    spec->stopwords = NewStopWordListCStr((const char **)options->stopwords,
                                                         options->stopwordsLen);
  }
  return spec;
}

void RediSearch_DropIndex(IndexSpec* sp) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec_FreeInternals(sp);
  RWLOCK_RELEASE();
}

RSFieldID RediSearch_CreateField(IndexSpec* sp, const char* name, unsigned types,
                                 unsigned options) {
  RS_LOG_ASSERT(types, "types should not be RSFLDTYPE_DEFAULT");
  RWLOCK_ACQUIRE_WRITE();

  // TODO: add a function which can take both path and name 
  FieldSpec* fs = IndexSpec_CreateField(sp, name, NULL);
  int numTypes = 0;

  if (types & RSFLDTYPE_FULLTEXT) {
    numTypes++;
    int txtId = IndexSpec_CreateTextId(sp);
    if (txtId < 0) {
      RWLOCK_RELEASE();
      return RSFIELD_INVALID;
    }
    fs->ftId = txtId;
    FieldSpec_Initialize(fs, INDEXFLD_T_FULLTEXT);
  }

  if (types & RSFLDTYPE_NUMERIC) {
    numTypes++;
    FieldSpec_Initialize(fs, INDEXFLD_T_NUMERIC);
  }
  if (types & RSFLDTYPE_GEO) {
    FieldSpec_Initialize(fs, INDEXFLD_T_GEO);
    numTypes++;
  }
  if (types & RSFLDTYPE_TAG) {
    FieldSpec_Initialize(fs, INDEXFLD_T_TAG);
    numTypes++;
  }

  if (numTypes > 1) {
    fs->options |= FieldSpec_Dynamic;
  }

  if (options & RSFLDOPT_NOINDEX) {
    fs->options |= FieldSpec_NotIndexable;
  }
  if (options & RSFLDOPT_SORTABLE) {
    fs->options |= FieldSpec_Sortable;
    fs->sortIdx = RSSortingTable_Add(&sp->sortables, fs->name, fieldTypeToValueType(fs->types));
  }
  if (options & RSFLDOPT_TXTNOSTEM) {
    fs->options |= FieldSpec_NoStemming;
  }
  if (options & RSFLDOPT_TXTPHONETIC) {
    fs->options |= FieldSpec_Phonetics;
    sp->flags |= Index_HasPhonetic;
  }

  RWLOCK_RELEASE();
  return fs->index;
}

void RediSearch_TextFieldSetWeight(IndexSpec* sp, RSFieldID id, double w) {
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_FULLTEXT), "types should be INDEXFLD_T_FULLTEXT");
  fs->ftWeight = w;
}

void RediSearch_TagFieldSetSeparator(IndexSpec* sp, RSFieldID id, char sep) {
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  fs->tagSep = sep;
}

void RediSearch_TagFieldSetCaseSensitive(IndexSpec* sp, RSFieldID id, int enable) {
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  if (enable) {
    fs->tagFlags |= TagField_CaseSensitive;
  } else {
    fs->tagFlags &= ~TagField_CaseSensitive;
  }
}

RSDoc* RediSearch_CreateDocument(const void* docKey, size_t len, double score, const char* lang) {
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);
  RSLanguage language = lang ? RSLanguage_Find(lang, 0) : DEFAULT_LANGUAGE;
  Document* ret = rm_calloc(1, sizeof(*ret));
  // TODO: Should we introduce DocumentType_LLAPI?
  Document_Init(ret, docKeyStr, score, language, DocumentType_Hash);
  Document_MakeStringsOwner(ret);
  RedisModule_FreeString(RSDummyContext, docKeyStr);
  return ret;
}

void RediSearch_FreeDocument(RSDoc* doc) {
  Document_Free(doc);
  rm_free(doc);
}

int RediSearch_DeleteDocument(IndexSpec* sp, const void* docKey, size_t len) {
  RWLOCK_ACQUIRE_WRITE();
  int rc = REDISMODULE_OK;
  t_docId id = DocTable_GetId(&sp->docs, docKey, len);
  if (id == 0) {
    rc = REDISMODULE_ERR;
  } else {
    if (DocTable_Delete(&sp->docs, docKey, len)) {
      // Delete returns true/false, not RM_{OK,ERR}
      sp->stats.numDocuments--;
      if (sp->gc) {
        GCContext_OnDelete(sp->gc);
      }
    } else {
      rc = REDISMODULE_ERR;
    }
  }

  RWLOCK_RELEASE();
  return rc;
}

void RediSearch_DocumentAddField(Document* d, const char* fieldName, RedisModuleString* value,
                                 unsigned as) {
  Document_AddField(d, fieldName, value, as);
}

void RediSearch_DocumentAddFieldString(Document* d, const char* fieldname, const char* s, size_t n,
                                       unsigned as) {
  Document_AddFieldC(d, fieldname, s, n, as);
}

void RediSearch_DocumentAddFieldNumber(Document* d, const char* fieldname, double n, unsigned as) {
  char buf[512];
  size_t len = sprintf(buf, "%lf", n);
  Document_AddFieldC(d, fieldname, buf, len, as);
}

int RediSearch_DocumentAddFieldGeo(Document* d, const char* fieldname, 
                                    double lat, double lon, unsigned as) {
  if (lat > GEO_LAT_MAX || lat < GEO_LAT_MIN || lon > GEO_LONG_MAX || lon < GEO_LONG_MIN) {
    // out of range
    return REDISMODULE_ERR;
  }                                      
  // The format for a geospacial point is "lon,lat"
  char buf[24];
  size_t len = sprintf(buf, "%.6lf,%.6lf", lon, lat);
  Document_AddFieldC(d, fieldname, buf, len, as);
  return REDISMODULE_OK;
}

typedef struct {
  char** s;
  int hasErr;
} RSError;

void RediSearch_AddDocDone(RSAddDocumentCtx* aCtx, RedisModuleCtx* ctx, void* err) {
  RSError* ourErr = err;
  if (QueryError_HasError(&aCtx->status)) {
    if (ourErr->s) {
      *ourErr->s = rm_strdup(QueryError_GetError(&aCtx->status));
    }
    ourErr->hasErr = aCtx->status.code;
  }
}

int RediSearch_IndexAddDocument(IndexSpec* sp, Document* d, int options, char** errs) {
  RWLOCK_ACQUIRE_WRITE();

  RSError err = {.s = errs};
  QueryError status = {0};
  RSAddDocumentCtx* aCtx = NewAddDocumentCtx(sp, d, &status);
  if (aCtx == NULL) {
    if (status.detail) {
      QueryError_ClearError(&status);
    }
    RWLOCK_RELEASE();
    return REDISMODULE_ERR;
  }
  aCtx->donecb = RediSearch_AddDocDone;
  aCtx->donecbData = &err;
  RedisSearchCtx sctx = {.redisCtx = NULL, .spec = sp};
  int exists = !!DocTable_GetIdR(&sp->docs, d->docKey);
  if (exists) {
    if (options & REDISEARCH_ADD_REPLACE) {
      options |= DOCUMENT_ADD_REPLACE;
    } else {
      if (errs) {
        *errs = rm_strdup("Document already exists");
      }
      AddDocumentCtx_Free(aCtx);
      RWLOCK_RELEASE();
      return REDISMODULE_ERR;
    }
  }

  options |= DOCUMENT_ADD_NOSAVE;
  aCtx->stateFlags |= ACTX_F_NOBLOCK;
  AddDocumentCtx_Submit(aCtx, &sctx, options);
  rm_free(d);

  RWLOCK_RELEASE();
  return err.hasErr ? REDISMODULE_ERR : REDISMODULE_OK;
}

QueryNode* RediSearch_CreateTokenNode(IndexSpec* sp, const char* fieldName, const char* token) {
  if (StopWordList_Contains(sp->stopwords, token, strlen(token))) {
    return NULL;
  }
  QueryNode* ret = NewQueryNode(QN_TOKEN);

  ret->tn = (QueryTokenNode){
      .str = (char*)rm_strdup(token), .len = strlen(token), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreateNumericNode(IndexSpec* sp, const char* field, double max, double min,
                                        int includeMax, int includeMin) {
  QueryNode* ret = NewQueryNode(QN_NUMERIC);
  ret->nn.nf = NewNumericFilter(min, max, includeMin, includeMax);
  ret->nn.nf->fieldName = rm_strdup(field);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, field, strlen(field));
  return ret;
}

QueryNode* RediSearch_CreateGeoNode(IndexSpec* sp, const char* field, double lat, double lon,
                                        double radius, RSGeoDistance unitType) {
  QueryNode* ret = NewQueryNode(QN_GEO);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, field, strlen(field));

  GeoFilter *flt = rm_malloc(sizeof(*flt));
  flt->lat = lat;
  flt->lon = lon;
  flt->radius = radius;
  flt->numericFilters = NULL;
  flt->property = rm_strdup(field);
  flt->unitType = (GeoDistance)unitType;

  ret->gn.gf = flt;

  return ret;
}

QueryNode* RediSearch_CreatePrefixNode(IndexSpec* sp, const char* fieldName, const char* s) {
  QueryNode* ret = NewQueryNode(QN_PREFIX);
  ret->pfx =
      (QueryPrefixNode){.str = (char*)rm_strdup(s), .len = strlen(s), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreateLexRangeNode(IndexSpec* sp, const char* fieldName, const char* begin,
                                         const char* end, int includeBegin, int includeEnd) {
  QueryNode* ret = NewQueryNode(QN_LEXRANGE);
  if (begin) {
    ret->lxrng.begin = begin ? rm_strdup(begin) : NULL;
    ret->lxrng.includeBegin = includeBegin;
  }
  if (end) {
    ret->lxrng.end = end ? rm_strdup(end) : NULL;
    ret->lxrng.includeEnd = includeEnd;
  }
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreateTagNode(IndexSpec* sp, const char* field) {
  QueryNode* ret = NewQueryNode(QN_TAG);
  ret->tag.fieldName = rm_strdup(field);
  ret->tag.len = strlen(field);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, field, strlen(field));
  return ret;
}

QueryNode* RediSearch_CreateIntersectNode(IndexSpec* sp, int exact) {
  QueryNode* ret = NewQueryNode(QN_PHRASE);
  ret->pn.exact = exact;
  return ret;
}

QueryNode* RediSearch_CreateUnionNode(IndexSpec* sp) {
  return NewQueryNode(QN_UNION);
}

QueryNode* RediSearch_CreateEmptyNode(IndexSpec* sp) {
  return NewQueryNode(QN_NULL);
}

QueryNode* RediSearch_CreateNotNode(IndexSpec* sp) {
  return NewQueryNode(QN_NOT);
}

int RediSearch_QueryNodeGetFieldMask(QueryNode* qn) {
  return qn->opts.fieldMask;
}

void RediSearch_QueryNodeAddChild(QueryNode* parent, QueryNode* child) {
  QueryNode_AddChild(parent, child);
}

void RediSearch_QueryNodeClearChildren(QueryNode* qn) {
  QueryNode_ClearChildren(qn, 1);
}

QueryNode* RediSearch_QueryNodeGetChild(const QueryNode* qn, size_t ix) {
  return QueryNode_GetChild(qn, ix);
}

size_t RediSearch_QueryNodeNumChildren(const QueryNode* qn) {
  return QueryNode_NumChildren(qn);
}

typedef struct RS_ApiIter {
  IndexIterator* internal;
  RSIndexResult* res;
  const RSDocumentMetadata* lastmd;
  ScoringFunctionArgs scargs;
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  double minscore;  // Used for scoring
  QueryAST qast;    // Used for string queries..
} RS_ApiIter;

#define QUERY_INPUT_STRING 1
#define QUERY_INPUT_NODE 2

typedef struct {
  int qtype;
  union {
    struct {
      const char* qs;
      size_t n;
    } s;
    QueryNode* qn;
  } u;
} QueryInput;

static RS_ApiIter* handleIterCommon(IndexSpec* sp, QueryInput* input, char** error) {
  // here we only take the read lock and we will free it when the iterator will be freed
  RWLOCK_ACQUIRE_READ();

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(NULL, sp);
  RSSearchOptions options = {0};
  QueryError status = {0};
  RSSearchOptions_Init(&options);
  RS_ApiIter* it = rm_calloc(1, sizeof(*it));

  if (input->qtype == QUERY_INPUT_STRING) {
    if (QAST_Parse(&it->qast, &sctx, &options, input->u.s.qs, input->u.s.n, &status) !=
        REDISMODULE_OK) {
      goto end;
    }
  } else {
    it->qast.root = input->u.qn;
  }

  if (QAST_Expand(&it->qast, NULL, &options, &sctx, &status) != REDISMODULE_OK) {
    goto end;
  }

  it->internal = QAST_Iterate(&it->qast, &options, &sctx, NULL);
  if (!it->internal) {
    goto end;
  }

  IndexSpec_GetStats(sp, &it->scargs.indexStats);
  ExtScoringFunctionCtx* scoreCtx = Extensions_GetScoringFunction(&it->scargs, DEFAULT_SCORER_NAME);
  RS_LOG_ASSERT(scoreCtx, "GetScoringFunction failed");
  it->scorer = scoreCtx->sf;
  it->scorerFree = scoreCtx->ff;
  it->minscore = DBL_MAX;

  // dummy statement for goto
  ;
end:

  if (QueryError_HasError(&status) || it->internal == NULL) {
    if (it) {
      RediSearch_ResultsIteratorFree(it);
      it = NULL;
    }
    if (error) {
      *error = rm_strdup(QueryError_GetError(&status));
    }
  }
  QueryError_ClearError(&status);
  return it;
}

int RediSearch_DocumentExists(IndexSpec* sp, const void* docKey, size_t len) {
  return DocTable_GetId(&sp->docs, docKey, len) != 0;
}

RS_ApiIter* RediSearch_IterateQuery(IndexSpec* sp, const char* s, size_t n, char** error) {
  QueryInput input = {.qtype = QUERY_INPUT_STRING, .u = {.s = {.qs = s, .n = n}}};
  return handleIterCommon(sp, &input, error);
}

RS_ApiIter* RediSearch_GetResultsIterator(QueryNode* qn, IndexSpec* sp) {
  QueryInput input = {.qtype = QUERY_INPUT_NODE, .u = {.qn = qn}};
  return handleIterCommon(sp, &input, NULL);
}

void RediSearch_QueryNodeFree(QueryNode* qn) {
  QueryNode_Free(qn);
}

int RediSearch_QueryNodeType(QueryNode* qn) {
  return qn->type;
}

// use only by LLAPI + unittest
const void* RediSearch_ResultsIteratorNext(RS_ApiIter* iter, IndexSpec* sp, size_t* len) {
  while (iter->internal->Read(iter->internal->ctx, &iter->res) != INDEXREAD_EOF) {
    const RSDocumentMetadata* md = DocTable_Get(&sp->docs, iter->res->docId);
    if (md == NULL || ((md)->flags & Document_Deleted)) {
      continue;
    }
    iter->lastmd = md;
    if (len) {
      *len = sdslen(md->keyPtr);
    }
    return md->keyPtr;
  }
  return NULL;
}

double RediSearch_ResultsIteratorGetScore(const RS_ApiIter* it) {
  return it->scorer(&it->scargs, it->res, it->lastmd, 0);
}

void RediSearch_ResultsIteratorFree(RS_ApiIter* iter) {
  if (iter->internal) {
    iter->internal->Free(iter->internal);
  } else {
    printf("Not freeing internal iterator. internal iterator is null\n");
  }
  if (iter->scorerFree) {
    iter->scorerFree(iter->scargs.extdata);
  }
  QAST_Destroy(&iter->qast);
  rm_free(iter);

  RWLOCK_RELEASE();
}

void RediSearch_ResultsIteratorReset(RS_ApiIter* iter) {
  iter->internal->Rewind(iter->internal->ctx);
}

RSIndexOptions* RediSearch_CreateIndexOptions() {
  RSIndexOptions* ret = rm_calloc(1, sizeof(RSIndexOptions));
  ret->gcPolicy = GC_POLICY_NONE;
  ret->stopwordsLen = -1;
  return ret;
}

void RediSearch_FreeIndexOptions(RSIndexOptions* options) {
  if (options->stopwordsLen > 0) {
    for (int i = 0; i < options->stopwordsLen; i++) {
      rm_free(options->stopwords[i]);
    }
    rm_free(options->stopwords);
  }
  rm_free(options);
}

void RediSearch_IndexOptionsSetGetValueCallback(RSIndexOptions* options, RSGetValueCallback cb,
                                                void* ctx) {
  options->gvcb = cb;
  options->gvcbData = ctx;
}

void RediSearch_IndexOptionsSetStopwords(RSIndexOptions* opts, const char **stopwords, int stopwordsLen) {
  if (stopwordsLen < 0) {
    return;
  }
  
  opts->stopwordsLen = stopwordsLen;
  if (stopwordsLen == 0) {
    return;
  }

  opts->stopwords = rm_malloc(sizeof(*opts->stopwords) * stopwordsLen);
  for (int i = 0; i < stopwordsLen; i++) {
    opts->stopwords[i] = rm_strdup(stopwords[i]);
  }
}

void RediSearch_IndexOptionsSetFlags(RSIndexOptions* options, uint32_t flags) {
  options->flags = flags;
}

void RediSearch_IndexOptionsSetGCPolicy(RSIndexOptions* options, int policy) {
  options->gcPolicy = policy;
}

#define REGISTER_API(name)                                                          \
  if (RedisModule_ExportSharedAPI(ctx, "RediSearch_" #name, RediSearch_##name) !=   \
      REDISMODULE_OK) {                                                             \
    RedisModule_Log(ctx, "warning", "could not register RediSearch_" #name "\r\n"); \
    return REDISMODULE_ERR;                                                         \
  }

int RediSearch_ExportCapi(RedisModuleCtx* ctx) {
  if (RedisModule_ExportSharedAPI == NULL) {
    RedisModule_Log(ctx, "warning", "Upgrade redis-server to use Redis Search's C API");
    return REDISMODULE_ERR;
  }
  RS_XAPIFUNC(REGISTER_API)
  return REDISMODULE_OK;
}

void RediSearch_SetCriteriaTesterThreshold(size_t num) {
  if (num == 0) {
    RSGlobalConfig.maxResultsToUnsortedMode = DEFAULT_MAX_RESULTS_TO_UNSORTED_MODE;
  } else {
    RSGlobalConfig.maxResultsToUnsortedMode = num;
  }
}

int RediSearch_StopwordsList_Contains(RSIndex* idx, const char *term, size_t len) {
  return StopWordList_Contains(idx->stopwords, term, len);
}
