/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "spec.h"
#include "field_spec.h"
#include "document.h"
#include "rmutil/rm_assert.h"
#include "util/dict.h"
#include "util/references.h"
#include "query_node.h"
#include "search_options.h"
#include "query_internal.h"
#include "numeric_filter.h"
#include "suffix.h"
#include "query.h"
#include "indexer.h"
#include "extension.h"
#include "ext/default.h"
#include <float.h>
#include "rwlock.h"
#include "fork_gc.h"
#include "module.h"
#include "cursor.h"

/**
 * Most of the spec interaction is done through the RefManager, which is wrapped by a strong or weak reference struct.
 * In the LLAPI we return a pointer to an RSIndex. In order to not break the API, we typedef the RSIndex to be RefManager
 * and we return it instead of the strong reference that should wrap it. we can assume that every time we get a RefManager,
 * we can cast it to (wrap it with) a strong reference and use it as such.
 */

int RediSearch_GetCApiVersion() {
  return REDISEARCH_CAPI_VERSION;
}

RefManager* RediSearch_CreateIndex(const char* name, const RSIndexOptions* options) {
  RSIndexOptions opts_s = {.gcPolicy = GC_POLICY_FORK, .stopwordsLen = -1};
  if (!options) {
    options = &opts_s;
  }
  IndexSpec* spec = NewIndexSpec(name);
  StrongRef ref = StrongRef_New(spec, (RefManager_Free)IndexSpec_Free);
  IndexSpec_MakeKeyless(spec);
  spec->flags |= Index_Temporary;  // temporary is so that we will not use threads!!
  spec->flags |= Index_FromLLAPI;
  if (!spec->indexer) {
    spec->indexer = NewIndexer(spec);
  }

  if (options->score || options->lang) {
    spec->rule = rm_calloc(1, sizeof *spec->rule);
    spec->rule->score_default = options->score ? options->score : DEFAULT_SCORE;
    spec->rule->lang_default = RSLanguage_Find(options->lang, 0);
  }

  spec->getValue = options->gvcb;
  spec->getValueCtx = options->gvcbData;
  if (options->flags & RSIDXOPT_DOCTBLSIZE_UNLIMITED) {
    spec->docs.maxSize = DOCID_MAX;
  }
  if (options->gcPolicy != GC_POLICY_NONE) {
    IndexSpec_StartGCFromSpec(ref, spec, options->gcPolicy);
  }
  if (options->stopwordsLen != -1) {
    // replace default list which is a global so no need to free anything.
    spec->stopwords = NewStopWordListCStr((const char **)options->stopwords,
                                                         options->stopwordsLen);
  }
  return ref.rm;
}

void RediSearch_DropIndex(RefManager* rm) {
  RWLOCK_ACQUIRE_WRITE();
  StrongRef ref = {rm};
  StrongRef_Invalidate(ref);
  StrongRef_Release(ref);
  RWLOCK_RELEASE();
}

char **RediSearch_IndexGetStopwords(RefManager* rm, size_t *size) {
  IndexSpec *spec = __RefManager_Get_Object(rm);
  return GetStopWordsList(spec->stopwords, size);
}

void RediSearch_StopwordsList_Free(char **list, size_t size) {
  for (int i = 0; i < size; ++i) {
    rm_free(list[i]);
  }
  rm_free(list);
}

double RediSearch_IndexGetScore(RefManager* rm) {
  IndexSpec *spec = __RefManager_Get_Object(rm);
  if (spec->rule) {
    return spec->rule->score_default;
  }
  return DEFAULT_SCORE;
}

const char *RediSearch_IndexGetLanguage(RefManager* rm) {
  IndexSpec *spec = __RefManager_Get_Object(rm);
  if (spec->rule) {
    return RSLanguage_ToString(spec->rule->lang_default);
  }
  return RSLanguage_ToString(DEFAULT_LANGUAGE);
}

int RediSearch_ValidateLanguage(const char *lang) {
  if (!lang || RSLanguage_Find(lang, 0) == RS_LANG_UNSUPPORTED) {
    return REDISEARCH_ERR;
  }
  return REDISEARCH_OK;
}

RSFieldID RediSearch_CreateField(RefManager* rm, const char* name, unsigned types,
                                 unsigned options) {
  RS_LOG_ASSERT(types, "types should not be RSFLDTYPE_DEFAULT");
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec *sp = __RefManager_Get_Object(rm);

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
    fs->types |= INDEXFLD_T_FULLTEXT;
  }

  if (types & RSFLDTYPE_NUMERIC) {
    numTypes++;
    fs->types |= INDEXFLD_T_NUMERIC;
  }
  if (types & RSFLDTYPE_GEO) {
    fs->types |= INDEXFLD_T_GEO;
    numTypes++;
  }
  if (types & RSFLDTYPE_VECTOR) {
    fs->types |= INDEXFLD_T_VECTOR;
    numTypes++;
  }
  if (types & RSFLDTYPE_TAG) {
    fs->types |= INDEXFLD_T_TAG;
    numTypes++;
  }
  // TODO: GEOMETRY
  // if (types & RSFLDTYPE_GEOMETRY) {
  //   fs->types |= INDEXFLD_T_GEOMETRY;
  //   numTypes++;
  // }

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
  if (options & RSFLDOPT_WITHSUFFIXTRIE) {
    fs->options |= FieldSpec_WithSuffixTrie;
    if (fs->types == INDEXFLD_T_FULLTEXT) {
      sp->suffixMask |= FIELD_BIT(fs);
      if (!sp->suffix) {
        sp->suffix = NewTrie(suffixTrie_freeCallback, Trie_Sort_Lex);
        sp->flags |= Index_HasSuffixTrie;
      }
    }
  }

  RWLOCK_RELEASE();
  return fs->index;
}

void RediSearch_TextFieldSetWeight(RefManager* rm, RSFieldID id, double w) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_FULLTEXT), "types should be INDEXFLD_T_FULLTEXT");
  fs->ftWeight = w;
}

void RediSearch_TagFieldSetSeparator(RefManager* rm, RSFieldID id, char sep) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  fs->tagOpts.tagSep = sep;
}

void RediSearch_TagFieldSetCaseSensitive(RefManager* rm, RSFieldID id, int enable) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(FIELD_IS(fs, INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  if (enable) {
    fs->tagOpts.tagFlags |= TagField_CaseSensitive;
  } else {
    fs->tagOpts.tagFlags &= ~TagField_CaseSensitive;
  }
}

RSDoc* RediSearch_CreateDocument(const void* docKey, size_t len, double score, const char* lang) {
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);
  RSLanguage language = lang ? RSLanguage_Find(lang, 0) : DEFAULT_LANGUAGE;
  Document* ret = rm_calloc(1, sizeof(*ret));
  Document_Init(ret, docKeyStr, score, language, DocumentType_Hash);
  Document_MakeStringsOwner(ret);
  RedisModule_FreeString(RSDummyContext, docKeyStr);
  return ret;
}

RSDoc* RediSearch_CreateDocument2(const void* docKey, size_t len, RefManager* rm,
                                  double score, const char* lang) {
  IndexSpec* sp = __RefManager_Get_Object(rm);
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);

  RSLanguage language = lang ? RSLanguage_Find(lang, 0) :
             (sp && sp->rule) ? sp->rule->lang_default : DEFAULT_LANGUAGE;
  double docScore = !isnan(score) ? score :
             (sp && sp->rule) ? sp->rule->score_default : DEFAULT_SCORE;

  Document* ret = rm_calloc(1, sizeof(*ret));
  Document_Init(ret, docKeyStr, docScore, language, DocumentType_Hash);
  Document_MakeStringsOwner(ret);
  RedisModule_FreeString(RSDummyContext, docKeyStr);
  return ret;
}

void RediSearch_FreeDocument(RSDoc* doc) {
  Document_Free(doc);
  rm_free(doc);
}

int RediSearch_DeleteDocument(RefManager* rm, const void* docKey, size_t len) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec* sp = __RefManager_Get_Object(rm);
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

void RediSearch_DocumentAddFieldNumber(Document* d, const char* fieldname, double val, unsigned as) {
  if (as == RSFLDTYPE_NUMERIC) {
    Document_AddNumericField(d, fieldname, val, as);
  } else {
    char buf[512];
    size_t len = sprintf(buf, "%lf", val);
    Document_AddFieldC(d, fieldname, buf, len, as);
  }
}

int RediSearch_DocumentAddFieldGeo(Document* d, const char* fieldname,
                                    double lat, double lon, unsigned as) {
  if (lat > GEO_LAT_MAX || lat < GEO_LAT_MIN || lon > GEO_LONG_MAX || lon < GEO_LONG_MIN) {
    // out of range
    return REDISMODULE_ERR;
  }

  if (as == RSFLDTYPE_GEO) {
    Document_AddGeoField(d, fieldname, lon, lat, as);
  } else {
    char buf[24];
    size_t len = sprintf(buf, "%.6lf,%.6lf", lon, lat);
    Document_AddFieldC(d, fieldname, buf, len, as);
  }

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

int RediSearch_IndexAddDocument(RefManager* rm, Document* d, int options, char** errs) {
  RWLOCK_ACQUIRE_WRITE();
  IndexSpec* sp = __RefManager_Get_Object(rm);

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
  AddDocumentCtx_Submit(aCtx, &sctx, options);
  QueryError_ClearError(&status);
  rm_free(d);

  RWLOCK_RELEASE();
  return err.hasErr ? REDISMODULE_ERR : REDISMODULE_OK;
}

QueryNode* RediSearch_CreateTokenNode(RefManager* rm, const char* fieldName, const char* token) {
  IndexSpec* sp = __RefManager_Get_Object(rm);
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

QueryNode* RediSearch_CreateTagTokenNode(RefManager* rm, const char* token) {
  QueryNode* ret = NewQueryNode(QN_TOKEN);
  ret->tn = (QueryTokenNode){
      .str = (char*)rm_strdup(token), .len = strlen(token), .expanded = 0, .flags = 0};
  return ret;
}

QueryNode* RediSearch_CreateNumericNode(RefManager* rm, const char* field, double max, double min,
                                        int includeMax, int includeMin) {
  QueryNode* ret = NewQueryNode(QN_NUMERIC);
  ret->nn.nf = NewNumericFilter(min, max, includeMin, includeMax, true);
  ret->nn.nf->fieldName = rm_strdup(field);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), field, strlen(field));
  return ret;
}

QueryNode* RediSearch_CreateGeoNode(RefManager* rm, const char* field, double lat, double lon,
                                        double radius, RSGeoDistance unitType) {
  QueryNode* ret = NewQueryNode(QN_GEO);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), field, strlen(field));

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

#define NODE_PREFIX 0x1
#define NODE_SUFFIX 0x2

static QueryNode* RediSearch_CreateAffixNode(IndexSpec* sp, const char* fieldName,
                                             const char* s, int flags) {
  QueryNode* ret = NewQueryNode(QN_PREFIX);
  ret->pfx = (QueryPrefixNode){
    .tok = (RSToken){.str = (char*)rm_strdup(s), .len = strlen(s), .expanded = 0, .flags = 0},
    .prefix = flags & NODE_PREFIX,
    .suffix = flags & NODE_SUFFIX,
  };
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreatePrefixNode(RefManager* rm, const char* fieldName, const char* s) {
  return RediSearch_CreateAffixNode(__RefManager_Get_Object(rm), fieldName, s, NODE_PREFIX);
}

QueryNode* RediSearch_CreateContainsNode(RefManager* rm, const char* fieldName, const char* s) {
  return RediSearch_CreateAffixNode(__RefManager_Get_Object(rm), fieldName, s, NODE_PREFIX | NODE_SUFFIX);
}

QueryNode* RediSearch_CreateSuffixNode(RefManager* rm, const char* fieldName, const char* s) {
  return RediSearch_CreateAffixNode(__RefManager_Get_Object(rm), fieldName, s, NODE_SUFFIX);
}

static QueryNode* RediSearch_CreateTagAffixNode(IndexSpec* sp, const char* s, int flags) {
  QueryNode* ret = NewQueryNode(QN_PREFIX);
  ret->pfx = (QueryPrefixNode){
    .tok = (RSToken){.str = (char*)rm_strdup(s), .len = strlen(s), .expanded = 0, .flags = 0},
    .prefix = flags & NODE_PREFIX,
    .suffix = flags & NODE_SUFFIX,
  };
  return ret;
}

QueryNode* RediSearch_CreateTagPrefixNode(RefManager* rm, const char* s) {
  return RediSearch_CreateTagAffixNode(__RefManager_Get_Object(rm), s, NODE_PREFIX);
}

QueryNode* RediSearch_CreateTagContainsNode(RefManager* rm, const char* s) {
  return RediSearch_CreateTagAffixNode(__RefManager_Get_Object(rm), s, NODE_PREFIX | NODE_SUFFIX);
}

QueryNode* RediSearch_CreateTagSuffixNode(RefManager* rm, const char* s) {
  return RediSearch_CreateTagAffixNode(__RefManager_Get_Object(rm), s, NODE_SUFFIX);
}

QueryNode* RediSearch_CreateLexRangeNode(RefManager* rm, const char* fieldName, const char* begin,
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
    ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RediSearch_CreateTagLexRangeNode(RefManager* rm, const char* begin,
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
  return ret;
}

QueryNode* RediSearch_CreateTagNode(RefManager* rm, const char* field) {
  QueryNode* ret = NewQueryNode(QN_TAG);
  ret->tag.fieldName = rm_strdup(field);
  ret->tag.len = strlen(field);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(__RefManager_Get_Object(rm), field, strlen(field));
  return ret;
}

QueryNode* RediSearch_CreateIntersectNode(RefManager* rm, int exact) {
  QueryNode* ret = NewQueryNode(QN_PHRASE);
  ret->pn.exact = exact;
  return ret;
}

QueryNode* RediSearch_CreateUnionNode(RefManager* rm) {
  return NewQueryNode(QN_UNION);
}

QueryNode* RediSearch_CreateEmptyNode(RefManager* rm) {
  return NewQueryNode(QN_NULL);
}

QueryNode* RediSearch_CreateNotNode(RefManager* rm) {
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
  RedisSearchCtx sctx;
  RSIndexResult* res;
  const RSDocumentMetadata* lastmd;
  ScoringFunctionArgs scargs;
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  double minscore;  // Used for scoring
  QueryAST qast;    // Used for string queries..
  IndexSpec* sp;
} RS_ApiIter;

#define QUERY_INPUT_STRING 1
#define QUERY_INPUT_NODE 2

typedef struct {
  int qtype;
  union {
    struct {
      const char* qs;
      size_t n;
      unsigned int dialect;
    } s;
    QueryNode* qn;
  } u;
} QueryInput;

static RS_ApiIter* handleIterCommon(IndexSpec* sp, QueryInput* input, char** error) {
  // here we only take the read lock and we will free it when the iterator will be freed
  RWLOCK_ACQUIRE_READ();
  /* We might have multiple readers that reads from the index,
   * Avoid rehashing the terms dictionary */
  dictPauseRehashing(sp->keysDict);

  RSSearchOptions options = {0};
  QueryError status = {0};
  RSSearchOptions_Init(&options);
  if(sp->rule != NULL && sp->rule->lang_default != DEFAULT_LANGUAGE) {
    options.language = sp->rule->lang_default;
  }

  RS_ApiIter* it = rm_calloc(1, sizeof(*it));
  it->sctx = SEARCH_CTX_STATIC(NULL, sp);

  if (input->qtype == QUERY_INPUT_STRING) {
    if (QAST_Parse(&it->qast, &it->sctx, &options, input->u.s.qs, input->u.s.n, input->u.s.dialect, &status) !=
        REDISMODULE_OK) {
      goto end;
    }
  } else {
    it->qast.root = input->u.qn;
  }

  // set queryAST configuration parameters
  iteratorsConfig_init(&it->qast.config);

  if (QAST_Expand(&it->qast, NULL, &options, &it->sctx, &status) != REDISMODULE_OK) {
    goto end;
  }

  it->internal = QAST_Iterate(&it->qast, &options, &it->sctx, NULL, 0, &status);
  if (!it->internal) {
    goto end;
  }

  IndexSpec_GetStats(sp, &it->scargs.indexStats);
  ExtScoringFunctionCtx* scoreCtx = Extensions_GetScoringFunction(&it->scargs, DEFAULT_SCORER_NAME);
  RS_LOG_ASSERT(scoreCtx, "GetScoringFunction failed");
  it->scorer = scoreCtx->sf;
  it->scorerFree = scoreCtx->ff;
  it->minscore = DBL_MAX;
  it->sp = sp;

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

int RediSearch_DocumentExists(RefManager* rm, const void* docKey, size_t len) {
  IndexSpec* sp = __RefManager_Get_Object(rm);
  return DocTable_GetId(&sp->docs, docKey, len) != 0;
}

RS_ApiIter* RediSearch_IterateQuery(RefManager* rm, const char* s, size_t n, char** error) {
  QueryInput input = {.qtype = QUERY_INPUT_STRING, .u = {.s = {.qs = s, .n = n, .dialect = 1}}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, error);
}

RS_ApiIter* RediSearch_IterateQueryWithDialect(RefManager* rm, const char* s, size_t n, unsigned int dialect, char** error) {
  QueryInput input = {.qtype = QUERY_INPUT_STRING, .u = {.s = {.qs = s, .n = n, .dialect = dialect}}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, error);
}

RS_ApiIter* RediSearch_GetResultsIterator(QueryNode* qn, RefManager* rm) {
  QueryInput input = {.qtype = QUERY_INPUT_NODE, .u = {.qn = qn}};
  return handleIterCommon(__RefManager_Get_Object(rm), &input, NULL);
}

void RediSearch_QueryNodeFree(QueryNode* qn) {
  QueryNode_Free(qn);
}

int RediSearch_QueryNodeType(QueryNode* qn) {
  return qn->type;
}

// use only by LLAPI + unittest
const void* RediSearch_ResultsIteratorNext(RS_ApiIter* iter, RefManager* rm, size_t* len) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  while (iter->internal->Read(iter->internal->ctx, &iter->res) != INDEXREAD_EOF) {
    const RSDocumentMetadata* md = DocTable_Borrow(&sp->docs, iter->res->docId);
    if (md == NULL) {
      continue;
    }
    DMD_Return(iter->lastmd);
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
  DMD_Return(iter->lastmd);
  if (iter->sp && iter->sp->keysDict) {
    dictResumeRehashing(iter->sp->keysDict);
  }
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
  if (opts->stopwordsLen > 0) {
    for (int i = 0; i < opts->stopwordsLen; i++) {
      rm_free(opts->stopwords[i]);
    }
    rm_free(opts->stopwords);
  }

  opts->stopwords = NULL;

  if (stopwordsLen > 0) {
    opts->stopwords = rm_malloc(sizeof(*opts->stopwords) * stopwordsLen);
    for (int i = 0; i < stopwordsLen; i++) {
      opts->stopwords[i] = rm_strdup(stopwords[i]);
    }
  }
  opts->stopwordsLen = stopwordsLen;
}

void RediSearch_IndexOptionsSetFlags(RSIndexOptions* options, uint32_t flags) {
  options->flags = flags;
}

void RediSearch_IndexOptionsSetGCPolicy(RSIndexOptions* options, int policy) {
  options->gcPolicy = policy;
}

int RediSearch_IndexOptionsSetScore(RSIndexOptions* options, double score) {
  if (score < 0 || score > 1) {
    return REDISEARCH_ERR;
  }
  options->score = score;
  return REDISEARCH_OK;
}

int RediSearch_IndexOptionsSetLanguage(RSIndexOptions* options, const char *lang) {
  if (!lang || RediSearch_ValidateLanguage(lang) != REDISEARCH_OK) {
    return REDISEARCH_ERR;
  }
  options->lang = lang;
  return REDISEARCH_OK;
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
}

int RediSearch_StopwordsList_Contains(RSIndex* idx, const char *term, size_t len) {
  IndexSpec *sp = __RefManager_Get_Object(idx);
  return StopWordList_Contains(sp->stopwords, term, len);
}

void RediSearch_FieldInfo(struct RSIdxField *infoField, FieldSpec *specField) {
  infoField->name = rm_strdup(specField->name);
  infoField->path = rm_strdup(specField->path);
  if (specField->types & INDEXFLD_T_FULLTEXT) {
    infoField->types |= RSFLDTYPE_FULLTEXT;
    infoField->textWeight = specField->ftWeight;
  }
  if (specField->types & INDEXFLD_T_NUMERIC) {
    infoField->types |= RSFLDTYPE_NUMERIC;
  }
  if (specField->types & INDEXFLD_T_TAG) {
    infoField->types |= RSFLDTYPE_TAG;
    infoField->tagSeperator = specField->tagOpts.tagSep;
    infoField->tagCaseSensitive = specField->tagOpts.tagFlags & TagField_CaseSensitive ? 1 : 0;
  }
  if (specField->types & INDEXFLD_T_GEO) {
    infoField->types |= RSFLDTYPE_GEO;
  }
  // TODO: GEMOMETRY
  // if (specField->types & INDEXFLD_T_GEOMETRY) {
  //   infoField->types |= RSFLDTYPE_GEOMETRY;
  // }


  if (FieldSpec_IsSortable(specField)) {
    infoField->options |= RSFLDOPT_SORTABLE;
  }
  if (FieldSpec_IsNoStem(specField)) {
    infoField->options |= RSFLDOPT_TXTNOSTEM;
  }
  if (FieldSpec_IsPhonetics(specField)) {
    infoField->options |= RSFLDOPT_TXTPHONETIC;
  }
  if (!FieldSpec_IsIndexable(specField)) {
    infoField->options |= RSFLDOPT_NOINDEX;
  }
}

int RediSearch_IndexInfo(RSIndex* rm, RSIdxInfo *info) {
  if (info->version < RS_INFO_INIT_VERSION || info->version > RS_INFO_CURRENT_VERSION) {
    return REDISEARCH_ERR;
  }

  RWLOCK_ACQUIRE_READ();
  IndexSpec *sp = __RefManager_Get_Object(rm);
  /* We might have multiple readers that reads from the index,
   * Avoid rehashing the terms dictionary */
  dictPauseRehashing(sp->keysDict);

  info->gcPolicy = sp->gc ? GC_POLICY_FORK : GC_POLICY_NONE;
  if (sp->rule) {
    info->score = sp->rule->score_default;
    info->lang = RSLanguage_ToString(sp->rule->lang_default);
  } else {
    info->score = DEFAULT_SCORE;
    info->lang = RSLanguage_ToString(DEFAULT_LANGUAGE);
  }

  info->numFields = sp->numFields;
  info->fields = rm_calloc(sp->numFields, sizeof(*info->fields));
  for (int i = 0; i < info->numFields; ++i) {
    RediSearch_FieldInfo(&info->fields[i], &sp->fields[i]);
  }

  info->numDocuments = sp->stats.numDocuments;
  info->maxDocId = sp->docs.maxDocId;
  info->docTableSize = sp->docs.memsize;
  info->sortablesSize = sp->docs.sortablesSize;
  info->docTrieSize = TrieMap_MemUsage(sp->docs.dim.tm);
  info->numTerms = sp->stats.numTerms;
  info->numRecords = sp->stats.numRecords;
  info->invertedSize = sp->stats.invertedSize;
  info->invertedCap = sp->stats.invertedCap;
  info->skipIndexesSize = sp->stats.skipIndexesSize;
  info->scoreIndexesSize = sp->stats.scoreIndexesSize;
  info->offsetVecsSize = sp->stats.offsetVecsSize;
  info->offsetVecRecords = sp->stats.offsetVecRecords;
  info->termsSize = sp->stats.termsSize;
  info->indexingFailures = sp->stats.indexError.error_count;

  if (sp->gc) {
    // LLAPI always uses ForkGC
    ForkGCStats gcStats = ((ForkGC *)sp->gc->gcCtx)->stats;

    info->totalCollected = gcStats.totalCollected;
    info->numCycles = gcStats.numCycles;
    info->totalMSRun = gcStats.totalMSRun;
    info->lastRunTimeMs = gcStats.lastRunTimeMs;
  }

  dictResumeRehashing(sp->keysDict);
  RWLOCK_RELEASE();

  return REDISEARCH_OK;
}

size_t RediSearch_MemUsage(RSIndex* rm) {
  IndexSpec *sp = __RefManager_Get_Object(rm);
  size_t res = 0;
  res += sp->docs.memsize;
  res += sp->docs.sortablesSize;
  res += TrieMap_MemUsage(sp->docs.dim.tm);
  res += IndexSpec_collect_text_overhead(sp);
  res += IndexSpec_collect_tags_overhead(sp);
  res += sp->stats.invertedSize;
  res += sp->stats.skipIndexesSize;
  res += sp->stats.scoreIndexesSize;
  res += sp->stats.offsetVecsSize;
  res += sp->stats.termsSize;
  return res;
}

// Collect mem-usage, indexing time and gc statistics of all the currently
// existing indexes
TotalSpecsInfo RediSearch_TotalInfo(void) {
  TotalSpecsInfo info = {0};
  // Traverse `specDict_g`, and aggregate the mem-usage and indexing time of each index
  dictIterator *iter = dictGetIterator(specDict_g);
  dictEntry *entry;
  while ((entry = dictNext(iter))) {
    StrongRef ref = dictGetRef(entry);
    IndexSpec *sp = (IndexSpec *)StrongRef_Get(ref);
    if (!sp) {
      continue;
    }
    // Lock for read
    pthread_rwlock_rdlock(&sp->rwlock);
    info.total_mem += RediSearch_MemUsage((RSIndex *)ref.rm);
    info.indexing_time += sp->stats.totalIndexTime;

    if (sp->gc) {
      ForkGCStats gcStats = ((ForkGC *)sp->gc->gcCtx)->stats;
      info.gc_stats.totalCollectedBytes += gcStats.totalCollected;
      info.gc_stats.totalCycles += gcStats.numCycles;
      info.gc_stats.totalTime += gcStats.totalMSRun;
    }
    pthread_rwlock_unlock(&sp->rwlock);
  }
  dictReleaseIterator(iter);
  return info;
}

void RediSearch_IndexInfoFree(RSIdxInfo *info) {
  for (int i = 0; i < info->numFields; ++i) {
    rm_free(info->fields[i].name);
    rm_free(info->fields[i].path);
  }
  rm_free((void *)info->fields);
}
