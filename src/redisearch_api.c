#include "spec.h"
#include "field_spec.h"
#include "document.h"
#include "query_node.h"
#include "search_options.h"
#include "query_internal.h"
#include "numeric_filter.h"
#include "query.h"
#include "indexer.h"
#include "extension.h"
#include "ext/default.h"
#include "rwlock.h"
#include "fork_gc.h"
#include "module.h"

#include "rmutil/rm_assert.h"

#include <float.h>

///////////////////////////////////////////////////////////////////////////////////////////////

int RediSearch_GetCApiVersion() {
  return REDISEARCH_CAPI_VERSION;
}

//---------------------------------------------------------------------------------------------

IndexSpec* RediSearch_CreateIndex(const char* name, const RSIndexOptions* options) {
  RSIndexOptions opts_s = {.gcPolicy = GC_POLICY_FORK};
  if (!options) {
    options = &opts_s;
  }
  IndexSpec spec(name);
  spec.MakeKeyless();
  spec.flags |= Index_Temporary;  // temporary is so that we will not use threads!!
  if (!spec.indexer) {
    spec.indexer = new DocumentIndexer(spec);
  }

  spec.getValue = options->gvcb;
  spec.getValueCtx = options->gvcbData;
  spec.minPrefix = 0;
  spec.maxPrefixExpansions = -1;
  if (options->flags & RSIDXOPT_DOCTBLSIZE_UNLIMITED) {
    spec.docs.maxSize = DOCID_MAX;
  }
  if (options->gcPolicy != GC_POLICY_NONE) {
    spec.StartGCFromSpec(GC_DEFAULT_HZ, options->gcPolicy);
  }
  return &spec;
}

//---------------------------------------------------------------------------------------------

void RediSearch_DropIndex(IndexSpec* sp) {
  RWLOCK_ACQUIRE_WRITE();
  sp->FreeSync();
  RWLOCK_RELEASE();
}

//---------------------------------------------------------------------------------------------

RSFieldID RediSearch_CreateField(IndexSpec* sp, const char* name, unsigned types, unsigned options) {
  RS_LOG_ASSERT(types, "types should not be RSFLDTYPE_DEFAULT");
  RWLOCK_ACQUIRE_WRITE();

  FieldSpec* fs = sp->CreateField(name);
  int numTypes = 0;

  if (types & RSFLDTYPE_FULLTEXT) {
    numTypes++;
    int txtId = sp->CreateTextId();
    if (txtId < 0) {
      RWLOCK_RELEASE();
      return RSFIELD_INVALID;
    }
    fs->ftId = txtId;
    fs->Initialize(INDEXFLD_T_FULLTEXT);
  }

  if (types & RSFLDTYPE_NUMERIC) {
    numTypes++;
    fs->Initialize(INDEXFLD_T_NUMERIC);
  }
  if (types & RSFLDTYPE_GEO) {
    fs->Initialize(INDEXFLD_T_GEO);
    numTypes++;
  }
  if (types & RSFLDTYPE_TAG) {
    fs->Initialize(INDEXFLD_T_TAG);
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
    fs->sortIdx = sp->sortables->Add(fs->name, fieldTypeToValueType(fs->types));
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

//---------------------------------------------------------------------------------------------

void RediSearch_TextFieldSetWeight(IndexSpec* sp, RSFieldID id, double w) {
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(fs->IsFieldType(INDEXFLD_T_FULLTEXT), "types should be INDEXFLD_T_FULLTEXT");
  fs->ftWeight = w;
}

//---------------------------------------------------------------------------------------------

void RediSearch_TagFieldSetSeparator(IndexSpec* sp, RSFieldID id, char sep) {
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(fs->IsFieldType(INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  fs->tagSep = sep;
}

//---------------------------------------------------------------------------------------------

void RediSearch_TagFieldSetCaseSensitive(IndexSpec* sp, RSFieldID id, int enable) {
  FieldSpec* fs = sp->fields + id;
  RS_LOG_ASSERT(fs->IsFieldType(INDEXFLD_T_TAG), "types should be INDEXFLD_T_TAG");
  if (enable) {
    fs->tagFlags |= TagField_CaseSensitive;
  } else {
    fs->tagFlags &= ~TagField_CaseSensitive;
  }
}

//---------------------------------------------------------------------------------------------

RSDoc* RediSearch_CreateDocument(const void* docKey, size_t len, double score, const char* lang) {
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);
  RSLanguage language = lang ? RSLanguage_Find(lang) : DEFAULT_LANGUAGE;
  Document* ret = new Document(docKeyStr, score, language);
  ret->MakeStringsOwner();
  RedisModule_FreeString(RSDummyContext, docKeyStr);
  return ret;
}

//---------------------------------------------------------------------------------------------

void RediSearch_FreeDocument(RSDoc* doc) {
  delete doc;
}

//---------------------------------------------------------------------------------------------

int RediSearch_DeleteDocument(IndexSpec* sp, const void* docKey, size_t len) {
  RWLOCK_ACQUIRE_WRITE();
  int rc = REDISMODULE_OK;
  t_docId id = sp->docs.GetId(docKey, len);
  if (id == 0) {
    rc = REDISMODULE_ERR;
  } else {
    if (sp->docs.Delete(docKey, len)) {
      // Delete returns true/false, not RM_{OK,ERR}
      sp->stats.numDocuments--;
    } else {
      rc = REDISMODULE_ERR;
    }
  }

  RWLOCK_RELEASE();
  return rc;
}

//---------------------------------------------------------------------------------------------

void RediSearch_DocumentAddField(Document* d, const char* fieldName, RedisModuleString* value,
                                 RedisModuleCtx* ctx, unsigned as) {
  d->AddField(fieldName, value, as);
}

//---------------------------------------------------------------------------------------------

void RediSearch_DocumentAddFieldString(Document* d, const char* fieldname, const char* s, size_t n,
                                       unsigned as) {
  d->AddFieldC(fieldname, s, n, as);
}

//---------------------------------------------------------------------------------------------

void RediSearch_DocumentAddFieldNumber(Document* d, const char* fieldname, double n, unsigned as) {
  char buf[512];
  size_t len = sprintf(buf, "%lf", n);
  d->AddFieldC(fieldname, buf, len, as);
}

//---------------------------------------------------------------------------------------------

typedef struct {
  char** s;
  int hasErr;
} RSError;

void RediSearch_AddDocDone(AddDocumentCtx* aCtx, RedisModuleCtx* ctx, void* err) {
  RSError* ourErr = err; //@@ can we use `Error` here instead of RSError?
  if (aCtx->status.HasError()) {
    if (ourErr->s) {
      *ourErr->s = rm_strdup(aCtx->status.GetError());
    }
    ourErr->hasErr = aCtx->status.code;
  }
}

//---------------------------------------------------------------------------------------------

int RediSearch_IndexAddDocument(IndexSpec* sp, Document* d, int options, char** errs) {
  RWLOCK_ACQUIRE_WRITE();

  RSError err = {.s = errs}; //@@ Can we use here Error insead?
  QueryError status;
  AddDocumentCtx* aCtx = new AddDocumentCtx(sp, d, &status);
  aCtx->donecb = RediSearch_AddDocDone;
  aCtx->donecbData = &err;
  RedisSearchCtx sctx(NULL, sp);
  int exists = !!sp->docs.GetId(d->docKey);
  if (exists) {
    if (options & REDISEARCH_ADD_REPLACE) {
      options |= DOCUMENT_ADD_REPLACE;
    } else {
      if (errs) {
        *errs = rm_strdup("Document already exists");
      }
      delete aCtx;
      RWLOCK_RELEASE();
      return REDISMODULE_ERR;
    }
  }

  options |= DOCUMENT_ADD_NOSAVE;
  aCtx->stateFlags |= ACTX_F_NOBLOCK;
  aCtx->Submit(&sctx, options);
  delete d;

  RWLOCK_RELEASE();
  return err.hasErr ? REDISMODULE_ERR : REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

QueryTokenNode* RediSearch_CreateTokenNode(IndexSpec* sp, const char* fieldName, const char* token) {
  QueryTokenNode *ret(NULL, (char*)rm_strdup(token), strlen(token));
  if (fieldName) {
    ret->opts.fieldMask = sp->GetFieldBit(fieldName, strlen(fieldName));
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryNumericNode* RediSearch_CreateNumericNode(IndexSpec* sp, const char* field, double max, double min,
                                               int includeMax, int includeMin) {
  NumericFilter* nf = new NumericFilter(min, max, includeMin, includeMax);
  nf->fieldName = rm_strdup(field);
  QueryNumericNode *ret(nf);
  ret->opts.fieldMask = sp->GetFieldBit(field, strlen(field));
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryPrefixNode* RediSearch_CreatePrefixNode(IndexSpec* sp, const char* fieldName, const char* s) {
  QueryPrefixNode *ret = new QueryPrefixNode(NULL, (char*)rm_strdup(s), strlen(s));

  if (fieldName) {
    ret->opts.fieldMask = sp->GetFieldBit(fieldName, strlen(fieldName));
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryLexRangeNode* RediSearch_CreateLexRangeNode(IndexSpec* sp, const char* fieldName, const char* begin,
                                                 const char* end, int includeBegin, int includeEnd) {
  QueryLexRangeNode* ret;
  if (begin) {
    ret->begin = begin ? rm_strdup(begin) : NULL;
    ret->includeBegin = includeBegin;
  }
  if (end) {
    ret->end = end ? rm_strdup(end) : NULL;
    ret->includeEnd = includeEnd;
  }
  if (fieldName) {
    ret->opts.fieldMask = sp->GetFieldBit(fieldName, strlen(fieldName));
  }
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryTagNode* RediSearch_CreateTagNode(IndexSpec* sp, const char* field) {
  QueryTagNode *ret = new QueryTagNode(rm_strdup(field), strlen(field));
  ret->opts.fieldMask = sp->GetFieldBit(field, strlen(field));
  return ret;
}

//---------------------------------------------------------------------------------------------

QueryPhraseNode* RediSearch_CreateIntersectNode(IndexSpec* sp, int exact) {
  return new QueryPhraseNode(exact);
}

QueryUnionNode* RediSearch_CreateUnionNode(IndexSpec* sp) {
  return new QueryUnionNode();
}

QueryNode* RediSearch_CreateEmptyNode(IndexSpec* sp) {
  return new QueryNode();
}

QueryNotNode* RediSearch_CreateNotNode(IndexSpec* sp) {
  return new QueryNotNode();
}

//---------------------------------------------------------------------------------------------

int RediSearch_QueryNodeGetFieldMask(QueryNode* qn) {
  return qn->opts.fieldMask;
}

void RediSearch_QueryNodeAddChild(QueryNode* parent, QueryNode* child) {
  parent->AddChild(child);
}

void RediSearch_QueryNodeClearChildren(QueryNode* qn) {
  qn->ClearChildren(true);
}

QueryNode* RediSearch_QueryNodeGetChild(const QueryNode* qn, size_t ix) {
  return qn->GetChild(ix);
}

size_t RediSearch_QueryNodeNumChildren(const QueryNode* qn) {
  return qn->NumChildren();
}

//---------------------------------------------------------------------------------------------

struct RS_ApiIter {
  IndexIterator* internal;
  IndexResult* res;
  const RSDocumentMetadata* lastmd;
  ScoringFunctionArgs scargs;
  RSScoringFunction scorer;
  RSFreeFunction scorerFree;
  double minscore;  // Used for scoring
  QueryAST qast;    // Used for string queries..
};

//---------------------------------------------------------------------------------------------

#define QUERY_INPUT_STRING 1
#define QUERY_INPUT_NODE 2

struct QueryInput {
  int qtype;
  union {
    struct {
      const char* qs;
      size_t n;
    } s;

    QueryNode* qn;
  } u;
};

//---------------------------------------------------------------------------------------------

static RS_ApiIter* handleIterCommon(IndexSpec* sp, QueryInput* input, char** error) {
  // here we only take the read lock and we will free it when the iterator will be freed
  RWLOCK_ACQUIRE_READ();

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(NULL, sp);
  RSSearchOptions options;
  QueryError status;
  ExtScoringFunction* scoreCtx = NULL;

  RS_ApiIter* it = rm_calloc(1, sizeof(*it));

  if (input->qtype == QUERY_INPUT_STRING) {
    it->qast = *new QueryAST(sctx, options, input->u.s.qs, input->u.s.n, &status);
  } else {
    it->qast.root = input->u.qn;
  }

  if (it->qast.Expand(NULL, &options, sctx, &status) != REDISMODULE_OK) {
    goto end;
  }

  it->internal = it->qast.Iterate(options, sctx, NULL);
  if (!it->internal) {
    goto end;
  }

  sp->GetStats(&it->scargs.indexStats);
  scoreCtx = Extensions::GetScoringFunction(&it->scargs, DEFAULT_SCORER_NAME);
  RS_LOG_ASSERT(scoreCtx, "GetScoringFunction failed");
  it->scorer = scoreCtx->sf;
  it->scorerFree = scoreCtx->ff;
  it->minscore = DBL_MAX;

  // dummy statement for goto
  ;
end:
  if (status.HasError() || it->internal == NULL) {
    if (it) {
      RediSearch_ResultsIteratorFree(it);
      it = NULL;
    }
    if (error) {
      *error = rm_strdup(status.GetError());
    }
  }
  status.ClearError();
  return it;
}

//---------------------------------------------------------------------------------------------

int RediSearch_DocumentExists(IndexSpec* sp, const void* docKey, size_t len) {
  return sp->docs.GetId(docKey, len) != 0;
}

//---------------------------------------------------------------------------------------------

RS_ApiIter* RediSearch_IterateQuery(IndexSpec* sp, const char* s, size_t n, char** error) {
  QueryInput input = {qtype: QUERY_INPUT_STRING,
                      u: {s: {s, n}}};
  return handleIterCommon(sp, &input, error);
}

//---------------------------------------------------------------------------------------------

RS_ApiIter* RediSearch_GetResultsIterator(QueryNode* qn, IndexSpec* sp) {
  QueryInput input = {.qtype = QUERY_INPUT_NODE, .u = {.qn = qn}};
  return handleIterCommon(sp, &input, NULL);
}

//---------------------------------------------------------------------------------------------

void RediSearch_QueryNodeFree(QueryNode* qn) {
  delete qn;
}

//---------------------------------------------------------------------------------------------

int RediSearch_QueryNodeType(QueryNode* qn) {
  return qn->type;
}

//---------------------------------------------------------------------------------------------

const void* RediSearch_ResultsIteratorNext(RS_ApiIter* iter, IndexSpec* sp, size_t* len) {
  while (iter->internal->Read(&iter->res) != INDEXREAD_EOF) {
    const RSDocumentMetadata* md = sp->docs.Get(iter->res->docId);
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

//---------------------------------------------------------------------------------------------

double RediSearch_ResultsIteratorGetScore(const RS_ApiIter* it) {
  return it->scorer(&it->scargs, it->res, it->lastmd, 0);
}

//---------------------------------------------------------------------------------------------

void RediSearch_ResultsIteratorFree(RS_ApiIter* iter) {
  if (iter->internal) {
    delete iter->internal;
  } else {
    printf("Not freeing internal iterator. internal iterator is null\n");
  }
  if (iter->scorerFree) {
    iter->scorerFree(iter->scargs.extdata);
  }
  delete &iter->qast;
  rm_free(iter);

  RWLOCK_RELEASE();
}

void RediSearch_ResultsIteratorReset(RS_ApiIter* iter) {
  iter->internal->Rewind();
}

//---------------------------------------------------------------------------------------------

RSIndexOptions* RediSearch_CreateIndexOptions() {
  RSIndexOptions* ret = rm_calloc(1, sizeof(RSIndexOptions));
  ret->gcPolicy = GC_POLICY_NONE;
  return ret;
}

//---------------------------------------------------------------------------------------------

void RediSearch_FreeIndexOptions(RSIndexOptions* options) {
  rm_free(options);
}

//---------------------------------------------------------------------------------------------

void RediSearch_IndexOptionsSetGetValueCallback(RSIndexOptions* options, RSGetValueCallback cb,
                                                void* ctx) {
  options->gvcb = cb;
  options->gvcbData = ctx;
}

//---------------------------------------------------------------------------------------------

void RediSearch_IndexOptionsSetFlags(RSIndexOptions* options, uint32_t flags) {
  options->flags = flags;
}

//---------------------------------------------------------------------------------------------

void RediSearch_IndexOptionsSetGCPolicy(RSIndexOptions* options, int policy) {
  options->gcPolicy = policy;
}

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

void RediSearch_SetCriteriaTesterThreshold(size_t num) {
  if (num == 0) {
    RSGlobalConfig.maxResultsToUnsortedMode = DEFAULT_MAX_RESULTS_TO_UNSORTED_MODE;
  } else {
    RSGlobalConfig.maxResultsToUnsortedMode = num;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
