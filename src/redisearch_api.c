#include "spec.h"
#include "field_spec.h"
#include "redisearch_api.h"
#include "document.h"
#include <assert.h>
#include "util/dict.h"
#include "query_node.h"
#include "search_options.h"
#include "query_internal.h"
#include "numeric_filter.h"
#include "query.h"

static void RS_ResultsIteratorFree(struct RS_ApiIter* iter);

int RS_GetCApiVersion() {
  return REDISEARCH_CAPI_VERSION;
}

static dictType invidxDictType = {0};

static void valFreeCb(void* unused, void* p) {
  KeysDictValue* kdv = p;
  if (kdv->dtor) {
    kdv->dtor(kdv->p);
  }
  free(kdv);
}

static IndexSpec* RS_CreateIndex(const char* name, const RSIndexOptions* options) {
  RSIndexOptions opts_s = {0};
  if (!options) {
    options = &opts_s;
  }
  IndexSpec* spec = NewIndexSpec(name);
  spec->flags |= Index_Temporary;  // temporary is so that we will not use threads!!

  // Initialize only once:
  if (!invidxDictType.valDestructor) {
    invidxDictType = dictTypeHeapRedisStrings;
    invidxDictType.valDestructor = valFreeCb;
  }
  spec->getValue = options->gvcb;
  spec->getValueCtx = options->gvcbData;
  spec->keysDict = dictCreate(&invidxDictType, NULL);
  spec->minPrefix = 0;
  spec->maxPrefixExpansions = -1;
  if (options->flags & RSIDXOPT_DOCTBLSIZE_UNLIMITED) {
    spec->docs.maxSize = DOCID_MAX;
  }
  return spec;
}

static void RS_DropIndex(IndexSpec* sp) {
  dict* d = sp->keysDict;
  dictRelease(d);
  sp->keysDict = NULL;
  IndexSpec_FreeSync(sp);
}

static RSField* RS_CreateField(IndexSpec* sp, const char* name, unsigned types, unsigned options) {
  assert(types);
  RSField* fs = IndexSpec_CreateField(sp, name);
  int numTypes = 0;

  if (types & RSFLDTYPE_FULLTEXT) {
    numTypes++;
    int txtId = IndexSpec_CreateTextId(sp);
    if (txtId < 0) {
      return NULL;
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
    fs->sortIdx = RSSortingTable_Add(sp->sortables, fs->name, fieldTypeToValueType(fs->types));
  }
  if (options & RSFLDOPT_TXTNOSTEM) {
    fs->options |= FieldSpec_NoStemming;
  }
  if (options & RSFLDOPT_TXTPHONETIC) {
    fs->options |= FieldSpec_Phonetics;
    sp->flags |= Index_HasPhonetic;
  }

  return fs;
}

static void RS_TextFieldSetWeight(IndexSpec* sp, FieldSpec* fs, double w) {
  assert(FIELD_IS(fs, INDEXFLD_T_FULLTEXT));
  fs->ftWeight = w;
}

static void RS_TagSetSeparator(FieldSpec* fs, char sep) {
  assert(FIELD_IS(fs, INDEXFLD_T_TAG));
  fs->tagSep = sep;
}

static Document* RS_CreateDocument(const void* docKey, size_t len, double score, const char* lang) {
  RedisModuleString* docKeyStr = RedisModule_CreateString(NULL, docKey, len);
  const char* language = lang ? lang : "english";
  Document* ret = rm_calloc(1, sizeof(*ret));
  Document_Init(ret, docKeyStr, score, 0, language, NULL, 0);
  ret->language = strdup(ret->language);
  return ret;
}

static int RS_DropDocument(IndexSpec* sp, const void* docKey, size_t len) {
  RedisModuleString* docId = RedisModule_CreateString(NULL, docKey, len);
  t_docId id = DocTable_GetIdR(&sp->docs, docId);
  if (id == 0) {
    RedisModule_FreeString(NULL, docId);
    return 0;
  }
  int rc = DocTable_DeleteR(&sp->docs, docId);
  if (rc) {
    sp->stats.numDocuments--;
    return 1;
  }
  return 0;
}

static void RS_DocumentAddField(Document* d, const char* fieldName, RedisModuleString* value,
                                unsigned as) {
  Document_AddField(d, fieldName, value, as);
  RedisModule_RetainString(NULL, value);
}

static void RS_DocumentAddFieldString(Document* d, const char* fieldname, const char* s, size_t n,
                                      unsigned as) {
  RedisModuleString* r = RedisModule_CreateString(NULL, s, n);
  Document_AddField(d, fieldname, r, as);
}

static void RS_DocumentAddFieldNumber(Document* d, const char* fieldname, double n, unsigned as) {
  RedisModuleString* r = RedisModule_CreateStringPrintf(NULL, "%lf", n);
  Document_AddField(d, fieldname, r, as);
}

static void RS_AddDocDone(RSAddDocumentCtx* aCtx, RedisModuleCtx* ctx, void* unused) {
}

static void RS_SpecAddDocument(IndexSpec* sp, Document* d) {
  uint32_t options = 0;
  QueryError status = {0};
  RSAddDocumentCtx* aCtx = NewAddDocumentCtx(sp, d, &status);
  aCtx->donecb = RS_AddDocDone;
  RedisSearchCtx sctx = {.redisCtx = NULL, .spec = sp};
  int exists = !!DocTable_GetIdR(&sp->docs, d->docKey);
  if (exists) {
    options |= DOCUMENT_ADD_REPLACE;
  }
  options |= DOCUMENT_ADD_NOSAVE;
  aCtx->stateFlags |= ACTX_F_NOBLOCK;
  AddDocumentCtx_Submit(aCtx, &sctx, options);
  rm_free(d);
}

static QueryNode* RS_CreateTokenNode(IndexSpec* sp, const char* fieldName, const char* token) {
  QueryNode* ret = NewQueryNode(QN_TOKEN);

  ret->tn = (QueryTokenNode){
      .str = (char*)strdup(token), .len = strlen(token), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

static QueryNode* RS_CreateNumericNode(IndexSpec* sp, const char* field, double max, double min,
                                       int includeMax, int includeMin) {
  QueryNode* ret = NewQueryNode(QN_NUMERIC);
  ret->nn.nf = NewNumericFilter(min, max, includeMin, includeMax);
  ret->nn.nf->fieldName = strdup(field);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, field, strlen(field));
  return ret;
}

static QueryNode* RS_CreatePrefixNode(IndexSpec* sp, const char* fieldName, const char* s) {
  QueryNode* ret = NewQueryNode(QN_PREFX);
  ret->pfx =
      (QueryPrefixNode){.str = (char*)strdup(s), .len = strlen(s), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

static QueryNode* RS_CreateLexRangeNode(IndexSpec* sp, const char* fieldName, const char* begin,
                                        const char* end) {
  QueryNode* ret = NewQueryNode(QN_LEXRANGE);
  if (begin) {
    ret->lxrng.begin = begin;
  }
  if (end) {
    ret->lxrng.end = end;
  }
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

static QueryNode* RS_CreateTagNode(IndexSpec* sp, const char* field) {
  QueryNode* ret = NewQueryNode(QN_TAG);
  ret->tag.fieldName = strdup(field);
  ret->tag.len = strlen(field);
  ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, field, strlen(field));
  return ret;
}

static QueryNode* RS_CreateIntersectNode(IndexSpec* sp, int exact) {
  QueryNode* ret = NewQueryNode(QN_PHRASE);
  ret->pn.exact = exact;
  return ret;
}

static QueryNode* RS_CreateUnionNode(IndexSpec* sp) {
  return NewQueryNode(QN_UNION);
}

static int RS_QueryNodeGetFieldMask(QueryNode* qn) {
  return qn->opts.fieldMask;
}

#define RS_QueryNodeAddChild QueryNode_AddChild

static void RS_QueryNodeClearChildren(QueryNode* qn) {
  QueryNode_ClearChildren(qn, 1);
}

static QueryNode* RS_QueryNodeGetChild(QueryNode* qn, size_t ix) {
  return QueryNode_GetChild(qn, ix);
}

static size_t RS_QueryNodeNumChildren(const QueryNode* qn) {
  return QueryNode_NumChildren(qn);
}

typedef struct RS_ApiIter {
  IndexIterator* internal;
  QueryAST qast;  // Used for string queries..
} RS_ApiIter;

static RS_ApiIter* RS_IterateQuery(IndexSpec* sp, const char* s, size_t n, char** error) {
  RS_ApiIter* ret = rm_calloc(1, sizeof(*ret));
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(NULL, sp);
  RSSearchOptions options = {0};
  QueryError status = {0};
  RSSearchOptions_Init(&options);

  if (QAST_Parse(&ret->qast, &sctx, &options, s, n, &status) != REDISMODULE_OK) {
    goto end;
  }

  if (QAST_Expand(&ret->qast, NULL, &options, &sctx, &status) != REDISMODULE_OK) {
    goto end;
  }

  ret->internal = QAST_Iterate(&ret->qast, &options, &sctx, NULL, &status);
  if (!ret->internal) {
    goto end;
  }

  // dummy statement for goto
  ;
end:
  if (QueryError_HasError(&status)) {
    if (ret) {
      RS_ResultsIteratorFree(ret);
      ret = NULL;
    }
    if (error) {
      *error = rm_strdup(QueryError_GetError(&status));
    }
  }
  QueryError_ClearError(&status);
  return ret;
}

static RS_ApiIter* RS_GetResultsIterator(QueryNode* qn, IndexSpec* sp) {
  RS_ApiIter* ret = rm_calloc(1, sizeof(*ret));
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(NULL, sp);
  RSSearchOptions searchOpts = {0};
  RSSearchOptions_Init(&searchOpts);

  QueryAST ast = {.root = qn};

  QueryError status = {0};
  QAST_Expand(&ast, NULL, &searchOpts, &sctx, &status);

  QueryEvalCtx qectx = {
      .conc = NULL,
      .opts = &searchOpts,
      .numTokens = 0,
      .docTable = &sp->docs,
      .sctx = &sctx,
  };
  ret->internal = Query_EvalNode(&qectx, ast.root);
  if (!ret->internal) {
    rm_free(ret);
    ret = NULL;
  }
  QueryNode_Free(qn);
  return ret;
}

static void RS_QueryNodeFree(QueryNode* qn) {
  QueryNode_Free(qn);
}

static int RS_QueryNodeType(QueryNode* qn) {
  return qn->type;
}

static const void* RS_ResultsIteratorNext(RS_ApiIter* iter, IndexSpec* sp, size_t* len) {
  RSIndexResult* e = NULL;
  while (iter->internal->Read(iter->internal->ctx, &e) != INDEXREAD_EOF) {
    const char* docId = DocTable_GetKey(&sp->docs, e->docId, len);
    if (docId) {
      return docId;
    }
  }
  return NULL;
}

static void RS_ResultsIteratorFree(RS_ApiIter* iter) {
  if (iter->internal) {
    iter->internal->Free(iter->internal);
  }
  QAST_Destroy(&iter->qast);
  rm_free(iter);
}

static void RS_ResultsIteratorReset(RS_ApiIter* iter) {
  iter->internal->Rewind(iter->internal->ctx);
}

static RSIndexOptions* RS_CreateIndexOptions() {
  return rm_calloc(1, sizeof(RSIndexOptions));
}
static void RS_FreeIndexOptions(RSIndexOptions* options) {
  rm_free(options);
}

static void RS_IndexOptionsSetGetValueCallback(RSIndexOptions* options, RSGetValueCallback cb,
                                               void* ctx) {
  options->gvcb = cb;
  options->gvcbData = ctx;
}

static void RS_IndexOptionsSetFlags(RSIndexOptions* options, uint32_t flags) {
  options->flags = flags;
}

#define REGISTER_API(name)                                                   \
  if (moduleRegisterApi("RediSearch_" #name, RS_##name) != REDISMODULE_OK) { \
    printf("could not register RediSearch_" #name "\r\n");                   \
    return REDISMODULE_ERR;                                                  \
  }

int moduleRegisterApi(const char* funcname, void* funcptr);

int RS_InitializeLibrary(RedisModuleCtx* ctx) {
  RS_XAPIFUNC(REGISTER_API)
  return REDISMODULE_OK;
}
