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

int RS_GetLowLevelApiVersion() {
  return REDISEARCH_LOW_LEVEL_API_VERSION;
}

IndexSpec* RS_CreateSpec(const char* name, GetValueCallback getValue, void* getValueCtx) {
  IndexSpec* spec = NewIndexSpec(name);
  spec->flags |= Index_Temporary;  // temporary is so that we will not use threads!!
  spec->keysDict = dictCreate(&dictTypeHeapRedisStrings, NULL);
  spec->minPrefix = 0;
  spec->maxPrefixExpansions = -1;
  spec->getValue = getValue;
  spec->getValueCtx = getValueCtx;
  return spec;
}

static inline FieldSpec* RS_CreateField(IndexSpec* sp, const char* name) {
  FieldSpec* fs = IndexSpec_CreateField(sp);
  FieldSpec_SetName(fs, name);
  return fs;
}

FieldSpec* RS_CreateTextField(IndexSpec* sp, const char* name) {
  FieldSpec* fs = RS_CreateField(sp, name);
  FieldSpec_InitializeText(fs);
  return fs;
}

void RS_TextFieldSetWeight(FieldSpec* fs, double w) {
  assert(fs->type == FIELD_FULLTEXT);
  FieldSpec_TextSetWeight(fs, w);
}

void RS_TextFieldNoStemming(FieldSpec* fs) {
  assert(fs->type == FIELD_FULLTEXT);
  FieldSpec_TextNoStem(fs);
}

void RS_TextFieldPhonetic(FieldSpec* fs, IndexSpec* sp) {
  assert(fs->type == FIELD_FULLTEXT);
  FieldSpec_TextPhonetic(fs);
  sp->flags |= Index_HasPhonetic;
}

FieldSpec* RS_CreateGeoField(IndexSpec* sp, const char* name) {
  FieldSpec* fs = RS_CreateField(sp, name);
  FieldSpec_InitializeGeo(fs);
  return fs;
}

FieldSpec* RS_CreateNumericField(IndexSpec* sp, const char* name) {
  FieldSpec* fs = RS_CreateField(sp, name);
  FieldSpec_InitializeNumeric(fs);
  return fs;
}

FieldSpec* RS_CreateTagField(IndexSpec* sp, const char* name) {
  FieldSpec* fs = RS_CreateField(sp, name);
  FieldSpec_InitializeTag(fs);
  return fs;
}

void RS_TagSetSeparator(FieldSpec* fs, char sep) {
  assert(fs->type == FIELD_TAG);
  FieldSpec_TagSetSeparator(fs, sep);
}

void RS_FieldSetSortable(FieldSpec* fs, IndexSpec* sp) {
  FieldSpec_SetSortable(fs);
  fs->sortIdx = RSSortingTable_Add(sp->sortables, fs->name, fieldTypeToValueType(fs->type));
}

void RS_FieldSetNoIndex(FieldSpec* fs) {
  FieldSpec_SetNoIndex(fs);
}

Document* RS_CreateDocument(const void* docKey, size_t len, double score, const char* lang) {
  return Document_Create(docKey, len, score, lang);
}

int RS_DropDocument(IndexSpec* sp, const void* docKey, size_t len) {
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

void RS_DocumentAddTextField(Document* d, const char* fieldName, const char* val) {
  Document_AddTextField(d, fieldName, val);
}

void RS_DocumentAddNumericField(Document* d, const char* fieldName, double num) {
  Document_AddNumericField(d, fieldName, num);
}

static void RS_AddDocDone(RSAddDocumentCtx* aCtx, RedisModuleCtx* ctx, void* unused) {
}

void RS_SpecAddDocument(IndexSpec* sp, Document* d) {
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

QueryNode* RS_CreateTokenNode(IndexSpec* sp, const char* fieldName, const char* token) {
  QueryNode* ret = NewQueryNode(QN_TOKEN);

  ret->tn = (QueryTokenNode){
      .str = (char*)strdup(token), .len = strlen(token), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RS_CreateNumericNode(IndexSpec* sp, const char* field, double max, double min,
                                int includeMax, int includeMin) {
  QueryNode* ret = NewQueryNode(QN_NUMERIC);
  ret->nn.nf = NewNumericFilter(max, min, includeMax, includeMin);
  ret->nn.nf->fieldName = strdup(field);
  return ret;
}

QueryNode* RS_CreatePrefixNode(IndexSpec* sp, const char* fieldName, const char* s) {
  QueryNode* ret = NewQueryNode(QN_PREFX);
  ret->pfx =
      (QueryPrefixNode){.str = (char*)strdup(s), .len = strlen(s), .expanded = 0, .flags = 0};
  if (fieldName) {
    ret->opts.fieldMask = IndexSpec_GetFieldBit(sp, fieldName, strlen(fieldName));
  }
  return ret;
}

QueryNode* RS_CreateTagNode(IndexSpec* sp, const char* field) {
  QueryNode* ret = NewQueryNode(QN_TAG);
  ret->tag.fieldName = strdup(field);
  ret->tag.len = strlen(field);
  ret->tag.numChildren = 0;
  ret->tag.children = NULL;
  return ret;
}

void RS_TagNodeAddChild(QueryNode* qn, QueryNode* child) {
  QueryTagNode_AddChildren(qn, &child, 1);
}

QueryNode* RS_CreateIntersectNode(IndexSpec* sp, int exact) {
  QueryNode* ret = NewQueryNode(QN_PHRASE);
  ret->pn = (QueryPhraseNode){.children = NULL, .numChildren = 0, .exact = exact};
  return ret;
}

void RS_IntersectNodeAddChild(QueryNode* qn, QueryNode* child) {
  QueryPhraseNode_AddChild(qn, child);
}

QueryNode* RS_CreateUnionNode(IndexSpec* sp) {
  QueryNode* ret = NewQueryNode(QN_UNION);
  ret->un = (QueryUnionNode){.children = NULL, .numChildren = 0};
  return ret;
}

void RS_UnionNodeAddChild(QueryNode* qn, QueryNode* child) {
  QueryUnionNode_AddChild(qn, child);
}

IndexIterator* RS_GetResutlsIterator(QueryNode* qn, IndexSpec* sp) {
  RedisSearchCtx sctx = {.redisCtx = NULL, .spec = sp};
  RSSearchOptions searchOpts;
  searchOpts.fieldmask = RS_FIELDMASK_ALL;
  searchOpts.slop = -1;
  QueryEvalCtx qectx = {
      .conc = NULL,
      .opts = &searchOpts,
      .numTokens = 0,
      .docTable = &sp->docs,
      .sctx = &sctx,
  };
  IndexIterator* ret = Query_EvalNode(&qectx, qn);
  QueryNode_Free(qn);
  return ret;
}

const void* RS_ResutlsIteratorNext(IndexIterator* iter, IndexSpec* sp, size_t* len) {
  RSIndexResult* e = NULL;
  while (iter->Read(iter->ctx, &e) != INDEXREAD_EOF) {
    const char* docId = DocTable_GetKey(&sp->docs, e->docId, len);
    if (docId) {
      return docId;
    }
  }
  return NULL;
}

void RS_ResutlsIteratorFree(IndexIterator* iter) {
  iter->Free(iter);
}

#define REGISTER_API(name, registerApiCallback)                                \
  if (registerApiCallback("RediSearch_" #name, RS_##name) != REDISMODULE_OK) { \
    printf("could not register RediSearch_" #name "\r\n");                     \
    return REDISMODULE_ERR;                                                    \
  }

int moduleRegisterApi(const char* funcname, void* funcptr);

int RS_InitializeLibrary(RedisModuleCtx* ctx) {
  REGISTER_API(GetLowLevelApiVersion, moduleRegisterApi);

  REGISTER_API(CreateSpec, moduleRegisterApi);
  REGISTER_API(CreateTextField, moduleRegisterApi);
  REGISTER_API(TextFieldSetWeight, moduleRegisterApi);
  REGISTER_API(TextFieldNoStemming, moduleRegisterApi);
  REGISTER_API(TextFieldPhonetic, moduleRegisterApi);
  REGISTER_API(CreateGeoField, moduleRegisterApi);
  REGISTER_API(CreateNumericField, moduleRegisterApi);
  REGISTER_API(CreateTagField, moduleRegisterApi);
  REGISTER_API(TagSetSeparator, moduleRegisterApi);
  REGISTER_API(FieldSetSortable, moduleRegisterApi);
  REGISTER_API(FieldSetNoIndex, moduleRegisterApi);

  REGISTER_API(CreateDocument, moduleRegisterApi);
  REGISTER_API(DocumentAddTextField, moduleRegisterApi);
  REGISTER_API(DocumentAddNumericField, moduleRegisterApi);

  REGISTER_API(SpecAddDocument, moduleRegisterApi);

  REGISTER_API(CreateTokenNode, moduleRegisterApi);
  REGISTER_API(CreateNumericNode, moduleRegisterApi);
  REGISTER_API(CreatePrefixNode, moduleRegisterApi);
  REGISTER_API(CreateTagNode, moduleRegisterApi);
  REGISTER_API(TagNodeAddChild, moduleRegisterApi);
  REGISTER_API(CreateIntersectNode, moduleRegisterApi);
  REGISTER_API(IntersectNodeAddChild, moduleRegisterApi);
  REGISTER_API(CreateUnionNode, moduleRegisterApi);
  REGISTER_API(UnionNodeAddChild, moduleRegisterApi);

  REGISTER_API(GetResutlsIterator, moduleRegisterApi);
  REGISTER_API(ResutlsIteratorNext, moduleRegisterApi);
  REGISTER_API(ResutlsIteratorFree, moduleRegisterApi);

  return REDISMODULE_OK;
}
