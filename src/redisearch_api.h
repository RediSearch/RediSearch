#ifndef SRC_REDISEARCH_API_H_
#define SRC_REDISEARCH_API_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

#define REDISEARCH_CAPI_VERSION 1

#define MODULE_API_FUNC(T, N) extern T(*N)

typedef struct IndexSpec RSIndex;
typedef struct FieldSpec RSField;
typedef struct Document RSDoc;
typedef struct RSQueryNode RSQNode;
typedef struct indexIterator RSResultsIterator;

#define RSVALTYPE_NOTFOUND 0
#define RSVALTYPE_STRING 1
#define RSVALTYPE_DOUBLE 2

#define RSRANGE_INF (1.0 / 0.0)
#define RSRANGE_NEG_INF (-1.0 / 0.0)

#define RSLECRANGE_INF NULL
#define RSLEXRANGE_NEG_INF NULL

#define RSQNTYPE_INTERSECT 1
#define RSQNTYPE_UNION 2
#define RSQNTYPE_TOKEN 3
#define RSQNTYPE_NUMERIC 4
#define RSQNTYPE_NOT 5
#define RSQNTYPE_OPTIONAL 6
#define RSQNTYPE_GEO 7
#define RSQNTYPE_PREFX 8
#define RSQNTYPE_TAG 11
#define RSQNTYPE_FUZZY 12
#define RSQNTYPE_LEXRANGE 13

typedef int (*RSGetValueCallback)(void* ctx, const char* fieldName, const void* id, char** strVal,
                                  double* doubleVal);

MODULE_API_FUNC(int, RediSearch_GetCApiVersion)();

MODULE_API_FUNC(RSIndex*, RediSearch_CreateIndex)
(const char* name, RSGetValueCallback getValue, void* getValueCtx);

MODULE_API_FUNC(void, RediSearch_DropIndex)(RSIndex*);

MODULE_API_FUNC(RSField*, RediSearch_CreateTextField)(RSIndex* sp, const char* name);

MODULE_API_FUNC(void, RediSearch_TextFieldSetWeight)(RSField* fs, double w);

MODULE_API_FUNC(void, RediSearch_TextFieldNoStemming)(RSField* fs);

MODULE_API_FUNC(void, RediSearch_TextFieldPhonetic)(RSField* fs, RSIndex* sp);

MODULE_API_FUNC(RSField*, RediSearch_CreateGeoField)(RSIndex* sp, const char* name);

MODULE_API_FUNC(RSField*, RediSearch_CreateNumericField)(RSIndex* sp, const char* name);

MODULE_API_FUNC(RSField*, RediSearch_CreateTagField)(RSIndex* sp, const char* name);

MODULE_API_FUNC(void, RediSearch_TagSetSeparator)(RSField* fs, char sep);

MODULE_API_FUNC(void, RediSearch_FieldSetSortable)(RSField* fs, RSIndex* sp);

MODULE_API_FUNC(void, RediSearch_FieldSetNoIndex)(RSField* fs);

MODULE_API_FUNC(RSDoc*, RediSearch_CreateDocument)
(const void* docKey, size_t len, double score, const char* lang);

MODULE_API_FUNC(int, RediSearch_DropDocument)(RSIndex* sp, const void* docKey, size_t len);

MODULE_API_FUNC(void, RediSearch_DocumentAddTextField)
(RSDoc* d, const char* fieldName, const char* val, size_t n);

#define RediSearch_DocumentAddTextFieldC(d, f, v) \
  RediSearch_DocumentAddTextField(d, f, v, strlen(v))

MODULE_API_FUNC(void, RediSearch_DocumentAddNumericField)
(RSDoc* d, const char* fieldName, double num);

MODULE_API_FUNC(void, RediSearch_SpecAddDocument)(RSIndex* sp, RSDoc* d);

MODULE_API_FUNC(RSQNode*, RediSearch_CreateTokenNode)
(RSIndex* sp, const char* fieldName, const char* token);

MODULE_API_FUNC(RSQNode*, RediSearch_CreateNumericNode)
(RSIndex* sp, const char* field, double max, double min, int includeMax, int includeMin);

MODULE_API_FUNC(RSQNode*, RediSearch_CreatePrefixNode)
(RSIndex* sp, const char* fieldName, const char* s);

MODULE_API_FUNC(RSQNode*, RediSearch_CreateLexRangeNode)
(RSIndex* sp, const char* fieldName, const char* begin, const char* end);

MODULE_API_FUNC(RSQNode*, RediSearch_CreateTagNode)(RSIndex* sp, const char* field);

MODULE_API_FUNC(void, RediSearch_TagNodeAddChild)(RSQNode* qn, RSQNode* child);

MODULE_API_FUNC(RSQNode*, RediSearch_CreateIntersectNode)(RSIndex* sp, int exact);

MODULE_API_FUNC(void, RediSearch_IntersectNodeAddChild)(RSQNode* qn, RSQNode* child);

MODULE_API_FUNC(RSQNode*, RediSearch_CreateUnionNode)(RSIndex* sp);

MODULE_API_FUNC(void, RediSearch_UnionNodeAddChild)(RSQNode* qn, RSQNode* child);

MODULE_API_FUNC(void, RediSearch_QueryNodeFree)(RSQNode* qn);

MODULE_API_FUNC(void, RediSearch_UnionNodeClearChildren)(RSQNode* qn);

MODULE_API_FUNC(void, RediSearch_IntersectNodeClearChildren)(RSQNode* qn);

MODULE_API_FUNC(int, RediSearch_QueryNodeType)(RSQNode* qn);

MODULE_API_FUNC(size_t, RediSearch_UnionNodeGetNumChildren)(RSQNode* qn);

MODULE_API_FUNC(RSQNode*, RediSearch_UnionNodeGetChild)(RSQNode* qn, size_t index);

MODULE_API_FUNC(size_t, RediSearch_IntersectNodeGetNumChildren)(RSQNode* qn);

MODULE_API_FUNC(RSQNode*, RediSearch_IntersectNodeGetChild)(RSQNode* qn, size_t index);

MODULE_API_FUNC(int, RediSearch_QueryNodeGetFieldMask)(RSQNode* qn);

MODULE_API_FUNC(RSResultsIterator*, RediSearch_GetResultsIterator)(RSQNode* qn, RSIndex* sp);

const MODULE_API_FUNC(void*, RediSearch_ResultsIteratorNext)(RSResultsIterator* iter, RSIndex* sp,
                                                             size_t* len);

MODULE_API_FUNC(void, RediSearch_ResultsIteratorFree)(RSResultsIterator* iter);

MODULE_API_FUNC(void, RediSearch_ResultsIteratorReset)(RSResultsIterator* iter);

#define RS_XAPIFUNC(X)           \
  X(GetCApiVersion)              \
  X(CreateIndex)                 \
  X(DropIndex)                   \
  X(CreateTextField)             \
  X(TextFieldSetWeight)          \
  X(TextFieldNoStemming)         \
  X(TextFieldPhonetic)           \
  X(CreateGeoField)              \
  X(CreateNumericField)          \
  X(CreateTagField)              \
  X(TagSetSeparator)             \
  X(FieldSetSortable)            \
  X(FieldSetNoIndex)             \
  X(CreateDocument)              \
  X(DropDocument)                \
  X(DocumentAddTextField)        \
  X(DocumentAddNumericField)     \
  X(SpecAddDocument)             \
  X(CreateTokenNode)             \
  X(CreateNumericNode)           \
  X(CreatePrefixNode)            \
  X(CreateLexRangeNode)          \
  X(CreateTagNode)               \
  X(TagNodeAddChild)             \
  X(CreateIntersectNode)         \
  X(IntersectNodeAddChild)       \
  X(CreateUnionNode)             \
  X(UnionNodeAddChild)           \
  X(QueryNodeFree)               \
  X(UnionNodeClearChildren)      \
  X(IntersectNodeClearChildren)  \
  X(QueryNodeType)               \
  X(UnionNodeGetNumChildren)     \
  X(UnionNodeGetChild)           \
  X(IntersectNodeGetNumChildren) \
  X(IntersectNodeGetChild)       \
  X(QueryNodeGetFieldMask)       \
  X(GetResultsIterator)          \
  X(ResultsIteratorNext)         \
  X(ResultsIteratorFree)         \
  X(ResultsIteratorReset)

#define REDISEARCH_MODULE_INIT_FUNCTION(name)                                  \
  if (RedisModule_GetApi("RediSearch_" #name, ((void**)&RediSearch_##name))) { \
    printf("could not initialize RediSearch_" #name "\r\n");                   \
    rv__ = REDISMODULE_ERR;                                                    \
    goto rsfunc_init_end__;                                                    \
  }

/**
 * This is implemented as a macro rather than a function so that the inclusion of this
 * header file does not automatically require the symbols to be defined above.
 *
 * We are making use of special GCC statement-expressions `({...})`. This is also
 * supported by clang
 */
#define RediSearch_Initialize()                                  \
  ({                                                             \
    int rv__ = REDISMODULE_OK;                                   \
    RS_XAPIFUNC(REDISEARCH_MODULE_INIT_FUNCTION);                \
    if (RediSearch_GetCApiVersion() > REDISEARCH_CAPI_VERSION) { \
      rv__ = REDISMODULE_ERR;                                    \
    }                                                            \
  rsfunc_init_end__:;                                            \
    rv__;                                                        \
  })

#define REDISEARCH__API_INIT_NULL(s) __typeof__(RediSearch_##s) RediSearch_##s = NULL;
#define REDISEARCH_API_INIT_SYMBOLS() RS_XAPIFUNC(REDISEARCH__API_INIT_NULL)

#ifdef __cplusplus
}
#endif
#endif /* SRC_REDISEARCH_API_H_ */
