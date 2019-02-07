#ifndef SRC_REDISEARCH_API_H_
#define SRC_REDISEARCH_API_H_

#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif

#define REDISEARCH_LOW_LEVEL_API_VERSION 1

#define MODULE_API_FUNC(T, N) extern T(*N)

typedef struct IndexSpec Index;
typedef struct FieldSpec Field;
typedef struct Document Doc;
typedef struct RSQueryNode QN;
typedef struct indexIterator ResultsIterator;

#define VALUE_NOT_FOUND 0
#define STR_VALUE_TYPE 1
#define DOUBLE_VALUE_TYPE 2
typedef int (*GetValueCallback)(void* ctx, const char* fieldName, const void* id, char** strVal,
                                double* doubleVal);

MODULE_API_FUNC(int, RediSearch_GetLowLevelApiVersion)();

MODULE_API_FUNC(Index*, RediSearch_CreateSpec)
(const char* name, GetValueCallback getValue, void* getValueCtx);

MODULE_API_FUNC(Field*, RediSearch_CreateTextField)(Index* sp, const char* name);

MODULE_API_FUNC(void, RediSearch_TextFieldSetWeight)(Field* fs, double w);

MODULE_API_FUNC(void, RediSearch_TextFieldNoStemming)(Field* fs);

MODULE_API_FUNC(void, RediSearch_TextFieldPhonetic)(Field* fs, Index* sp);

MODULE_API_FUNC(Field*, RediSearch_CreateGeoField)(Index* sp, const char* name);

MODULE_API_FUNC(Field*, RediSearch_CreateNumericField)(Index* sp, const char* name);

MODULE_API_FUNC(Field*, RediSearch_CreateTagField)(Index* sp, const char* name);

MODULE_API_FUNC(void, RediSearch_TagSetSeparator)(Field* fs, char sep);

MODULE_API_FUNC(void, RediSearch_FieldSetSortable)(Field* fs, Index* sp);

MODULE_API_FUNC(void, RediSearch_FieldSetNoIndex)(Field* fs);

MODULE_API_FUNC(Doc*, RediSearch_CreateDocument)
(const void* docKey, size_t len, double score, const char* lang);

MODULE_API_FUNC(int, RediSearch_DropDocument)(Index* sp, const void* docKey, size_t len);

MODULE_API_FUNC(void, RediSearch_DocumentAddTextField)
(Doc* d, const char* fieldName, const char* val);

MODULE_API_FUNC(void, RediSearch_DocumentAddNumericField)
(Doc* d, const char* fieldName, double num);

MODULE_API_FUNC(void, RediSearch_SpecAddDocument)(Index* sp, Doc* d);

MODULE_API_FUNC(QN*, RediSearch_CreateTokenNode)
(Index* sp, const char* fieldName, const char* token);

MODULE_API_FUNC(QN*, RediSearch_CreateNumericNode)
(Index* sp, const char* field, double max, double min, int includeMax, int includeMin);

MODULE_API_FUNC(QN*, RediSearch_CreatePrefixNode)(Index* sp, const char* fieldName, const char* s);

MODULE_API_FUNC(QN*, RediSearch_CreateLexRangeNode)
(Index* sp, const char* fieldName, const char* begin, const char* end);

MODULE_API_FUNC(QN*, RediSearch_CreateTagNode)(Index* sp, const char* field);

MODULE_API_FUNC(void, RediSearch_TagNodeAddChild)(QN* qn, QN* child);

MODULE_API_FUNC(QN*, RediSearch_CreateIntersectNode)(Index* sp, int exact);

MODULE_API_FUNC(void, RediSearch_IntersectNodeAddChild)(QN* qn, QN* child);

MODULE_API_FUNC(QN*, RediSearch_CreateUnionNode)(Index* sp);

MODULE_API_FUNC(void, RediSearch_UnionNodeAddChild)(QN* qn, QN* child);

MODULE_API_FUNC(ResultsIterator*, RediSearch_GetResutlsIterator)(QN* qn, Index* sp);

const MODULE_API_FUNC(void*, RediSearch_ResutlsIteratorNext)(ResultsIterator* iter, Index* sp,
                                                             size_t* len);

MODULE_API_FUNC(void, RediSearch_ResutlsIteratorFree)(ResultsIterator* iter);

MODULE_API_FUNC(void, RediSearch_ResutlsIteratorReset)(ResultsIterator* iter);

#define REDISEARCH_MODULE_INIT_FUNCTION(name)                                  \
  if (RedisModule_GetApi("RediSearch_" #name, ((void**)&RediSearch_##name))) { \
    printf("could not initialize RediSearch_" #name "\r\n");                   \
    return REDISMODULE_ERR;                                                    \
  }

#define RS_XAPIFUNC(X)       \
  X(GetLowLevelApiVersion)   \
  X(CreateSpec)              \
  X(CreateTextField)         \
  X(TextFieldSetWeight)      \
  X(TextFieldNoStemming)     \
  X(TextFieldPhonetic)       \
  X(CreateGeoField)          \
  X(CreateNumericField)      \
  X(CreateTagField)          \
  X(TagSetSeparator)         \
  X(FieldSetSortable)        \
  X(FieldSetNoIndex)         \
  X(CreateDocument)          \
  X(DropDocument)            \
  X(DocumentAddTextField)    \
  X(DocumentAddNumericField) \
  X(SpecAddDocument)         \
  X(CreateTokenNode)         \
  X(CreateNumericNode)       \
  X(CreatePrefixNode)        \
  X(CreateLexRangeNode)      \
  X(CreateTagNode)           \
  X(TagNodeAddChild)         \
  X(CreateIntersectNode)     \
  X(IntersectNodeAddChild)   \
  X(CreateUnionNode)         \
  X(UnionNodeAddChild)       \
  X(GetResutlsIterator)      \
  X(ResutlsIteratorNext)     \
  X(ResutlsIteratorFree)     \
  X(ResutlsIteratorReset)

static bool RediSearch_Initialize() {
  RS_XAPIFUNC(REDISEARCH_MODULE_INIT_FUNCTION);
  if (RediSearch_GetLowLevelApiVersion() > REDISEARCH_LOW_LEVEL_API_VERSION) {
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

#define REDISEARCH__API_INIT_NULL(s) __typeof__(RediSearch_##s) RediSearch_##s = NULL;
#define REDISEARCH_API_INIT_SYMBOLS() RS_XAPIFUNC(REDISEARCH__API_INIT_NULL)

#ifdef __cplusplus
}
#endif
#endif /* SRC_REDISEARCH_API_H_ */
