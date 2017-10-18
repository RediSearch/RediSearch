#ifndef RS_ATTRIBUTE_INDEX_H_
#define RS_ATTRIBUTE_INDEX_H_

#include "redismodule.h"
#include "doc_table.h"
#include "document.h"
#include "value.h"
#include "geo_index.h"

typedef enum {
  AttributeTokenizer_CaseSensitive = 0x01,
  AttributeTokenizer_TrimSpace = 0x02,
  AttributeTokenizer_RemoveAccents = 0x04,
} AttributeTokenizerFlags;

typedef struct {
  const char *separators;
  AttributeTokenizerFlags flags;
} AttributeTokenizeCtx;

typedef struct {
  const char *namespace;
  const char **fields;
  size_t numFields;
  TrieMap *values;
  AttributeTokenizeCtx tokCtx;
} AttributeIndex;

AttributeIndex *NewAttributeIndex(const char *namespace, const char *fieldName);
const char *AttributeIndex_EncodeSingle(RSValue *attr, size_t *enclen);
Vector *AttributeIndex_Preprocess(AttributeIndex *idx, DocumentField *data);
size_t AttributeIndex_Index(AttributeIndex *idx, Vector *values, t_docId docId);
IndexIterator *AttributeIndex_OpenReader(AttributeIndex *idx, DocTable *dt, const char *value,
                                         size_t len);

#define ATTRIDX_CURRENT_VERSION 1
extern RedisModuleType *AttributeIndexType;
int IndexSpec_RegisterType(RedisModuleCtx *ctx);
#endif