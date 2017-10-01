#ifndef RS_ATTRIBUTE_INDEX_H_
#define RS_ATTRIBUTE_INDEX_H_

#include "redismodule.h"
#include "doc_table.h"
#include "document.h"
#include "geo_index.h"

typedef enum {
  AttributeType_String,
  AttributeType_Number,
  AttributeType_Geopoint,
} AttributeType;

typedef struct {
  union {
    const char *strval;
    double numval;
    struct {
      float lon;
      float lat;
    } geoval;
  };
  AttributeType t;
} Attribute;

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
  const char **fields;
  size_t numFields;
  TrieMap *values;
  AttributeTokenizeCtx tokCtx;
} AttributeIndex;

AttributeIndex *NewAttributeIndex(const char *namespace, const char *fieldName);
const char *AttributeIndex_EncodeSingle(Attribute *attr, size_t *sz);
Vector *AttributeIndex_Preprocess(AttributeIndex *idx, DocumentField *data);
int AttributeIndex_IndexString(AttributeIndex *idx, Vector *values);

IndexIterator *AttributeIndex_OpenReader(AttributeIndex *idx, DocTable *dt, const char *value,
                                         size_t len);

#endif