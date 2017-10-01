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
  } v;
  AttributeType t;
} Attribute;

typedef struct {
  const char **fields;
  size_t numFields;
  TrieMap *values;
} AttributeIndex;

AttributeIndex *NewAttributeIndex(const char *namespace, const char *fieldName);
int AttributeIndex_Index(DocumentField *f);
const char *AttributeIndex_Encode(Attribute *attrs, size_t num);
const char *AttributeIndex_EncodeSingle(Attribute *attr);

#endif  // !RS_ATTRIBUTE_INDEX_H_