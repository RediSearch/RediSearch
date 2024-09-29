#include "obfuscation_api.h"
#include "rmalloc.h"
#include "query_node.h"

#include <string.h>

void Obfuscate_Index(t_uniqueId indexId, char* buffer) {
  sprintf(buffer, "Index@%zu", indexId);
}

void Obfuscate_Field(t_uniqueId fieldId, char* buffer) {
  sprintf(buffer, "Field@%zu", fieldId);
}

void Obfuscate_Document(t_uniqueId docId, char* buffer) {
  sprintf(buffer, "Document@%zu", docId);
}

char *Obfuscate_Text(const char* text) {
  return "Text";
}

char *Obfuscate_Number(size_t number) {
  return "Number";
}

char *Obfuscate_Vector(const char* vector, size_t dim) {
  return "Vector";
}

char *Obfuscate_Tag(const char* tag) {
  return "Tag";
}

char *Obfuscate_Geo(uint16_t longitude, uint16_t latitude) {
  return "Geo";
}

char *Obfuscate_GeoShape() {
  return "GeoShape";
}

char *Obfuscate_QueryNode(struct QueryNode *node) {
  switch (node->type) {
    case QN_PHRASE:
      return "Phrase";
    case QN_UNION:
      return "Union";
    case QN_TOKEN:
      return "Token";
    case QN_NUMERIC:
      return "Numeric";
    case QN_NOT:
      return "Not";
    case QN_OPTIONAL:
      return "Optional";
    case QN_GEO:
      return "Geo";
    case QN_GEOMETRY:
      return "Geometry";
    case QN_PREFIX:
      return "Prefix";
    case QN_IDS:
      return "Ids";
    case QN_WILDCARD:
      return "Wildcard";
    case QN_TAG:
      return "Tag";
    case QN_FUZZY:
      return "Fuzzy";
    case QN_LEXRANGE:
      return "LexRange";
    case QN_VECTOR:
      return "Vector";
    case QN_NULL:
      return "Null";
    case QN_MISSING:
      return "Missing";
    case QN_WILDCARD_QUERY:
      return "WildcardQuery";
  }
}