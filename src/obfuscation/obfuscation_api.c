/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "obfuscation_api.h"
#include "rmalloc.h"

#include "query_node.h"

#include <string.h>

void Obfuscate_Index(const Sha1 *hash, char* buffer) {
  const char prefix[] = "Index@";
  strcpy(buffer, prefix);
  Sha1_FormatIntoBuffer(hash, buffer + strlen(prefix));
}

void Obfuscate_Field(t_uniqueId fieldId, char* buffer) {
  sprintf(buffer, "Field@%zu", fieldId);
}

void Obfuscate_FieldPath(t_uniqueId fieldId, char* buffer) {
  sprintf(buffer, "FieldPath@%zu", fieldId);
}

void Obfuscate_Document(t_uniqueId docId, char* buffer) {
  sprintf(buffer, "Document@%zu", docId);
}

void Obfuscate_KeyWithTime(struct timespec spec, char* buffer) {
  const size_t epoch = spec.tv_sec * 1000 + spec.tv_nsec / 1000000;
  sprintf(buffer, "Key@%zu", epoch);
}

const char *Obfuscate_Text(const char* text) {
  return "Text";
}

const char *Obfuscate_Number(double number) {
  return "Number";
}

const char *Obfuscate_Vector(const char* vector, size_t dim) {
  return "Vector";
}

const char *Obfuscate_Tag(const char* tag) {
  return "Tag";
}

const char *Obfuscate_Geo(uint16_t longitude, uint16_t latitude) {
  return "Geo";
}

const char *Obfuscate_GeoShape() {
  return "GeoShape";
}

const char *Obfuscate_QueryNode(struct RSQueryNode *node) {
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