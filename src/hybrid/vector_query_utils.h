/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#ifndef VECTOR_QUERY_UTILS_H
#define VECTOR_QUERY_UTILS_H

#include <stdbool.h>
#include <stddef.h>
#include "../vector_index.h"
#include "../query_node.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * OWNERSHIP: fieldName/vector NOT owned (point to args), attributes OWNED.
 * VectorQuery only references this data, doesn't free it.
 */
typedef struct ParsedVectorQuery {
  const char *fieldName;      // Field name string (NOT owned - points to args)
  const void *vector;         // Vector data OR parameter name (NOT owned - points to args)
  size_t vectorLen;           // Vector length
  bool isParameter;           // true if vector is a parameter name, false if direct data
  VectorQueryType type;       // KNN or RANGE
  union {
    size_t k;                 // For KNN
    double radius;            // For RANGE
  };
  QueryAttribute *attributes; // Self-describing array (OWNED)
} ParsedVectorQuery;

void ParsedVectorQuery_Free(ParsedVectorQuery *pvq);

#ifdef __cplusplus
}
#endif

#endif // VECTOR_QUERY_UTILS_H
