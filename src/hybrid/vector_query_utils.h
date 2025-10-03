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
 * Simplified vector data structure for hybrid queries.
 */
typedef struct {
  VectorQueryType queryType;
  VectorQuery *query;
  const char *fieldName;       // Field name for later resolution (NOT owned - points to args)
  QueryAttribute *attributes;  // Non-vector-specific attributes like YIELD_SCORE_AS (OWNED)
  bool isParameter;            // true if vector data is a parameter
  bool hasExplicitK;           // Flag to track if K was explicitly set in KNN query
  size_t kValue;               // Value of K (if explicitly set, otherwise HYBRID_DEFAULT_KNN_K)
  char *vectorScoreFieldAlias; // Alias for the vector score field (OWNED) - NULL if not explicitly set
  uint32_t queryNodeFlags;     // QueryNode flags to be applied when creating the vector node
} ParsedVectorData;

void ParsedVectorData_Free(ParsedVectorData *pvd);

#ifdef __cplusplus
}
#endif

#endif // VECTOR_QUERY_UTILS_H
