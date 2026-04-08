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

#include <stdbool.h>          // for bool
#include <stddef.h>
#include <stdint.h>           // for uint32_t

#include "../vector_index.h"  // for VectorQuery
#include "../query_node.h"    // for QueryAttribute

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Simplified vector data structure for hybrid queries.
 */
typedef struct {
  VectorQuery *query;
  const char *fieldName;       // Field name for later resolution (NOT owned - points to args)
  QueryAttribute *attributes;  // Non-vector-specific attributes like YIELD_SCORE_AS, SHARD_K_RATIO (OWNED)
  bool isParameter;            // true if vector data is a parameter
  bool hasExplicitK;           // Flag to track if K was explicitly set in KNN query
  char *vectorScoreFieldAlias; // Alias for the vector score field (OWNED) - NULL if not explicitly set
  uint32_t queryNodeFlags;     // QueryNode flags to be applied when creating the vector node
  bool skipFilterIntegration;  // true to make vector node root without filter wrapping (RANGE without explicit FILTER)
} ParsedVectorData;

void ParsedVectorData_Free(ParsedVectorData *pvd);

#ifdef __cplusplus
}
#endif

#endif // VECTOR_QUERY_UTILS_H
