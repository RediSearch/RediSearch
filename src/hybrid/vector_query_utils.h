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
#include "hybrid_arm.h"
#include "sparse_vector_index.h"

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
  bool skipFilterIntegration;  // Make the vector node the root without parsing a filter query.

  // The K the arm returns, after the cap at WINDOW. Valid only when explainQueryType is
  // VECSIM_QT_KNN. Kept here because the query node that carries K differs by field type, and a
  // sparse node carries none.
  size_t knnK;
  // The first option given that only a dense field supports, as the user spelled it; NULL if
  // none. applyVectorQuery rejects it on a sparse field.
  const char *denseOnlyOption;
  // VECTOR_INDEX or SPARSE_INDEX, by the field's type. Set by applyVectorQuery.
  HybridArmSlot armSlot;
  SparseMetric sparseMetric;  // valid only when armSlot == SPARSE_INDEX

  // EXPLAINSCORE snapshot: applyVectorQuery later nulls `query`, so we keep a
  // copy of the fields needed to render the hybrid explain envelope.
  VectorQueryType explainQueryType;
  double explainRangeRadius;        // valid only when explainQueryType == VECSIM_QT_RANGE
  char *explainRangeEpsilon;        // OWNED, NULL if user did not set EPSILON
} ParsedVectorData;

void ParsedVectorData_Free(ParsedVectorData *pvd);

#ifdef __cplusplus
}
#endif

#endif // VECTOR_QUERY_UTILS_H
