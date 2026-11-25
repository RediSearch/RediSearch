/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "vecsim/vec_sim_common.h"
#include "rmutil/rm_assert.h"
#include "sparse_vector_index.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Vector Normalization Functions
 *
 * This file contains normalization functions for converting vector distance scores
 * to [0,1] range using metric-specific formulas for FT.HYBRID queries.
 */

/**
 * Function pointer type for vector normalization functions.
 * Takes a distance/similarity score and returns a normalized [0,1] value.
 */
typedef double (*VectorNormFunction)(double);

/**
 * L2 Distance Normalization
 * Formula: 1 / (1 + distance)
 * Input: L2 distance (>= 0)
 * Output: [0, 1] where 1 = perfect match (distance=0), approaches 0 as distance increases
 */
static inline double VectorNorm_L2(double distance) {
  return 1.0 / (1.0 + distance);
}

/**
 * Inner Product (Dot Product) Normalization
 * Formula: (1 + dot_product) / 2
 * Input: Inner product score (can be negative)
 * Output: [0, 1] where 1 = maximum similarity, 0.5 = orthogonal, 0 = opposite
 */
static inline double VectorNorm_IP(double dot_product) {
  return (1.0 + dot_product) / 2.0;
}

/**
 * Cosine Distance Normalization
 * Formula: (1 + cosine_similarity) / 2
 * Input: Cosine distance (1 - cosine_similarity)
 * Output: [0, 1] where 1 = perfect similarity, 0.5 = orthogonal, 0 = opposite
 *
 * Note: The system returns cosine distance, so we convert back to similarity first
 */
static inline double VectorNorm_Cosine(double cosine_distance) {
  // Convert distance to similarity: cosine_similarity = 1 - cosine_distance
  // Then normalize: (1 + cosine_similarity) / 2
  return (1.0 + (1.0 - cosine_distance)) / 2.0;
}

/**
 * Get the appropriate normalization function for a given VecSimMetric
 * This function is used during pipeline construction to resolve the metric
 * and select the corresponding normalization function.
 *
 * @param metric VecSimMetric enum value
 * @return VectorNormFunction pointer to the appropriate normalization function
 */
static inline VectorNormFunction getVectorNormalizationFunction(VecSimMetric metric) {
  switch (metric) {
    case VecSimMetric_L2:
      return VectorNorm_L2;
    case VecSimMetric_IP:
      return VectorNorm_IP;
    case VecSimMetric_Cosine:
      return VectorNorm_Cosine;
    default:
      // This should never happen - all VecSimMetric values should be handled
      RS_ABORT("Unknown VecSimMetric in GetVectorNormalizationFunction");
  }
}

/**
 * Sparse inner product: the raw dot product, unchanged.
 * It has no upper bound, so no fixed mapping puts it in [0, 1]. RRF ranks it as it is; under
 * LINEAR the weights have to account for its scale.
 */
static inline double SparseVectorNorm_IP(double dot_product) {
  return dot_product;
}

/**
 * Sparse cosine: (1 + cosine_similarity) / 2, as for a dense COSINE field.
 * The sparse index yields the similarity itself, the dot product of the normalized vectors,
 * rather than a distance.
 */
static inline double SparseVectorNorm_Cosine(double cosine_similarity) {
  return (1.0 + cosine_similarity) / 2.0;
}

/** The normalization function for a sparse vector field's metric. */
static inline VectorNormFunction getSparseVectorNormalizationFunction(SparseMetric metric) {
  switch (metric) {
    case SparseMetric_InnerProduct:
      return SparseVectorNorm_IP;
    case SparseMetric_Cosine:
      return SparseVectorNorm_Cosine;
  }
  RS_ABORT("Unknown SparseMetric in getSparseVectorNormalizationFunction");
  return NULL;
}

#ifdef __cplusplus
}
#endif
