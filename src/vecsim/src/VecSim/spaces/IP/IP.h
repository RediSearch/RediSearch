/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstdlib>

// SQ8-FP32: Common inner product implementation that returns the raw inner product value
// (not distance). Used by SQ8_FP32_InnerProduct, SQ8_FP32_Cosine, and SQ8_FP32_L2Sqr.
// pVect1 is storage (SQ8): [uint8_t values (dim)] [min_val] [delta] [x_sum] [x_sum_squares (L2
// only)]
// pVect2 is query (FP32): [float values (dim)] [y_sum] [y_sum_squares (L2 only)]
float SQ8_FP32_InnerProduct_Impl(const void *pVect1v, const void *pVect2v, size_t dimension);

// pVect1v vector of type uint8 (SQ8) and pVect2v vector of type fp32
float SQ8_FP32_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension);

// pVect1v vector of type uint8 (SQ8) and pVect2v vector of type fp32
float SQ8_FP32_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension);

// SQ8-FP16: Common inner product implementation that returns the raw inner product value
// (not distance). Used by SQ8_FP16_InnerProduct, SQ8_FP16_Cosine, and SQ8_FP16_L2Sqr.
// pVect1 is storage (SQ8): [uint8_t values (dim)] [min_val (float)] [delta (float)]
//                          [x_sum (float)] [x_sum_squares (float, L2 only)]
// pVect2 is query (FP16):  [float16 values (dim)] [y_sum (float)] [y_sum_squares (float, L2 only)]
float SQ8_FP16_InnerProduct_Impl(const void *pVect1v, const void *pVect2v, size_t dimension);

// pVect1v vector of type uint8 (SQ8) and pVect2v vector of type fp16
float SQ8_FP16_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension);

// pVect1v vector of type uint8 (SQ8) and pVect2v vector of type fp16
float SQ8_FP16_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension);

// SQ8-to-SQ8: Common inner product implementation that returns the raw inner product value
// (not distance). Used by both SQ8_SQ8_InnerProduct, SQ8_SQ8_Cosine, and SQ8_SQ8_L2Sqr.
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
float SQ8_SQ8_InnerProduct_Impl(const void *pVect1v, const void *pVect2v, size_t dimension);

// SQ8-to-SQ8: Both vectors are uint8 quantized with precomputed sum
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
float SQ8_SQ8_InnerProduct(const void *pVect1v, const void *pVect2v, size_t dimension);

// SQ8-to-SQ8: Both vectors are uint8 quantized and normalized with precomputed sum
// Vector layout: [uint8_t values (dim)] [min_val (float)] [delta (float)] [sum (float)]
float SQ8_SQ8_Cosine(const void *pVect1v, const void *pVect2v, size_t dimension);

float FP32_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension);

double FP64_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension);

float FP16_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension);

float BF16_InnerProduct_LittleEndian(const void *pVect1v, const void *pVect2v, size_t dimension);
float BF16_InnerProduct_BigEndian(const void *pVect1v, const void *pVect2v, size_t dimension);

float INT8_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension);
float INT8_Cosine(const void *pVect1, const void *pVect2, size_t dimension);

float UINT8_InnerProduct(const void *pVect1, const void *pVect2, size_t dimension);
float UINT8_Cosine(const void *pVect1, const void *pVect2, size_t dimension);
