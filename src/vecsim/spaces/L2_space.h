/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "vecsim/spaces/spaces.h"

namespace spaces {
// Four candidates, one shared query, dimension, and four output distances in candidate order.
using dist_func4_t = void (*)(const float *, const float *, const float *, const float *,
                              const float *, size_t, float *);

// Returns nullptr unless grouped FP32 L2 supports the active scalar dispatch. Select on the
// searching thread; its SVE vector length must stay fixed until the query finishes.
dist_func4_t L2_FP32_GetDistFunc4(size_t dim, dist_func_t<float> actual_scalar);

dist_func_t<float> L2_FP32_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                       const void *arch_opt = nullptr);
dist_func_t<double> L2_FP64_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                        const void *arch_opt = nullptr);
dist_func_t<float> L2_BF16_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                       const void *arch_opt = nullptr);
dist_func_t<float> L2_FP16_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                       const void *arch_opt = nullptr);
dist_func_t<float> L2_INT8_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                       const void *arch_opt = nullptr);
dist_func_t<float> L2_UINT8_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                        const void *arch_opt = nullptr);
// SQ4-FP32: packed storage has no alignment requirement, including its metadata.
dist_func_t<float> L2_SQ4_FP32_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                           const void *arch_opt = nullptr);
// SQ4-SQ4: packed storage has no alignment requirement, including its metadata.
dist_func_t<float> L2_SQ4_SQ4_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                          const void *arch_opt = nullptr);
// SQ8-FP32: asymmetric L2 distance between FP32 query and SQ8 storage
dist_func_t<float> L2_SQ8_FP32_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                           const void *arch_opt = nullptr);
// SQ8-FP16: asymmetric L2 distance between FP16 query and SQ8 storage
dist_func_t<float> L2_SQ8_FP16_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                           const void *arch_opt = nullptr);
dist_func_t<float> L2_SQ8_SQ8_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                          const void *arch_opt = nullptr);
} // namespace spaces
