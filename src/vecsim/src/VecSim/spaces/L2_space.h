/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "VecSim/spaces/spaces.h"

namespace spaces {
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
// SQ8-FP32: asymmetric L2 distance between FP32 query and SQ8 storage
dist_func_t<float> L2_SQ8_FP32_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                           const void *arch_opt = nullptr);
// SQ8-FP16: asymmetric L2 distance between FP16 query and SQ8 storage
dist_func_t<float> L2_SQ8_FP16_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                           const void *arch_opt = nullptr);
dist_func_t<float> L2_SQ8_SQ8_GetDistFunc(size_t dim, unsigned char *alignment = nullptr,
                                          const void *arch_opt = nullptr);
} // namespace spaces
