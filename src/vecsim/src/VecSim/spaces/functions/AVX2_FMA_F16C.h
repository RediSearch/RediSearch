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

// SQ8↔FP16 kernels for the AVX2+FMA tier. Live in a sibling TU compiled only when the
// toolchain supports F16C (via `-mf16c`), so this header has no preprocessor guard. Callers
// still gate the calls themselves with `#ifdef OPT_F16C`.

namespace spaces {

dist_func_t<float> Choose_SQ8_FP16_IP_implementation_AVX2_FMA(size_t dim);
dist_func_t<float> Choose_SQ8_FP16_Cosine_implementation_AVX2_FMA(size_t dim);
dist_func_t<float> Choose_SQ8_FP16_L2_implementation_AVX2_FMA(size_t dim);

} // namespace spaces
