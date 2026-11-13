/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"
#include "VecSim/types/sq8.h"
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/spaces/IP_space.h"
#include "VecSim/spaces/L2_space.h"
#include "VecSim/spaces/normalize/normalize_naive.h"

#include <stdexcept>
namespace spaces {

// Set the distance function for a given data type, metric and dimension. The alignment hint is
// determined according to the chosen implementation and available optimizations.

template <>
dist_func_t<float> GetDistFunc<vecsim_types::bfloat16, float>(VecSimMetric metric, size_t dim,
                                                              unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
    case VecSimMetric_IP:
        return IP_BF16_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_BF16_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float> GetDistFunc<vecsim_types::float16, float>(VecSimMetric metric, size_t dim,
                                                             unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
    case VecSimMetric_IP:
        return IP_FP16_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_FP16_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float> GetDistFunc<float, float>(VecSimMetric metric, size_t dim,
                                             unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
    case VecSimMetric_IP:
        return IP_FP32_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_FP32_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<double> GetDistFunc<double, double>(VecSimMetric metric, size_t dim,
                                                unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
    case VecSimMetric_IP:
        return IP_FP64_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_FP64_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float> GetDistFunc<int8_t, float>(VecSimMetric metric, size_t dim,
                                              unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
        return Cosine_INT8_GetDistFunc(dim, alignment);
    case VecSimMetric_IP:
        return IP_INT8_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_INT8_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float> GetDistFunc<uint8_t, float>(VecSimMetric metric, size_t dim,
                                               unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
        return Cosine_UINT8_GetDistFunc(dim, alignment);
    case VecSimMetric_IP:
        return IP_UINT8_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_UINT8_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float> GetDistFunc<vecsim_types::sq8, float>(VecSimMetric metric, size_t dim,
                                                         unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
        return Cosine_SQ8_SQ8_GetDistFunc(dim, alignment);
    case VecSimMetric_IP:
        return IP_SQ8_SQ8_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_SQ8_SQ8_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float> GetDistFunc<vecsim_types::sq8, float, float>(VecSimMetric metric, size_t dim,
                                                                unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
        return Cosine_SQ8_FP32_GetDistFunc(dim, alignment);
    case VecSimMetric_IP:
        return IP_SQ8_FP32_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_SQ8_FP32_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
dist_func_t<float>
GetDistFunc<vecsim_types::sq8, float, vecsim_types::float16>(VecSimMetric metric, size_t dim,
                                                             unsigned char *alignment) {
    switch (metric) {
    case VecSimMetric_Cosine:
        return Cosine_SQ8_FP16_GetDistFunc(dim, alignment);
    case VecSimMetric_IP:
        return IP_SQ8_FP16_GetDistFunc(dim, alignment);
    case VecSimMetric_L2:
        return L2_SQ8_FP16_GetDistFunc(dim, alignment);
    }
    throw std::invalid_argument("Invalid metric");
}

template <>
normalizeVector_f<float> GetNormalizeFunc<float>(void) {
    return normalizeVector_imp<float>;
}

template <>
normalizeVector_f<double> GetNormalizeFunc<double>(void) {
    return normalizeVector_imp<double>;
}

template <>
normalizeVector_f<vecsim_types::bfloat16> GetNormalizeFunc<vecsim_types::bfloat16>(void) {
    if (is_little_endian()) {
        return bfloat16_normalizeVector<true>;
    } else {
        return bfloat16_normalizeVector<false>;
    }
}

template <>
normalizeVector_f<vecsim_types::float16> GetNormalizeFunc<vecsim_types::float16>(void) {
    return float16_normalizeVector;
}

/** The returned function computes the norm and stores it at the end of the given vector */
template <>
normalizeVector_f<int8_t> GetNormalizeFunc<int8_t>(void) {
    return integer_normalizeVector<int8_t>;
}
template <>
normalizeVector_f<uint8_t> GetNormalizeFunc<uint8_t>(void) {
    return integer_normalizeVector<uint8_t>;
}

} // namespace spaces
