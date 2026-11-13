/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "VecSim/spaces/space_includes.h"
#include "VecSim/spaces/IP/IP_NEON_DOTPROD_SQ4_SQ4.h" // SQ4_SQ4_InnerProductSIMD32_NEON_DOTPROD_IMP
#include "VecSim/types/sq4.h"
#include "VecSim/utils/alignment.h"

namespace {

/*
 * Symmetric SQ4-SQ4 L2 squared distance by the stored-sum identity, the same one SQ8-SQ8 uses:
 *
 *   ||x - y||^2 = ||x||^2 + ||y||^2 - 2 * IP(x, y)
 *
 * where ||x||^2 and ||y||^2 are the stored SUM_SQUARES metadata and IP(x, y) comes from the
 * integer code dot of the inner product kernel. Exact-dim bound and dispatch as the inner product
 * kernel.
 */
template <unsigned char residual> // 0..31
float SQ4_SQ4_L2SqrSIMD32_NEON_DOTPROD(const void *pVect1v, const void *pVect2v, size_t dimension) {
    using vecsim_types::sq4;
    const float ip =
        SQ4_SQ4_InnerProductSIMD32_NEON_DOTPROD_IMP<residual>(pVect1v, pVect2v, dimension);
    const auto *x = static_cast<const uint8_t *>(pVect1v);
    const auto *y = static_cast<const uint8_t *>(pVect2v);
    const size_t data_bytes = sq4::data_bytes_count(dimension);
    const float sum_sq_x = load_unaligned<float>(x + data_bytes + sq4::SUM_SQUARES * sizeof(float));
    const float sum_sq_y = load_unaligned<float>(y + data_bytes + sq4::SUM_SQUARES * sizeof(float));
    return sum_sq_x + sum_sq_y - 2.0f * ip;
}

} // namespace
