/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "NEON_BF16.h"

#include "VecSim/spaces/L2/L2_NEON_BF16.h"
#include "VecSim/spaces/IP/IP_NEON_BF16.h"

namespace spaces {

#include "implementation_chooser.h"

dist_func_t<float> Choose_BF16_L2_implementation_NEON_BF16(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, BF16_L2Sqr_NEON);
    return ret_dist_func;
}

dist_func_t<float> Choose_BF16_IP_implementation_NEON_BF16(size_t dim) {
    dist_func_t<float> ret_dist_func;
    CHOOSE_IMPLEMENTATION(ret_dist_func, dim, 32, BF16_InnerProduct_NEON);
    return ret_dist_func;
}

#include "implementation_chooser_cleanup.h"

} // namespace spaces
