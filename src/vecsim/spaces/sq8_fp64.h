/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstddef>

namespace spaces {
double L2_SQ8FP64(const void *left, const void *right, size_t dim);
double L2_SQ8FP64_FP64(const void *left, const void *right, size_t dim);
double IP_SQ8FP64(const void *left, const void *right, size_t dim);
double IP_SQ8FP64_FP64(const void *left, const void *right, size_t dim);
} // namespace spaces
