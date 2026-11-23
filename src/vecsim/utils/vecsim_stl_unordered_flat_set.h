/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vecsim/memory/vecsim_malloc.h"

#include <boost/unordered/unordered_flat_set.hpp>
#include <functional>

namespace vecsim_stl {

template <typename T>
using unordered_flat_set =
    boost::unordered_flat_set<T, boost::hash<T>, std::equal_to<T>, VecsimSTLAllocator<T>>;

} // namespace vecsim_stl
