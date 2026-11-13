/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/utils/vecsim_stl.h"
#include "VecSim/query_results.h"

#include <cstdlib>
#include <limits>

// Use the "not a number" value to represent invalid score. This is for distinguishing the invalid
// score from "inf" score (which is valid).
#define INVALID_SCORE std::numeric_limits<double>::quiet_NaN()

/**
 * This file contains the headers to be used internally for creating an array of results in
 * TopKQuery methods.
 */
struct VecSimQueryResult {
    size_t id;
    double score;
};

using VecSimQueryResultContainer = vecsim_stl::vector<VecSimQueryResult>;

struct VecSimQueryReply {
    VecSimQueryResultContainer results;
    VecSimQueryReply_Code code;

    VecSimQueryReply(std::shared_ptr<VecSimAllocator> allocator,
                     VecSimQueryReply_Code code = VecSim_QueryReply_OK)
        : results(allocator), code(code) {}
};

#ifdef BUILD_TESTS
#include <iostream>

// Print operators
inline std::ostream &operator<<(std::ostream &os, const VecSimQueryResult &result) {
    os << "id: " << result.id << ", score: " << result.score;
    return os;
}

inline std::ostream &operator<<(std::ostream &os, const VecSimQueryReply &reply) {
    for (const auto &result : reply.results) {
        os << result << std::endl;
    }
    return os;
}
#endif
