/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <benchmark/benchmark.h>
#include "rocksdb/db.h"
#include "VecSim/vec_sim.h"

namespace bm_common {

/**
 * Get memory usage from an index.
 */
inline size_t GetMemoryUsage(VecSimIndex* index) {
    if (!index) {
        return 0;
    }
    return VecSimIndex_StatsInfo(index).memory;
}

/**
 * Get the total disk usage of a SpeedB database.
 */
inline size_t GetDiskUsage(rocksdb::DB* db) {
    if (!db) {
        return 0;
    }
    std::string value;
    // TODO: verify this is what we want.
    if (db->GetProperty("rocksdb.total-sst-files-size", &value)) {
        return std::stoull(value);
    }
    return 0;
}

/**
 * Report memory counter in a consistent format.
 */
inline void ReportMemory(benchmark::State& state, VecSimIndex* index) {
    state.counters["memory"] = benchmark::Counter(static_cast<double>(GetMemoryUsage(index)),
                                                  benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);
}

/**
 * Report disk usage counter in a consistent format.
 */
inline void ReportDiskUsage(benchmark::State& state, rocksdb::DB* db) {
    state.counters["disk"] = benchmark::Counter(static_cast<double>(GetDiskUsage(db)), benchmark::Counter::kDefaults,
                                                benchmark::Counter::OneK::kIs1024);
}

} // namespace bm_common
