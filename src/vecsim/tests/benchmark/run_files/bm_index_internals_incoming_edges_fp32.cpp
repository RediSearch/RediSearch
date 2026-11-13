/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Run file for incoming edges ghost memory benchmarks (fp32).
// This file will instantiate and register the benchmark classes defined
// in index_internals/bm_incoming_edges.h once they are implemented.

#include "benchmark/index_internals/bm_incoming_edges.h"

BENCHMARK_DEFINE_F(BM_IncomingEdgesBase, DeleteZeroVectorsAsync)
(benchmark::State &st) { DeleteZeroVectorsAsync(st); }
BENCHMARK_REGISTER_F(BM_IncomingEdgesBase, DeleteZeroVectorsAsync)
    ->Iterations(3)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_DEFINE_F(BM_IncomingEdgesBase, InsertZeroVectorsTimed)
(benchmark::State &st) { InsertZeroVectorsTimed(st); }
BENCHMARK_REGISTER_F(BM_IncomingEdgesBase, InsertZeroVectorsTimed)
    ->Iterations(3)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_DEFINE_F(BM_IncomingEdgesBase, DeleteZeroVectorsInPlace)
(benchmark::State &st) { DeleteZeroVectorsInPlace(st); }
BENCHMARK_REGISTER_F(BM_IncomingEdgesBase, DeleteZeroVectorsInPlace)
    ->Iterations(3)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
