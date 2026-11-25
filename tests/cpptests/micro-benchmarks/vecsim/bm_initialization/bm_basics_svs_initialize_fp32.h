/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
/**************************************
  Define and register tests
  NOTE: benchmarks' tests order can affect their results. Please add new benchmarks at the end of
the file.
***************************************/
// deleteLabel one by one
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_FUNC_NAME(BM_RunGC), DATA_TYPE_INDEX_T)
(benchmark::State &st) { RunGC(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_FUNC_NAME(BM_RunGC))
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->ArgsProduct({{50, 100, 500}, {1, 4}})
    ->ArgNames({"num_deletions", "thread_count"});

// AddLabel one by one
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_FUNC_NAME(BM_AddLabelOneByOne), DATA_TYPE_INDEX_T)
(benchmark::State &st) { AddLabel(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_FUNC_NAME(BM_AddLabelOneByOne))
    ->Unit(benchmark::kMillisecond)
    ->Iterations(BM_VecSimGeneral::block_size);

// Add vectors in batches via tiered index
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_FUNC_NAME(BM_TriggerUpdateTiered), DATA_TYPE_INDEX_T)
(benchmark::State &st) { TriggerUpdateTiered(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_FUNC_NAME(BM_TriggerUpdateTiered))
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->ArgsProduct({{static_cast<long int>(BM_VecSimGeneral::block_size), 5000,
                    static_cast<long int>(10 * BM_VecSimGeneral::block_size)},
                   {2, 4, 8}})
    ->ArgNames({"update_threshold", "thread_count"})
    ->MeasureProcessCPUTime();

// Add vectors to reach training threshold in tiered index, and then add more vectors in parallel to
// backend training job. Measure time to add new vectors in this scenario.
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_FUNC_NAME(BM_AddVectorsDuringTraining),
                            DATA_TYPE_INDEX_T)
(benchmark::State &st) { AddVectorsDuringTraining(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_FUNC_NAME(BM_AddVectorsDuringTraining))
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1)
    ->ArgsProduct({{static_cast<long int>(BM_VecSimGeneral::block_size), 5000,
                    static_cast<long int>(10 * BM_VecSimGeneral::block_size)},
                   {2, 4}})
    ->ArgNames({"training_threshold", "thread_count"})
    ->UseRealTime();

// TopK search on the loaded SVS index, using the search window_size and k defined below.
// {window_size, k} (recall that always window_size >= k)
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_FUNC_NAME(BM_TopK), DATA_TYPE_INDEX_T)
(benchmark::State &st) { TopK_SVS(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_FUNC_NAME(BM_TopK))
    ->Unit(benchmark::kMillisecond)
    ->Iterations(10)
    ->Args({10, 10})
    ->Args({200, 10})
    ->Args({100, 100})
    ->Args({200, 100})
    ->Args({500, 500})
    ->ArgNames({"window_size", "k"});

// Allocator memory of the loaded backend, without the exact-reference index.
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_FUNC_NAME(BM_Memory), DATA_TYPE_INDEX_T)
(benchmark::State &st) { Memory(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_FUNC_NAME(BM_Memory))->Iterations(1);
