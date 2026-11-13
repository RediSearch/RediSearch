/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

// Training thresholds from the SVS test plan (1K-100K). The SVS training suite covers up to 50K.
// The run files load enough vectors for the largest threshold plus 1,000 ingest-overlap writes.
#define SQ8_TRAINING_THRESHOLDS {1024, 5000, 10000, 50000, 100000}

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSQ8Training, BM_Train, DATA_TYPE_INDEX_T)
(benchmark::State &st) { Train(st); }
BENCHMARK_REGISTER_F(BM_VecSimSQ8Training, BM_Train)
    ->Args({1024})
    ->Args({5000})
    ->Args({10000})
    ->Args({50000})
    ->Args({100000})
    ->ArgNames({"training_threshold"})
    ->Unit(benchmark::kMillisecond)
    ->Iterations(2);

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSQ8Training, BM_TrainAsync, DATA_TYPE_INDEX_T)
(benchmark::State &st) { TrainAsync(st); }
BENCHMARK_REGISTER_F(BM_VecSimSQ8Training, BM_TrainAsync)
    ->ArgsProduct({SQ8_TRAINING_THRESHOLDS, {2, 4, 8, 16}})
    ->ArgNames({"training_threshold", "thread_count"})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->Iterations(2);

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSQ8Training, BM_AddVectorsDuringInitialIngest,
                            DATA_TYPE_INDEX_T)
(benchmark::State &st) { AddVectorsDuringInitialIngest(st); }
BENCHMARK_REGISTER_F(BM_VecSimSQ8Training, BM_AddVectorsDuringInitialIngest)
    ->ArgsProduct({SQ8_TRAINING_THRESHOLDS, {2, 4, 8}})
    ->ArgNames({"training_threshold", "thread_count"})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->Iterations(1);
