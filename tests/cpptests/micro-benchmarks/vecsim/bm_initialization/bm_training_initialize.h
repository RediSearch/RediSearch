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
  DATA_TYPE_INDEX_T should be defined by the including run_files/bm_svs_training_*.cpp file.
  And Should be of the format datatpye_index_t.
***************************************/

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_TrainNoCompression, DATA_TYPE_INDEX_T)
(benchmark::State &st) { Train(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_TrainNoCompression)
    ->UNIT_AND_ITERATIONS
    ->Args({VecSimSvsQuant_NONE, static_cast<long int>(BM_VecSimGeneral::block_size)})
    ->ArgNames({"quant_bits", "training_threshold"});

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_TrainAsyncNoCompression, DATA_TYPE_INDEX_T)
(benchmark::State &st) { TrainAsync(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_TrainAsyncNoCompression)
    ->UNIT_AND_ITERATIONS
    ->ArgsProduct({{VecSimSvsQuant_NONE},
                   {static_cast<long int>(BM_VecSimGeneral::block_size)},
                   {4, 8, 16}})
    ->ArgNames({"quant_bits", "training_threshold", "thread_count"})
    ->MeasureProcessCPUTime()
    ->UseRealTime();

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_TrainCompressed, DATA_TYPE_INDEX_T)
(benchmark::State &st) { Train(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_TrainCompressed)
    ->UNIT_AND_ITERATIONS->ArgsProduct({QUANT_BITS_ARGS, COMPRESSED_TRAINING_THRESHOLD_ARGS})
    ->ArgNames({"quant_bits", "training_threshold"});

BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_TrainCompressedAsync, DATA_TYPE_INDEX_T)
(benchmark::State &st) { TrainAsync(st); }
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_TrainCompressedAsync)
    ->UNIT_AND_ITERATIONS
    ->ArgsProduct({QUANT_BITS_ARGS, COMPRESSED_ASYNC_TRAINING_THRESHOLD_ARGS, {4, 8, 16}})
    ->ArgNames({"quant_bits", "training_threshold", "thread_count"})
    ->MeasureProcessCPUTime()
    ->UseRealTime();
