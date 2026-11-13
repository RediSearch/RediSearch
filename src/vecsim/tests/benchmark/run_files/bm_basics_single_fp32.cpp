/*
 * SPDX-FileCopyrightText: Modifications Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 */
#include "benchmark/bm_vecsim_basics.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "VecSim/algorithms/hnsw/hnsw_single.h"

/**************************************
  Basic tests for single value index with fp32 data type.
***************************************/

bool BM_VecSimGeneral::is_multi = false;
uint32_t BM_VecSimGeneral::enabled_index_types =
    DEFAULT_BM_INDEXES_MASK | INDEX_MASK_HNSW_SQ8 | INDEX_MASK_TIERED_HNSW_SQ8;

size_t BM_VecSimGeneral::n_queries = 10000;
size_t BM_VecSimGeneral::n_vectors = 1000000;
size_t BM_VecSimGeneral::dim = 768;
size_t BM_VecSimGeneral::M = 64;
size_t BM_VecSimGeneral::EF_C = 512;

const char *BM_VecSimGeneral::hnsw_index_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-M64-efc512.hnsw_v3";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-test_vectors.raw";

#define BM_FUNC_NAME(bm_func, algo) CONCAT_WITH_UNDERSCORE_ARCH(bm_func, algo, Single)
#define BM_ADD_LABEL                CONCAT_WITH_UNDERSCORE_ARCH(AddLabel, Single)
#define BM_UPDATE_LABEL             CONCAT_WITH_UNDERSCORE_ARCH(UpdateLabel, Single)
#define BM_ADD_LABEL_ASYNC          CONCAT_WITH_UNDERSCORE_ARCH(AddLabel, Async, Single)
#define BM_DELETE_LABEL_ASYNC       CONCAT_WITH_UNDERSCORE_ARCH(DeleteLabel_Async, Single)
#define BM_RELABEL_LABEL            CONCAT_WITH_UNDERSCORE_ARCH(RelabelLabel, Single)

DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, BF), fp32_index_t, BruteForceIndex_Single, float,
                    float, INDEX_BF)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, HNSW), fp32_index_t, HNSWIndex_Single, float, float,
                    INDEX_HNSW)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, Tiered), fp32_index_t, TieredHNSWIndex, float, float,
                    INDEX_TIERED_HNSW)
DEFINE_DELETE_LABEL_WITH_DATA_SOURCE(BM_FUNC_NAME(DeleteLabel, Tiered_SQ8), fp32_index_t,
                                     TieredHNSWIndex, BruteForceIndex_Single, float, float,
                                     INDEX_TIERED_HNSW_SQ8, INDEX_BF)

// Test Oscilations
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Single),
                            fp32_index_t)
(benchmark::State &st) { UpdateAtBlockSize(st); }
REGISTER_UpdateAtBlockSize(CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Single), INDEX_BF);
REGISTER_UpdateAtBlockSize(CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Single), INDEX_HNSW);
REGISTER_UpdateAtBlockSize(CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Single),
                           INDEX_TIERED_HNSW);
#include "benchmark/bm_initialization/bm_basics_initialize_fp32.h"
// Registered last: UpdateLabel rebuilds graph links that the other cases measure.
REGISTER_UpdateLabel(BM_UPDATE_LABEL, INDEX_HNSW);
REGISTER_UpdateLabel(BM_UPDATE_LABEL, INDEX_HNSW_SQ8);
BENCHMARK_MAIN();
