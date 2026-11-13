/*
 * SPDX-FileCopyrightText: Modifications Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 */
#include "benchmark/bm_vecsim_basics.h"
#include "VecSim/algorithms/brute_force/brute_force_multi.h"
#include "VecSim/algorithms/hnsw/hnsw_multi.h"

/**************************************
  Basic tests for multi value index.
***************************************/

bool BM_VecSimGeneral::is_multi = true;
uint32_t BM_VecSimGeneral::enabled_index_types = DEFAULT_BM_INDEXES_MASK |
                                                 IndexTypeFlags::INDEX_MASK_HNSW_SQ8 |
                                                 IndexTypeFlags::INDEX_MASK_TIERED_HNSW_SQ8;

size_t BM_VecSimGeneral::n_queries = 10000;
size_t BM_VecSimGeneral::n_vectors = 1111025;
size_t BM_VecSimGeneral::dim = 512;
size_t BM_VecSimGeneral::M = 64;
size_t BM_VecSimGeneral::EF_C = 512;

const char *BM_VecSimGeneral::hnsw_index_file =
    "tests/benchmark/data/fashion_images_multi_value-cosine-dim512-M64-efc512.hnsw_v3";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/fashion_images_multi_value-cosine-dim512-test_vectors.raw";

#define BM_FUNC_NAME(bm_func, algo) CONCAT_WITH_UNDERSCORE_ARCH(bm_func, algo, Multi)
#define BM_ADD_LABEL                CONCAT_WITH_UNDERSCORE_ARCH(AddLabel, Multi)
#define BM_UPDATE_LABEL             CONCAT_WITH_UNDERSCORE_ARCH(UpdateLabel, Multi)
#define BM_ADD_LABEL_ASYNC          CONCAT_WITH_UNDERSCORE_ARCH(AddLabel_Async, Multi)
#define BM_DELETE_LABEL_ASYNC       CONCAT_WITH_UNDERSCORE_ARCH(DeleteLabel_Async, Multi)
#define BM_RELABEL_LABEL            CONCAT_WITH_UNDERSCORE_ARCH(RelabelLabel, Multi)

DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, BF), fp32_index_t, BruteForceIndex_Multi, float,
                    float, INDEX_BF)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, HNSW), fp32_index_t, HNSWIndex_Multi, float, float,
                    INDEX_HNSW)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, Tiered), fp32_index_t, TieredHNSWIndex, float, float,
                    INDEX_TIERED_HNSW)
DEFINE_DELETE_LABEL_WITH_DATA_SOURCE(BM_FUNC_NAME(DeleteLabel, Tiered_SQ8), fp32_index_t,
                                     TieredHNSWIndex, BruteForceIndex_Multi, float, float,
                                     INDEX_TIERED_HNSW_SQ8, INDEX_BF)
#include "benchmark/bm_initialization/bm_basics_initialize_fp32.h"

// Test oscillations at block size boundaries.
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Multi),
                            fp32_index_t)
(benchmark::State &st) { UpdateAtBlockSize(st); }
REGISTER_UpdateAtBlockSize(CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Multi), INDEX_BF);
REGISTER_UpdateAtBlockSize(CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Multi), INDEX_HNSW);
REGISTER_UpdateAtBlockSize(CONCAT_WITH_UNDERSCORE_ARCH(UpdateAtBlockSize, Multi),
                           INDEX_TIERED_HNSW);
// Registered last: UpdateLabel rebuilds graph links that the other cases measure.
REGISTER_UpdateLabel(BM_UPDATE_LABEL, INDEX_HNSW);
REGISTER_UpdateLabel(BM_UPDATE_LABEL, INDEX_HNSW_SQ8);
BENCHMARK_MAIN();
