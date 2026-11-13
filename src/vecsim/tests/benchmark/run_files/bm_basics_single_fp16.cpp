/*
 * SPDX-FileCopyrightText: Modifications Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 */
#include "benchmark/bm_vecsim_basics.h"
#include "VecSim/algorithms/brute_force/brute_force_single.h"
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "VecSim/types/float16.h"

/**************************************
  Basic tests for single value index with bf16 data type.
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
    "tests/benchmark/data/dbpedia-cosine-dim768-M64-efc512-fp16.hnsw_v3";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-fp16-test_vectors.raw";

#define BM_FUNC_NAME(bm_func, algo) CONCAT_WITH_UNDERSCORE_ARCH(bm_func, algo, Single)
#define BM_ADD_LABEL                CONCAT_WITH_UNDERSCORE_ARCH(AddLabel, Single)
#define BM_UPDATE_LABEL             CONCAT_WITH_UNDERSCORE_ARCH(UpdateLabel, Single)
#define BM_ADD_LABEL_ASYNC          CONCAT_WITH_UNDERSCORE_ARCH(AddLabel, Async, Single)
#define BM_DELETE_LABEL_ASYNC       CONCAT_WITH_UNDERSCORE_ARCH(DeleteLabel, Async, Single)

DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, BF), fp16_index_t, BruteForceIndex_Single,
                    vecsim_types::float16, float, INDEX_BF)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, HNSW), fp16_index_t, HNSWIndex_Single,
                    vecsim_types::float16, float, INDEX_HNSW)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, Tiered), fp16_index_t, TieredHNSWIndex,
                    vecsim_types::float16, float, INDEX_TIERED_HNSW)
DEFINE_DELETE_LABEL_WITH_DATA_SOURCE(BM_FUNC_NAME(DeleteLabel, Tiered_SQ8), fp16_index_t,
                                     TieredHNSWIndex, BruteForceIndex_Single, vecsim_types::float16,
                                     float, INDEX_TIERED_HNSW_SQ8, INDEX_BF)
#include "benchmark/bm_initialization/bm_basics_initialize_fp16.h"
// Registered last: UpdateLabel rebuilds graph links that the other cases measure.
REGISTER_UpdateLabel(BM_UPDATE_LABEL, INDEX_HNSW);
REGISTER_UpdateLabel(BM_UPDATE_LABEL, INDEX_HNSW_SQ8);
BENCHMARK_MAIN();
