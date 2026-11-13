#include "benchmark/bm_vecsim_basics.h"
#include "VecSim/algorithms/brute_force/brute_force_multi.h"
#include "VecSim/algorithms/hnsw/hnsw_multi.h"

/**************************************
  Basic tests for multi value index.
***************************************/

bool BM_VecSimGeneral::is_multi = true;
uint32_t BM_VecSimGeneral::enabled_index_types = IndexTypeFlags::INDEX_MASK_BF |
                                                 IndexTypeFlags::INDEX_MASK_HNSW |
                                                 IndexTypeFlags::INDEX_MASK_TIERED_HNSW;

size_t BM_VecSimGeneral::n_queries = 10000;
size_t BM_VecSimGeneral::n_vectors = 1111025;
size_t BM_VecSimGeneral::dim = 512;
size_t BM_VecSimGeneral::M = 64;
size_t BM_VecSimGeneral::EF_C = 512;

const char *BM_VecSimGeneral::hnsw_index_file =
    "tests/benchmark/data/fashion_images_multi_value-cosine-dim512-M64-efc512-fp64.hnsw_v3";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/fashion_images_multi_value-cosine-dim512-fp64-test_vectors.raw";

#define BM_FUNC_NAME(bm_func, algo) CONCAT_WITH_UNDERSCORE_ARCH(bm_func, algo, Multi)
#define BM_ADD_LABEL                CONCAT_WITH_UNDERSCORE_ARCH(AddLabel, Multi)
#define BM_ADD_LABEL_ASYNC          CONCAT_WITH_UNDERSCORE_ARCH(AddLabel_Async, Multi)
#define BM_DELETE_LABEL_ASYNC       CONCAT_WITH_UNDERSCORE_ARCH(DeleteLabel_Async, Multi)

DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, BF), fp64_index_t, BruteForceIndex_Multi, double,
                    double, INDEX_BF)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, HNSW), fp64_index_t, HNSWIndex_Multi, double, double,
                    INDEX_HNSW)
DEFINE_DELETE_LABEL(BM_FUNC_NAME(DeleteLabel, Tiered), fp64_index_t, TieredHNSWIndex, double,
                    double, INDEX_TIERED_HNSW)
#include "benchmark/bm_initialization/bm_basics_initialize_fp64.h"

BENCHMARK_MAIN();
