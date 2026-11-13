#include "benchmark/bm_batch_iterator.h"

bool BM_VecSimGeneral::is_multi = true;
uint32_t BM_VecSimGeneral::enabled_index_types = IndexTypeFlags::INDEX_MASK_BF |
                                                 IndexTypeFlags::INDEX_MASK_HNSW |
                                                 IndexTypeFlags::INDEX_MASK_TIERED_HNSW;

size_t BM_VecSimGeneral::n_queries = 10000;
size_t BM_VecSimGeneral::n_vectors = 1000000;
size_t BM_VecSimGeneral::dim = 1024;
size_t BM_VecSimGeneral::M = 64;
size_t BM_VecSimGeneral::EF_C = 512;
size_t BM_VecSimGeneral::block_size = 1024;

const char *BM_VecSimGeneral::hnsw_index_file =
    "tests/benchmark/data/wipedia_multi-cosine-dim1024-M64-efc512-int8.hnsw_v3";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/wipedia_multi-cosine-dim1024-int8-test_vectors.raw";

#define BM_FUNC_NAME(bm_func, algo) algo##_##bm_func##_Multi

#include "benchmark/bm_initialization/bm_batch_initialize_int8.h"

BENCHMARK_MAIN();
