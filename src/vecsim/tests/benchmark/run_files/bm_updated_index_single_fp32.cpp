#include "benchmark/bm_updated_index.h"

/**************************************
  Basic tests for updated single value index.
***************************************/
bool BM_VecSimGeneral::is_multi = false;
uint32_t BM_VecSimGeneral::enabled_index_types =
    IndexTypeFlags::INDEX_MASK_BF | IndexTypeFlags::INDEX_MASK_HNSW |
    IndexTypeFlags::INDEX_MASK_HNSW_UPDATED | IndexTypeFlags::INDEX_MASK_BF_UPDATED;

size_t BM_VecSimGeneral::n_queries = 10000;
size_t BM_VecSimGeneral::dim = 768;
size_t BM_VecSimGeneral::M = 65;
size_t BM_VecSimGeneral::EF_C = 512;
size_t BM_VecSimGeneral::n_vectors = 500000;

const char *BM_VecSimGeneral::hnsw_index_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-M65-efc512-n500k.hnsw_v3";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-test_vectors.raw";

template <>
const char *BM_VecSimUpdatedIndex<fp32_index_t>::updated_hnsw_index_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-M65-efc512-n500k-updated.hnsw_v3";

#define BM_BEFORE_FUNC_NAME(bm_func, algo)                                                         \
    CONCAT_WITH_UNDERSCORE_ARCH(bm_func, algo, before, Single)
#define BM_UPDATED_FUNC_NAME(bm_func, algo) bm_func##_##algo##_updated_Single

#include "benchmark/bm_initialization/bm_updated_initialize_fp32.h"
BENCHMARK_MAIN();
