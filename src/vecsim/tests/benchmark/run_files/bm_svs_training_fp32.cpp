#include "benchmark/bm_vecsim_svs.h"

/**************************************
Training threshold benchmarks
***************************************/

bool BM_VecSimGeneral::is_multi = false;

size_t BM_VecSimGeneral::n_queries = 100000;
size_t BM_VecSimGeneral::dim = 768;
size_t BM_VecSimGeneral::M = 128;
size_t BM_VecSimGeneral::EF_C = 512;
size_t BM_VecSimGeneral::block_size = 1024;

#define DATA_TYPE_INDEX_T fp32_index_t
template <>
const char *BM_VecSimSVS<fp32_index_t>::svs_index_tar_file = nullptr;
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-1M-vectors.raw";
#include "benchmark/bm_initialization/bm_training_initialize.h"
BENCHMARK_MAIN();
