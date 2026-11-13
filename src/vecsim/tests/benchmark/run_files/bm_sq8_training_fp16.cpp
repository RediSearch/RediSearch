#include "benchmark/bm_vecsim_sq8_training.h"

bool BM_VecSimGeneral::is_multi = false;
size_t BM_VecSimGeneral::n_queries = 101000;
size_t BM_VecSimGeneral::dim = 768;
size_t BM_VecSimGeneral::M = 64;
size_t BM_VecSimGeneral::EF_C = 512;
size_t BM_VecSimGeneral::block_size = 1024;

#define DATA_TYPE_INDEX_T fp16_index_t
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-1M-fp16-vectors.raw";
#include "benchmark/bm_initialization/bm_sq8_training_initialize.h"
BENCHMARK_MAIN();
