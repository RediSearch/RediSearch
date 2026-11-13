#include "benchmark/bm_vecsim_svs.h"
#define DATA_TYPE_INDEX_T fp32_index_t

/**************************************
  Basic tests for single value index with fp32 data type.
***************************************/

bool BM_VecSimGeneral::is_multi = false;

size_t BM_VecSimGeneral::n_queries = 20000;
size_t BM_VecSimGeneral::n_vectors = 1000000;
size_t BM_VecSimGeneral::dim = 768;
size_t BM_VecSimGeneral::M = 128;
size_t BM_VecSimGeneral::EF_C = 512;
size_t BM_VecSimGeneral::block_size = 1024;

template <>
VecSimSvsQuantBits BM_VecSimSVS<DATA_TYPE_INDEX_T>::quantBits = VecSimSvsQuant_8;
template <>
const char *BM_VecSimSVS<DATA_TYPE_INDEX_T>::svs_index_tar_file =
    "tests/benchmark/data/svs-dbpedia-cosine-dim768-quant-8.tar.gz";
const char *BM_VecSimGeneral::test_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-1M-vectors.raw";

const char *BM_VecSimGeneral::hnsw_index_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-M64-efc512.hnsw_v3";
template <>
const char *BM_VecSimSVS<DATA_TYPE_INDEX_T>::search_queries_file =
    "tests/benchmark/data/dbpedia-cosine-dim768-test_vectors.raw";

#define BM_FUNC_NAME(bm_func) CONCAT_WITH_UNDERSCORE_ARCH(bm_func, SVS, LVQ8)
#if HAVE_SVS_LVQ // Currently we don't have a serialized version of GlobalQ index.

#include "benchmark/bm_initialization/bm_basics_svs_initialize_fp32.h"
#else
BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimSVS, BM_DUMMY_SVS_LVQ8, DATA_TYPE_INDEX_T)
(benchmark::State &st) {
    // Do nothing.
}
BENCHMARK_REGISTER_F(BM_VecSimSVS, BM_DUMMY_SVS_LVQ8)->Iterations(1);
#endif // HAVE_SVS_LVQ
BENCHMARK_MAIN();
