#include "redismock/redismock.h"
#include "redismock/util.h"

#include "redismodule.h"
#include "module.h"
#include "version.h"

#include "src/buffer.h"
#include "src/index.h"
#include "src/inverted_index.h"
#include "src/index_result.h"
#include "src/query_parser/tokenizer.h"
#include "src/spec.h"
#include "src/tokenize.h"
#include "src/varint.h"
#include "src/hybrid_reader.h"

#include "rmutil/alloc.h"
#include "index_utils.h"

#include <cassert>
#include <vector>
#include <random>
#include <chrono>
#include <iostream>
#include <random>

extern "C" {

static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

    if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                         REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    return RediSearch_InitModuleInternal(ctx, argv, argc);
}

}

void run_hybrid_benchmark(VecSimIndex *index, size_t max_id, size_t d, std::mt19937 rng,
                          std::uniform_real_distribution<> distrib) {
  for (size_t k = 10; k <= 100; k *= 10) {
    for (size_t percent = 100; percent <= 500; percent += 100) {
      size_t NUM_ITERATIONS = 100;
      size_t step = 1000;
      size_t n = max_id / step;

      std::cout << std::endl
                << "ratio between child and index size is: " << percent / 1000.0 << std::endl;
      std::cout << "k is: " << k << std::endl;

      // Create a union iterator - the number of results that the iterator should return is determined
      // based on the current <percent>. Every child iterator of the union contains ids: [i, step+i, 2*step+i , ...]
      InvertedIndex *inv_indices[percent];
      IndexReader *ind_readers[percent];
      for (size_t i = 0; i < percent; i++) {
        InvertedIndex *w = createIndex(n, step, i);
        inv_indices[i] = w;
        IndexReader *r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);
        ind_readers[i] = r;
      }
      IndexIterator **irs = (IndexIterator **)calloc(percent, sizeof(IndexIterator *));
      for (size_t i = 0; i < percent; i++) {
        irs[i] = NewReadIterator(ind_readers[i]);
      }
      IteratorsConfig config{};
      iteratorsConfig_init(&config);
      IndexIterator *ui = NewUnionIterator(irs, percent, NULL, 0, 1, QN_UNION, NULL, &config);
      std::cout << "Expected child res: " << ui->NumEstimated(ui->ctx) << std::endl;

      float query[NUM_ITERATIONS][d];
      KNNVectorQuery top_k_query = {.vector = NULL, .vecLen = d, .k = k, .order = BY_SCORE};
      VecSimQueryParams queryParams = {.hnswRuntimeParams = HNSWRuntimeParams{.efRuntime = 0}};
      HybridIteratorParams hParams = {.index = index,
                                      .dim = d,
                                      .elementType = VecSimType_FLOAT32,
                                      .spaceMetric = VecSimMetric_L2,
                                      .query = top_k_query,
                                      .qParams = queryParams,
                                      .vectorScoreField = (char *)"__v_score",
                                      .ignoreDocScore = true,
                                      .childIt = ui
      };
      QueryError err = {QUERY_OK};
      IndexIterator *hybridIt = NewHybridVectorIterator(hParams, &err);
      assert(!QueryError_HasError(&err));

      // Run in batches mode.
      HybridIterator *hr = (HybridIterator *)hybridIt->ctx;
      RSIndexResult *h = NULL;
      hr->searchMode = VECSIM_HYBRID_BATCHES;

      size_t hnsw_ids[NUM_ITERATIONS][k];
      int count = 0;
      int num_batches_count = 0;
      auto start = std::chrono::high_resolution_clock::now();

      // For every iteration, create a random query and save it.
      for (size_t i = 0; i < NUM_ITERATIONS; i++) {
        count = 0;
        for (size_t j = 0; j < d; ++j) {
          query[i][j] = (float)distrib(rng);
        }
        hr->query.vector = query[i];

        // Run the iterator until it is depleted and save the results.
        while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
          hnsw_ids[i][count++] = h->docId;
        }
        num_batches_count += hr->numIterations;
        //      std::cout << "results: ";
        //      for (size_t j = 0; j < k; j++) {
        //        std::cout << hnsw_ids[i][j] << " - ";
        //      }
        //      std::cout << std::endl;
        if (i != NUM_ITERATIONS - 1) hybridIt->Rewind(hybridIt->ctx);
      }
      auto elapsed = std::chrono::high_resolution_clock::now() - start;
      auto search_time = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
      std::cout << "Avg number of batches: " << (float)num_batches_count / NUM_ITERATIONS
                << std::endl;
      std::cout << "Total search time batches mode: " << search_time / NUM_ITERATIONS
                << std::endl;
      //    std::cout << "results: ";
      //    for (size_t i = 0; i < k; i++) {
      //      std::cout << hnsw_ids[i] << " - ";
      //    }
      //    std::cout << std::endl;

      // Rerun in AD_HOC BF mode with the same queries.
      hybridIt->Rewind(hybridIt->ctx);
      assert(hybridIt->HasNext(hybridIt->ctx));
      hr->searchMode = VECSIM_HYBRID_ADHOC_BF;
      start = std::chrono::high_resolution_clock::now();

      size_t bf_ids[NUM_ITERATIONS][k];
      for (size_t i = 0; i < NUM_ITERATIONS; i++) {
        count = 0;
        hr->query.vector = query[i];
        while (hybridIt->Read(hybridIt->ctx, &h) != INDEXREAD_EOF) {
          bf_ids[i][count++] = h->docId;
        }
        //      std::cout << "results: ";
        //      for (size_t j = 0; j< k; j++) {
        //        std::cout << bf_ids[i][j] << " - ";
        //      }
        //      std::cout << std::endl;
        hybridIt->Rewind(hybridIt->ctx);
      }
      elapsed = std::chrono::high_resolution_clock::now() - start;
      search_time = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
      std::cout << "Total search time ad-hoc mode: " << search_time / NUM_ITERATIONS
                << std::endl;

      // Measure the overall recall.
      int correct = 0;
      for (size_t it = 0; it < NUM_ITERATIONS; it++) {
        for (size_t i = 0; i < k; i++) {
          bool found = false;
          for (size_t j = 0; j < k; j++) {
            if (hnsw_ids[it][i] == bf_ids[it][j]) {
              correct++;
              found = true;
              break;
            }
          }
          if (!found) {
            // std::cout << "iter: "<< it <<" id wasn't found: " << hnsw_ids[it][i] << std::endl;
          }
        }
      }
      std::cout << "Recall is: " << (float)(correct) / (k * NUM_ITERATIONS) << std::endl;

      // Cleanup.
      hybridIt->Free(hybridIt);
      for (size_t i = 0; i < percent; i++) {
        InvertedIndex_Free(inv_indices[i]);
      }
    }
  }
}

void SetUp() {
    const char *arguments[] = {"SAFEMODE", "NOGC"};
    // No arguments..
    RMCK_Bootstrap(my_OnLoad, arguments, 2);
}

void TearDown() {
    RMCK_Shutdown();
    RediSearch_CleanupModule();
}

/**
 * This benchmark is used for comparing between thw two hybrid queries approaches:
 * - BATCHES - get a batch of the next top vectors in the vector index, and then filter, until we reach k results
 * - AD-HOC brute force - compute distance for every vector whose id passes the filter, then take the top k
 * To reproduce and/or run the benchmark for different configurations:
 * 1. Set the parameters as desired (<max_id>, <d>, <M>, index type [HNSW or BF], <percent> and <k>)
 * 2. build the project with `make`
 * 3. Run the executable: `make cpp_tests BENCHMARK=1`
 */
int main(int argc, char **argv) {
    SetUp();
    std::cout << "\nRunning hybrid queries benchmark..." << std::endl;

    for (size_t max_id = 1e5; max_id <= 5e5; max_id += 1e5) {
      for (size_t d = 10; d <= 1000; d *= 10) {
          for (size_t M = 4; M <= 64; M *= 4) {
              // Print index parameters
              std::cout << std::endl << "Index size is: " << max_id << std::endl;
              std::cout << "d is: " << d << std::endl;
              std::cout << "M is: " << M << std::endl;

              // Create vector from random data.
              std::mt19937 rng;
              rng.seed(47);
              std::uniform_real_distribution<> distrib;
              std::vector<float> data(max_id * d);
              for (size_t i = 0; i < max_id * d; ++i) {
                  data[i] = (float) distrib(rng);
              }
              // Create HNSW index. This can be replaced with FLAT index as well (then M parameter
              // is not required).
              VecSimParams params{.algo = VecSimAlgo_HNSWLIB,
                      .hnswParams = HNSWParams{.type = VecSimType_FLOAT32,
                              .dim = d,
                              .metric = VecSimMetric_L2,
                              .initialCapacity = max_id,
                              .M = M}};
              VecSimIndex *index = VecSimIndex_New(&params);
              auto start = std::chrono::high_resolution_clock::now();
              for (size_t i = 0; i < max_id; i++) {
                  VecSimIndex_AddVector(index, data.data() + d * i, (int) i+1);
              }
              auto elapsed = std::chrono::high_resolution_clock::now() - start;
              long long search_time =
                      std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
              std::cout << std::endl << "Total build time: " << search_time << std::endl;
              assert(VecSimIndex_IndexSize(index) == max_id);

              run_hybrid_benchmark(index, max_id, d, rng, distrib);
              VecSimIndex_Free(index);
          }
      }
    }
    TearDown();
}
