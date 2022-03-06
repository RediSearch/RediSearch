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
#include "rmutil/alloc.h"

#include "gtest/gtest.h"

#include "common.h"

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <time.h>
#include <float.h>
#include <vector>
#include <cstdint>
#include <random>
#include <chrono>
#include <iostream>

TEST_F(IndexTest, benchmarkHybridVector) {

  // Create vector index with random data
  std::mt19937 rng;
  rng.seed(47);
  std::uniform_real_distribution<> distrib;
  for (size_t max_id = 2e5; max_id <= 2e5; max_id *=10) {
    for (size_t d=240; d<=240; d+=10) {
      std::vector<float> data(max_id * d);
      //if ((d == 500 && max_id == 5e6) || (d==50 && max_id==5e5)) continue;
      for (size_t M = 32; M <= 32; M *= 2) {
        std::cout << std::endl << std::endl << "d is: " << d << std::endl;
        std::cout << "Index size is: " << max_id << std::endl;
        // std::cout << "M is: " << M << std::endl;
        // size_t max_id = 1e6;
        // size_t d = 100;

        //        char *location = getcwd(NULL, 0);
        //        //auto file_name = std::string(location) + "/../VectorSimilarity/tests/benchmark/data/random-1M-100-l2.hnsw";
        //        auto file_name = std::string(location) +
        //                         "/../VectorSimilarity/tests/benchmark/data/l2-random-size=" + std::to_string(max_id) +
        //                         "d=" + std::to_string(d) + "M=" + std::to_string(M) + ".hnsw";
        //        VecSimIndex_Load(index, file_name.c_str());
        //        char *location = getcwd(NULL, 0);
        //        auto file_name = std::string(location) +
        //                         "/../VectorSimilarity/tests/benchmark/data/l2-random-size=" + std::to_string(max_id) +
        //                         "d=" + std::to_string(d) + "M=" + std::to_string(M) + ".hnsw";
        //        VecSimIndex_Save(index, file_name.c_str());
        for (size_t k = 100; k <= 100; k *= 10) {
          for (size_t percent = 100; percent <= 500; percent += 100) {
            for (size_t i = 0; i < max_id * d; ++i) {
              data[i] = (float)distrib(rng);
            }
            // size_t M = 16;
            // size_t ef = 200;
            VecSimParams params{.algo = VecSimAlgo_BF,
                                .bfParams = BFParams{.type = VecSimType_FLOAT32,
                                                     .dim = d,
                                                     .metric = VecSimMetric_L2,
                                                     .initialCapacity = max_id,
                                                     .blockSize = max_id}};
            VecSimIndex *index = VecSimIndex_New(&params);
            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 1; i <= max_id; i++) {
              VecSimIndex_AddVector(index, data.data() + d * i, (int)i);
            }
            auto elapsed = std::chrono::high_resolution_clock::now() - start;
            long long search_time =
                std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            std::cout << std::endl << "Total build time: " << search_time << std::endl;
            ASSERT_EQ(VecSimIndex_IndexSize(index), max_id);
            size_t NUM_ITERATIONS = 100;
            size_t step = 1000;
            size_t n = max_id / step;
            //size_t k = 100;

            std::cout << std::endl
                      << "ratio between child and index size is: " << percent / 1000.0 << std::endl;
            std::cout << "k is: " << k << std::endl;

            InvertedIndex *inv_indices[percent];
            IndexReader *ind_readers[percent];
            for (size_t i = 0; i < percent; i++) {
              InvertedIndex *w = createIndex(n, step, i);
              inv_indices[i] = w;
              IndexReader *r = NewTermIndexReader(w, NULL, RS_FIELDMASK_ALL, NULL, 1);
              ind_readers[i] = r;
            }

            float query[NUM_ITERATIONS][d];
            KNNVectorQuery top_k_query = {.vector = NULL, .vecLen = d, .k = k, .order = BY_SCORE};
            VecSimQueryParams queryParams;
            queryParams.hnswRuntimeParams.efRuntime = 500;

            RSIndexResult *h = NULL;
            IndexIterator **irs = (IndexIterator **)calloc(percent, sizeof(IndexIterator *));
            for (size_t i = 0; i < percent; i++) {
              irs[i] = NewReadIterator(ind_readers[i]);
            }
            IndexIterator *ui = NewUnionIterator(irs, percent, NULL, 0, 1, QN_UNION, NULL);
            std::cout << "Expected child res: " << ui->NumEstimated(ui->ctx) << std::endl;

            IndexIterator *hybridIt =
                NewHybridVectorIterator(index, (char *)"__v_score", top_k_query, queryParams, ui);

            // run in batches mode
            HybridIterator *hr = (HybridIterator *)hybridIt->ctx;
            hr->mode = VECSIM_HYBRID_BATCHES;

            size_t hnsw_ids[NUM_ITERATIONS][k];
            int count = 0;
            int num_batches_count = 0;
            start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < NUM_ITERATIONS; i++) {
              count = 0;
              for (size_t j = 0; j < d; ++j) {
                query[i][j] = (float)distrib(rng);
              }
              hr->query.vector = query[i];
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
            elapsed = std::chrono::high_resolution_clock::now() - start;
            search_time = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
            std::cout << "Avg number of batches: " << (float)num_batches_count / NUM_ITERATIONS
                      << std::endl;
            std::cout << "Total search time batches mode: " << search_time / NUM_ITERATIONS
                      << std::endl;
            //    std::cout << "results: ";
            //    for (size_t i = 0; i < k; i++) {
            //      std::cout << hnsw_ids[i] << " - ";
            //    }
            //    std::cout << std::endl;

            hybridIt->Rewind(hybridIt->ctx);
            ASSERT_TRUE(hybridIt->HasNext(hybridIt->ctx));

            // Rerun in AD_HOC BF MODE.
            hybridIt->Rewind(hybridIt->ctx);
            hr->mode = VECSIM_HYBRID_ADHOC_BF;
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
            // std::cout << search_time / NUM_ITERATIONS << ", ";
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
                  std::cout << "iter: "<< it <<" id wasn't found: " << hnsw_ids[it][i] << std::endl;
                }
              }
            }
            std::cout << "Recall is: " << (float)(correct) / (k * NUM_ITERATIONS) << std::endl;

            hybridIt->Free(hybridIt);
            for (size_t i = 0; i < percent; i++) {
              InvertedIndex_Free(inv_indices[i]);
            }
            VecSimIndex_Free(index);
          }
        }
      }
    }
  }
}

int main(int argc, char **argv) {


}