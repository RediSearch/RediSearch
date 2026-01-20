/*
 * Simple VecSim benchmark for comparing C++ and Rust backends
 * Tests: Index creation, vector insertion, KNN search, range search
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <cmath>
#include <iomanip>

#include "VecSim/vec_sim.h"

// Rust debug functions (only available when using Rust backend)
#ifdef USE_RUST_VECSIM
extern "C" {
    size_t VecSim_GetRangeSearchIterations();
    size_t VecSim_GetRangeSearchCalls();
    void VecSim_ResetRangeSearchCounters();
}
#endif

using namespace std;
using namespace std::chrono;

struct BenchmarkResult {
    string name;
    long long time_us;
    size_t ops;
    double ops_per_sec;
};

void print_result(const BenchmarkResult& r) {
    cout << left << setw(35) << r.name 
         << right << setw(12) << r.time_us << " μs"
         << setw(15) << fixed << setprecision(0) << r.ops_per_sec << " ops/s" << endl;
}

int main(int argc, char** argv) {
    const size_t NUM_VECTORS = 50000;
    const size_t DIM = 128;
    const size_t NUM_QUERIES = 1000;
    const size_t K = 10;
    const float RANGE = 0.5f;

    cout << "\n========================================" << endl;
    cout << "VecSim Backend Benchmark" << endl;
    cout << "========================================" << endl;
    cout << "Vectors: " << NUM_VECTORS << ", Dim: " << DIM << ", Queries: " << NUM_QUERIES << endl;
    cout << "K: " << K << ", Range: " << RANGE << endl;
    cout << "========================================\n" << endl;

    // Generate random data
    mt19937 rng(42);
    uniform_real_distribution<float> dist(-1.0f, 1.0f);
    
    vector<float> vectors(NUM_VECTORS * DIM);
    for (auto& v : vectors) v = dist(rng);
    
    vector<float> queries(NUM_QUERIES * DIM);
    for (auto& v : queries) v = dist(rng);

    vector<BenchmarkResult> results;

    // ===== HNSW Benchmarks =====
    cout << "--- HNSW Index ---" << endl;
    {
        VecSimParams params{
            .algo = VecSimAlgo_HNSWLIB,
            .algoParams = {.hnswParams = HNSWParams{
                .type = VecSimType_FLOAT32,
                .dim = DIM,
                .metric = VecSimMetric_L2,
                .initialCapacity = NUM_VECTORS,
                .M = 16,
                .efConstruction = 200
            }}
        };

        // Index creation
        auto start = high_resolution_clock::now();
        VecSimIndex* index = VecSimIndex_New(&params);
        auto elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"HNSW: Index creation", elapsed, 1, 1e6 / elapsed});
        print_result(results.back());

        // Vector insertion
        start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_VECTORS; i++) {
            VecSimIndex_AddVector(index, vectors.data() + i * DIM, i + 1);
        }
        elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"HNSW: Insert " + to_string(NUM_VECTORS) + " vectors", elapsed, NUM_VECTORS, (double)NUM_VECTORS * 1e6 / elapsed});
        print_result(results.back());

        // KNN search
        start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_QUERIES; i++) {
            VecSimQueryReply* reply = VecSimIndex_TopKQuery(index, queries.data() + i * DIM, K, nullptr, BY_SCORE);
            VecSimQueryReply_Free(reply);
        }
        elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"HNSW: KNN search (k=" + to_string(K) + ")", elapsed, NUM_QUERIES, (double)NUM_QUERIES * 1e6 / elapsed});
        print_result(results.back());

        // Range search
        // First, do a single range query to check results
        {
            VecSimQueryReply* reply = VecSimIndex_RangeQuery(index, queries.data(), RANGE, nullptr, BY_SCORE);
            size_t num_results = VecSimQueryReply_Len(reply);
            cout << "  (First range query returned " << num_results << " results)" << endl;
            VecSimQueryReply_Free(reply);
        }

#ifdef USE_RUST_VECSIM
        VecSim_ResetRangeSearchCounters();
#endif
        start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_QUERIES; i++) {
            VecSimQueryReply* reply = VecSimIndex_RangeQuery(index, queries.data() + i * DIM, RANGE, nullptr, BY_SCORE);
            VecSimQueryReply_Free(reply);
        }
        elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"HNSW: Range search (r=" + to_string(RANGE) + ")", elapsed, NUM_QUERIES, (double)NUM_QUERIES * 1e6 / elapsed});
        print_result(results.back());
#ifdef USE_RUST_VECSIM
        {
            size_t total_iters = VecSim_GetRangeSearchIterations();
            size_t total_calls = VecSim_GetRangeSearchCalls();
            cout << "  (Rust range search: " << total_calls << " calls, " << total_iters << " total iterations, "
                 << (total_calls > 0 ? total_iters / total_calls : 0) << " avg iters/call)" << endl;
        }
#endif

        VecSimIndex_Free(index);
    }

    // ===== Brute Force Benchmarks =====
    cout << "\n--- Brute Force Index ---" << endl;
    {
        VecSimParams params{
            .algo = VecSimAlgo_BF,
            .algoParams = {.bfParams = BFParams{
                .type = VecSimType_FLOAT32,
                .dim = DIM,
                .metric = VecSimMetric_L2,
                .initialCapacity = NUM_VECTORS
            }}
        };

        // Index creation
        auto start = high_resolution_clock::now();
        VecSimIndex* index = VecSimIndex_New(&params);
        auto elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"BF: Index creation", elapsed, 1, 1e6 / elapsed});
        print_result(results.back());

        // Vector insertion
        start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_VECTORS; i++) {
            VecSimIndex_AddVector(index, vectors.data() + i * DIM, i + 1);
        }
        elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"BF: Insert " + to_string(NUM_VECTORS) + " vectors", elapsed, NUM_VECTORS, (double)NUM_VECTORS * 1e6 / elapsed});
        print_result(results.back());

        // KNN search
        start = high_resolution_clock::now();
        for (size_t i = 0; i < NUM_QUERIES; i++) {
            VecSimQueryReply* reply = VecSimIndex_TopKQuery(index, queries.data() + i * DIM, K, nullptr, BY_SCORE);
            VecSimQueryReply_Free(reply);
        }
        elapsed = duration_cast<microseconds>(high_resolution_clock::now() - start).count();
        results.push_back({"BF: KNN search (k=" + to_string(K) + ")", elapsed, NUM_QUERIES, (double)NUM_QUERIES * 1e6 / elapsed});
        print_result(results.back());

        VecSimIndex_Free(index);
    }

    cout << "\n========================================" << endl;
    cout << "Benchmark Complete" << endl;
    cout << "========================================" << endl;

    return 0;
}

