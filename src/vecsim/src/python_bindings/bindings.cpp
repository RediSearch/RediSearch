/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/vec_sim.h"
#include "VecSim/algorithms/hnsw/hnsw.h"
#include "VecSim/index_factories/hnsw_factory.h"
#if HAVE_SVS
#include "VecSim/algorithms/svs/svs.h"
#include "VecSim/index_factories/svs_factory.h"
#endif
#include "VecSim/batch_iterator.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"

#include "pybind11/pybind11.h"
#include "pybind11/numpy.h"
#include "pybind11/stl.h"
#include <cmath>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <thread>
#include <VecSim/algorithms/hnsw/hnsw_single.h>
#include <VecSim/algorithms/brute_force/brute_force_single.h>
#include "mock_thread_pool.h"

namespace py = pybind11;

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;

using QuantizationMean = py::array_t<float, py::array::c_style | py::array::forcecast>;

std::optional<QuantizationMean> validateQuantizationMean(const py::object &quantization_mean,
                                                         const HNSWParams &params) {
    if (quantization_mean.is_none()) {
        return std::nullopt;
    }
    if (params.quantType != VecSimQuant_SQ8) {
        throw py::value_error("quantization_mean requires quantType VecSimQuant_SQ8");
    }

    QuantizationMean mean(quantization_mean);
    if (mean.ndim() != 1) {
        throw py::value_error("quantization_mean must be a one-dimensional array");
    }
    if (static_cast<size_t>(mean.shape(0)) != params.dim) {
        throw py::value_error("quantization_mean length must match the index dimension");
    }

    const auto values = mean.unchecked<1>();
    for (py::ssize_t i = 0; i < values.shape(0); ++i) {
        if (!std::isfinite(values(i))) {
            throw py::value_error(
                "quantization_mean values must be finite after conversion to float32");
        }
    }
    return mean;
}

// Helper function that iterates query results and wrap them in python numpy object -
// a tuple of two 2D arrays: (labels, distances)
py::object wrap_results(VecSimQueryReply **res, size_t num_res, size_t num_queries = 1) {
    auto *data_numpy_l = new long[num_res * num_queries];
    auto *data_numpy_d = new double[num_res * num_queries];
    // Default "padding" for the entries that will stay empty (in case of less than k results return
    // in KNN, or results of range queries with number of results lower than the maximum in the
    // batch (which determines the arrays' shape)
    std::fill_n(data_numpy_l, num_res * num_queries, -1);
    std::fill_n(data_numpy_d, num_res * num_queries, -1.0);

    for (size_t i = 0; i < num_queries; i++) {
        VecSimQueryReply_Iterator *iterator = VecSimQueryReply_GetIterator(res[i]);
        size_t res_ind = i * num_res;
        while (VecSimQueryReply_IteratorHasNext(iterator)) {
            VecSimQueryResult *item = VecSimQueryReply_IteratorNext(iterator);
            data_numpy_d[res_ind] = VecSimQueryResult_GetScore(item);
            data_numpy_l[res_ind++] = (long)VecSimQueryResult_GetId(item);
        }
        VecSimQueryReply_IteratorFree(iterator);
        VecSimQueryReply_Free(res[i]);
    }

    py::capsule free_when_done_l(data_numpy_l, [](void *labels) { delete[] (long *)labels; });
    py::capsule free_when_done_d(data_numpy_d, [](void *dists) { delete[] (double *)dists; });
    return py::make_tuple(
        py::array_t<long>(
            {(size_t)num_queries, num_res},         // shape
            {num_res * sizeof(long), sizeof(long)}, // C-style contiguous strides for size_t
            data_numpy_l,                           // the data pointer (labels array)
            free_when_done_l),
        py::array_t<double>(
            {(size_t)num_queries, num_res},             // shape
            {num_res * sizeof(double), sizeof(double)}, // C-style contiguous strides for double
            data_numpy_d,                               // the data pointer (distances array)
            free_when_done_d));
}

class PyBatchIterator {
private:
    // Hold the index pointer, so that it will be destroyed **after** the batch iterator. Hence,
    // the index field should come before the iterator field.
    std::shared_ptr<VecSimIndex> vectorIndex;
    std::shared_ptr<VecSimBatchIterator> batchIterator;

public:
    PyBatchIterator(const std::shared_ptr<VecSimIndex> &vecIndex,
                    const std::shared_ptr<VecSimBatchIterator> &batchIterator)
        : vectorIndex(vecIndex), batchIterator(batchIterator) {}

    bool hasNext() { return VecSimBatchIterator_HasNext(batchIterator.get()); }

    py::object getNextResults(size_t n_res, VecSimQueryReply_Order order) {
        VecSimQueryReply *results;
        {
            // We create this object inside the scope to enable parallel execution of the batch
            // iterator from different Python threads.
            py::gil_scoped_release py_gil;
            results = VecSimBatchIterator_Next(batchIterator.get(), n_res, order);
        }
        // The number of results may be lower than n_res, if there are less than n_res remaining
        // vectors in the index that hadn't been returned yet.
        size_t actual_n_res = VecSimQueryReply_Len(results);
        return wrap_results(&results, actual_n_res);
    }
    void reset() { VecSimBatchIterator_Reset(batchIterator.get()); }
    virtual ~PyBatchIterator() = default;
};

// @input or @query arguments are a py::object object. (numpy arrays are acceptable)
class PyVecSimIndex {
private:
    template <typename DataType, typename DistType, typename NPArrayType = DataType>
    inline py::object rawVectorsAsNumpy(labelType label, size_t dim) {
        std::vector<std::vector<DataType>> vectors;
        if (auto *tiered =
                dynamic_cast<VecSimTieredIndex<DataType, DistType> *>(this->index.get())) {
            tiered->getDataByLabel(label, vectors);
        } else {
            dynamic_cast<VecSimIndexAbstract<DataType, DistType> *>(this->index.get())
                ->getDataByLabel(label, vectors);
        }
        size_t n_vectors = vectors.size();
        auto *data_numpy = new NPArrayType[n_vectors * dim];

        // Copy the vector blobs into one contiguous array of data, and free the original buffer
        // afterwards.
        if constexpr (std::is_same_v<DataType, bfloat16>) {
            for (size_t i = 0; i < n_vectors; i++) {
                for (size_t j = 0; j < dim; j++) {
                    data_numpy[i * dim + j] = vecsim_types::bfloat16_to_float32(vectors[i][j]);
                }
            }
        } else if constexpr (std::is_same_v<DataType, float16>) {
            for (size_t i = 0; i < n_vectors; i++) {
                for (size_t j = 0; j < dim; j++) {
                    data_numpy[i * dim + j] = vecsim_types::FP16_to_FP32(vectors[i][j]);
                }
            }
        } else {
            for (size_t i = 0; i < n_vectors; i++) {
                memcpy(data_numpy + i * dim, vectors[i].data(), dim * sizeof(NPArrayType));
            }
        }

        py::capsule free_when_done(data_numpy,
                                   [](void *vector_data) { delete[] (NPArrayType *)vector_data; });
        return py::array_t<NPArrayType>(
            {n_vectors, dim}, // shape
            {dim * sizeof(NPArrayType),
             sizeof(NPArrayType)}, // C-style contiguous strides for the data type
            data_numpy,            // the data pointer
            free_when_done);
    }

protected:
    std::shared_ptr<VecSimIndex> index;

    static std::shared_ptr<VecSimIndex> ownIndex(VecSimIndex *native_index) {
        return std::shared_ptr<VecSimIndex>(native_index, VecSimIndex_Free);
    }

    // A null factory result must never reach a shared_ptr: it faults on first use, and for a
    // tiered index it also leaves the mock thread pool's context without the strong reference
    // that its destructor dereferences.
    static std::shared_ptr<VecSimIndex> createIndex(const VecSimParams &params) {
        auto *native_index = VecSimIndex_New(&params);
        if (!native_index) {
            throw std::invalid_argument("Unsupported vector index parameters");
        }
        return ownIndex(native_index);
    }

    // Deserialization fails on unreadable or malformed input rather than on bad parameters.
    static std::shared_ptr<VecSimIndex> loadIndexOrThrow(VecSimIndex *native_index) {
        if (!native_index) {
            throw std::runtime_error("Index creation failed");
        }
        return ownIndex(native_index);
    }

    inline VecSimQueryReply *searchKnnInternal(const char *query, size_t k,
                                               VecSimQueryParams *query_params) {
        return VecSimIndex_TopKQuery(index.get(), query, k, query_params, BY_SCORE);
    }

    inline void addVectorInternal(const char *vector_data, size_t id) {
        VecSimIndex_AddVector(index.get(), vector_data, id);
    }

    inline VecSimQueryReply *searchRangeInternal(const char *query, double radius,
                                                 VecSimQueryParams *query_params) {
        return VecSimIndex_RangeQuery(index.get(), query, radius, query_params, BY_SCORE);
    }

public:
    PyVecSimIndex() = default;

    explicit PyVecSimIndex(const VecSimParams &params) : index(createIndex(params)) {}

    void addVector(const py::object &input, size_t id) {
        py::array vector_data(input);
        py::gil_scoped_release py_gil;
        addVectorInternal((const char *)vector_data.data(0), id);
    }

    void deleteVector(size_t id) { VecSimIndex_DeleteVector(index.get(), id); }

    py::object knn(const py::object &input, size_t k, VecSimQueryParams *query_params) {
        py::array query(input);
        VecSimQueryReply *res;
        {
            py::gil_scoped_release py_gil;
            res = searchKnnInternal((const char *)query.data(0), k, query_params);
        }
        return wrap_results(&res, k);
    }

    py::object range(const py::object &input, double radius, VecSimQueryParams *query_params) {
        py::array query(input);
        VecSimQueryReply *res;
        {
            py::gil_scoped_release py_gil;
            res = searchRangeInternal((const char *)query.data(0), radius, query_params);
        }
        return wrap_results(&res, VecSimQueryReply_Len(res));
    }

    size_t indexSize() { return VecSimIndex_IndexSize(index.get()); }

    VecSimType indexType() { return index->basicInfo().type; }

    size_t indexMemory() { return this->index->getAllocationSize(); }

    virtual PyBatchIterator createBatchIterator(const py::object &input,
                                                VecSimQueryParams *query_params) {
        py::array query(input);
        auto py_batch_ptr = std::shared_ptr<VecSimBatchIterator>(
            VecSimBatchIterator_New(index.get(), (const char *)query.data(0), query_params),
            VecSimBatchIterator_Free);
        return PyBatchIterator(index, py_batch_ptr);
    }

    // Not VecSimTieredIndex_GC(): that is a no-op unless the index is tiered, while
    // SVSIndex::runGC() compacts and rebuilds reverse edges on a plain index too.
    void runGC() { index->runGC(); }

    VecSimRelabelCode relabelVector(labelType old_label, labelType new_label) {
        py::gil_scoped_release py_gil;
        return VecSimIndex_RelabelVector(index.get(), old_label, new_label);
    }

    // Accepts a single vector or a 2D array of them, so that the multi-value case reads as one
    // call with the whole new contents of the label.
    VecSimUpdateCode updateVectors(labelType label, const py::object &input) {
        // The vectors are handed over as one packed buffer, which the index walks with a stride of
        // its own element size - so a caller's array that is not C-contiguous (a strided view, or
        // Fortran order) has to be copied into packed form first, or every vector after the first
        // would be read from the wrong offset. `addVector` and the parallel helpers get away
        // without this because they index one row at a time, which numpy resolves through the
        // array's own strides. The dtype is left alone: it has to keep matching the index's.
        py::array vectors = py::array::ensure(input, py::array::c_style);
        if (!vectors) {
            throw std::runtime_error("Input vectors must be a numpy array");
        }
        if (vectors.ndim() > 2) {
            throw std::runtime_error("Input vectors array must be 1D or 2D");
        }
        // A single vector may be passed on its own rather than as a one-row array, so that the
        // common case reads the way `add_vector` does.
        const size_t n = vectors.ndim() > 1 ? vectors.shape(0) : (vectors.size() == 0 ? 0 : 1);
        // An empty update has nothing to point at, and `data(0)` on an empty array is out of range.
        const char *blobs = n == 0 ? nullptr : (const char *)vectors.data(0);
        py::gil_scoped_release py_gil;
        return VecSimIndex_UpdateVectors(index.get(), label, blobs, n);
    }

    py::object getVector(labelType label) {
        VecSimIndexBasicInfo info = index->basicInfo();
        size_t dim = info.dim;
        if (info.type == VecSimType_FLOAT32) {
            return rawVectorsAsNumpy<float, float>(label, dim);
        } else if (info.type == VecSimType_FLOAT64) {
            return rawVectorsAsNumpy<double, double>(label, dim);
        } else if (info.type == VecSimType_BFLOAT16) {
            return rawVectorsAsNumpy<bfloat16, float, float>(label, dim);
        } else if (info.type == VecSimType_FLOAT16) {
            return rawVectorsAsNumpy<float16, float, float>(label, dim);
        } else if (info.type == VecSimType_INT8) {
            return rawVectorsAsNumpy<int8_t, float>(label, dim);
        } else if (info.type == VecSimType_UINT8) {
            return rawVectorsAsNumpy<uint8_t, float>(label, dim);
        } else {
            throw std::runtime_error("Invalid vector data type");
        }
    }

    virtual ~PyVecSimIndex() = default; // Delete function was given to the shared pointer object
};

class PyHNSWLibIndex : public PyVecSimIndex {
private:
    std::shared_ptr<std::shared_mutex>
        indexGuard; // to protect parallel operations on the index. Make sure to release the GIL
                    // while locking the mutex.
    template <typename search_param_t> // size_t/double for KNN/range queries.
    using QueryFunc =
        std::function<VecSimQueryReply *(const char *, search_param_t, VecSimQueryParams *)>;

    template <typename search_param_t> // size_t/double for KNN / range queries.
    void runParallelQueries(const py::array &queries, size_t n_queries, search_param_t param,
                            VecSimQueryParams *query_params, int n_threads,
                            QueryFunc<search_param_t> queryFunc, VecSimQueryReply **results) {

        // Use number of hardware cores as default number of threads, unless specified otherwise.
        if (n_threads <= 0) {
            n_threads = (int)std::thread::hardware_concurrency();
        }
        std::atomic_int global_counter(0);

        auto parallel_search = [&](const py::array &items) {
            while (true) {
                int ind = global_counter.fetch_add(1);
                if (ind >= n_queries) {
                    break;
                }
                {
                    std::shared_lock<std::shared_mutex> lock(*indexGuard);
                    results[ind] = queryFunc((const char *)items.data(ind), param, query_params);
                }
            }
        };
        std::thread thread_objs[n_threads];
        {
            // Release python GIL while threads are running.
            py::gil_scoped_release py_gil;
            for (size_t i = 0; i < n_threads; i++) {
                thread_objs[i] = std::thread(parallel_search, queries);
            }
            for (size_t i = 0; i < n_threads; i++) {
                thread_objs[i].join();
            }
        }
    }

public:
    explicit PyHNSWLibIndex(const HNSWParams &hnsw_params) {
        VecSimParams params = {.algo = VecSimAlgo_HNSWLIB,
                               .algoParams = {.hnswParams = HNSWParams{hnsw_params}}};
        this->index = createIndex(params);
        this->indexGuard = std::make_shared<std::shared_mutex>();
    }

    // @params is required only in V1.
    explicit PyHNSWLibIndex(const std::string &location) {
        this->index = loadIndexOrThrow(HNSWFactory::NewIndex(location));
        this->indexGuard = std::make_shared<std::shared_mutex>();
    }

    void setDefaultEf(size_t ef) {
        auto *hnsw = reinterpret_cast<HNSWIndex<float, float> *>(index.get());
        hnsw->setEf(ef);
    }
    void saveIndex(const std::string &location) {
        auto type = VecSimIndex_BasicInfo(this->index.get()).type;
        if (type == VecSimType_FLOAT32) {
            auto *hnsw = dynamic_cast<HNSWIndex<float, float> *>(index.get());
            hnsw->saveIndex(location);
        } else if (type == VecSimType_FLOAT64) {
            auto *hnsw = dynamic_cast<HNSWIndex<double, double> *>(index.get());
            hnsw->saveIndex(location);
        } else if (type == VecSimType_BFLOAT16) {
            auto *hnsw = dynamic_cast<HNSWIndex<bfloat16, float> *>(index.get());
            hnsw->saveIndex(location);
        } else if (type == VecSimType_FLOAT16) {
            auto *hnsw = dynamic_cast<HNSWIndex<float16, float> *>(index.get());
            hnsw->saveIndex(location);
        } else if (type == VecSimType_INT8) {
            auto *hnsw = dynamic_cast<HNSWIndex<int8_t, float> *>(index.get());
            hnsw->saveIndex(location);
        } else if (type == VecSimType_UINT8) {
            auto *hnsw = dynamic_cast<HNSWIndex<uint8_t, float> *>(index.get());
            hnsw->saveIndex(location);
        } else {
            throw std::runtime_error("Invalid index data type");
        }
    }
    py::object searchKnnParallel(const py::object &input, size_t k, VecSimQueryParams *query_params,
                                 int n_threads) {

        py::array queries(input);
        if (queries.ndim() != 2) {
            throw std::runtime_error("Input queries array must be 2D array");
        }
        size_t n_queries = queries.shape(0);
        QueryFunc<size_t> searchKnnWrapper(
            [this](const char *query_, size_t k_,
                   VecSimQueryParams *query_params_) -> VecSimQueryReply * {
                return this->searchKnnInternal(query_, k_, query_params_);
            });
        VecSimQueryReply *results[n_queries];
        runParallelQueries<size_t>(queries, n_queries, k, query_params, n_threads, searchKnnWrapper,
                                   results);
        return wrap_results(results, k, n_queries);
    }
    py::object searchRangeParallel(const py::object &input, double radius,
                                   VecSimQueryParams *query_params, int n_threads) {
        py::array queries(input);
        if (queries.ndim() != 2) {
            throw std::runtime_error("Input queries array must be 2D array");
        }
        size_t n_queries = queries.shape(0);
        QueryFunc<double> searchRangeWrapper(
            [this](const char *query_, double radius_,
                   VecSimQueryParams *query_params_) -> VecSimQueryReply * {
                return this->searchRangeInternal(query_, radius_, query_params_);
            });
        VecSimQueryReply *results[n_queries];
        runParallelQueries<double>(queries, n_queries, radius, query_params, n_threads,
                                   searchRangeWrapper, results);
        size_t max_results_num = 1;
        for (size_t i = 0; i < n_queries; i++) {
            if (VecSimQueryReply_Len(results[i]) > max_results_num) {
                max_results_num = VecSimQueryReply_Len(results[i]);
            }
        }
        // We return 2D numpy array of results (labels and distances), use padding of "-1" in the
        // empty entries of the matrices.
        return wrap_results(results, max_results_num, n_queries);
    }

    void addVectorsParallel(const py::object &input, const py::object &vectors_labels,
                            int n_threads) {
        py::array vectors_data(input);
        py::array_t<labelType, py::array::c_style | py::array::forcecast> labels(vectors_labels);

        if (vectors_data.ndim() != 2) {
            throw std::runtime_error("Input vectors data array must be 2D array");
        }
        if (labels.ndim() != 1) {
            throw std::runtime_error("Input vectors labels array must be 1D array");
        }
        if (vectors_data.shape(0) != labels.shape(0)) {
            throw std::runtime_error(
                "The first dim of vectors data and labels arrays must be equal");
        }
        size_t n_vectors = vectors_data.shape(0);
        // Use number of hardware cores as default number of threads, unless specified otherwise.
        if (n_threads <= 0) {
            n_threads = (int)std::thread::hardware_concurrency();
        }
        // The decision as to when to allocate a new block is made by the index internally in the
        // "addVector" function, where there is an internal counter that is incremented for each
        // vector. To ensure that the thread which is taking the write lock is the one that performs
        // the resizing, we make sure that no other thread is allowed to bypass the thread for which
        // the global counter is a multiple of the block size. Hence, we use the barrier lock and
        // lock in every iteration to ensure we acquire the right lock (read/write) based on the
        // global counter, so threads won't call "addVector" with the inappropriate lock.
        std::mutex barrier;
        std::atomic<size_t> global_counter{};
        size_t block_size = VecSimIndex_BasicInfo(this->index.get()).blockSize;
        auto parallel_insert =
            [&](const py::array &data,
                const py::array_t<labelType, py::array::c_style | py::array::forcecast> &labels) {
                while (true) {
                    bool exclusive = true;
                    barrier.lock();
                    int ind = global_counter++;
                    if (ind >= n_vectors) {
                        barrier.unlock();
                        break;
                    }
                    if (ind % block_size != 0) {
                        // Read lock for normal operations
                        indexGuard->lock_shared();
                        exclusive = false;
                    } else {
                        // Exclusive lock for block resizing operations
                        indexGuard->lock();
                    }
                    barrier.unlock();
                    this->addVectorInternal((const char *)data.data(ind), labels.at(ind));
                    exclusive ? indexGuard->unlock() : indexGuard->unlock_shared();
                }
            };
        std::thread thread_objs[n_threads];
        {
            // Release python GIL while threads are running.
            py::gil_scoped_release py_gil;
            for (size_t i = 0; i < n_threads; i++) {
                thread_objs[i] = std::thread(parallel_insert, vectors_data, labels);
            }
            for (size_t i = 0; i < n_threads; i++) {
                thread_objs[i].join();
            }
        }
    }

    bool checkIntegrity() {
        auto type = VecSimIndex_BasicInfo(this->index.get()).type;
        if (type == VecSimType_FLOAT32) {
            return dynamic_cast<HNSWIndex<float, float> *>(this->index.get())
                ->checkIntegrity()
                .valid_state;
        } else if (type == VecSimType_FLOAT64) {
            return dynamic_cast<HNSWIndex<double, double> *>(this->index.get())
                ->checkIntegrity()
                .valid_state;
        } else if (type == VecSimType_BFLOAT16) {
            return dynamic_cast<HNSWIndex<bfloat16, float> *>(this->index.get())
                ->checkIntegrity()
                .valid_state;
        } else if (type == VecSimType_FLOAT16) {
            return dynamic_cast<HNSWIndex<float16, float> *>(this->index.get())
                ->checkIntegrity()
                .valid_state;
        } else if (type == VecSimType_INT8) {
            return dynamic_cast<HNSWIndex<int8_t, float> *>(this->index.get())
                ->checkIntegrity()
                .valid_state;
        } else if (type == VecSimType_UINT8) {
            return dynamic_cast<HNSWIndex<uint8_t, float> *>(this->index.get())
                ->checkIntegrity()
                .valid_state;
        } else {
            throw std::runtime_error("Invalid index data type");
        }
    }
    PyBatchIterator createBatchIterator(const py::object &input,
                                        VecSimQueryParams *query_params) override {

        py::array query(input);
        py::gil_scoped_release py_gil;
        // Passing indexGuardPtr by value, so that the refCount of the mutex
        auto del = [indexGuardPtr = this->indexGuard](VecSimBatchIterator *pyBatchIter) {
            VecSimBatchIterator_Free(pyBatchIter);
            indexGuardPtr->unlock_shared();
        };
        indexGuard->lock_shared();
        auto py_batch_ptr = std::shared_ptr<VecSimBatchIterator>(
            VecSimBatchIterator_New(index.get(), (const char *)query.data(0), query_params), del);
        return PyBatchIterator(index, py_batch_ptr);
    }
};

class PyTieredIndex : public PyVecSimIndex {
protected:
    tieredIndexMock mock_thread_pool;
    bool manual_jobs;

    void initializeIndex(const VecSimParams &params) {
        try {
            this->index = createIndex(params);
        } catch (...) {
            // The mock destructor requires a valid index whenever its context is present.
            mock_thread_pool.reset_ctx();
            throw;
        }
        mock_thread_pool.ctx->index_strong_ref = this->index;
    }

    VecSimIndexAbstract<float, float> *getFlatBuffer() {
        return reinterpret_cast<VecSimTieredIndex<float, float> *>(this->index.get())
            ->getFlatBufferIndex();
    }

    TieredIndexParams getTieredIndexParams(size_t buffer_limit) {
        // Create TieredIndexParams using the mock thread pool.
        return TieredIndexParams{
            .jobQueue = &(this->mock_thread_pool.jobQ),
            .jobQueueCtx = this->mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = buffer_limit,
        };
    }

public:
    explicit PyTieredIndex(bool manual_jobs = false) : manual_jobs(manual_jobs) {
        if (!manual_jobs) {
            mock_thread_pool.init_threads();
        }
    }

    // Testing-only control for observing a tiered index while migration is partially complete.
    // Keep the GIL held while executing jobs so Python callers cannot consume the queue
    // concurrently. Any live batch iterator must be destroyed first because it retains the
    // tiered index guard for its full lifetime.
    size_t runPendingJobs(size_t max_jobs = 1) {
        if (!manual_jobs) {
            throw std::runtime_error("Pending jobs can only be run in manual_jobs mode");
        }

        size_t jobs_run = 0;
        while (jobs_run < max_jobs) {
            {
                std::lock_guard<std::mutex> lock(mock_thread_pool.queue_guard);
                if (mock_thread_pool.jobQ.empty()) {
                    break;
                }
            }
            mock_thread_pool.thread_iteration();
            jobs_run++;
        }
        return jobs_run;
    }

    void WaitForIndex(size_t waiting_duration = 10) {
        if (manual_jobs) {
            while (runPendingJobs() != 0) {
            }
        } else {
            mock_thread_pool.thread_pool_wait(waiting_duration);
        }
    }

    size_t getFlatIndexSize() { return getFlatBuffer()->indexLabelCount(); }

    size_t getThreadsNum() { return manual_jobs ? 0 : mock_thread_pool.thread_pool_size; }

    size_t getBufferLimit() {
        return reinterpret_cast<VecSimTieredIndex<float, float> *>(this->index.get())
            ->getFlatBufferLimit();
    }
};

class PyTiered_HNSWIndex : public PyTieredIndex {
public:
    explicit PyTiered_HNSWIndex(const HNSWParams &hnsw_params,
                                const TieredHNSWParams &tiered_hnsw_params, size_t buffer_limit,
                                bool manual_jobs = false)
        : PyTieredIndex(manual_jobs) {

        // Create primaryIndexParams and specific params for hnsw tiered index.
        VecSimParams primary_index_params = {.algo = VecSimAlgo_HNSWLIB,
                                             .algoParams = {.hnswParams = HNSWParams{hnsw_params}}};

        auto tiered_params = this->getTieredIndexParams(buffer_limit);
        tiered_params.primaryIndexParams = &primary_index_params;
        tiered_params.specificParams.tieredHnswParams = tiered_hnsw_params;

        // Create VecSimParams for TieredIndexParams
        VecSimParams params = {.algo = VecSimAlgo_TIERED,
                               .algoParams = {.tieredParams = TieredIndexParams{tiered_params}}};

        initializeIndex(params);
    }

    size_t HNSWLabelCount() {
        return this->index->debugInfo().tieredInfo.backendCommonInfo.indexLabelCount;
    }
};

class PyBFIndex : public PyVecSimIndex {
public:
    explicit PyBFIndex(const BFParams &bf_params) {
        VecSimParams params = {.algo = VecSimAlgo_BF,
                               .algoParams = {.bfParams = BFParams{bf_params}}};
        this->index = std::shared_ptr<VecSimIndex>(VecSimIndex_New(&params), VecSimIndex_Free);
    }
};

#if HAVE_SVS
class PySVSIndex : public PyVecSimIndex {
public:
    explicit PySVSIndex(const SVSParams &svs_params) {
        VecSimParams params = {.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = svs_params}};
        this->index = std::shared_ptr<VecSimIndex>(VecSimIndex_New(&params), VecSimIndex_Free);
        if (!this->index) {
            throw std::runtime_error("Index creation failed");
        }
    }

    explicit PySVSIndex(const std::string &location, const SVSParams &svs_params) {
        VecSimParams params = {.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = svs_params}};
        this->index =
            std::shared_ptr<VecSimIndex>(SVSFactory::NewIndex(location, &params), VecSimIndex_Free);
        if (!this->index) {
            throw std::runtime_error("Index creation failed");
        }
    }

    void addVectorsParallel(const py::object &input, const py::object &vectors_labels) {
        py::array vectors_data(input);
        // py::array labels(vectors_labels);
        py::array_t<labelType, py::array::c_style | py::array::forcecast> labels(vectors_labels);

        if (vectors_data.ndim() != 2) {
            throw std::runtime_error("Input vectors data array must be 2D array");
        }
        if (labels.ndim() != 1) {
            throw std::runtime_error("Input vectors labels array must be 1D array");
        }
        if (vectors_data.shape(0) != labels.shape(0)) {
            throw std::runtime_error(
                "The first dim of vectors data and labels arrays must be equal");
        }
        size_t n_vectors = vectors_data.shape(0);

        auto svs_index = dynamic_cast<SVSIndexBase *>(this->index.get());
        assert(svs_index);
        svs_index->addVectors(vectors_data.data(), labels.data(), n_vectors);
    }

    void checkIntegrity() {
        auto svs_index = dynamic_cast<SVSIndexBase *>(this->index.get());
        assert(svs_index);
        try {
            svs_index->checkIntegrity();
        } catch (const std::exception &e) {
            throw std::runtime_error(std::string("SVSIndex integrity check failed: ") + e.what());
        }
    }

    void saveIndex(const std::string &location) {
        auto svs_index = dynamic_cast<SVSIndexBase *>(this->index.get());
        assert(svs_index);
        svs_index->saveIndex(location);
    }

    void loadIndex(const std::string &location) {
        auto svs_index = dynamic_cast<SVSIndexBase *>(this->index.get());
        assert(svs_index);
        svs_index->loadIndex(location);
    }

    size_t getLabelsCount() const { return this->index->debugInfo().commonInfo.indexLabelCount; }
};

class PyTiered_SVSIndex : public PyTieredIndex {
public:
    explicit PyTiered_SVSIndex(const SVSParams &svs_params,
                               const TieredSVSParams &tiered_svs_params, size_t buffer_limit) {

        // Create primaryIndexParams and specific params for svs tiered index.
        VecSimParams primary_index_params = {.algo = VecSimAlgo_SVS,
                                             .algoParams = {.svsParams = svs_params}};

        if (primary_index_params.algoParams.svsParams.num_threads == 0) {
            primary_index_params.algoParams.svsParams.num_threads =
                this->mock_thread_pool.thread_pool_size; // Use the mock thread pool size as default
        }

        auto tiered_params = this->getTieredIndexParams(buffer_limit);
        tiered_params.primaryIndexParams = &primary_index_params;
        tiered_params.specificParams.tieredSVSParams = tiered_svs_params;

        // Create VecSimParams for TieredIndexParams
        VecSimParams params = {.algo = VecSimAlgo_TIERED,
                               .algoParams = {.tieredParams = tiered_params}};

        this->index = std::shared_ptr<VecSimIndex>(VecSimIndex_New(&params), VecSimIndex_Free);

        // Set the created tiered index in the index external context.
        this->mock_thread_pool.ctx->index_strong_ref = this->index;
    }

    size_t SVSLabelCount() {
        return this->index->debugInfo().tieredInfo.backendCommonInfo.indexLabelCount;
    }
};
#endif

PYBIND11_MODULE(VecSim, m) {
    py::enum_<VecSimAlgo>(m, "VecSimAlgo")
        .value("VecSimAlgo_HNSWLIB", VecSimAlgo_HNSWLIB)
        .value("VecSimAlgo_BF", VecSimAlgo_BF)
        .value("VecSimAlgo_SVS", VecSimAlgo_SVS)
        .export_values();

    py::enum_<VecSimType>(m, "VecSimType")
        .value("VecSimType_FLOAT32", VecSimType_FLOAT32)
        .value("VecSimType_FLOAT64", VecSimType_FLOAT64)
        .value("VecSimType_BFLOAT16", VecSimType_BFLOAT16)
        .value("VecSimType_FLOAT16", VecSimType_FLOAT16)
        .value("VecSimType_INT8", VecSimType_INT8)
        .value("VecSimType_UINT8", VecSimType_UINT8)
        .value("VecSimType_INT32", VecSimType_INT32)
        .value("VecSimType_INT64", VecSimType_INT64)
        .export_values();

    py::enum_<VecSimMetric>(m, "VecSimMetric")
        .value("VecSimMetric_L2", VecSimMetric_L2)
        .value("VecSimMetric_IP", VecSimMetric_IP)
        .value("VecSimMetric_Cosine", VecSimMetric_Cosine)
        .export_values();

    py::enum_<VecSimQuantType>(m, "VecSimQuantType")
        .value("VecSimQuant_NONE", VecSimQuant_NONE)
        .value("VecSimQuant_SQ8", VecSimQuant_SQ8)
        .export_values();

    py::enum_<VecSimOptionMode>(m, "VecSimOptionMode")
        .value("VecSimOption_AUTO", VecSimOption_AUTO)
        .value("VecSimOption_ENABLE", VecSimOption_ENABLE)
        .value("VecSimOption_DISABLE", VecSimOption_DISABLE)
        .export_values();

    py::enum_<VecSimQueryReply_Order>(m, "VecSimQueryReply_Order")
        .value("BY_SCORE", BY_SCORE)
        .value("BY_ID", BY_ID)
        .export_values();

    py::class_<HNSWParams>(m, "HNSWParams")
        .def(py::init())
        .def_readwrite("type", &HNSWParams::type)
        .def_readwrite("dim", &HNSWParams::dim)
        .def_readwrite("metric", &HNSWParams::metric)
        .def_readwrite("multi", &HNSWParams::multi)
        .def_readwrite("initialCapacity", &HNSWParams::initialCapacity)
        .def_readwrite("M", &HNSWParams::M)
        .def_readwrite("efConstruction", &HNSWParams::efConstruction)
        .def_readwrite("efRuntime", &HNSWParams::efRuntime)
        .def_readwrite("epsilon", &HNSWParams::epsilon)
        .def_readwrite("quantType", &HNSWParams::quantType);

    py::class_<BFParams>(m, "BFParams")
        .def(py::init())
        .def_readwrite("type", &BFParams::type)
        .def_readwrite("dim", &BFParams::dim)
        .def_readwrite("metric", &BFParams::metric)
        .def_readwrite("multi", &BFParams::multi)
        .def_readwrite("initialCapacity", &BFParams::initialCapacity)
        .def_readwrite("blockSize", &BFParams::blockSize);

    py::enum_<VecSimRelabelCode>(m, "VecSimRelabelCode")
        .value("VecSimRelabel_OK", VecSimRelabel_OK)
        .value("VecSimRelabel_OldLabelMissing", VecSimRelabel_OldLabelMissing)
        .value("VecSimRelabel_NewLabelTaken", VecSimRelabel_NewLabelTaken)
        .value("VecSimRelabel_SameLabel", VecSimRelabel_SameLabel)
        .value("VecSimRelabel_Unsupported", VecSimRelabel_Unsupported)
        .export_values();

    py::enum_<VecSimUpdateCode>(m, "VecSimUpdateCode")
        .value("VecSimUpdate_OK", VecSimUpdate_OK)
        .value("VecSimUpdate_MultiNotSupported", VecSimUpdate_MultiNotSupported)
        .value("VecSimUpdate_Unsupported", VecSimUpdate_Unsupported)
        .value("VecSimUpdate_Failed", VecSimUpdate_Failed)
        .export_values();

    py::enum_<VecSimSvsQuantBits>(m, "VecSimSvsQuantBits")
        .value("VecSimSvsQuant_NONE", VecSimSvsQuant_NONE)
        .value("VecSimSvsQuant_Scalar", VecSimSvsQuant_Scalar)
        .value("VecSimSvsQuant_4", VecSimSvsQuant_4)
        .value("VecSimSvsQuant_8", VecSimSvsQuant_8)
        .value("VecSimSvsQuant_4x4", VecSimSvsQuant_4x4)
        .value("VecSimSvsQuant_4x8", VecSimSvsQuant_4x8)
        .value("VecSimSvsQuant_4x8_LeanVec", VecSimSvsQuant_4x8_LeanVec)
        .value("VecSimSvsQuant_8x8_LeanVec", VecSimSvsQuant_8x8_LeanVec)
        .export_values();

    py::class_<SVSParams>(m, "SVSParams")
        .def(py::init())
        .def_readwrite("type", &SVSParams::type)
        .def_readwrite("dim", &SVSParams::dim)
        .def_readwrite("metric", &SVSParams::metric)
        .def_readwrite("multi", &SVSParams::multi)
        .def_readwrite("blockSize", &SVSParams::blockSize)
        .def_readwrite("quantBits", &SVSParams::quantBits)
        .def_readwrite("alpha", &SVSParams::alpha)
        .def_readwrite("graph_max_degree", &SVSParams::graph_max_degree)
        .def_readwrite("construction_window_size", &SVSParams::construction_window_size)
        .def_readwrite("max_candidate_pool_size", &SVSParams::max_candidate_pool_size)
        .def_readwrite("prune_to", &SVSParams::prune_to)
        .def_readwrite("use_search_history", &SVSParams::use_search_history)
        .def_readwrite("search_window_size", &SVSParams::search_window_size)
        .def_readwrite("search_buffer_capacity", &SVSParams::search_buffer_capacity)
        .def_readwrite("leanvec_dim", &SVSParams::leanvec_dim)
        .def_readwrite("epsilon", &SVSParams::epsilon)
        .def_readwrite("num_threads", &SVSParams::num_threads);

    py::class_<TieredHNSWParams>(m, "TieredHNSWParams")
        .def(py::init())
        .def_readwrite("swapJobThreshold", &TieredHNSWParams::swapJobThreshold)
        .def_readwrite("QuantNormalizationSetSize", &TieredHNSWParams::QuantNormalizationSetSize);

    py::class_<TieredSVSParams>(m, "TieredSVSParams")
        .def(py::init())
        .def_readwrite("trainingTriggerThreshold", &TieredSVSParams::trainingTriggerThreshold)
        .def_readwrite("updateTriggerThreshold", &TieredSVSParams::updateTriggerThreshold)
        .def_readwrite("updateJobWaitTime", &TieredSVSParams::updateJobWaitTime);

    py::class_<AlgoParams>(m, "AlgoParams")
        .def(py::init())
        .def_readwrite("hnswParams", &AlgoParams::hnswParams)
        .def_readwrite("bfParams", &AlgoParams::bfParams)
        .def_readwrite("svsParams", &AlgoParams::svsParams);

    py::class_<VecSimParams>(m, "VecSimParams")
        .def(py::init())
        .def_readwrite("algo", &VecSimParams::algo)
        .def_readwrite("algoParams", &VecSimParams::algoParams);

    py::class_<VecSimQueryParams> queryParams(m, "VecSimQueryParams");

    queryParams.def(py::init<>())
        .def_readwrite("hnswRuntimeParams", &VecSimQueryParams::hnswRuntimeParams)
        .def_readwrite("svsRuntimeParams", &VecSimQueryParams::svsRuntimeParams)
        .def_readwrite("batchSize", &VecSimQueryParams::batchSize);

    py::class_<HNSWRuntimeParams>(queryParams, "HNSWRuntimeParams")
        .def(py::init<>())
        .def_readwrite("efRuntime", &HNSWRuntimeParams::efRuntime)
        .def_readwrite("epsilon", &HNSWRuntimeParams::epsilon);

    py::class_<SVSRuntimeParams>(queryParams, "SVSRuntimeParams")
        .def(py::init<>())
        .def_readwrite("windowSize", &SVSRuntimeParams::windowSize)
        .def_readwrite("bufferCapacity", &SVSRuntimeParams::bufferCapacity)
        .def_readwrite("searchHistory", &SVSRuntimeParams::searchHistory)
        .def_readwrite("epsilon", &SVSRuntimeParams::epsilon);

    py::class_<PyVecSimIndex>(m, "VecSimIndex")
        .def(py::init([](const VecSimParams &params, const py::object &quantization_mean) {
                 VecSimParams native_params = params;
                 std::optional<QuantizationMean> mean;
                 if (!quantization_mean.is_none()) {
                     if (params.algo != VecSimAlgo_HNSWLIB) {
                         throw py::value_error(
                             "quantization_mean is only supported for HNSW VecSimIndex parameters");
                     }
                     mean = validateQuantizationMean(quantization_mean,
                                                     native_params.algoParams.hnswParams);
                     native_params.algoParams.hnswParams.quantParams = mean->data();
                 }
                 return new PyVecSimIndex(native_params);
             }),
             py::arg("params"), py::kw_only(), py::arg("quantization_mean") = py::none())
        .def("add_vector", &PyVecSimIndex::addVector)
        .def("delete_vector", &PyVecSimIndex::deleteVector)
        .def("knn_query", &PyVecSimIndex::knn, py::arg("vector"), py::arg("k"),
             py::arg("query_param") = nullptr)
        .def("range_query", &PyVecSimIndex::range, py::arg("vector"), py::arg("radius"),
             py::arg("query_param") = nullptr)
        .def("index_size", &PyVecSimIndex::indexSize)
        .def("index_type", &PyVecSimIndex::indexType)
        .def("index_memory", &PyVecSimIndex::indexMemory)
        .def("create_batch_iterator", &PyVecSimIndex::createBatchIterator, py::arg("query_blob"),
             py::arg("query_param") = nullptr)
        .def("get_vector", &PyVecSimIndex::getVector)
        .def("update_vectors", &PyVecSimIndex::updateVectors, py::arg("label"), py::arg("vectors"))
        .def("relabel_vector", &PyVecSimIndex::relabelVector, py::arg("old_label"),
             py::arg("new_label"))
        .def("run_gc", &PyVecSimIndex::runGC);

    py::class_<PyHNSWLibIndex, PyVecSimIndex>(m, "HNSWIndex")
        .def(py::init([](const HNSWParams &params, const py::object &quantization_mean) {
                 HNSWParams native_params = params;
                 auto mean = validateQuantizationMean(quantization_mean, native_params);
                 if (mean) {
                     native_params.quantParams = mean->data();
                 }
                 return new PyHNSWLibIndex(native_params);
             }),
             py::arg("params"), py::kw_only(), py::arg("quantization_mean") = py::none())
        .def(py::init([](const std::string &location) { return new PyHNSWLibIndex(location); }),
             py::arg("location"))
        .def("set_ef", &PyHNSWLibIndex::setDefaultEf)
        .def("save_index", &PyHNSWLibIndex::saveIndex)
        .def("knn_parallel", &PyHNSWLibIndex::searchKnnParallel, py::arg("queries"), py::arg("k"),
             py::arg("query_param") = nullptr, py::arg("num_threads") = -1)
        .def("add_vector_parallel", &PyHNSWLibIndex::addVectorsParallel, py::arg("vectors"),
             py::arg("labels"), py::arg("num_threads") = -1)
        .def("check_integrity", &PyHNSWLibIndex::checkIntegrity)
        .def("range_parallel", &PyHNSWLibIndex::searchRangeParallel, py::arg("queries"),
             py::arg("radius"), py::arg("query_param") = nullptr, py::arg("num_threads") = -1)
        .def("create_batch_iterator", &PyHNSWLibIndex::createBatchIterator, py::arg("query_blob"),
             py::arg("query_param") = nullptr);

    py::class_<PyTieredIndex, PyVecSimIndex>(m, "TieredIndex")
        .def("wait_for_index", &PyTieredIndex::WaitForIndex, py::arg("waiting_duration") = 10)
        .def("get_curr_bf_size", &PyTieredIndex::getFlatIndexSize)
        .def("get_buffer_limit", &PyTieredIndex::getBufferLimit)
        .def("get_threads_num", &PyTieredIndex::getThreadsNum);

    py::class_<PyTiered_HNSWIndex, PyTieredIndex>(m, "Tiered_HNSWIndex")
        .def(py::init([](const HNSWParams &hnsw_params, const TieredHNSWParams &tiered_hnsw_params,
                         size_t flat_buffer_size, bool manual_jobs,
                         const py::object &quantization_mean) {
                 HNSWParams native_hnsw_params = hnsw_params;
                 auto mean = validateQuantizationMean(quantization_mean, native_hnsw_params);
                 if (mean) {
                     native_hnsw_params.quantParams = mean->data();
                 }
                 return new PyTiered_HNSWIndex(native_hnsw_params, tiered_hnsw_params,
                                               flat_buffer_size, manual_jobs);
             }),
             py::arg("hnsw_params"), py::arg("tiered_hnsw_params"), py::arg("flat_buffer_size"),
             py::kw_only(), py::arg("manual_jobs") = false,
             py::arg("quantization_mean") = py::none(),
             R"doc(Create a tiered HNSW index.

If QuantNormalizationSetSize is positive, native tiered training takes precedence over a supplied
quantization_mean. The supplied mean is still validated before index creation.
)doc")
        .def("hnsw_label_count", &PyTiered_HNSWIndex::HNSWLabelCount)
        .def("_run_pending_jobs", &PyTiered_HNSWIndex::runPendingJobs, py::arg("max_jobs") = 1);

    py::class_<PyBFIndex, PyVecSimIndex>(m, "BFIndex")
        .def(py::init([](const BFParams &params) { return new PyBFIndex(params); }),
             py::arg("params"));
#if HAVE_SVS
    py::class_<PySVSIndex, PyVecSimIndex>(m, "SVSIndex")
        .def(py::init([](const SVSParams &params) { return new PySVSIndex(params); }),
             py::arg("params"))
        .def(py::init([](const std::string &location, const SVSParams &params) {
                 return new PySVSIndex(location, params);
             }),
             py::arg("location"), py::arg("params"))
        .def("add_vector_parallel", &PySVSIndex::addVectorsParallel, py::arg("vectors"),
             py::arg("labels"))
        .def("check_integrity", &PySVSIndex::checkIntegrity)
        .def("save_index", &PySVSIndex::saveIndex, py::arg("location"))
        .def("load_index", &PySVSIndex::loadIndex, py::arg("location"))
        .def("get_labels_count", &PySVSIndex::getLabelsCount);

    py::class_<PyTiered_SVSIndex, PyTieredIndex>(m, "Tiered_SVSIndex")
        .def(py::init([](const SVSParams &svs_params, const TieredSVSParams &tiered_svs_params,
                         size_t flat_buffer_size = DEFAULT_BLOCK_SIZE) {
                 return new PyTiered_SVSIndex(svs_params, tiered_svs_params, flat_buffer_size);
             }),
             py::arg("svs_params"), py::arg("tiered_svs_params"),
             py::arg("flat_buffer_size") = DEFAULT_BLOCK_SIZE)
        .def("svs_label_count", &PyTiered_SVSIndex::SVSLabelCount);
#endif

    py::class_<PyBatchIterator>(m, "BatchIterator")
        .def("has_next", &PyBatchIterator::hasNext)
        .def("get_next_results", &PyBatchIterator::getNextResults)
        .def("reset", &PyBatchIterator::reset);

    m.def(
        "set_log_context",
        [](const std::string &test_name, const std::string &test_type) {
            // Call the C++ function to set the global context
            VecSim_SetTestLogContext(test_name.c_str(), test_type.c_str());
        },
        "Set the context (test name) for logging");
}
