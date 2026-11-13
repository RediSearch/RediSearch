/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include "VecSim/vec_sim_index.h"
#include "VecSim/query_result_definitions.h"
#include "VecSim/utils/vec_utils.h"

#include <cmath>
#include <memory>
#include <cassert>
#include <functional>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <type_traits>
#include <vector>

#include "svs/index/vamana/dynamic_index.h"
#include "svs/index/vamana/multi.h"
#include "spdlog/sinks/callback_sink.h"

#include "VecSim/algorithms/svs/svs_utils.h"
#include "VecSim/algorithms/svs/svs_batch_iterator.h"
#include "VecSim/algorithms/svs/svs_extensions.h"

#ifdef BUILD_TESTS
#include "svs_serializer.h"
#endif

struct SVSIndexBase
#ifdef BUILD_TESTS
    : public SVSSerializer
#endif
{
    SVSIndexBase() : num_marked_deleted{0} {};
    virtual ~SVSIndexBase() = default;

    virtual int addVector(const void *vector_data, labelType label) = 0;
    virtual int addVectors(const void *vectors_data, const labelType *labels, size_t n) = 0;
    virtual int deleteVector(labelType label) = 0;
    virtual int deleteVectors(const labelType *labels, size_t n) = 0;
    virtual void consolidate(const std::vector<labelType> &labels) = 0;
    virtual bool isLabelExists(labelType label) const = 0;
    virtual size_t indexStorageSize() const = 0;
    virtual size_t getParallelism() const = 0;
    virtual void setParallelism(size_t parallelism) = 0;
    virtual size_t getPoolSize() const = 0;
    // Run synchronously using caller-reserved threads, without changing the
    // backend's per-operation parallelism.
    virtual void runParallel(std::function<void(size_t)> task, size_t num_threads) = 0;
    virtual bool isCompressed() const = 0;
    virtual bool ready() const = 0;

    size_t getNumMarkedDeleted() const {
        return num_marked_deleted.load(std::memory_order_relaxed);
    }

    // Abstract handler to manage SVS implementation instance
    // declared to avoid unsafe unique_ptr<void> usage
    // Derived SVSIndex class should implement it
    struct ImplHandler {
        virtual ~ImplHandler() = default;
        virtual void deleteVectors(const labelType *labels, size_t n) = 0;
    };
    virtual std::unique_ptr<ImplHandler> createImpl(const void *vectors_data,
                                                    const labelType *labels, size_t n) = 0;
    virtual void setImpl(std::unique_ptr<ImplHandler> impl) = 0;
#ifdef BUILD_TESTS
    virtual svs::logging::logger_ptr getLogger() const = 0;
    // Makes the next write throw, once, the way SVS itself would. The paths that turn an SVS
    // exception into a reported code cannot be reached otherwise: SVS throws only for the states
    // the checks preceding those calls rule out, and it grows its dataset rather than refusing a
    // write, so without this the code that reports a failed write would go untested.
    virtual void throwOnNextWriteForTest() = 0;
    virtual void setAfterCompactCallbackForTest(std::function<void()> callback) = 0;
#endif
protected:
    // Index marked deleted vectors counter to initiate reindexing if it exceeds threshold
    // markIndexUpdate() manages this counter
    std::atomic<size_t> num_marked_deleted;
};

/** Thread Management Strategy:
 * - addVector(): Requires parallelism == 1
 * - addVectors(): Allows any parallelism value, but prohibits n=1 with parallelism>1
 * - Callers are responsible for setting appropriate parallelism
 **/
template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
class SVSIndex : public VecSimIndexAbstract<svs_details::vecsim_dt<DataType>, float>,
                 public SVSIndexBase {
protected:
    using data_type = DataType;
    using distance_f = MetricType;
    using Base = VecSimIndexAbstract<svs_details::vecsim_dt<DataType>, float>;
    using index_component_t = IndexComponents<svs_details::vecsim_dt<DataType>, float>;

    using storage_traits_t = SVSStorageTraits<DataType, QuantBits, ResidualBits, IsLeanVec>;
    using index_storage_type = typename storage_traits_t::index_storage_type;

    using graph_builder_t = SVSGraphBuilder<uint32_t>;
    using graph_type = typename graph_builder_t::graph_type;

    // Note. svs::concurrent::* classes differ from the svs::index::vamana::* classes
    // in their mutation contracts.
    using impl_type = std::conditional_t<
        isMulti,
        svs::concurrent::MultiMutableVamanaIndex<graph_type, index_storage_type, distance_f>,
        svs::concurrent::MutableVamanaIndex<graph_type, index_storage_type, distance_f>>;

    bool forcePreprocessing;

    // Index build parameters
    svs::index::vamana::VamanaBuildParameters buildParams;

    // Index search parameters
    size_t search_window_size;
    size_t search_buffer_capacity;
    // LeanVec dataset dimension
    // This parameter allows to tune LeanVec dimension if LeanVec is enabled
    size_t leanvec_dim;
    double epsilon;

    // Check if the dataset is Two-level LVQ
    // This allows to tune default window capacity during search
    bool is_two_level_lvq;

    // SVS thread pool
    VecSimSVSThreadPool threadpool_;
    svs::logging::logger_ptr logger_;
    // SVS Index implementation instance.
    // initImpl() needs at least one point to compute an entry point.
    // so impl_ is null until the first insert
    std::atomic<impl_type *> impl_{nullptr};

    impl_type *getImpl() const { return this->impl_.load(std::memory_order_acquire); }

    static double toVecSimDistance(float v) { return svs_details::toVecSimDistance<distance_f>(v); }

    svs::logging::logger_ptr makeLogger() {
        spdlog::custom_log_callback callback = [this](const spdlog::details::log_msg &msg) {
            if (!VecSimIndexInterface::logCallback) {
                return; // No callback function provided
            }
            // Custom callback implementation
            const char *vecsim_level = [msg]() {
                switch (msg.level) {
                case spdlog::level::trace:
                    return VecSimCommonStrings::LOG_DEBUG_STRING;
                case spdlog::level::debug:
                    return VecSimCommonStrings::LOG_VERBOSE_STRING;
                case spdlog::level::info:
                    return VecSimCommonStrings::LOG_NOTICE_STRING;
                case spdlog::level::warn:
                case spdlog::level::err:
                case spdlog::level::critical:
                    return VecSimCommonStrings::LOG_WARNING_STRING;
                default:
                    return "UNKNOWN";
                }
            }();

            std::string msg_str{msg.payload.data(), msg.payload.size()};
            // Log the message using the custom callback
            VecSimIndexInterface::logCallback(this->logCallbackCtx, vecsim_level, msg_str.c_str());
        };

        // Create a logger with the custom callback
        auto sink = std::make_shared<spdlog::sinks::callback_sink_mt>(callback);
        auto logger = std::make_shared<spdlog::logger>("SVSIndex", sink);
        // Sink all messages to VecSim
        logger->set_level(spdlog::level::trace);
        return logger;
    }

    // Create SVS index instance with initial data
    // Data should not be empty
    template <svs::data::ImmutableMemoryDataset Dataset>
    std::unique_ptr<impl_type> initImpl(const Dataset &points,
                                        std::span<const labelType> ids) const {
        svs::threads::ThreadPoolHandle threadpool_handle{VecSimSVSThreadPool{threadpool_}};

        // Construct SVS index initial storage with compression if needed
        auto data = storage_traits_t::create_storage(points, this->blockSize, threadpool_handle,
                                                     this->getAllocator(), this->leanvec_dim);
        // Compute the entry point.
        auto entry_point =
            svs::index::vamana::extensions::compute_entry_point(data, threadpool_handle);

        // Perform graph construction.
        auto distance = distance_f{};
        const auto &parameters = this->buildParams;

        // Construct initial Vamana Graph
        auto graph =
            graph_builder_t::build_graph(parameters, data, distance, threadpool_handle, entry_point,
                                         this->blockSize, this->getAllocator(), logger_);

        // Create SVS MutableIndex instance
        auto impl = std::make_unique<impl_type>(std::move(graph), std::move(data), entry_point,
                                                std::move(distance), ids, threadpool_, logger_);

        // Set SVS MutableIndex build parameters to be used in future updates
        impl->set_construction_window_size(parameters.window_size);
        impl->set_max_candidates(parameters.max_candidate_pool_size);
        impl->set_prune_to(parameters.prune_to);
        impl->set_alpha(parameters.alpha);
        impl->set_full_search_history(parameters.use_full_search_history);

        // Configure default search parameters
        auto sp = impl->get_search_parameters();
        sp.buffer_config({this->search_window_size, this->search_buffer_capacity});
        sp.search_buffer_visited_set(false);
        impl->set_search_parameters(sp);
        impl->reset_performance_parameters();
        return impl;
    }

    // Preprocess batch of vectors
    MemoryUtils::unique_blob preprocessForBatchStorage(const void *original_data, size_t n) const {
        // Buffer alignment isn't necessary for storage since SVS index will copy the data
        if (!this->forcePreprocessing) {
            return MemoryUtils::unique_blob{const_cast<void *>(original_data), [](void *) {}};
        }

        const auto data_size = this->getStoredDataSize() * n;

        auto processed_blob =
            MemoryUtils::unique_blob{this->allocator->allocate(data_size),
                                     [this](void *ptr) { this->allocator->free_allocation(ptr); }};
        // Assuming original data size equals to processed data size
        assert(this->getInputBlobSize() == this->getStoredDataSize());
        memcpy(processed_blob.get(), original_data, data_size);
        // Preprocess each vector in place
        for (size_t i = 0; i < n; i++) {
            this->preprocessStorageInPlace(static_cast<DataType *>(processed_blob.get()) +
                                           i * this->dim);
        }
        return processed_blob;
    }

    // Handler to manage SVS implementation instance
    struct SVSImplHandler : public SVSIndexBase::ImplHandler {
        std::unique_ptr<impl_type> impl;
        size_t num_deleted{0};
        SVSImplHandler(std::unique_ptr<impl_type> impl) : impl{std::move(impl)} {}

        void deleteVectors(const labelType *labels, size_t n) override {
            if (impl) {
                num_deleted += impl->delete_entries(std::span{labels, n});
            }
        }
    };

    std::unique_ptr<ImplHandler> createImpl(const void *vectors_data, const labelType *labels,
                                            size_t n) override {
        // If no data provided, return empty handler
        if (n == 0) {
            return std::make_unique<SVSImplHandler>(nullptr);
        }

        std::span<const labelType> ids(labels, n);
        auto processed_blob = this->preprocessForBatchStorage(vectors_data, n);
        auto typed_vectors_data = static_cast<DataType *>(processed_blob.get());
        // Wrap data into SVS SimpleDataView for SVS API
        auto points = svs::data::SimpleDataView<DataType>{typed_vectors_data, n, this->dim};

        return std::make_unique<SVSImplHandler>(initImpl(points, ids));
    }

    // if impl_ == nullptr, store impl, return true
    // if impl_ != nullptr, do nothing, return false
    bool CASImpl(impl_type *impl) {
        impl_type *expected = nullptr;
        return this->impl_.compare_exchange_strong(expected, impl, std::memory_order_acq_rel);
    }

    void setImpl(std::unique_ptr<ImplHandler> handler) override {
        SVSImplHandler *svs_handler = dynamic_cast<SVSImplHandler *>(handler.get());
        if (!svs_handler) {
            throw std::logic_error("Failed to cast to SVSImplHandler");
        }

        if (!CASImpl(svs_handler->impl.get())) {
            throw std::logic_error("SVSIndex::setImpl called on non-empty impl_");
        }
        svs_handler->impl.release();
        if (svs_handler->num_deleted > 0) {
            this->markIndexUpdate(svs_handler->num_deleted);
        }
    }

    // Assuming parallelism was updated to reflect the number of available threads before this
    // function was called.
    // This function assumes that the caller has already set parallelism to the appropriate value
    // for the operation.
    // Important NOTE: For single vector operations (n=1), parallelism should be 1.
    // For bulk operations (n>1), parallelism should reflect the number of available threads.
#ifdef BUILD_TESTS
public:
    void throwOnNextWriteForTest() override { throw_on_next_write_ = true; }
    void setAfterCompactCallbackForTest(std::function<void()> callback) override {
        after_compact_callback_ = std::move(callback);
    }

private:
    bool throw_on_next_write_ = false;
    std::function<void()> after_compact_callback_;
    void maybeThrowForTest() {
        if (throw_on_next_write_) {
            throw_on_next_write_ = false;
            throw std::runtime_error("injected write failure");
        }
    }
#else
    void maybeThrowForTest() {
        // In production, we do nothing.
    }
#endif

    int addVectorsImpl(const void *vectors_data, const labelType *labels, size_t n) {
        maybeThrowForTest();
        if (n == 0) {
            return 0;
        }

        int deleted_num = 0;

        std::span<const labelType> ids(labels, n);
        auto processed_blob = this->preprocessForBatchStorage(vectors_data, n);
        auto typed_vectors_data = static_cast<DataType *>(processed_blob.get());
        // Wrap data into SVS SimpleDataView for SVS API
        auto points = svs::data::SimpleDataView<DataType>{typed_vectors_data, n, this->dim};

        if constexpr (!isMulti) {
            // SVS index does not support overriding vectors with the same label
            // so we have to delete them first if needed
            deleted_num = deleteVectorsImpl(labels, n);
        }

        if (auto impl = getImpl()) {
            // Add new points to existing SVS index
            impl->add_points(points, ids, /*reuse_empty*/ false);
        } else {
            // SVS index instance cannot be empty, so we have to construct it at first rows
            auto built = initImpl(points, ids);
            if (CASImpl(built.get())) {
                built.release();
            } else {
                // Another writer published first.
                if constexpr (!isMulti) {
                    deleted_num = deleteVectorsImpl(labels, n);
                }
                getImpl()->add_points(points, ids, /*reuse_empty*/ false);
            }
        }

        return n - deleted_num;
    }

    void consolidate(const std::vector<labelType> &labels) override {
        if (auto impl = getImpl()) {
            impl->consolidate(labels);
        }
    }

    int deleteVectorImpl(const labelType label) {
        if (auto impl = getImpl()) {
            const int deleted_num = impl->delete_entries(std::span{&label, 1});
            this->markIndexUpdate(deleted_num);
            return deleted_num;
        } else {
            return 0;
        }
    }

    int deleteVectorsImpl(const labelType *labels, size_t n) {
        if (auto impl = getImpl()) {
            const int deleted_num = impl->delete_entries(std::span{labels, n});
            if (deleted_num > 0) {
                this->markIndexUpdate(deleted_num);
            }
            return deleted_num;
        } else {
            return 0;
        }
    }

    void markIndexUpdate(size_t n = 1) {
        num_marked_deleted.fetch_add(n, std::memory_order_relaxed);
    }

    static size_t labelCountOf(const impl_type &impl) {
        if constexpr (isMulti) {
            return impl.labelcount();
        } else {
            return impl.size();
        }
    }

    bool isTwoLevelLVQ(const VecSimSvsQuantBits &qbits) {
        switch (qbits) {
        case VecSimSvsQuant_4x4:
        case VecSimSvsQuant_4x8:
        case VecSimSvsQuant_4x8_LeanVec:
        case VecSimSvsQuant_8x8_LeanVec:
            return true;
        default:
            return false;
        }
    }

public:
    SVSIndex(const SVSParams &params, const AbstractIndexInitParams &abstractInitParams,
             const index_component_t &components, bool force_preprocessing)
        : Base{abstractInitParams, components}, forcePreprocessing{force_preprocessing},
          buildParams{svs_details::makeVamanaBuildParameters(params)},
          search_window_size{svs_details::getOrDefault(params.search_window_size,
                                                       SVS_VAMANA_DEFAULT_SEARCH_WINDOW_SIZE)},
          search_buffer_capacity{
              svs_details::getOrDefault(params.search_buffer_capacity, search_window_size)},
          leanvec_dim{
              svs_details::getOrDefault(params.leanvec_dim, SVS_VAMANA_DEFAULT_LEANVEC_DIM)},
          epsilon{svs_details::getOrDefault(params.epsilon, SVS_VAMANA_DEFAULT_EPSILON)},
          is_two_level_lvq{isTwoLevelLVQ(params.quantBits)},
          threadpool_{this->allocator, this->logCallbackCtx} {
        logger_ = makeLogger();
        if (params.num_threads != 0) {
            this->log(VecSimCommonStrings::LOG_WARNING_STRING,
                      "SVSParams.num_threads is deprecated and ignored. "
                      "Thread pool size is controlled globally via VecSim_UpdateThreadPoolSize().");
        }
    }

    ~SVSIndex() { delete this->impl_.load(std::memory_order_relaxed); }

    bool ready() const override { return getImpl() != nullptr; }

    size_t indexSize() const override { return indexStorageSize(); }

    size_t indexStorageSize() const override {
        auto impl = getImpl();
        return impl ? impl->view_data().size() : 0;
    }

    size_t indexCapacity() const override {
        auto impl = getImpl();
        return impl ? storage_traits_t::storage_capacity(impl->view_data()) : 0;
    }

    size_t indexLabelCount() const override {
        auto impl = getImpl();
        return impl ? labelCountOf(*impl) : 0;
    }

    vecsim_stl::set<size_t> getLabelsSet() const override {
        vecsim_stl::set<size_t> labels(this->allocator);
        if (auto impl = getImpl()) {
            impl->on_ids([&labels](size_t label) { labels.insert(label); });
        }
        return labels;
    }

    VecSimIndexBasicInfo basicInfo() const override {
        VecSimIndexBasicInfo info = this->getBasicInfo();
        info.algo = VecSimAlgo_SVS;
        info.isTiered = false;
        return info;
    }

    VecSimIndexDebugInfo debugInfo() const override {
        VecSimIndexDebugInfo info;
        info.commonInfo = this->getCommonInfo();
        info.commonInfo.basicInfo.algo = VecSimAlgo_SVS;

        info.svsInfo =
            svsInfoStruct{.quantBits = getCompressionMode(),
                          .alpha = this->buildParams.alpha,
                          .graphMaxDegree = this->buildParams.graph_max_degree,
                          .constructionWindowSize = this->buildParams.window_size,
                          .maxCandidatePoolSize = this->buildParams.max_candidate_pool_size,
                          .pruneTo = this->buildParams.prune_to,
                          .useSearchHistory = this->buildParams.use_full_search_history,
                          .numThreads = this->getPoolSize(),
                          .lastReservedThreads = this->getParallelism(),
                          .numberOfMarkedDeletedNodes = this->num_marked_deleted,
                          .searchWindowSize = this->search_window_size,
                          .searchBufferCapacity = this->search_buffer_capacity,
                          .leanvecDim = this->leanvec_dim,
                          .epsilon = this->epsilon};
        return info;
    }

    VecSimDebugInfoIterator *debugInfoIterator() const override {
        VecSimIndexDebugInfo info = this->debugInfo();
        // Capacity hint: 26 = 25 fields added below (1 ALGORITHM + 9 from
        // addCommonInfoToIterator + 15 SVS-specific) + 1 for the SHARED_MEMORY field
        // that the C API wrapper VecSimIndex_DebugInfoIterator appends after this
        // method returns. Reserving the extra slot avoids a reallocation on the
        // top-level path; when nested in a tiered BACKEND_INDEX the C API does not
        // append it, so the hint over-reserves by one (harmless).
        // Update this number when fields are added or removed.
        size_t numberOfInfoFields = 26;
        VecSimDebugInfoIterator *infoIterator =
            new VecSimDebugInfoIterator(numberOfInfoFields, this->allocator);

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::ALGORITHM_STRING,
            .fieldType = INFOFIELD_STRING,
            .fieldValue = {
                FieldValue{.stringValue = VecSimAlgo_ToString(info.commonInfo.basicInfo.algo)}}});
        this->addCommonInfoToIterator(infoIterator, info.commonInfo);

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::BLOCK_SIZE_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.commonInfo.basicInfo.blockSize}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_QUANT_BITS_STRING,
            .fieldType = INFOFIELD_STRING,
            .fieldValue = {
                FieldValue{.stringValue = VecSimQuantBits_ToString(info.svsInfo.quantBits)}}});

        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::SVS_ALPHA_STRING,
                             .fieldType = INFOFIELD_FLOAT64,
                             .fieldValue = {FieldValue{.floatingPointValue = info.svsInfo.alpha}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_GRAPH_MAX_DEGREE_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.graphMaxDegree}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_CONSTRUCTION_WS_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.constructionWindowSize}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_MAX_CANDIDATE_POOL_SIZE_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.maxCandidatePoolSize}}});

        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::SVS_PRUNE_TO_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.pruneTo}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_USE_SEARCH_HISTORY_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.useSearchHistory}}});

        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::SVS_NUM_THREADS_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.numThreads}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_LAST_RESERVED_THREADS_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.lastReservedThreads}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::NUM_MARKED_DELETED,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.numberOfMarkedDeletedNodes}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_SEARCH_WS_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.searchWindowSize}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SVS_SEARCH_BC_STRING,
            .fieldType = INFOFIELD_UINT64,
            .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.searchBufferCapacity}}});

        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::SVS_LEANVEC_DIM_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.svsInfo.leanvecDim}}});

        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::EPSILON_STRING,
            .fieldType = INFOFIELD_FLOAT64,
            .fieldValue = {FieldValue{.floatingPointValue = info.svsInfo.epsilon}}});

        return infoIterator;
    }

    int addVector(const void *vector_data, labelType label) override {
        // Enforce single-threaded execution for single vector operations to ensure optimal
        // performance and consistent behavior. Callers must set parallelism=1 before calling this
        // method.
        assert(getParallelism() == 1 && "Can't use more than one thread to insert a single vector");
        return addVectorsImpl(vector_data, &label, 1);
    }

    int addVectors(const void *vectors_data, const labelType *labels, size_t n) override {
        // Prevent misuse: single vector operations should use addVector(), not addVectors() with
        // n=1 This ensures proper thread management and API contract enforcement.
        assert(!(n == 1 && getParallelism() > 1) &&
               "Can't use more than one thread to insert a single vector");
        return addVectorsImpl(vectors_data, labels, n);
    }

    int deleteVector(labelType label) override { return deleteVectorImpl(label); }

    int deleteVectors(const labelType *labels, size_t n) override {
        return deleteVectorsImpl(labels, n);
    }

    /**
     * Set the vectors stored under `label` to the given ones. Removed and re-added rather than
     * written over: SVS owns both the stored form and the graph built from it, so the values a
     * vector was placed by cannot be changed underneath it.
     *
     * The delete is what makes this a replacement in both label kinds. A single-value add already
     * replaces the label - `addVectorsImpl` deletes it before adding - but a multi-value one
     * appends, so without it the label would end up holding its old vectors as well.
     *
     * The insertion is one batch add, which is the shape SVS wants anyway: it preprocesses the
     * whole buffer in one pass and, for a compressed index, lets the quantizer see all the
     * vectors at once.
     */
    VecSimUpdateCode updateVectors(labelType label, const void *new_blobs, size_t n) override {
        if (!isMulti && n > 1) {
            return VecSimUpdate_MultiNotSupported;
        }

        // SVS reports a failure by throwing - `ANNException`, derived from `std::runtime_error` -
        // and neither the add nor the delete has a status to return: the batch add answers with the
        // number of *new* labels, which says nothing about whether it succeeded. This is reached
        // through an `extern "C"` boundary, where an escaping exception is undefined behaviour, so
        // a throw is reported as a failure instead. A real one, not a no-op: the label's vectors go
        // before the new ones are stored, so the label can be left holding nothing.
        try {
            // One vector replacing a single-value label *is* an overwrite, and `addVectorsImpl`
            // performs that itself - it deletes an existing single-value label before adding, since
            // SVS cannot store two vectors under one id - so the delete below would only cost a
            // second lookup for the same end state.
            if (!isMulti && n == 1) {
                addVectorsImpl(new_blobs, &label, 1);
                return VecSimUpdate_OK;
            }

            deleteVectorImpl(label);
            if (n > 0) {
                // Every vector goes under the same label, which is what the batch add takes a
                // label per vector for.
                std::vector<labelType> labels(n, label);
                addVectorsImpl(new_blobs, labels.data(), n);
            }
        } catch (const std::exception &) {
            return VecSimUpdate_Failed;
        }
        return VecSimUpdate_OK;
    }

#if HAVE_SVS_REPLACE_EXTERNAL_ID
    // Only declared when the SVS this was built against offers `replace_external_id`. The
    // pre-built SVS releases predate it, so where it is missing this override is left out and the
    // interface default reports `VecSimRelabel_Unsupported` - which tells a caller to fall back to
    // delete + insert rather than read it as a no-op.
    VecSimRelabelCode relabelVector(labelType old_label, labelType new_label) override {
        if (old_label == new_label) {
            return VecSimRelabel_SameLabel;
        }

        auto impl = getImpl();

        // A null impl_ is an index that never held a vector, so it holds neither label.
        if (!impl) {
            return VecSimRelabel_OldLabelMissing;
        }

        // SVS checks both ends under its own translator lock
        switch (impl->replace_external_id(old_label, new_label)) {
        case svs::concurrent::ReplaceExternalIdResult::Ok:
            return VecSimRelabel_OK;
        case svs::concurrent::ReplaceExternalIdResult::NewIdExists:
            return VecSimRelabel_NewLabelTaken;
        case svs::concurrent::ReplaceExternalIdResult::OldIdMissing:
            return VecSimRelabel_OldLabelMissing;
        default:
            throw std::logic_error("SVSIndex::relabelVector: unreachable SVS result");
        }
    }
#endif // HAVE_SVS_REPLACE_EXTERNAL_ID

    bool isLabelExists(labelType label) const override {
        auto impl = getImpl();
        return impl ? impl->has_id(label) : false;
    }

    size_t getParallelism() const override { return threadpool_.getParallelism(); }
    void setParallelism(size_t parallelism) override { threadpool_.setParallelism(parallelism); }

    size_t getPoolSize() const override { return threadpool_.poolSize(); }

    void runParallel(std::function<void(size_t)> task, size_t num_threads) override {
        threadpool_.parallel_for(std::move(task), num_threads);
    }

    bool isCompressed() const override { return storage_traits_t::is_compressed(); }

    VecSimSvsQuantBits getCompressionMode() const {
        return storage_traits_t::get_compression_mode();
    }

    double getDistanceFrom_Unsafe(labelType label, const void *vector_data) const override {
        auto impl = getImpl();
        if (!impl || !impl->has_id(label)) {
            return std::numeric_limits<double>::quiet_NaN();
        }

        auto query_datum = std::span{static_cast<const DataType *>(vector_data), this->dim};
        auto dist = impl->get_distance(label, query_datum);
        return toVecSimDistance(dist);
    }

    VecSimQueryReply *topKQuery(const void *queryBlob, size_t k,
                                VecSimQueryParams *queryParams) const override {
        auto rep = new VecSimQueryReply(this->allocator);
        this->lastMode = STANDARD_KNN;
        auto impl = getImpl();
        if (k == 0 || !impl) {
            return rep;
        }
        {
            const auto label_count = labelCountOf(*impl);
            if (label_count == 0) {
                return rep;
            }
            // limit result size to index size
            k = std::min(k, label_count);

            auto processed_query_ptr = this->preprocessQuery(queryBlob);
            const void *processed_query = processed_query_ptr.get();

            auto query = svs::data::ConstSimpleDataView<DataType>{
                static_cast<const DataType *>(processed_query), 1, this->dim};
            auto result = svs::QueryResult<size_t>{query.size(), k};
            auto sp = svs_details::joinSearchParams(impl->get_search_parameters(), queryParams,
                                                    is_two_level_lvq);

            auto timeoutCtx = queryParams ? queryParams->timeoutCtx : nullptr;
            auto cancel = [timeoutCtx]() { return VECSIM_TIMEOUT(timeoutCtx); };

            impl->search(result.view(), query, sp, cancel);
            if (cancel()) {
                rep->code = VecSim_QueryReply_TimedOut;
                return rep;
            }

            assert(result.n_queries() == 1);

            const auto n_neighbors = result.n_neighbors();
            rep->results.reserve(n_neighbors);

            for (size_t i = 0; i < n_neighbors; i++) {
                if (result.index(0, i) == svs::concurrent::no_external_id) {
                    continue;
                }
                rep->results.push_back(
                    VecSimQueryResult{result.index(0, i), toVecSimDistance(result.distance(0, i))});
            }
        }
        // Workaround for VecSim merge_results() that expects results to be sorted
        // by score, then by id from both indices.
        // TODO: remove this workaround when merge_results() is fixed.
        sort_results_by_score_then_id(rep);
        return rep;
    }

    VecSimQueryReply *rangeQuery(const void *queryBlob, double radius,
                                 VecSimQueryParams *queryParams) const override {
        auto rep = new VecSimQueryReply(this->allocator);
        this->lastMode = RANGE_QUERY;
        auto impl = getImpl();
        if (radius == 0 || !impl || labelCountOf(*impl) == 0) {
            return rep;
        }
        {
            auto timeoutCtx = queryParams ? queryParams->timeoutCtx : nullptr;
            auto cancel = [timeoutCtx]() { return VECSIM_TIMEOUT(timeoutCtx); };

            // Prepare query blob for SVS
            auto processed_query_ptr = this->preprocessQuery(queryBlob);
            const void *processed_query = processed_query_ptr.get();
            std::span<const data_type> query{static_cast<const data_type *>(processed_query),
                                             this->dim};

            // Base search parameters for the SVS iterator schedule.
            auto sp = svs_details::joinSearchParams(impl->get_search_parameters(), queryParams,
                                                    is_two_level_lvq);
            // SVS BatchIterator handles the search in batches
            // The batch size is set to the index search window size by default
            const size_t batch_size = sp.buffer_config_.get_search_window_size();

            // Create SVS BatchIterator for range search
            // Search result is cached in the iterator and can be accessed by the user
            auto svs_it = impl->make_batch_iterator(query);
            svs_it.next(batch_size, cancel);
            if (cancel()) {
                rep->code = VecSim_QueryReply_TimedOut;
                return rep;
            }

            // range search using epsilon
            const auto epsilon = queryParams && queryParams->svsRuntimeParams.epsilon != 0
                                     ? queryParams->svsRuntimeParams.epsilon
                                     : this->epsilon;

            const auto range_search_boundaries = radius * (1.0 + std::abs(epsilon));
            bool keep_searching = true;

            // Loop while iterator cache is not empty and search radius + epsilon is not exceeded
            while (keep_searching && svs_it.size() > 0) {
                // Iterate over the cached search results
                for (auto &neighbor : svs_it) {
                    const auto dist = toVecSimDistance(neighbor.distance());
                    if (dist <= radius) {
                        rep->results.push_back(VecSimQueryResult{neighbor.id(), dist});
                    } else if (dist > range_search_boundaries) {
                        keep_searching = false;
                    }
                }
                // If search radius + epsilon is not exceeded, request SVS BatchIterator for the
                // next batch
                if (keep_searching) {
                    svs_it.next(batch_size, cancel);
                    if (cancel()) {
                        rep->code = VecSim_QueryReply_TimedOut;
                        return rep;
                    }
                }
            }
        }
        // Workaround for VecSim merge_results() that expects results to be sorted
        // by score, then by id from both indices.
        // TODO: remove this workaround when merge_results() is fixed.
        sort_results_by_score_then_id(rep);
        return rep;
    }

    VecSimBatchIterator *newBatchIterator(const void *queryBlob,
                                          VecSimQueryParams *queryParams) const override {
        // force_copy == true.
        auto queryBlobCopy = this->preprocessQuery(queryBlob, true);

        // take ownership of the blob copy and pass it to the batch iterator.
        auto *queryBlobCopyPtr = queryBlobCopy.release();
        // Ownership of queryBlobCopy moves to VecSimBatchIterator that will free it at the end.
        auto impl = getImpl();
        if (!impl || labelCountOf(*impl) == 0) {
            return new (this->getAllocator())
                NullSVS_BatchIterator(queryBlobCopyPtr, queryParams, this->getAllocator());
        } else {
            return new (this->getAllocator()) SVS_BatchIterator<impl_type, data_type>(
                queryBlobCopyPtr, impl, queryParams, this->getAllocator(), is_two_level_lvq);
        }
    }

    bool preferAdHocSearch(size_t subsetSize, size_t k, bool initial_check) const override {
        size_t index_size = this->indexLabelCount();

        // Calculate the ratio of the subset size to the total index size.
        double subsetRatio = (index_size == 0) ? 0.f : static_cast<double>(subsetSize) / index_size;

        // Heuristic thresholds
        const double smallSubsetThreshold = 0.07;  // Subset is small if less than 7% of index.
        const double largeSubsetThreshold = 0.21;  // Subset is large If more than 21% of index.
        const double smallIndexThreshold = 75000;  // Index is small if size is less than 75k.
        const double largeIndexThreshold = 750000; // Index is large if size is more than 750k.

        bool res = false;
        if (subsetRatio < smallSubsetThreshold) {
            // For small subsets, ad-hoc if index is not large.
            res = (index_size < largeIndexThreshold);
        } else if (subsetRatio < largeSubsetThreshold) {
            // For medium subsets, ad-hoc if index is small or k is big.
            res = (index_size < smallIndexThreshold) || (k > 12);
        } else {
            // For large subsets, ad-hoc only if index is small.
            res = (index_size < smallIndexThreshold);
        }

        this->lastMode =
            res ? (initial_check ? HYBRID_ADHOC_BF : HYBRID_BATCHES_TO_ADHOC_BF) : HYBRID_BATCHES;
        return res;
    }

    void runGC() override {
        if (auto impl = getImpl()) {
            // concurrent compact() consolidates every soft-deleted points first
            // pending per-label consolidate jobs need not be drained - they become no-ops
            impl->compact();
        }
#ifdef BUILD_TESTS
        if (after_compact_callback_) {
            after_compact_callback_();
        }
#endif
        num_marked_deleted.store(0, std::memory_order_relaxed);
    }

private:
    // appends the raw stored elements for `label` to `vectors_output`, one entry per stored vector
    // -- zero if the label isn't held. Only meaningful when the caller has already excluded
    // compressed storage; nothing here dequantizes.
    template <typename OutputElement>
    void appendStoredDataByLabel(labelType label,
                                 std::vector<std::vector<OutputElement>> &vectors_output) const {
        // The spans handed to `append_datum` point straight into the dataset, so the instance has
        // to stay alive for the whole walk.
        auto impl = getImpl();
        if (!impl) {
            return;
        }
        auto append_datum = [&](auto indexed_span) {
            if constexpr (std::is_same_v<OutputElement, DataType>) {
                // The span's element type already is `OutputElement` here, so build the
                // output vector directly from it instead of a raw byte copy.
                vectors_output.emplace_back(indexed_span.begin(), indexed_span.end());
            } else {
                // FP16 and the test-only byte output: `OutputElement` differs from the
                // span's element type but is bit-identical size, so copy the raw bytes.
                std::vector<OutputElement> vec_data(this->getStoredDataSize() /
                                                    sizeof(OutputElement));
                const char *data_ptr = reinterpret_cast<const char *>(indexed_span.data());
                std::memcpy(vec_data.data(), data_ptr, this->getStoredDataSize());
                vectors_output.push_back(std::move(vec_data));
            }
        };

        if constexpr (isMulti) {
            impl->on_data(label, append_datum);
        } else {
            impl->on_datum(label, append_datum);
        }
    }

public:
#ifdef BUILD_TESTS

private:
    void saveIndexIMP(std::ofstream &output) override;
    void impl_save(const std::string &location) override;
    void saveIndexFields(std::ofstream &output) const override;

    bool compareMetadataFile(const std::string &metadataFilePath) const override;
    void loadIndex(const std::string &folder_path) override;
    bool checkIntegrity() const override;

public:
    void fitMemory() override {}
    size_t indexMetaDataCapacity() const override { return this->indexCapacity(); }
    std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType label) const override {
        // For compressed/quantized indices, this function is not meaningful
        // since the stored data is in compressed format and not directly accessible
        if constexpr (storage_traits_t::is_compressed()) {
            throw std::runtime_error(
                "getStoredVectorDataByLabel is not supported for compressed/quantized indices");
        } else {
            std::vector<std::vector<char>> vectors_output;
            appendStoredDataByLabel(label, vectors_output);
            return vectors_output;
        }
    }
    svs::logging::logger_ptr getLogger() const override { return logger_; }
#endif

    // Same rule as the HNSW implementations: report nothing rather than an approximation.
    // `storage_traits_t::is_compressed()` covers quantized (LVQ/scalar) and LeanVec-reduced
    // storage alike -- nothing here dequantizes, so a caller comparing a new value against the
    // stored one byte for byte would read a dequantized reconstruction as a difference, or
    // worse, as a match. Only an uncompressed index can answer truthfully.
    void getDataByLabel(
        labelType label,
        std::vector<std::vector<svs_details::vecsim_dt<DataType>>> &vectors_output) const override {
        if constexpr (storage_traits_t::is_compressed()) {
            return;
        } else {
            appendStoredDataByLabel(label, vectors_output);
        }
    }
};

#ifdef BUILD_TESTS
// Including implementations for Serializer base
#include "svs_serializer_impl.h"
#endif
