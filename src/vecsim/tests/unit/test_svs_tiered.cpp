#include "VecSim/index_factories/tiered_factory.h"
#include "VecSim/vec_sim_debug.h"
#include <atomic>
#include <string>
#include <array>
#include <iostream>
#include <vector>
#include <random>

#include "unit_test_utils.h"
#include "mock_thread_pool.h"

#if HAVE_SVS
#include <algorithm>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <chrono>
#include <unordered_set>
// For getAvailableCPUs():
#include <sched.h>

#include "VecSim/algorithms/svs/svs.h"
#include "VecSim/algorithms/svs/svs_tiered.h"

// There are possible cases when SVS Index cannot be created with the requested quantization mode
// due to platform and/or hardware limitations or combination of requested 'compression' modes.
// This assert handle those cases and skip a test if the mode is not supported.
// Elsewhere, test will fail if the index creation failed with no reason explained above.
#define ASSERT_INDEX(index)                                                                        \
    if (index == nullptr) {                                                                        \
        if (std::get<1>(svs_details::isSVSQuantBitsSupported(TypeParam::get_quant_bits()))) {      \
            GTEST_FAIL() << "Failed to create SVS index";                                          \
        } else {                                                                                   \
            GTEST_SKIP() << "SVS LVQ is not supported.";                                           \
        }                                                                                          \
    }

static size_t EstimateReverseEdgesSize(size_t num_elements) {
    return SVSGraphBuilder<uint32_t>::reverse_edges_element_size() *
           svs::lib::SegmentedVector<uint8_t>(num_elements).capacity();
}

// Get available number of CPUs
// Returns the number of logical processors on the process
// Returns std::thread::hardware_concurrency() if the number of logical processors is not available
static unsigned int getAvailableCPUs() {
#ifdef __linux__
    // On Linux, use sched_getaffinity to get the number of CPUs available to the current process.
    cpu_set_t cpu_set;
    if (sched_getaffinity(0, sizeof(cpu_set), &cpu_set) == 0) {
        return CPU_COUNT(&cpu_set);
    }
#endif
    // Fallback.
    return std::thread::hardware_concurrency();
}

// Log callback function to print non-debug log messages
static void svsTestLogCallBackNoDebug(void *ctx, const char *level, const char *message) {
    if (level == nullptr || message == nullptr) {
        return; // Skip null messages
    }
    if (std::string_view{level} == VecSimCommonStrings::LOG_DEBUG_STRING) {
        return; // Skip debug messages
    }
    // Print other log levels
    std::cout << level << ": " << message << std::endl;
}

// Runs the test for combination of data type and quantization mode.
// TODO: Add support for label type combination(single/multi)
template <typename index_type_t>
class SVSTieredIndexTest : public ::testing::Test {
public:
    using data_t = typename index_type_t::data_t;
    static const size_t TestsDefaultTrainingThreshold = 1024;
    static const size_t TestsDefaultUpdateThreshold = 16;

protected:
    TieredSVSIndex<data_t> *CastToTieredSVS(VecSimIndex *index) {
        return reinterpret_cast<TieredSVSIndex<data_t> *>(index);
    }

    TieredIndexParams
    CreateTieredSVSParams(VecSimParams &svs_params, tieredIndexMock &mock_thread_pool,
                          size_t training_threshold = TestsDefaultTrainingThreshold,
                          size_t update_threshold = TestsDefaultUpdateThreshold,
                          size_t update_job_wait_time = SVS_DEFAULT_UPDATE_JOB_WAIT_TIME) {
        trainingThreshold = training_threshold;
        updateThreshold = update_threshold;
        svs_params.algoParams.svsParams.quantBits = index_type_t::get_quant_bits();
        return TieredIndexParams{
            .jobQueue = &mock_thread_pool.jobQ,
            .jobQueueCtx = mock_thread_pool.ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .primaryIndexParams = &svs_params,
            .specificParams = {.tieredSVSParams =
                                   TieredSVSParams{.trainingTriggerThreshold = training_threshold,
                                                   .updateTriggerThreshold = update_threshold,
                                                   .updateJobWaitTime = update_job_wait_time}}};
    }

    void verifyNumThreads(TieredSVSIndex<data_t> *tiered_index, size_t expected_capcity) {
        ASSERT_EQ(tiered_index->GetSVSIndex()->getParallelism(), 1);
        ASSERT_EQ(tiered_index->GetSVSIndex()->getPoolSize(), expected_capcity);
    }
    TieredSVSIndex<data_t> *CreateTieredSVSIndex(const TieredIndexParams &tiered_params,
                                                 tieredIndexMock &mock_thread_pool) {
        // Resize the shared SVS thread pool singleton to match the mock thread pool size,
        // simulating what RediSearch does via VecSim_UpdateThreadPoolSize during module init.
        VecSim_UpdateThreadPoolSize(mock_thread_pool.thread_pool_size);

        auto *tiered_index =
            reinterpret_cast<TieredSVSIndex<data_t> *>(TieredFactory::NewIndex(&tiered_params));

        // Set the created tiered index in the index external context (it will take ownership over
        // the index, and we'll need to release the ctx at the end of the test.
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);
        verifyNumThreads(tiered_index, mock_thread_pool.thread_pool_size);

        return tiered_index;
    }

    TieredSVSIndex<data_t> *
    CreateTieredSVSIndex(VecSimParams &svs_params, tieredIndexMock &mock_thread_pool,
                         size_t training_threshold = TestsDefaultTrainingThreshold,
                         size_t update_threshold = TestsDefaultUpdateThreshold,
                         size_t update_job_wait_time = SVS_DEFAULT_UPDATE_JOB_WAIT_TIME) {
        svs_params.algoParams.svsParams.quantBits = index_type_t::get_quant_bits();
        TieredIndexParams tiered_params =
            CreateTieredSVSParams(svs_params, mock_thread_pool, training_threshold,
                                  update_threshold, update_job_wait_time);
        return CreateTieredSVSIndex(tiered_params, mock_thread_pool);
    }

    void SetUp() override {
        // Restore the write mode to default.
        VecSim_UpdateThreadPoolSize(0);
        // Limit VecSim log level to avoid printing too much information
        VecSimIndexInterface::setLogCallbackFunction(svsTestLogCallBackNoDebug);
    }

    // Check if the test is running in fallback mode to scalar quantization.
    bool isFallbackToSQ() const {
        // Get the fallback quantization mode and compare it to the scalar quantization mode.
        return VecSimSvsQuant_Scalar ==
               std::get<0>(svs_details::isSVSQuantBitsSupported(index_type_t::get_quant_bits()));
    }

    size_t getTrainingThreshold() const { return trainingThreshold; }

    size_t getUpdateThreshold() const { return updateThreshold; }

private:
    size_t trainingThreshold = TestsDefaultTrainingThreshold;
    size_t updateThreshold = TestsDefaultUpdateThreshold;
};

// TEST_DATA_T and TEST_DIST_T are defined in test_utils.h

template <VecSimType type, typename DataType, VecSimSvsQuantBits quantBits, bool IsMulti>
struct SVSIndexType {
    static constexpr VecSimType get_index_type() { return type; }
    static constexpr VecSimSvsQuantBits get_quant_bits() { return quantBits; }
    static constexpr bool isMulti() { return IsMulti; }
    typedef DataType data_t;
};

// clang-format off
using SVSDataTypeSet = ::testing::Types<SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_NONE, false>
                                       ,SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_NONE, true>
                                       ,SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_8, false>
                                        >;
// clang-format on

TYPED_TEST_SUITE(SVSTieredIndexTest, SVSDataTypeSet);

// Runs the test for each data type(float/double). The label type should be explicitly
// set in the test.

template <typename index_type_t>
class SVSTieredIndexTestBasic : public SVSTieredIndexTest<index_type_t> {};

template <VecSimType type, typename DataType, VecSimSvsQuantBits quantBits>
struct SVSIndexTypeBasic {
    static constexpr VecSimType get_index_type() { return type; }
    static constexpr VecSimSvsQuantBits get_quant_bits() { return quantBits; }
    typedef DataType data_t;
};
using SVSBasicDataTypeSet =
    ::testing::Types<SVSIndexTypeBasic<VecSimType_FLOAT32, float, VecSimSvsQuant_NONE>>;

TYPED_TEST_SUITE(SVSTieredIndexTestBasic, SVSBasicDataTypeSet);

// A multi-value, compressed combination isn't in SVSDataTypeSet (compression there is only
// paired with single-value), but it is exactly the combination that can leave a label split
// across a non-empty flat buffer and a backend that cannot report its values.
template <typename index_type_t>
class SVSTieredIndexTestCompressedMulti : public SVSTieredIndexTest<index_type_t> {};

using SVSCompressedMultiDataTypeSet =
    ::testing::Types<SVSIndexType<VecSimType_FLOAT32, float, VecSimSvsQuant_8, true>>;

TYPED_TEST_SUITE(SVSTieredIndexTestCompressedMulti, SVSCompressedMultiDataTypeSet);

TYPED_TEST(SVSTieredIndexTest, ThreadsReservation) {
    // Set thread_pool_size to 4 or actual number of available CPUs
    const auto num_threads = std::min(4U, getAvailableCPUs());
    if (num_threads < 2) {
        // If the number of threads is less than 2, we can't run the test
        GTEST_SKIP() << "No threads available";
    }

    std::chrono::milliseconds timeout{1000}; // long enough to reserve all threads
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = 4, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(num_threads);
    ASSERT_EQ(mock_thread_pool.thread_pool_size, num_threads);

    // Create TieredSVS index instance with a mock queue.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    // Counter of reserved threads
    // This is set in the update_job_mock callback only
    std::atomic<size_t> num_reserved_threads = 0;

    auto update_job_mock = [&num_reserved_threads](VecSimIndex * /*unused*/, size_t num_threads) {
        num_reserved_threads = num_threads;
    };

    SVSMultiThreadJob::JobsRegistry registry(allocator);
    // Request 4 threads but just 1 thread is available
    auto jobs = SVSMultiThreadJob::createJobs(allocator, SVS_BATCH_UPDATE_JOB, update_job_mock,
                                              tiered_index, 4, timeout, &registry);
    ASSERT_EQ(jobs.size(), 4);
    tiered_index->submitJobs(jobs);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 4);
    // emulate 1 thread availability
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 3);
    ASSERT_EQ(num_reserved_threads, 1);

    // Complete rest of wait jobs
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_wait();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Request and run exact number of available threads
    jobs = SVSMultiThreadJob::createJobs(allocator, SVS_BATCH_UPDATE_JOB, update_job_mock,
                                         tiered_index, num_threads, timeout, &registry);
    tiered_index->submitJobs(jobs);
    mock_thread_pool.thread_pool_wait();
    ASSERT_EQ(num_reserved_threads, num_threads);

    // Request and run 1 thread
    jobs = SVSMultiThreadJob::createJobs(allocator, SVS_BATCH_UPDATE_JOB, update_job_mock,
                                         tiered_index, 1, timeout, &registry);
    tiered_index->submitJobs(jobs);
    mock_thread_pool.thread_pool_wait();
    ASSERT_EQ(num_reserved_threads, 1);

    // Request and run less threads than available
    jobs = SVSMultiThreadJob::createJobs(allocator, SVS_BATCH_UPDATE_JOB, update_job_mock,
                                         tiered_index, num_threads - 1, timeout, &registry);
    tiered_index->submitJobs(jobs);
    mock_thread_pool.thread_pool_wait();
    // The number of reserved threads should be equal to requested
    ASSERT_EQ(num_reserved_threads, num_threads - 1);

    // Request more threads than available
    jobs = SVSMultiThreadJob::createJobs(allocator, SVS_BATCH_UPDATE_JOB, update_job_mock,
                                         tiered_index, num_threads + 1, timeout, &registry);
    tiered_index->submitJobs(jobs);
    mock_thread_pool.thread_pool_wait();
    // The number of reserved threads should be equal to the number of available threads
    ASSERT_EQ(num_reserved_threads, num_threads);
    mock_thread_pool.thread_pool_join();
}

TYPED_TEST(SVSTieredIndexTest, updateVectorsReportsAFailedWrite) {
    // The tier's update reports a failed write rather than letting the backend's exception cross
    // the API boundary. In-place write mode, so the write reaches the backend synchronously and
    // the injected throw is on the path; in async mode the vector would only land in the flat
    // buffer and the backend would not be touched until a job ran.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    TEST_DATA_T replacement[dim];
    GenerateVector<TEST_DATA_T>(replacement, dim, 10);
    // Several labels, not one: a multi-value update deletes the label's vectors before adding the
    // new ones, and had the backend held only that label it would be left empty - after which the
    // add buffers in the flat index instead of reaching the backend, and the injected throw would
    // never be on the path.
    for (size_t i = 0; i < 5; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_GT(tiered_index->GetBackendIndex()->indexSize(), 1);

    tiered_index->setWriteMode(VecSim_WriteInPlace);
    tiered_index->GetSVSIndex()->throwOnNextWriteForTest();
    ASSERT_EQ(tiered_index->updateVectors(1, replacement, 1), VecSimUpdate_Failed);

    // Still usable, and the injection was one-shot.
    ASSERT_EQ(tiered_index->updateVectors(1, replacement, 1), VecSimUpdate_OK);
    ASSERT_EQ(tiered_index->indexLabelCount(), 5);
    VecSim_SetWriteMode(VecSim_WriteAsync);
}

TYPED_TEST(SVSTieredIndexTest, updateVectors) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Thresholds of 1, so one vector is enough to trigger the update job that moves it back.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    TEST_DATA_T buffered[dim], first_replacement[dim], second_replacement[dim];
    GenerateVector<TEST_DATA_T>(buffered, dim, 7);
    GenerateVector<TEST_DATA_T>(first_replacement, dim, 70);
    GenerateVector<TEST_DATA_T>(second_replacement, dim, 700);

    VecSimIndex_AddVector(tiered_index, buffered, 7);

    // Buffered: nothing has run, so the label lives only in the flat buffer and it is the copy
    // there that has to be replaced.
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->updateVectors(7, first_replacement, 1), VecSimUpdate_OK);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1) << "the update must not add a label";
    auto verify_first = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 7); };
    runTopKSearchTest(tiered_index, first_replacement, 1, verify_first);

    // Drain: the update job carries the label to the backend, and has to carry the value the
    // update left in the buffer rather than the one that was there when it was queued.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_GT(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(7));
    runTopKSearchTest(tiered_index, first_replacement, 1, verify_first);

    // Backend only: the same update again, now served by the backend tier.
    ASSERT_EQ(tiered_index->updateVectors(7, second_replacement, 1), VecSimUpdate_OK);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    auto verify_second = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 7); };
    runTopKSearchTest(tiered_index, second_replacement, 1, verify_second);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(7));
    runTopKSearchTest(tiered_index, second_replacement, 1, verify_second);

    if (TypeParam::isMulti()) {
        // A multi-value label may be given as many vectors as the caller likes, and ends up
        // holding exactly those.
        TEST_DATA_T two[2 * dim];
        GenerateVector<TEST_DATA_T>(two, dim, 800);
        GenerateVector<TEST_DATA_T>(two + dim, dim, 801);
        ASSERT_EQ(tiered_index->updateVectors(7, two, 2), VecSimUpdate_OK);
        mock_thread_pool.init_threads();
        mock_thread_pool.thread_pool_join();
        // The replaced vectors leave tombstones behind, so compact before counting.
        VecSimTieredIndex_GC(tiered_index);
        mock_thread_pool.init_threads();
        mock_thread_pool.thread_pool_join();
        ASSERT_EQ(tiered_index->indexSize(), 2);
        ASSERT_EQ(tiered_index->indexLabelCount(), 1);
        for (size_t i = 0; i < 2; i++) {
            auto verify_pair = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, 7); };
            runTopKSearchTest(tiered_index, two + i * dim, 1, verify_pair);
        }
    } else {
        // Two vectors under one label is not a state a single-value index can hold, in either
        // tier. Refused before anything is removed.
        TEST_DATA_T two[2 * dim];
        GenerateVector<TEST_DATA_T>(two, dim, 800);
        GenerateVector<TEST_DATA_T>(two + dim, dim, 801);
        ASSERT_EQ(tiered_index->updateVectors(7, two, 2), VecSimUpdate_MultiNotSupported);
        ASSERT_EQ(tiered_index->indexLabelCount(), 1);
        ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(7));
        runTopKSearchTest(tiered_index, second_replacement, 1, verify_second);
    }
}

// MOD-18994: `GenerateVector` fills every dimension with the same scalar, so consecutive labels
// (value, value+1, value+2, ...) all sit on a single straight line in vector space. Vamana's
// diversity-based edge pruning collapses near-duplicate candidates on a line like that, which can
// leave a node critically under-connected (see MOD-18994's investigation). A small, per-label
// deterministic jitter across dimensions breaks that collinearity while staying reproducible: the
// same label always gets the same jitter, so a query built from the same (label, value) pair
// lands exactly on the vector that was stored for it.
template <typename data_t>
static void GenerateJitteredVector(data_t *output, size_t dim, size_t label, double value) {
    std::mt19937 rng(static_cast<uint32_t>(label) * 2654435761u + 1u);
    std::uniform_real_distribution<double> jitter(-0.3, 0.3);
    for (size_t i = 0; i < dim; i++) {
        output[i] = (data_t)(value + jitter(rng));
    }
}

// Each rejection, asked of a tier: the target has to be free in *both* of them, so a target taken
// in the buffer and a target taken in the backend are separate cases.
TYPED_TEST(SVSTieredIndexTest, updateVectorsDuringUpdateJob) {
    // The tier's update job moves a whole batch out of the flat buffer into the backend and
    // reconciles it through `swaps_journal` / `deleted_labels_journal`. An update landing inside
    // that window is where the replaced vectors could be carried into the backend behind the
    // delete, or the new ones dropped along with the batch - so the updates here run while the
    // workers are mid-flight rather than between drains, as `relabelVectorDuringUpdateJob` does
    // for the move.
    size_t dim = 4;
    size_t n = 200;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Thresholds of 1, so the backend is initialized at the first vector and every batch that
    // follows triggers another update job. On the fixture's defaults (a training threshold of
    // 1024) 200 vectors never leave the flat buffer, and the window this test is named for would
    // not exist. These are trigger thresholds, not batch sizes: only one update job is pending at
    // a time, it waits `updateJobWaitTime` before running, and it then drains the whole flat
    // buffer - so with the loops below far outpacing it, a single drain moves many vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    // A multi-value label ends up holding more vectors than it started with, which is the part the
    // journals have to get right; a single-value one can only ever hold the one.
    const size_t new_per_label = TypeParam::isMulti() ? 2 : 1;
    // Replacement values sit far past the originals, so a query at a replacement cannot be
    // answered by a vector that was supposed to be gone.
    const float replacement_base = 10000.0f;

    mock_thread_pool.init_threads();
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T original[dim];
        GenerateJitteredVector<TEST_DATA_T>(original, dim, i, i);
        VecSimIndex_AddVector(tiered_index, original, i);
    }
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T replacements[2 * dim];
        for (size_t j = 0; j < new_per_label; j++) {
            GenerateJitteredVector<TEST_DATA_T>(replacements + j * dim, dim, i,
                                                replacement_base * (j + 1) + i);
        }
        // Whichever tier holds the label by now, and whether or not a job is mid-flight, the
        // update must be accepted.
        ASSERT_EQ(tiered_index->updateVectors(i, replacements, new_per_label), VecSimUpdate_OK)
            << "label " << i;
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();

    auto *svs_index = tiered_index->GetSVSIndex();
    // The backend really was in play: without this the test would still pass as a flat-buffer
    // exercise if the thresholds ever changed, which is not what it is here to cover.
    ASSERT_GT(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0) << "jobs were left unrun, so the join raced";

    // Every label is still there exactly once, in one tier or the other.
    ASSERT_EQ(tiered_index->indexLabelCount(), n);
    for (size_t i = 0; i < n; i++) {
        ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(i) || svs_index->isLabelExists(i))
            << "lost label " << i;
    }

    // And holds only what the update put there. SVS deletes softly, so the replaced vectors are
    // still occupying backend entries until a consolidation - subtracting them is what makes this
    // an exact count, and it is the assertion that fails if a job carried a replaced vector into
    // the backend after its delete.
    ASSERT_EQ(tiered_index->indexSize() - svs_index->getNumMarkedDeleted(), n * new_per_label);

    if (!svs_index->isCompressed()) {
        // Every replacement finds its own label. Asserted only for an uncompressed index: a
        // compressed one trains its stored form on the vectors it was given, so values this far
        // outside the original range are clipped.
        // MOD-18994: a replacement vector written late in the update loop can land in the first
        // sub-batch of a large incremental backend drain, before any of its true neighbors (the
        // other replacements) exist in the graph yet (VamanaBuilder::construct's fixed entry
        // point + sequential sub-batches), leaving it under-connected relative to vectors drained
        // later in the same call. It is still reachable, just via few edges - a wider runtime
        // search window than the 200 used at construction reliably finds it without touching the
        // graph's actual connectivity.
        SVSRuntimeParams wideWindow = {.windowSize = 250};
        VecSimQueryParams wideWindowParams = CreateQueryParams(wideWindow);
        for (size_t i : {(size_t)0, (size_t)1, n / 2, n - 1}) {
            for (size_t j = 0; j < new_per_label; j++) {
                TEST_DATA_T query[dim];
                GenerateJitteredVector<TEST_DATA_T>(query, dim, i, replacement_base * (j + 1) + i);
                auto verify = [&](size_t id, double score, size_t rank) { ASSERT_EQ(id, i); };
                runTopKSearchTest(tiered_index, query, 1, verify, &wideWindowParams);
            }
        }
    }
}

TYPED_TEST(SVSTieredIndexTest, addRelabelAndUpdateTogetherUnderLoad) {
    // The three writes that a partial update puts through an index - inserting a new document,
    // moving a label onto a new doc id, and replacing a label's vectors - all run here against the
    // same index while its update jobs are in flight. Individually each has its own test; what
    // this covers is their interaction: a label updated while its insert job is still pending and
    // then moved before that job runs, a label moved and only then updated, and fresh inserts
    // arriving throughout so the batch the jobs are moving never settles.
    size_t dim = 4;
    size_t n = 100;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Thresholds of 1, so the backend is live from the first vector and every batch that follows
    // triggers another update job for these writes to race.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    // Label ranges are kept apart so no write can land on a label another one owns.
    const labelType moved_offset = 10 * n;
    const labelType fresh_base = 5 * n;
    // Each role writes into its own value range, so the checks at the end can tell which write a
    // vector came from.
    const size_t per_label = TypeParam::isMulti() ? 2 : 1;

    // A pre-built SVS cannot relabel and the tier reports it, which is a build difference rather
    // than a runtime one. Probed instead of compiled out, so that the add and update halves of
    // this test run in either build: `relabelVector` with equal labels is a no-op that still
    // answers whether the capability is there.
    const bool can_relabel = tiered_index->relabelVector(0, 0) != VecSimRelabel_Unsupported;

    mock_thread_pool.init_threads();
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }

    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T replacements[2 * dim];
        for (size_t j = 0; j < per_label; j++) {
            GenerateVector<TEST_DATA_T>(replacements + j * dim, dim,
                                        1000.0f + 3000.0f * j + 10.0f * i);
        }
        const labelType moved = i + moved_offset;

        // Both orders, because they are different races: updating a label whose insert job is
        // still pending and then moving it, against moving it first and updating what the move
        // left behind.
        if (i % 2 == 0) {
            ASSERT_EQ(tiered_index->updateVectors(i, replacements, per_label), VecSimUpdate_OK)
                << "label " << i;
            if (can_relabel) {
                ASSERT_EQ(tiered_index->relabelVector(i, moved), VecSimRelabel_OK) << "label " << i;
            }
        } else {
            if (can_relabel) {
                ASSERT_EQ(tiered_index->relabelVector(i, moved), VecSimRelabel_OK) << "label " << i;
            }
            const labelType target = can_relabel ? moved : i;
            ASSERT_EQ(tiered_index->updateVectors(target, replacements, per_label), VecSimUpdate_OK)
                << "label " << target;
        }

        // A new document arriving in the middle of all that, so the jobs never run out of work.
        TEST_DATA_T fresh[dim];
        GenerateVector<TEST_DATA_T>(fresh, dim, 7000.0f + 10.0f * i);
        ASSERT_EQ(VecSimIndex_AddVector(tiered_index, fresh, fresh_base + i), 1);
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();

    auto *svs_index = tiered_index->GetSVSIndex();
    ASSERT_GT(tiered_index->GetBackendIndex()->indexSize(), 0)
        << "the backend never came into play";
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0) << "jobs were left unrun, so the join raced";

    // Every label ended up where the writes left it, exactly once: the n moved (or original, where
    // this build cannot move) labels and the n new ones.
    ASSERT_EQ(tiered_index->indexLabelCount(), 2 * n);
    for (size_t i = 0; i < n; i++) {
        const labelType expected = can_relabel ? i + moved_offset : i;
        ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(expected) ||
                    svs_index->isLabelExists(expected))
            << "lost label " << expected;
        if (can_relabel) {
            ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(i)) << "stale label " << i;
            ASSERT_FALSE(svs_index->isLabelExists(i)) << "stale label " << i;
        }
        ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(fresh_base + i) ||
                    svs_index->isLabelExists(fresh_base + i))
            << "lost label " << fresh_base + i;
    }

    // And holds only what those writes put there - the replaced vectors are marked, not gone, so
    // they are subtracted. This is what fails if an update's delete or a move crossed with a job
    // moving the same batch.
    ASSERT_EQ(tiered_index->indexSize() - svs_index->getNumMarkedDeleted(), n * per_label + n);

    if (!svs_index->isCompressed()) {
        // Each label holds exactly what the writes put there, asked of the label directly rather
        // than through a query: a k-nearest search over a graph that has just absorbed n deletes
        // and n moves is approximate, and it does miss - it answered a neighbour 400 away instead
        // of the vector itself often enough to fail about one run in fifteen. That says nothing
        // about whether the writes landed, which is what this test is for, so it asks for the
        // distance from the label to the vector it should hold. Uncompressed only: a compressed
        // backend stores a lossy form and does not answer 0 for its own vector.
        for (size_t i : {(size_t)0, (size_t)1, n / 2, n - 1}) {
            const labelType expected = can_relabel ? i + moved_offset : i;
            for (size_t j = 0; j < per_label; j++) {
                TEST_DATA_T stored[dim];
                GenerateVector<TEST_DATA_T>(stored, dim, 1000.0f + 3000.0f * j + 10.0f * i);
                ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, expected, stored), 0)
                    << "label " << expected << " does not hold the vector the update gave it";
            }
            TEST_DATA_T fresh[dim];
            GenerateVector<TEST_DATA_T>(fresh, dim, 7000.0f + 10.0f * i);
            ASSERT_EQ(VecSimIndex_GetDistanceFrom_Unsafe(tiered_index, fresh_base + i, fresh), 0)
                << "label " << fresh_base + i << " does not hold the vector it was added with";
        }
    }
}

#if HAVE_SVS_REPLACE_EXTERNAL_ID

// Relabel on a tier, in the two write states a vector can be in: buffered with its update job
// still pending, and moved to the backend. These are the states `addVector` and `insertJob` cover
// for insertion, and the tier has to move the label in whichever one holds it.
//
// The fixture's type set spans single-value, multi-value and Quant_8, so this also answers whether
// a compressed backend can move a label -- `replace_external_id` renames an id and never touches
// the stored vector, so it can, even where the backend cannot report values.
TYPED_TEST(SVSTieredIndexTest, relabelVectorMovesTheLabelInBothWriteStates) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Thresholds of 1, as `insertJob` uses, so one vector is enough to trigger the update job.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 7);
    VecSimIndex_AddVector(tiered_index, vector, 7);

    // Buffered: nothing has run, so the label lives only in the flat buffer.
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OK);
    ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(70));
    ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(7));
    ASSERT_EQ(tiered_index->indexLabelCount(), 1) << "the move must not add a label";

    // Drain: the update job carries the vector to the backend, and must carry the *new* label,
    // not the one it had when the job was queued.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(70));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(7));

    // Backend only: the same move again, now served by the backend tier.
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 70, 700), VecSimRelabel_OK);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(700));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(70));

    // Still the same vector, and findable under the label it ended up with. Asserted by search
    // rather than by reading values back, because a compressed backend reports no values.
    auto verify = [&](size_t label, double score, size_t rank) {
        ASSERT_EQ(label, 700);
        ASSERT_NEAR(score, 0, 1e-4);
    };
    runTopKSearchTest(tiered_index, vector, 1, verify);
}

// The test above never reaches `labelToInsertJobs`: with a training threshold of 1 the first
// vector is training data, and training data has no insert job. Draining once makes the backend
// ready, so the next add does get one.
TYPED_TEST(SVSTieredIndexTest, relabelVectorRemapsAPendingInsertJob) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    mock_thread_pool.init_threads();
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0, 0);
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->ready());
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    const size_t per_label = TypeParam::isMulti() ? 2 : 1;
    for (size_t j = 0; j < per_label; j++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, 7 + j);
    }
    ASSERT_EQ(mock_thread_pool.jobQ.size(), per_label);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(7).size(), per_label);

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OK);

    ASSERT_EQ(tiered_index->labelToInsertJobs.count(7), 0);
    ASSERT_EQ(tiered_index->labelToInsertJobs.at(70).size(), per_label);
    for (auto *job : tiered_index->labelToInsertJobs.at(70)) {
        ASSERT_EQ(job->label, 70);
        // Left Delayed, the ingest below would defer forever.
        ASSERT_EQ(static_cast<SVSInsertJob *>(job)->status.load(), SVSInsertJob::Status::Pending);
    }
    ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(70));
    ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(7));

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(70));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(7));
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);
}

// The worker consumes the delayed submission; relabel must resume it exactly once.
TYPED_TEST(SVSTieredIndexTest, relabelVectorDefersAnInsertJobInsideTheDelayWindow) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    mock_thread_pool.init_threads();
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0, 0);
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->ready());
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, 7);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);

    auto *job = static_cast<SVSInsertJob *>(mock_thread_pool.jobQ.front().job);
    tiered_index->registerTracingCallback("Relabel::before_backend_remap", [&]() {
        std::thread worker([&]() { mock_thread_pool.thread_iteration(); });
        worker.join();
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        EXPECT_EQ(job->status.load(), SVSInsertJob::Status::Parked);
        EXPECT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(70));
    });
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OK);

    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(7));
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job, job);

    ASSERT_EQ(job->status.load(), SVSInsertJob::Status::Pending);
    ASSERT_EQ(job->label, 70);
    mock_thread_pool.thread_iteration();

    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(70));
    ASSERT_TRUE(tiered_index->labelToInsertJobs.empty());
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);
}

// isLabelExists() reports a soft-deleted label as free, but SVS keeps its translator entry
// until the label's consolidate job runs, and would refuse the remap. So relabelVector takes
// those jobs over and consolidates them itself.
TYPED_TEST(SVSTieredIndexTest, relabelVectorTakesOverPendingConsolidatesOfTheTarget) {
    size_t dim = 4;
    const size_t n = 3;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    mock_thread_pool.init_threads();
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    for (size_t i = 0; i < n; i++) {
        ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(i));
    }

    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 1), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, SVS_CONSOLIDATE_JOB);
    auto *first_job = static_cast<SVSConsolidateJob *>(mock_thread_pool.jobQ.front().job);

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 2, 1), VecSimRelabel_OK);
    ASSERT_EQ(first_job->status.load(), SVSConsolidateJob::Status::TakenOver);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(1));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(2));

    // The first job is still registered under label 1, so this one is appended to it.
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 1), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);

    // The takeover now has to skip the job it already invalidated.
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 0, 1), VecSimRelabel_OK);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(1));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(0));
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
}

// A consolidate job that is already inside consolidate() cannot be taken over, so the takeover
// waits for it to leave. The test holds the flag and does the consolidation the wrapper would,
// so the remap that follows really is unblocked.
TYPED_TEST(SVSTieredIndexTest, relabelVectorWaitsForAnExecutingConsolidate) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    mock_thread_pool.init_threads();
    for (size_t i = 0; i < 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(1));

    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 1), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    auto *consolidate_job = static_cast<SVSConsolidateJob *>(mock_thread_pool.jobQ.front().job);
    consolidate_job->status.store(SVSConsolidateJob::Status::Executing, std::memory_order_release);

    std::atomic<int> relabel_code{-1};
    std::thread relabeler(
        [&]() { relabel_code = static_cast<int>(VecSimIndex_RelabelVector(tiered_index, 0, 1)); });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // EXPECT, so the thread is always joined below.
    EXPECT_EQ(relabel_code.load(), -1) << "the takeover did not wait for the running job";

    tiered_index->GetSVSIndex()->consolidate({1});
    consolidate_job->status.store(SVSConsolidateJob::Status::Done, std::memory_order_release);
    relabeler.join();

    ASSERT_EQ(relabel_code.load(), static_cast<int>(VecSimRelabel_OK));
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(1));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(0));

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
}

// Use the real worker's Done -> unregister -> delete path while relabel holds its job pointer.
// Without the shared registry guard, deletion can precede waitForJob's next load (an ASan UAF).
TYPED_TEST(SVSTieredIndexTest, relabelVectorProtectsExecutingConsolidateJobLifetime) {
    const size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    tieredIndexMock mock_thread_pool(1);
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 2);
    ASSERT_INDEX(tiered_index);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    mock_thread_pool.thread_iteration(); // initialize before starting the consolidate worker
    ASSERT_TRUE(tiered_index->GetSVSIndex()->ready());
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 1), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, SVS_CONSOLIDATE_JOB);

    std::promise<void> worker_paused, relabel_waiting, job_deleted;
    auto paused = worker_paused.get_future();
    auto waiting = relabel_waiting.get_future();
    auto deleted = job_deleted.get_future();
    const auto timeout = std::chrono::seconds(5);
    bool worker_saw_waiter = false;
    bool waiter_entered = false;
    bool deleted_while_waiting = false;
    tiered_index->registerTracingCallback("ConsolidateJob::before_done", [&] {
        worker_paused.set_value();
        worker_saw_waiter = waiting.wait_for(timeout) == std::future_status::ready;
    });
    tiered_index->registerTracingCallback("Relabel::before_wait_for_consolidate", [&] {
        waiter_entered = true;
        relabel_waiting.set_value();
        // Let the worker publish Done and attempt cleanup before waitForJob reads the job.
        // The registry guard must prevent deletion until this callback and the wait return.
        deleted_while_waiting =
            deleted.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready;
    });
    tiered_index->registerTracingCallback("ConsolidateJob::after_delete",
                                          [&] { job_deleted.set_value(); });

    mock_thread_pool.init_threads();
    const bool reached_worker = paused.wait_for(timeout) == std::future_status::ready;
    auto relabel_code = VecSimRelabel_OldLabelMissing;
    if (reached_worker) {
        // Public writes stay on the test thread; the worker alone changes the job's status.
        relabel_code = VecSimIndex_RelabelVector(tiered_index, 0, 1);
    }
    if (!waiter_entered) {
        relabel_waiting.set_value(); // release the worker if relabel never reached its wait
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();

    ASSERT_TRUE(reached_worker);
    ASSERT_TRUE(worker_saw_waiter);
    ASSERT_TRUE(waiter_entered);
    EXPECT_FALSE(deleted_while_waiting);
    EXPECT_EQ(deleted.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(relabel_code, VecSimRelabel_OK);
    EXPECT_TRUE(mock_thread_pool.jobQ.empty());
    EXPECT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(1));
    EXPECT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(0));
    EXPECT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    EXPECT_EQ(tiered_index->indexLabelCount(), 1);
}

// Rollback needs the backend to refuse the target, and to hold old_label live while the buffer
// still holds it with a registered job. Both are built by hand:
//  - the target keeps a stale translator entry, because a backend-side delete leaves no
//    consolidate job for the takeover to run. Single-value only - the multi backend checks live
//    labels only, so a soft-deleted target never blocks it.
//  - old_label is live in both tiers, the state an insert job is in after it published and
//    before it dropped its buffer copy. Published here directly, then the job marked Done.
TYPED_TEST(SVSTieredIndexTestBasic, relabelVectorRollsBackWhenTheBackendRefusesTheTarget) {
    size_t dim = 4;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    mock_thread_pool.init_threads();
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0, 0);
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->ready());
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index->GetBackendIndex(), dim, 5, 5);
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index->GetBackendIndex(), 5), 1);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(5));
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 1);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 7, 7);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    auto *job = static_cast<SVSInsertJob *>(mock_thread_pool.jobQ.front().job);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index->GetBackendIndex(), dim, 7, 7);
    job->status.store(SVSInsertJob::Status::Done, std::memory_order_relaxed);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(7));
    ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(7));

    // Keep the original completed-job case, then exercise rollback with a parked submission.
    for (auto status : {SVSInsertJob::Status::Done, SVSInsertJob::Status::Pending}) {
        job->status.store(status, std::memory_order_relaxed);
        tiered_index->registerTracingCallback("Relabel::before_backend_remap", [&, status]() {
            if (status == SVSInsertJob::Status::Pending) {
                std::thread worker([&]() { mock_thread_pool.thread_iteration(); });
                worker.join();
                EXPECT_TRUE(mock_thread_pool.jobQ.empty());
                EXPECT_EQ(job->status.load(), SVSInsertJob::Status::Parked);
            }
        });
        ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 5), VecSimRelabel_NewLabelTaken);

        ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(7));
        ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(5));
        ASSERT_EQ(tiered_index->labelToInsertJobs.count(5), 0);
        ASSERT_EQ(tiered_index->labelToInsertJobs.at(7).size(), 1);
        ASSERT_EQ(job->label, 7);
        ASSERT_EQ(job->status.load(), status);
        ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
        ASSERT_EQ(mock_thread_pool.jobQ.front().job, job);
        ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(7));
        ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(5));
    }

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(7));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(5));
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);
}

TYPED_TEST(SVSTieredIndexTest, relabelVectorRejectsOnATier) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Thresholds of 1, as `insertJob` uses, so one vector is enough to trigger the update job.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 1);
    VecSimIndex_AddVector(tiered_index, vector, 1);
    GenerateVector<TEST_DATA_T>(vector, dim, 2);
    VecSimIndex_AddVector(tiered_index, vector, 2);

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 1, 1), VecSimRelabel_SameLabel);
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 42, 43), VecSimRelabel_OldLabelMissing);
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 1, 2), VecSimRelabel_NewLabelTaken)
        << "target held by the buffer";

    // Move label 2 to the backend, so the next rejection comes from the other tier.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(2));
    GenerateVector<TEST_DATA_T>(vector, dim, 3);
    VecSimIndex_AddVector(tiered_index, vector, 3);
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 3, 2), VecSimRelabel_NewLabelTaken)
        << "target held by the backend";

    // A rejection leaves both tiers as they were.
    ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(3));
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(2));
}

// The analogue of `insertJobAsync` for relabel: workers running while the label moves. This is
// the case `updateJobMutex` is held for -- an update job snapshots the buffer's labels by value
// and afterwards reconciles only id swaps and deletions, so a rename that landed inside its
// window would be invisible to it and the vector would reach the backend under the old label.
// The tier's version of the deleted-label contract, which has an extra dimension the plain index
// does not: the delete can land while the vector is still buffered or after it reached the
// backend, and a move must report the label absent either way rather than resurrecting it in the
// tier the delete missed.
//
// Unlike a plain SVS index, no tombstone is observable here -- `getNumMarkedDeleted()` reads 0
// after the delete -- so this asserts the refusal and the absence rather than the marking. The
// plain index covers the soft-delete state itself.
TYPED_TEST(SVSTieredIndexTest, relabelVectorAfterDeleteOnATier) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // Thresholds of 1 so a single vector reaches the backend, as `insertJob` does.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    // Buffered: deleted before any job ran, so the flat buffer is the tier that held it.
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 7);
    VecSimIndex_AddVector(tiered_index, vector, 7);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 7), 1);
    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 7, 70), VecSimRelabel_OldLabelMissing);
    ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(70));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(70));

    // Ingested, then deleted: now the backend is the tier that held it, and its delete is soft.
    GenerateVector<TEST_DATA_T>(vector, dim, 8);
    VecSimIndex_AddVector(tiered_index, vector, 8);
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(8));
    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 8), 1);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(8));

    ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, 8, 80), VecSimRelabel_OldLabelMissing);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(80)) << "a tombstone was renamed";
    ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(80));
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
}

// The lock orders in this class disagree, and relabel has to survive that. Nearly everything
// takes the flat guard before the main one, but in-place `addVector` takes `mainIndexGuard`
// *before* `flatIndexGuard` while the backend is still empty. Acquiring relabel's guards one
// after another closes a cycle with that path: relabel holds flat and waits for main, the add
// holds main and waits for flat.
//
// A regression shows up as the global 300s timeout rather than an assertion, since a deadlock
// hangs. The two threads touch disjoint label ranges, so every relabel here should report OK and
// the only thing under test is that both threads finish.
TYPED_TEST(SVSTieredIndexTest, relabelVectorDoesNotDeadlockAgainstInPlaceAdd) {
    size_t dim = 4;
    const size_t iterations = 300;
    const labelType add_base = 10000; // disjoint from the relabelled range
    const labelType relabel_offset = 50000;

    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    // A training threshold far above what this test inserts, so the backend stays empty and the
    // in-place add keeps taking the main-then-flat path that inverts against relabel.
    auto *tiered_index =
        this->CreateTieredSVSIndex(svs_params, mock_thread_pool, iterations * 10, iterations * 10);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < iterations; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0) << "the backend must stay empty";

    VecSim_SetWriteMode(VecSim_WriteInPlace);
    std::atomic<size_t> relabels_ok{0};

    std::thread adder([&]() {
        TEST_DATA_T vector[dim];
        for (size_t i = 0; i < iterations; i++) {
            GenerateVector<TEST_DATA_T>(vector, dim, i);
            VecSimIndex_AddVector(tiered_index, vector, add_base + i);
        }
    });
    std::thread relabeler([&]() {
        for (size_t i = 0; i < iterations; i++) {
            if (VecSimIndex_RelabelVector(tiered_index, i, i + relabel_offset) ==
                VecSimRelabel_OK) {
                relabels_ok++;
            }
        }
    });
    adder.join();
    relabeler.join();
    VecSim_SetWriteMode(VecSim_WriteAsync);

    // Reaching here at all is the result. The counts just confirm the threads did real work
    // rather than bailing out early on some other rejection.
    ASSERT_EQ(relabels_ok, iterations);
    for (size_t i = 0; i < iterations; i++) {
        ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(i + relabel_offset))
            << "label " << i;
        ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(i)) << "stale label " << i;
    }
}

TYPED_TEST(SVSTieredIndexTest, relabelVectorDuringUpdateJob) {
    size_t dim = 4;
    size_t n = 200;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    // Labels are moved into a range past `n`, so a move can never collide with a label that is
    // still waiting to be ingested.
    const labelType offset = n + 500;
    mock_thread_pool.init_threads();
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    for (size_t i = 0; i < n; i++) {
        // Whichever tier holds it by now, and whether or not a job is mid-flight, the move must
        // be accepted and must be the only thing that changes.
        ASSERT_EQ(VecSimIndex_RelabelVector(tiered_index, i, i + offset), VecSimRelabel_OK)
            << "label " << i;
    }
    mock_thread_pool.thread_pool_join();

    // Every vector survived under its new label and under no other, which is what fails if a job
    // ingested one under the label it was queued with.
    ASSERT_EQ(tiered_index->indexLabelCount(), n);
    for (size_t i = 0; i < n; i++) {
        ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(i)) << "stale label " << i;
        ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(i)) << "stale label " << i;
        ASSERT_TRUE(tiered_index->GetFlatIndex()->isLabelExists(i + offset) ||
                    tiered_index->GetSVSIndex()->isLabelExists(i + offset))
            << "lost label " << i + offset;
    }
}

// What `updateJobMutex` is actually for, pinned. An update job snapshots the buffer's labels by
// value, then adds to the backend; the tracing hook sits between those two steps, which is the
// only window where a rename is invisible to the job. Land one there and, unheld, the job would
// carry the vector into the backend under the label it snapshotted -- the old one.
//
// The relabel has to come from another thread: the job holds `updateJobMutex` for its whole
// duration, so relabeling from inside the hook would block on a lock this thread already holds.
// Blocking is the expected outcome here, and is what makes the result correct.
TYPED_TEST(SVSTieredIndexTest, relabelVectorCannotLandInsideAnUpdateJobsWindow) {
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti(),
                        .quantBits = TypeParam::get_quant_bits()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    std::thread relabel_thread;
    std::atomic<bool> relabel_entered{false};
    bool hooked = false;
    tiered_index->registerTracingCallback("UpdateJob::before_add_to_svs", [&]() {
        if (hooked) {
            return;
        }
        hooked = true;
        relabel_thread = std::thread([&]() {
            relabel_entered = true;
            VecSimIndex_RelabelVector(tiered_index, 7, 70);
        });
        while (!relabel_entered) {
            std::this_thread::yield();
        }
        // Long enough that the move would have completed if it were free to.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    });

    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 7);
    VecSimIndex_AddVector(tiered_index, vector, 7);
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_TRUE(hooked) << "the hook never ran, so the window was not exercised";
    relabel_thread.join();

    // The move happened, and it happened to the vector the job had already ingested -- so the
    // backend holds the new label and nothing holds the old one.
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(7)) << "ingested under the stale label";
    ASSERT_FALSE(tiered_index->GetFlatIndex()->isLabelExists(7));
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(70) ||
                tiered_index->GetFlatIndex()->isLabelExists(70));
}

#endif // HAVE_SVS_REPLACE_EXTERNAL_ID

TYPED_TEST(SVSTieredIndexTestBasic, addVectorDuringInitializationWithFullFlatBuffer) {
    constexpr size_t dim = 4;
    constexpr size_t n = 8;
    for (bool multi : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "multi=" << multi);
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = dim,
                            .metric = VecSimMetric_L2,
                            .multi = multi};
        auto svs_params = CreateParams(params);
        std::atomic<size_t> submitted_insert_jobs{0};
        tieredIndexMock mock_thread_pool(1);
        auto tiered_params = this->CreateTieredSVSParams(svs_params, mock_thread_pool, n);
        tiered_params.flatBufferLimit = n;
        // The mock resolves its queue through index_ctx; use jobQueue to count submissions.
        tiered_params.jobQueue = &submitted_insert_jobs;
        tiered_params.submitCb = [](void *counter, void *ctx, AsyncJob **jobs,
                                    JobCallback *callbacks, size_t count) {
            for (size_t i = 0; i < count; ++i) {
                if (jobs[i]->jobType == SVS_INSERT_VECTOR_JOB) {
                    static_cast<std::atomic<size_t> *>(counter)->fetch_add(
                        1, std::memory_order_relaxed);
                }
            }
            return tieredIndexMock::submit_callback(nullptr, ctx, jobs, callbacks, count);
        };
        auto *index = this->CreateTieredSVSIndex(tiered_params, mock_thread_pool);
        ASSERT_INDEX(index);

        for (size_t i = 0; i < n; ++i) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
        ASSERT_EQ(index->GetFlatIndex()->indexSize(), n);
        ASSERT_FALSE(index->GetSVSIndex()->ready());
        ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);

        std::promise<void> snapshot_taken;
        auto snapshot_ready = snapshot_taken.get_future();
        std::promise<void> additions_finished;
        auto additions_ready = additions_finished.get_future();
        bool completed_during_init = false;
        size_t buffered_during_init = 0;
        // Pause with the snapshot fixed, flat unlocked, and updateJobMutex held exclusively.
        index->registerTracingCallback("UpdateJob::after_snapshot", [&] {
            snapshot_taken.set_value();
            completed_during_init =
                additions_ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
            if (completed_during_init) {
                buffered_during_init = index->GetFlatIndex()->indexSize();
            }
            // On timeout, let init finish so a blocked add can return without hanging the test.
        });
        mock_thread_pool.init_threads();
        const bool reached_snapshot =
            snapshot_ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
        if (reached_snapshot) {
            // Public writes stay on the test thread. The first add sees a full buffer;
            // the second sees a buffer above the limit. Both must return before init resumes.
            EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(index, dim, n, n), 1);
            EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(index, dim, n + 1, n + 1), 1);
            // The only worker is paused in init. Neither insert may be submitted yet,
            // where another worker could pick it up and block behind updateJobMutex.
            std::lock_guard queue_lock{mock_thread_pool.queue_guard};
            EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        }
        additions_finished.set_value();
        mock_thread_pool.thread_pool_wait();
        mock_thread_pool.thread_pool_join();

        ASSERT_TRUE(reached_snapshot) << "Initialization did not reach the snapshot hook";
        ASSERT_TRUE(completed_during_init) << "addVector waited for initialization";
        EXPECT_EQ(buffered_during_init, n + 2);
        EXPECT_TRUE(index->GetSVSIndex()->ready());
        EXPECT_EQ(index->GetFlatIndex()->indexSize(), 0);
        EXPECT_EQ(index->GetBackendIndex()->indexSize(), n + 2);
        EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), n + 2);
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        // Both deferred inserts were consumed by init, not queued and later drained by the mock.
        EXPECT_EQ(submitted_insert_jobs.load(std::memory_order_relaxed), 0);
        EXPECT_EQ(index->GetSVSIndex()->getParallelism(), 1);
        for (size_t i = 0; i < n + 2; ++i) {
            std::vector<std::vector<TEST_DATA_T>> stored;
            index->getDataByLabel(i, stored);
            const std::vector<std::vector<TEST_DATA_T>> expected{
                std::vector<TEST_DATA_T>(dim, static_cast<TEST_DATA_T>(i))};
            EXPECT_EQ(stored, expected) << "label=" << i;
        }
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, mainLockLimitWaitsForInitialization) {
    constexpr size_t dim = 4;
    constexpr size_t training_rows = 8;
    constexpr size_t main_lock_limit = 100'000;
    for (bool multi : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "multi=" << multi);
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = dim,
                            .metric = VecSimMetric_L2,
                            .multi = multi};
        auto svs_params = CreateParams(params);
        tieredIndexMock mock_thread_pool(1);
        auto tiered_params =
            this->CreateTieredSVSParams(svs_params, mock_thread_pool, training_rows);
        tiered_params.flatBufferLimit = training_rows;
        auto *index = this->CreateTieredSVSIndex(tiered_params, mock_thread_pool);
        ASSERT_INDEX(index);

        std::promise<void> before_publish, resume_init, before_wait;
        auto publish_ready = before_publish.get_future();
        auto resume_ready = resume_init.get_future();
        auto wait_ready = before_wait.get_future();
        std::atomic<bool> pause_timed_out{false};
        std::atomic<bool> wait_seen{false};
        index->registerTracingCallback("UpdateJob::before_publish", [&] {
            before_publish.set_value();
            pause_timed_out.store(resume_ready.wait_for(std::chrono::seconds(60)) !=
                                  std::future_status::ready);
        });
        index->registerTracingCallback("AddVector::before_init_wait", [&] {
            if (!wait_seen.exchange(true)) {
                before_wait.set_value();
            }
        });

        for (size_t i = 0; i < training_rows; ++i) {
            EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i), 1);
        }
        mock_thread_pool.init_threads();
        const bool reached_publish =
            publish_ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;

        size_t added = training_rows;
        if (reached_publish) {
            // Exercise the actual policy floor, not a test-only smaller limit.
            // Each addition below the limit must return while init is held open.
            for (; added < main_lock_limit; ++added) {
                EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(index, dim, added, added), 1);
                if (pause_timed_out.load()) {
                    break;
                }
            }
        }
        const bool filled_during_init =
            reached_publish && added == main_lock_limit && !pause_timed_out.load();
        std::future<int> writer;
        bool reached_wait = false;
        bool blocked_at_limit = false;
        bool still_waiting_after_delete = false;
        if (filled_during_init) {
            writer = std::async(std::launch::async, [&] {
                return GenerateAndAddVector<TEST_DATA_T>(index, dim, main_lock_limit,
                                                         main_lock_limit);
            });
            reached_wait =
                wait_ready.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
            blocked_at_limit = reached_wait && writer.wait_for(std::chrono::milliseconds(50)) ==
                                                   std::future_status::timeout;
            if (blocked_at_limit && !pause_timed_out.load()) {
                // Acquiring flat shared here also checks that the waiting add released it.
                index->acquireSharedLocks();
                EXPECT_EQ(index->GetFlatIndex()->indexSize(), main_lock_limit);
                index->releaseSharedLocks();
                EXPECT_FALSE(index->GetSVSIndex()->ready());

                // Keep the snapshot, but invalidate all deferred filler jobs. This avoids
                // constructing a large graph; the wait must still last until init completes.
                for (size_t i = training_rows; i < main_lock_limit; ++i) {
                    EXPECT_EQ(index->deleteVector(i), 1);
                }
                still_waiting_after_delete =
                    writer.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
            }
        }

        // Release init before any fatal assertion or future destruction.
        resume_init.set_value();
        int add_result = -1;
        if (writer.valid()) {
            EXPECT_NO_THROW(add_result = writer.get());
        }
        mock_thread_pool.thread_pool_wait();
        mock_thread_pool.thread_pool_join();

        ASSERT_TRUE(reached_publish);
        ASSERT_TRUE(filled_during_init);
        ASSERT_TRUE(reached_wait);
        ASSERT_TRUE(blocked_at_limit);
        ASSERT_TRUE(still_waiting_after_delete);
        ASSERT_FALSE(pause_timed_out.load());
        EXPECT_EQ(add_result, 1);
        EXPECT_TRUE(index->GetSVSIndex()->ready());
        EXPECT_EQ(index->GetFlatIndex()->indexSize(), 0);
        EXPECT_EQ(index->GetBackendIndex()->indexSize(), training_rows + 1);
        EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), training_rows + 1);
        EXPECT_TRUE(index->GetSVSIndex()->isLabelExists(main_lock_limit));
        EXPECT_TRUE(index->labelToInsertJobs.empty());
        EXPECT_TRUE(index->invalidJobs.empty());
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, overwriteSnapshotLabelDuringInitialization) {
    constexpr size_t dim = 4;
    constexpr size_t n = 8;
    constexpr labelType overwritten_label = 0;
    for (size_t overwrite_count : {size_t{1}, size_t{2}}) {
        SCOPED_TRACE(::testing::Message() << "overwrite_count=" << overwrite_count);
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = dim,
                            .metric = VecSimMetric_L2,
                            .multi = false};
        auto svs_params = CreateParams(params);
        tieredIndexMock mock_thread_pool(1);
        auto tiered_params = this->CreateTieredSVSParams(svs_params, mock_thread_pool, n);
        tiered_params.flatBufferLimit = n;
        auto *index = this->CreateTieredSVSIndex(tiered_params, mock_thread_pool);
        ASSERT_INDEX(index);

        for (size_t i = 0; i < n; ++i) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
        ASSERT_FALSE(index->GetSVSIndex()->ready());
        ASSERT_EQ(index->GetFlatIndex()->indexSize(), n);
        ASSERT_EQ(index->labelToInsertJobs.count(overwritten_label), 0);
        ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);

        std::promise<void> snapshot_taken, overwrites_finished;
        auto snapshot_ready = snapshot_taken.get_future();
        auto overwrites_ready = overwrites_finished.get_future();
        const auto timeout = std::chrono::seconds(5);
        bool completed_during_init = false;
        index->registerTracingCallback("UpdateJob::after_snapshot", [&] {
            snapshot_taken.set_value();
            completed_during_init = overwrites_ready.wait_for(timeout) == std::future_status::ready;
            // Release init on timeout as well, so a regressed blocking add can return.
        });

        mock_thread_pool.init_threads();
        const bool reached_snapshot = snapshot_ready.wait_for(timeout) == std::future_status::ready;
        if (reached_snapshot) {
            // The first overwrite cancels a captured row with no insert job. Removing
            // label 0 swaps the last captured row down; init cleanup must later preserve
            // the replacement and update its insert job's id as the buffer shrinks.
            // A second overwrite also exercises invalidation of an unsubmitted job.
            for (size_t i = 0; i < overwrite_count; ++i) {
                EXPECT_EQ(
                    GenerateAndAddVector<TEST_DATA_T>(index, dim, overwritten_label, n + i + 1), 0);
            }
        }
        overwrites_finished.set_value();
        mock_thread_pool.thread_pool_wait();
        mock_thread_pool.thread_pool_join();

        ASSERT_TRUE(reached_snapshot) << "Initialization did not reach the snapshot hook";
        ASSERT_TRUE(completed_during_init) << "Overwrite waited for initialization";
        EXPECT_TRUE(index->GetSVSIndex()->ready());
        EXPECT_EQ(index->GetFlatIndex()->indexSize(), 0);
        // Per-label consolidation does not compact the deleted snapshot row.
        EXPECT_EQ(index->GetBackendIndex()->indexSize(), n + 1);
        EXPECT_EQ(index->getNumMarkedDeleted(), 1);
        EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), n);
        EXPECT_TRUE(index->labelToInsertJobs.empty());
        EXPECT_TRUE(index->invalidJobs.empty());
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        for (size_t i = 0; i < n; ++i) {
            // Read the backend directly so a surviving flat row cannot hide a stale
            // snapshot copy. Exact cardinality and values detect duplicates as well.
            std::vector<std::vector<TEST_DATA_T>> stored;
            index->GetBackendIndex()->getDataByLabel(i, stored);
            const auto value = i == overwritten_label ? n + overwrite_count : i;
            const std::vector<std::vector<TEST_DATA_T>> expected{
                std::vector<TEST_DATA_T>(dim, static_cast<TEST_DATA_T>(value))};
            EXPECT_EQ(stored, expected) << "label=" << i;
        }
    }
}

// A search already occupies one worker, so init proceeds after its reservation timeout.
// The search reaches the backend while init is still paused immediately after publication.
TYPED_TEST(SVSTieredIndexTestBasic, deleteVectorBeforeInitializationPublishes) {
    if (std::thread::hardware_concurrency() < 2) {
        GTEST_SKIP() << "Requires two workers";
    }
    constexpr size_t dim = 4;
    constexpr size_t n = 8;
    for (bool multi : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "multi=" << multi);
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = dim,
                            .metric = VecSimMetric_L2,
                            .multi = multi};
        params.search_window_size = n;
        auto svs_params = CreateParams(params);
        tieredIndexMock mock_thread_pool(2);
        auto *index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n + (multi ? 1 : 0));
        ASSERT_INDEX(index);
        for (size_t i = 0; i < n - 1; ++i) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
        }
        if (multi) {
            GenerateAndAddVector<TEST_DATA_T>(index, dim, 0, 20);
        }

        std::promise<void> worker_started, start_search, build_finished;
        std::promise<void> query_between_tiers, published, search_finished;
        auto worker_ready = worker_started.get_future();
        auto start_ready = start_search.get_future();
        auto build_ready = build_finished.get_future();
        auto query_ready = query_between_tiers.get_future();
        auto publish_ready = published.get_future();
        auto search_ready = search_finished.get_future();
        const auto timeout = std::chrono::seconds(5);
        bool query_reached_hook = false;
        bool query_saw_publication = false;
        bool search_completed_during_init = false;
        index->registerTracingCallback("UpdateJob::before_publish", [&] {
            EXPECT_FALSE(index->GetSVSIndex()->ready());
            build_finished.set_value();
            query_reached_hook = query_ready.wait_for(timeout) == std::future_status::ready;
        });
        index->registerTracingCallback("Query::before_backend", [&] {
            query_between_tiers.set_value();
            query_saw_publication = publish_ready.wait_for(timeout) == std::future_status::ready;
        });
        index->registerTracingCallback("UpdateJob::after_publish", [&] {
            published.set_value();
            search_completed_during_init =
                search_ready.wait_for(timeout) == std::future_status::ready;
        });

        VecSimQueryReply *reply = nullptr;
        auto search = [&](VecSimIndex *, size_t) {
            worker_started.set_value();
            if (start_ready.wait_for(timeout) == std::future_status::ready) {
                TEST_DATA_T query[dim] = {};
                reply = VecSimIndex_TopKQuery(index, query, n, nullptr, BY_ID);
            }
            search_finished.set_value();
        };
        SVSMultiThreadJob::JobsRegistry registry(index->getAllocator());
        auto jobs = SVSMultiThreadJob::createJobs(index->getAllocator(), HNSW_SEARCH_JOB, search,
                                                  index, 1, timeout, &registry);
        index->submitJobs(jobs);
        mock_thread_pool.init_threads();
        const bool search_occupies_worker =
            worker_ready.wait_for(timeout) == std::future_status::ready;
        // Cross the training threshold only after the search worker has been occupied.
        GenerateAndAddVector<TEST_DATA_T>(index, dim, n - 1, n - 1);
        const bool reached_build_hook = build_ready.wait_for(timeout) == std::future_status::ready;
        if (reached_build_hook) {
            EXPECT_EQ(VecSimIndex_DeleteVector(index, 0), multi ? 2 : 1);
            EXPECT_EQ(VecSimIndex_DeleteVector(index, 0), 0);
        }
        start_search.set_value();
        // Init can still enqueue consolidation after the queue first becomes empty.
        mock_thread_pool.thread_pool_wait();
        mock_thread_pool.thread_pool_join();

        std::unique_ptr<VecSimQueryReply, decltype(&VecSimQueryReply_Free)> result(
            reply, VecSimQueryReply_Free);
        ASSERT_TRUE(search_occupies_worker);
        ASSERT_TRUE(reached_build_hook);
        ASSERT_TRUE(query_reached_hook);
        ASSERT_TRUE(query_saw_publication);
        ASSERT_TRUE(search_completed_during_init);
        ASSERT_NE(reply, nullptr);
        EXPECT_EQ(VecSimQueryReply_GetCode(reply), VecSim_QueryReply_OK);
        ASSERT_EQ(VecSimQueryReply_Len(reply), n - 1);
        for (size_t i = 0; i < n - 1; ++i) {
            EXPECT_EQ(VecSimQueryResult_GetId(reply->results.data() + i), i + 1);
        }
        EXPECT_TRUE(index->GetSVSIndex()->ready());
        EXPECT_FALSE(index->GetSVSIndex()->isLabelExists(0));
        EXPECT_EQ(index->GetFlatIndex()->indexSize(), 0);
        EXPECT_EQ(index->indexLabelCount(), n - 1);
        EXPECT_EQ(index->getNumMarkedDeleted(), multi ? 2 : 1);
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
    }
}

TYPED_TEST(SVSTieredIndexTest, TestDebugInfoThreadCount) {
    // Set thread_pool_size to 4 or actual number of available CPUs
    const auto num_threads = std::min(4U, getAvailableCPUs());
    if (num_threads < 2) {
        // If the number of threads is less than 2, we can't run the test
        GTEST_SKIP() << "No threads available";
    }

    constexpr size_t training_threshold = 10;
    constexpr size_t update_threshold = 10;
    constexpr size_t update_job_wait_time = 10000;
    constexpr size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(num_threads);
    ASSERT_EQ(mock_thread_pool.thread_pool_size, num_threads);

    // Create TieredSVS index instance with a mock queue.
    auto *tiered_index = this->CreateTieredSVSIndex(
        svs_params, mock_thread_pool, training_threshold, update_threshold, update_job_wait_time);
    ASSERT_INDEX(tiered_index);

    // Verify initial state: numThreads reflects the shared pool size,
    // lastReservedThreads starts at 1 (parallelism_ is initialized to 1).
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_EQ(backendIndexInfo.svsInfo.lastReservedThreads, 1);
    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    // Simulate thread contention by keeping one thread busy, to force reduced thread availability
    std::atomic<bool> is_reserved = false;
    std::atomic<bool> thread_wait = true;
    auto reserve_and_wait = [&](VecSimIndex * /*unused*/, size_t num_threads) {
        is_reserved = true;
        while (thread_wait) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    };

    // Keep one thread occupied
    mock_thread_pool.init_threads();
    SVSMultiThreadJob::JobsRegistry registry(allocator);
    std::chrono::milliseconds timeout{update_job_wait_time}; // long enough to reserve one thread
    auto jobs = SVSMultiThreadJob::createJobs(allocator, SVS_BATCH_UPDATE_JOB, reserve_and_wait,
                                              tiered_index, 1, timeout, &registry);
    ASSERT_EQ(jobs.size(), 1);
    tiered_index->submitJobs(jobs);
    // Wait for thread reservation to complete
    while (!is_reserved) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Trigger training with reduced thread availability
    for (size_t i = 0; i < training_threshold; ++i) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i);
    }
    while (tiered_index->GetBackendIndex()->indexSize() != training_threshold) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Verify: numThreads (pool size) unchanged, lastReservedThreads (parallelism) reflects
    // reduced availability — one thread was occupied, so at most num_threads - 1 could reserve.
    // Use LE because OS timing may cause fewer threads to check in before the reservation timeout.
    backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_LE(backendIndexInfo.svsInfo.lastReservedThreads, num_threads - 1);

    // Release occupied thread and trigger another background indexing
    thread_wait = false;
    mock_thread_pool.thread_pool_wait();
    // add more vectors to trigger background indexing
    for (size_t i = 0; i < update_threshold; ++i) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, training_threshold + i);
    }
    mock_thread_pool.thread_pool_join();

    // Verify: numThreads (pool size) unchanged, lastReservedThreads (parallelism) reflects
    // that all configured threads were available for this operation.
    // Use LE because OS timing may cause fewer threads to check in before the reservation timeout.
    backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_LE(backendIndexInfo.svsInfo.lastReservedThreads, num_threads);
}

TYPED_TEST(SVSTieredIndexTest, TestDebugInfoThreadCountWriteInPlace) {
    // Test that write-in-place mode correctly reports thread usage in debug info.
    // Even when the shared pool has multiple threads, write-in-place operations
    // should use only 1 thread and lastReservedThreads (parallelism) should reflect this.

    // Set thread_pool_size to 4 or actual number of available CPUs
    const auto num_threads = std::min(4U, getAvailableCPUs());
    if (num_threads < 2) {
        // If the number of threads is less than 2, we can't run the test
        GTEST_SKIP() << "No threads available";
    }

    // Test svs when mode is write in place, but the shared pool has multiple threads.
    constexpr size_t training_threshold = 10;
    constexpr size_t update_threshold = 10;
    constexpr size_t update_job_wait_time = 10000;
    constexpr size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(num_threads);
    ASSERT_EQ(mock_thread_pool.thread_pool_size, num_threads);

    // Create TieredSVS index instance with a mock queue.
    auto *tiered_index = this->CreateTieredSVSIndex(
        svs_params, mock_thread_pool, training_threshold, update_threshold, update_job_wait_time);
    ASSERT_INDEX(tiered_index);

    // Set mode to write in place even though the shared pool has multiple threads.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    // Verify initial state: numThreads reflects the shared pool size,
    // lastReservedThreads starts at 1 (parallelism_ is initialized to 1).
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_EQ(backendIndexInfo.svsInfo.lastReservedThreads, 1);
    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    mock_thread_pool.init_threads();

    // Trigger training
    for (size_t i = 0; i < training_threshold; ++i) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i);
    }
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    while (tiered_index->GetBackendIndex()->indexSize() != training_threshold) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Verify: numThreads (pool size) unchanged, lastReservedThreads (parallelism) reflects
    // we only used one thread (write-in-place calls initSVSIndexWrapper with availableThreads=1).
    backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_EQ(backendIndexInfo.svsInfo.lastReservedThreads, 1);

    // add more vectors, each will be directly inserted to the backend index
    size_t num_vectors = 10;
    for (size_t i = 0; i < num_vectors; ++i) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, training_threshold + i);
        // Verify: numThreads (pool size) unchanged, lastReservedThreads (parallelism) = 1
        backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
        ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
        ASSERT_EQ(backendIndexInfo.svsInfo.lastReservedThreads, 1);
        ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    }
}

TYPED_TEST(SVSTieredIndexTest, TestAddOneVectorAsync) {
    auto mock_thread_pool = tieredIndexMock();
    const auto num_threads = mock_thread_pool.thread_pool_size;
    if (num_threads < 2) {
        // If the number of threads is less than 2, this test has no point.
        GTEST_SKIP() << "No threads available";
    }
    constexpr size_t training_threshold = 1;
    constexpr size_t update_threshold = 1;
    constexpr size_t update_job_wait_time = 10000;
    constexpr size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);

    // Create TieredSVS index instance with a mock queue.
    auto *tiered_index = this->CreateTieredSVSIndex(
        svs_params, mock_thread_pool, training_threshold, update_threshold, update_job_wait_time);
    ASSERT_INDEX(tiered_index);
    mock_thread_pool.init_threads();

    // Verify initial state: both fields should equal configured thread count
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);

    // Add one vector - this should trigger background training
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0);
    mock_thread_pool.thread_pool_wait();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    // Verify: numThreads unchanged, and we used one thread
    backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_EQ(backendIndexInfo.svsInfo.lastReservedThreads, 1);

    // add another vectors to trigger background indexing
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1);
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 2);

    // Verify: yet again, numThreads unchanged, and we used one thread
    backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();
    ASSERT_EQ(backendIndexInfo.svsInfo.numThreads, num_threads);
    ASSERT_EQ(backendIndexInfo.svsInfo.lastReservedThreads, 1);
}

TYPED_TEST(SVSTieredIndexTest, CreateIndexInstance) {
    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = 4,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    // Add a vector to the flat index.
    TEST_DATA_T vector[params.dim];
    GenerateVector<TEST_DATA_T>(vector, params.dim);
    labelType vector_label = 1;
    VecSimIndex_AddVector(tiered_index, vector, vector_label);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);

    // Submit the index update job.
    tiered_index->scheduleSVSIndexInit();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), mock_thread_pool.thread_pool_size);

    // Execute the job from the queue and validate that the index was updated properly.
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(1, vector), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
}

// A label still sitting in the flat buffer must be readable from there regardless of what the
// SVS backend can or cannot report: the buffer is a plain, uncompressed BruteForceIndex, so
// nothing about the backend's own read limitations (MOD-17706) applies to it. This used to fail
// -- the backend's inability to report gated the flat-buffer read too, so a vector written
// recently enough to still be buffered (exactly when a document is most likely to be written
// again) was invisible to `getDataByLabel`.
TYPED_TEST(SVSTieredIndexTestBasic, getDataByLabelReadsFlatBufferForSvsBackend) {
    const size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Thresholds far above the single vector added below, so nothing is ingested and the label
    // stays in the flat buffer.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 100, 100);
    ASSERT_INDEX(tiered_index);

    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 0);
    VecSimIndex_AddVector(tiered_index, vector, 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1)
        << "premise: the label is in the flat buffer";

    std::vector<std::vector<TEST_DATA_T>> stored;
    tiered_index->getDataByLabel(0, stored);
    ASSERT_EQ(stored.size(), 1);
    EXPECT_EQ(stored[0], std::vector<TEST_DATA_T>(vector, vector + dim));
}

// Once the vector has been ingested into an uncompressed SVS backend, `getDataByLabel` now
// reads it from there too (MOD-17706) -- the backend read is no longer skipped for SVS.
TYPED_TEST(SVSTieredIndexTestBasic, getDataByLabelReadsSvsBackendWhenUncompressed) {
    const size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, 0);
    VecSimIndex_AddVector(tiered_index, vector, 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Ingest it into the SVS backend.
    tiered_index->scheduleSVSIndexInit();
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    std::vector<std::vector<TEST_DATA_T>> stored;
    tiered_index->getDataByLabel(0, stored);
    ASSERT_EQ(stored.size(), 1);
    EXPECT_EQ(stored[0], std::vector<TEST_DATA_T>(vector, vector + dim));
}

// A compressed SVS backend appends nothing to `getDataByLabel` regardless of whether it holds
// part of the label, so a multi-value label split across a non-empty flat buffer and such a
// backend must not be reported as the buffer's subset alone -- that subset would look like the
// complete answer when the backend may hold the rest and simply can't say so. The whole label
// must report nothing instead, the same rule `SVSIndex::getDataByLabel` already applies within
// a single (compressed) tier.
TYPED_TEST(SVSTieredIndexTestCompressedMulti, getDataByLabelReportsNothingForCompressedSplitLabel) {
    const size_t dim = 4;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Force the tiered index to submit the update job on every insert.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);

    // The label's first vector gets ingested into the compressed SVS backend...
    TEST_DATA_T vector1[dim];
    GenerateVector<TEST_DATA_T>(vector1, dim, 0);
    VecSimIndex_AddVector(tiered_index, vector1, 0);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1)
        << "premise: the first vector reached the compressed SVS backend";

    // ...and, because the update job only fires on the next insert, the label's second vector is
    // still sitting in the flat buffer: the label is now split across both tiers.
    TEST_DATA_T vector2[dim];
    GenerateVector<TEST_DATA_T>(vector2, dim, 1);
    VecSimIndex_AddVector(tiered_index, vector2, 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1)
        << "premise: the second vector is still in the flat buffer";

    std::vector<std::vector<TEST_DATA_T>> stored;
    tiered_index->getDataByLabel(0, stored);
    EXPECT_TRUE(stored.empty())
        << "the buffer's one vector must not be reported as if it were the whole label";
}

TYPED_TEST(SVSTieredIndexTestBasic, ShrinkDuringScheduledUpdateIsDeferred) {
    // Regression for the original crash: shrink after the update job reserved threads,
    // but before the SVS update uses them. The update must still complete safely.
    const auto num_threads = std::min(4U, getAvailableCPUs());
    if (num_threads < 2) {
        GTEST_SKIP() << "No threads available";
    }

    const size_t dim = 4;
    const size_t n = num_threads;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(num_threads);

    // Keep automatic training/update disabled so the test controls the scheduling point.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n + 10, n + 20);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        tiered_index->addVector(vector, i);
    }

    std::atomic_bool shrink_callback_ran{false};
    tiered_index->registerTracingCallback("UpdateJob::before_add_to_svs", [&]() {
        // This is the real production entrypoint: the job has already reserved its threads,
        // but updateSVSIndex() has not yet called setParallelism() / createImpl().
        VecSimSVSThreadPool::resize(1);
        shrink_callback_ran = true;
    });

    tiered_index->scheduleSVSIndexInit();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), num_threads);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    ASSERT_TRUE(shrink_callback_ran);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getPoolSize(), 1);
}

TYPED_TEST(SVSTieredIndexTestBasic, ScheduledJobsRegistryCleanupAppliesDeferredResize) {
    // Covers the simplified cleanup path: scheduled jobs are created but never run.
    // Destroying the registry must delete the jobs and release the deferred shrink.
    const auto num_threads = std::min(4U, getAvailableCPUs());
    if (num_threads < 2) {
        GTEST_SKIP() << "No threads available";
    }

    constexpr size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(num_threads);
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, num_threads + 10,
                                                    num_threads + 20);
    ASSERT_INDEX(tiered_index);

    auto allocator = tiered_index->getAllocator();
    std::atomic_bool callback_ran{false};

    {
        SVSMultiThreadJob::JobsRegistry registry(allocator);
        auto jobs = SVSMultiThreadJob::createScheduledJobs(
            allocator, SVS_BATCH_UPDATE_JOB,
            [&callback_ran](VecSimIndex * /*unused*/, size_t /*unused*/) { callback_ran = true; },
            tiered_index, std::chrono::microseconds(SVS_DEFAULT_UPDATE_JOB_WAIT_TIME), &registry);

        ASSERT_EQ(jobs.size(), num_threads);

        VecSimSVSThreadPool::resize(1);
        ASSERT_EQ(VecSimSVSThreadPool::poolSize(), num_threads);
    }

    ASSERT_FALSE(callback_ran);
    ASSERT_EQ(VecSimSVSThreadPool::poolSize(), 1);
}

TYPED_TEST(SVSTieredIndexTestBasic, ShrinkDuringScheduledGCIsDeferred) {
    // Mirror of the update regression for GC: shrink after reservation but before runGC().
    // The GC job must still finish, and the deferred shrink should apply afterward.
    const auto num_threads = std::min(4U, getAvailableCPUs());
    if (num_threads < 2) {
        GTEST_SKIP() << "No threads available";
    }

    constexpr size_t dim = 4;
    const size_t n = num_threads * 2;
    const size_t num_deleted = num_threads;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(num_threads);
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n + 10, n + 20);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i + 1);
        VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, i);
    }
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);

    for (size_t i = 0; i < num_deleted; i++) {
        tiered_index->deleteVector(i);
    }
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), num_deleted);

    std::atomic_bool shrink_callback_ran{false};
    tiered_index->registerTracingCallback("GCJob::before_run_gc", [&]() {
        VecSimSVSThreadPool::resize(1);
        shrink_callback_ran = true;
    });

    VecSimTieredIndex_GC(tiered_index);
    // Each soft-deleted slot has corresponding consolidate() job
    size_t num_consolidate = tiered_index->GetSVSIndex()->getNumMarkedDeleted();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), num_threads + num_consolidate);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    ASSERT_TRUE(shrink_callback_ran);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n - num_deleted);
    ASSERT_EQ(tiered_index->GetSVSIndex()->indexStorageSize(), n - num_deleted);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getPoolSize(), 1);
}

TYPED_TEST(SVSTieredIndexTest, addVector) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto tiered_params = this->CreateTieredSVSParams(svs_params, mock_thread_pool, 1, 1);
    auto *tiered_index = this->CreateTieredSVSIndex(tiered_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    BFParams bf_params = {.type = TypeParam::get_index_type(),
                          .dim = dim,
                          .metric = VecSimMetric_L2,
                          .multi = TypeParam::isMulti()};

    size_t expected_mem = TieredFactory::EstimateInitialSize(&tiered_params);
    ASSERT_LE(expected_mem, tiered_index->getAllocationSize());
    ASSERT_GE(expected_mem * 1.02, tiered_index->getAllocationSize());
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Create a vector and add it to the tiered index.
    labelType vec_label = 1;
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    // Validate that the vector was inserted to the flat buffer properly.
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexCapacity(), DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(tiered_index->indexCapacity(), DEFAULT_BLOCK_SIZE);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(), tiered_index->indexCapacity());
    ASSERT_EQ(tiered_index->GetFlatIndex()->getDistanceFrom_Unsafe(vec_label, vector), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), mock_thread_pool.thread_pool_size);

    // Account for the allocation of a new block due to the vector insertion.
    expected_mem += (BruteForceFactory::EstimateElementSize(&bf_params)) * DEFAULT_BLOCK_SIZE;
    // Account for the memory that was allocated in the labelToId map (approx.)
    expected_mem += sizeof(vecsim_stl::unordered_map<labelType, idType>::value_type) +
                    sizeof(void *) + sizeof(size_t);
    // Account for the insert job that was created.
    expected_mem +=
        SVSMultiThreadJob::estimateSize(mock_thread_pool.thread_pool_size) + sizeof(size_t);
    auto actual_mem = tiered_index->getAllocationSize();
    ASSERT_GE(expected_mem * 1.02, tiered_index->getAllocationSize());
    ASSERT_LE(expected_mem, tiered_index->getAllocationSize());

    if constexpr (TypeParam::isMulti()) {
        // Add another vector under the same label.
        VecSimIndex_AddVector(tiered_index, vector, vec_label);
        ASSERT_EQ(tiered_index->indexSize(), 2);
        ASSERT_EQ(tiered_index->indexLabelCount(), 1);
        ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);
        ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 2);
        // The batch-init jobs.
        ASSERT_EQ(mock_thread_pool.jobQ.size(), mock_thread_pool.thread_pool_size);
    }
}

TYPED_TEST(SVSTieredIndexTest, background_indexing_check) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 2;
    constexpr size_t training_th = DEFAULT_BLOCK_SIZE;
    constexpr size_t update_th = DEFAULT_BLOCK_SIZE;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);

    auto mock_thread_pool = tieredIndexMock();

    auto tiered_params =
        this->CreateTieredSVSParams(svs_params, mock_thread_pool, training_th, update_th);
    auto *tiered_index = this->CreateTieredSVSIndex(tiered_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    mock_thread_pool.init_threads();

    for (size_t i = 0; i < training_th; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        VecSimIndex_AddVector(tiered_index, vector, i);
    }

    while (tiered_index->debugInfo().tieredInfo.backgroundIndexing != VecSimBool_FALSE) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), training_th);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), training_th);

    constexpr size_t second_batch = 2500;

    for (size_t i = 0; i < second_batch; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        VecSimIndex_AddVector(tiered_index, vector, training_th + i);
    }

    while (tiered_index->debugInfo().tieredInfo.backgroundIndexing != VecSimBool_FALSE) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // The backgroundIndexing flag tracks the batch update job only; individual async
    // SVS_INSERT_VECTOR_JOBs may still be in flight, each transiently holding its vector in
    // both the flat and backend indices. Drain the queue so the total size is stable before
    // asserting the exact count.
    mock_thread_pool.thread_pool_join();

    ASSERT_GT(tiered_index->GetBackendIndex()->indexSize(), training_th + second_batch / update_th);
    ASSERT_LT(tiered_index->GetFlatIndex()->indexSize(), update_th);
    ASSERT_EQ(tiered_index->indexSize(), second_batch + training_th);
}

TYPED_TEST(SVSTieredIndexTest, insertJob) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Force thired index to submit the update job on every insert.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Create a vector and add it to the tiered index.
    labelType vec_label = 1;
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Execute the insert job manually (in a synchronous manner).
    ASSERT_EQ(mock_thread_pool.jobQ.size(), mock_thread_pool.thread_pool_size);
    auto *insertion_job = mock_thread_pool.jobQ.front().job;
    ASSERT_EQ(insertion_job->jobType, SVS_BATCH_UPDATE_JOB);

    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    // SVS index should have allocated a single record, while flat index should remove the
    // block.
    // Compression datasets do not provide a capacity method
    const size_t expected_capacity = TypeParam::get_quant_bits() == VecSimSvsQuant_NONE
                                         ? DEFAULT_BLOCK_SIZE
                                         : tiered_index->GetBackendIndex()->indexCapacity();
    ASSERT_EQ(tiered_index->indexCapacity(), expected_capacity);
    ASSERT_EQ(tiered_index->indexMetaDataCapacity(), tiered_index->indexCapacity());
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexCapacity(), 0);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(vec_label, vector), 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, insertJobAsync) {
    size_t dim = 4;
    size_t n = 1000;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly set the training threshold to a value that will trigger the update job
    // after inserting a half of vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n / 2);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();
    // Insert vectors
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i / (TEST_DATA_T)n);
    }

    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    // Verify that vectors were moved to SVS as expected
    auto sz_f = tiered_index->GetFlatIndex()->indexSize();
    auto sz_b = tiered_index->GetBackendIndex()->indexSize();
    EXPECT_LE(sz_f, this->getUpdateThreshold());
    EXPECT_EQ(sz_f + sz_b, n);

    // Quantization has limited accuracy, so we need to check the relative error.
    // If quantization is enabled, we allow a larger relative error.
    double abs_err = TypeParam::get_quant_bits() != VecSimSvsQuant_NONE ? 1e-2 : 1e-6;

    // Verify that the vectors were inserted to Flat/SVS as expected
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T expected_vector[dim];
        GenerateVector<TEST_DATA_T>(expected_vector, dim, i / (TEST_DATA_T)n);
        ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(i, expected_vector), 0, abs_err)
            << "Vector label: " << i;
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, insertJobAsyncMulti) {
    // Create TieredSVS index instance with a mock queue.
    const size_t dim = 4;
    const size_t n = 1000;
    const size_t per_label = 5;

    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly set the training threshold to a value that will trigger the update job
    // after inserting a half of vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n / 2);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    // Create and insert vectors, store them in this continuous array.
    TEST_DATA_T vectors[n * dim];
    for (size_t i = 0; i < n / per_label; i++) {
        for (size_t j = 0; j < per_label; j++) {
            GenerateVector<TEST_DATA_T>(vectors + i * dim * per_label + j * dim, dim,
                                        i * per_label + j);
            tiered_index->addVector(vectors + i * dim * per_label + j * dim, i);
        }
    }

    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
    EXPECT_EQ(tiered_index->indexLabelCount(), n / per_label);
    // Verify that vectors were moved to SVS as expected
    auto sz_f = tiered_index->GetFlatIndex()->indexSize();
    auto sz_b = tiered_index->GetBackendIndex()->indexSize();
    EXPECT_LE(sz_f, this->getUpdateThreshold());
    EXPECT_EQ(sz_f + sz_b, n);

    // Quantization has limited accuaracy, so we need to check the relative error.
    // If quantization is enabled, we allow a larger relative error.
    double abs_err = TypeParam::get_quant_bits() != VecSimSvsQuant_NONE ? 1e-2 : 1e-6;

    // Verify that the vectors were inserted to SVS as expected
    for (size_t i = 0; i < n / per_label; i++) {
        for (size_t j = 0; j < per_label; j++) {
            // The distance from every vector that is stored under the label i should be zero
            auto expected_vector = vectors + i * per_label * dim + j * dim;
            ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(i, expected_vector), 0, abs_err)
                << "Vector label: " << i;
        }
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, KNNSearch) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }

    size_t dim = 4;
    size_t k = 10;

    size_t n = k * 3;

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    size_t cur_memory_usage;

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, k, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    auto svs_index = tiered_index->GetBackendIndex();
    auto flat_index = tiered_index->GetFlatIndex();

    TEST_DATA_T query_0[dim];
    GenerateVector<TEST_DATA_T>(query_0, dim, 0);
    TEST_DATA_T query_1mid[dim];
    GenerateVector<TEST_DATA_T>(query_1mid, dim, n / 3);
    TEST_DATA_T query_2mid[dim];
    GenerateVector<TEST_DATA_T>(query_2mid, dim, n * 2 / 3);
    TEST_DATA_T query_n[dim];
    GenerateVector<TEST_DATA_T>(query_n, dim, n - 1);

    // Search for vectors when the index is empty.
    runTopKSearchTest(tiered_index, query_0, k, nullptr);

    // Define the verification functions.
    auto ver_res_0 = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, index);
        ASSERT_DOUBLE_EQ(score, dim * id * id);
    };

    auto ver_res_1mid = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - query_1mid[0])), (index + 1) / 2);
        ASSERT_DOUBLE_EQ(score, dim * pow((index + 1) / 2, 2));
    };

    auto ver_res_2mid = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(std::abs(int(id - query_2mid[0])), (index + 1) / 2);
        ASSERT_DOUBLE_EQ(score, dim * pow((index + 1) / 2, 2));
    };

    auto ver_res_n = [&](size_t id, double score, size_t index) {
        ASSERT_EQ(id, n - 1 - index);
        ASSERT_DOUBLE_EQ(score, dim * index * index);
    };

    // Insert n/2 vectors to the main index.
    for (size_t i = 0; i < n / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n / 2);
    ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize());

    // Search for k vectors with the flat index empty.
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Insert n/2 vectors to the flat index.
    for (size_t i = n / 2; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize() + flat_index->indexSize());

    cur_memory_usage = allocator->getAllocationSize();
    // Search for k vectors so all the vectors will be from the flat index.
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    // Search for k vectors so all the vectors will be from the main index.
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    // Search for k so some of the results will be from the main and some from the flat index.
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Add some overlapping vectors to the main and flat index.
    // adding directly to the underlying indexes to avoid jobs logic.
    // The main index will have vectors 0 - 2n/3 and the flat index will have vectors n/3 - n
    for (size_t i = n / 3; i < n / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i, i);
    }
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, i, i);
    }

    cur_memory_usage = allocator->getAllocationSize();
    // Search for k vectors so all the vectors will be from the main index.
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    // Search for k vectors so all the vectors will be from the flat index.
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    // Search for k so some of the results will be from the main and some from the flat index.
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // More edge cases:

    // Search for more vectors than the index size.
    k = n + 1;
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);

    // Search for less vectors than the index size, but more than the flat and main index sizes.
    k = n * 5 / 6;
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);

    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for more vectors than the main index size, but less than the flat index size.
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        VecSimIndex_DeleteVector(svs_index, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n * 2 / 3);
    ASSERT_EQ(svs_index->indexLabelCount(), n / 2);
    k = n * 2 / 3;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_n, k, ver_res_n);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    runTopKSearchTest(tiered_index, query_2mid, k, ver_res_2mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for more vectors than the flat index size, but less than the main index size.
    for (size_t i = n / 2; i < n; i++) {
        VecSimIndex_DeleteVector(flat_index, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n / 6);
    ASSERT_EQ(svs_index->indexLabelCount(), n / 2);
    k = n / 4;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Search for vectors when the flat index is not empty but the main index is empty.
    for (size_t i = 0; i < n * 2 / 3; i++) {
        VecSimIndex_DeleteVector(svs_index, i);
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i, i);
    }
    ASSERT_EQ(flat_index->indexSize(), n * 2 / 3);
    ASSERT_EQ(svs_index->indexLabelCount(), 0);
    k = n / 3;
    cur_memory_usage = allocator->getAllocationSize();
    runTopKSearchTest(tiered_index, query_0, k, ver_res_0);
    runTopKSearchTest(tiered_index, query_1mid, k, ver_res_1mid);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // // // // // // // // // // // //
    // Check behavior upon timeout.  //
    // // // // // // // // // // // //

    VecSimQueryReply *res;
    // Add a vector to the SVS index so there will be a reason to query it.
    GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, n, n);

    // Set timeout callback to always return 1 (will fail while querying the flat buffer).
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    res = VecSimIndex_TopKQuery(tiered_index, query_0, k, nullptr, BY_SCORE);
    ASSERT_TRUE(res->results.empty());
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);

    // Set timeout callback to return 1 after n checks (will fail while querying the SVS index).
    // Brute-force index checks for timeout after each vector.
    size_t checks_in_flat = flat_index->indexSize();
    VecSimQueryParams qparams = {.timeoutCtx = &checks_in_flat};
    VecSim_SetTimeoutCallbackFunction([](void *ctx) {
        auto count = static_cast<size_t *>(ctx);
        if (*count == 0) {
            return 1;
        }
        (*count)--;
        return 0;
    });
    res = VecSimIndex_TopKQuery(tiered_index, query_0, k, &qparams, BY_SCORE);
    ASSERT_TRUE(res->results.empty());
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);
    // Make sure we didn't get the timeout in the flat index.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_TopKQuery(flat_index, query_0, k, &qparams, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_OK);
    VecSimQueryReply_Free(res);

    // Clean up.
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; });
}

TYPED_TEST(SVSTieredIndexTestBasic, KNNSearchCosine) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    const size_t dim = 128;
    const size_t n = 100;

    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_Cosine};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    for (size_t i = 1; i <= n; i++) {
        TEST_DATA_T f[dim];
        f[0] = (TEST_DATA_T)i / n;
        for (size_t j = 1; j < dim; j++) {
            f[j] = 1.0;
        }
        VecSimIndex_AddVector(tiered_index, f, i);
    }

    mock_thread_pool.thread_pool_join();

    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n);
    // Verify that vectors were moved to SVS as expected
    auto sz_f = tiered_index->GetFlatIndex()->indexSize();
    auto sz_b = tiered_index->GetBackendIndex()->indexSize();
    EXPECT_LE(sz_f, this->getUpdateThreshold());
    EXPECT_EQ(sz_f + sz_b, n);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 1.0);

    // topK search will normalize the query so we keep the original data to
    // avoid normalizing twice.
    TEST_DATA_T normalized_query[dim];
    memcpy(normalized_query, query, dim * sizeof(TEST_DATA_T));
    VecSim_Normalize(normalized_query, dim, params.type);

    auto verify_res = [&](size_t id, double score, size_t result_rank) {
        ASSERT_EQ(id, (n - result_rank));
        TEST_DATA_T expected_score = tiered_index->getDistanceFrom_Unsafe(id, normalized_query);
        // Verify that abs difference between the actual and expected score is at most 1/10^5.
        ASSERT_NEAR((TEST_DATA_T)score, expected_score, 1e-5f);
    };
    runTopKSearchTest(tiered_index, query, 10, verify_res);
}

TYPED_TEST(SVSTieredIndexTest, deleteVector) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    labelType vec_label = 0;
    // Delete from an empty index.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 0);

    // Create a vector and add it to the tiered index (expect it to go into the flat buffer).
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Remove vector from flat buffer.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 1);
    ASSERT_EQ(tiered_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);

    mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);

    // Create a vector and add it to SVS in the tiered index.
    VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    // Remove from main index.
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();

    ASSERT_EQ(tiered_index->indexSize(), 0);

    // Re-insert a deleted label with a different vector.
    TEST_DATA_T new_vec_val = 2.0;
    GenerateVector<TEST_DATA_T>(vector, dim, new_vec_val);
    VecSimIndex_AddVector(tiered_index, vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Move the vector to SVS by executing the pending jobs.
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    // Scalar quantization accuracy is insufficient for this check.
    if (!this->isFallbackToSQ()) {
        // Check that the distance from the deleted vector (of zeros) to the label is the distance
        // to the new vector (L2 distance).
        TEST_DATA_T deleted_vector[dim];
        GenerateVector<TEST_DATA_T>(deleted_vector, dim, 0);
        ASSERT_EQ(
            tiered_index->GetBackendIndex()->getDistanceFrom_Unsafe(vec_label, deleted_vector),
            dim * pow(new_vec_val, 2));
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, markedDeleted) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    constexpr size_t n = 10;
    constexpr size_t transfer_trigger = n;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, transfer_trigger,
                                                    transfer_trigger);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i);
    }
    // Vectors are still in the flat buffer.
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);
    // Override a vector while in the flat buffer

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);

    // Move vectors to the backend
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);

    // Override a vector while in the backend
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1);
    ASSERT_EQ(tiered_index->indexSize(), n + 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n - 1);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 1);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 1);

    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Delete the overriden vector
    VecSimIndex_DeleteVector(tiered_index, 1);
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 1);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 1);
    ASSERT_EQ(tiered_index->indexLabelCount(), n - 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n - 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);

    // Delete another arbitrary vector
    VecSimIndex_DeleteVector(tiered_index, 0);
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n - 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), n - 2);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 2);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 2);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);

    // Empty Index
    for (size_t i = 2; i < n; i++) {
        VecSimIndex_DeleteVector(tiered_index, i);
    }

    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();

    ASSERT_EQ(tiered_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, deletionAfterCompactWaitsForCounterReset) {
    constexpr size_t dim = 4;
    constexpr size_t n = 2;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n, n);
    ASSERT_INDEX(tiered_index);

    for (labelType label = 0; label < n; ++label) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, label);
    }
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);

    ASSERT_EQ(tiered_index->deleteVector(0), 1);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 1);

    std::future<int> concurrent_delete;
    bool deletion_started_after_compact = false;
    bool deletion_was_blocked = false;
    tiered_index->GetSVSIndex()->setAfterCompactCallbackForTest([&] {
        // GC still holds mainIndexGuard exclusively here. A tiered deletion may reach this window
        // without updateJobMutex, but its shared mainIndexGuard must wait until after the reset.
        concurrent_delete =
            std::async(std::launch::async, [&] { return tiered_index->deleteVector(1); });
        deletion_started_after_compact = true;
        deletion_was_blocked = concurrent_delete.wait_for(std::chrono::milliseconds(100)) ==
                               std::future_status::timeout;
    });

    VecSimTieredIndex_GC(tiered_index);
    while (!deletion_started_after_compact && !mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }

    ASSERT_TRUE(deletion_started_after_compact);
    ASSERT_TRUE(deletion_was_blocked);
    ASSERT_EQ(concurrent_delete.get(), 1);
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 1);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(1));

    tiered_index->GetSVSIndex()->setAfterCompactCallbackForTest({});
    VecSimTieredIndex_GC(tiered_index);
    while (!mock_thread_pool.jobQ.empty()) {
        mock_thread_pool.thread_iteration();
    }
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, deleteVectorMulti) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    auto allocator = tiered_index->getAllocator();

    // Test some more scenarios that are relevant only for multi value index.
    labelType vec_label = 0;
    labelType other_vec_val = 2.0;
    idType invalidJobsCounter = 0;
    // Create a vector and add it to SVS in the tiered index.
    TEST_DATA_T vector[dim];
    GenerateVector<TEST_DATA_T>(vector, dim, vec_label);
    VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, vec_label);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);

    // Test deleting a label for which one of its vector's is in the flat index while the
    // second one is in SVS.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);

    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);

    // Test deleting a label for which both of its vector's is in the flat index.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, vec_label);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 2);
    ASSERT_EQ(tiered_index->indexSize(), 2);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Test deleting a label for which both of its vector's is in SVS index.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, vec_label);
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, other_vec_val);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 2);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->deleteVector(vec_label), 2);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 0);
}

TYPED_TEST(SVSTieredIndexTest, manageIndexOwnership) {

    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);

    // Get the allocator from the tiered index.
    auto allocator = tiered_index->getAllocator();

    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);
    size_t initial_mem = allocator->getAllocationSize();

    // Create a dummy job callback that insert one vector to the underline SVS index.
    auto dummy_job = [](AsyncJob *job) {
        auto *my_index = reinterpret_cast<TieredSVSIndex<TEST_DATA_T> *>(job->index);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        size_t dim = 4;
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim);
        my_index->GetBackendIndex()->addVector(vector, my_index->GetBackendIndex()->indexSize());
    };

    std::atomic_int successful_executions(0);
    auto job1 = new (allocator) AsyncJob(allocator, SVS_BATCH_UPDATE_JOB, dummy_job, tiered_index);
    auto job2 = new (allocator) AsyncJob(allocator, SVS_BATCH_UPDATE_JOB, dummy_job, tiered_index);

    // Wrap this job with an array and submit the jobs to the queue.
    tiered_index->submitSingleJob(job1);
    tiered_index->submitSingleJob(job2);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);

    // Execute the job from the queue asynchronously, delete the index in the meantime.
    auto run_fn = [&successful_executions, &mock_thread_pool]() {
        // Create a temporary strong reference of the index from the weak reference that the
        // job holds, to ensure that the index is not deleted while the job is running.
        if (auto temp_ref = mock_thread_pool.jobQ.front().index_weak_ref.lock()) {
            // At this point we wish to validate that we have both the index strong ref (stored
            // in index_ctx) and the weak ref owned by the job (that we currently promoted).
            EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 2);

            mock_thread_pool.jobQ.front().job->Execute(mock_thread_pool.jobQ.front().job);
            successful_executions++;
        }
        mock_thread_pool.jobQ.kick();
    };
    std::thread t1(run_fn);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // Delete the index while the job is still running, to ensure that the weak ref protects
    // the index.
    mock_thread_pool.reset_ctx();
    EXPECT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 1);
    t1.join();
    // Expect that the first job will succeed.
    ASSERT_EQ(successful_executions, 1);

    // The second job should not run, since the weak reference is not supposed to become a
    // strong references now.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().index_weak_ref.use_count(), 0);
    std::thread t2(run_fn);
    t2.join();
    // Expect that the second job is ot successful.
    ASSERT_EQ(successful_executions, 1);
}

TYPED_TEST(SVSTieredIndexTest, parallelSearch) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t k = 10;
    size_t n = 1000;

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .search_window_size = n,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly set the training threshold to a value that will trigger the update job
    // after inserting a half of vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n / 2);
    ASSERT_INDEX(tiered_index);

    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    std::atomic_int successful_searches(0);
    auto parallel_knn_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        size_t k = search_job->k;
        size_t dim = search_job->dim;
        auto query = search_job->query;

        auto verify_res = [&](size_t id, double score, size_t res_index) {
            TEST_DATA_T element = *(TEST_DATA_T *)query;
            ASSERT_EQ(std::abs(id - element), (res_index + 1) / 2);
            ASSERT_EQ(score, dim * (id - element) * (id - element));
        };
        runTopKSearchTest(job->index, query, k, verify_res);
        (*search_job->successful_searches)++;
        delete job;
    };

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Fill the job queue with insert and search jobs, while filling the flat index, before
    // initializing the thread pool.
    for (size_t i = 0; i < n; i++) {
        // Insert a vector to the flat index and add a job to insert it to the main index.
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);

        // Add a search job. Make sure the query element is between k and n - k.
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, (i % (n_labels - (2 * k))) + k);
        auto search_job = new (allocator) tieredIndexMock::SearchJobMock(
            allocator, parallel_knn_search, tiered_index, k, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    EXPECT_EQ(tiered_index->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->GetFlatIndex()->indexSize(), n);
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    // All the vectors are already in the tiered index, so we expect to find the expected
    // results from the get-go.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(SVSTieredIndexTest, parallelInsertSearch) {
    size_t dim = 4;
    size_t k = 10;
    size_t n = 1000;

    size_t block_size = n / 100;

    // Create TieredSVS index instance with a mock queue.
    size_t n_labels = TypeParam::isMulti() ? n / 25 : n;
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .blockSize = block_size,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly set the training threshold to a value that will trigger the update job
    // after inserting a half of vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n / 2);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    // Save the number fo tasks done by thread i in the i-th entry.
    std::vector<size_t> completed_tasks(mock_thread_pool.thread_pool_size, 0);
    mock_thread_pool.init_threads();
    std::atomic_int successful_searches(0);

    auto parallel_knn_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        size_t k = search_job->k;
        auto query = search_job->query;
        // In this test we don't care about the results, just that the search doesn't crash
        // and returns the correct number of valid results.
        auto verify_res = [&](size_t id, double score, size_t res_index) {};
        runTopKSearchTest(job->index, query, k, verify_res);
        (*search_job->successful_searches)++;
        delete job;
    };

    // Insert vectors in parallel to search.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, (TEST_DATA_T)n / 4 + (i % 1000) * M_PI);
        auto search_job = new (allocator) tieredIndexMock::SearchJobMock(
            allocator, parallel_knn_search, tiered_index, k, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();

    // Verify that vectors were moved to SVS as expected
    auto sz_f = tiered_index->GetFlatIndex()->indexSize();
    auto sz_b = tiered_index->GetBackendIndex()->indexSize();
    EXPECT_LE(sz_f, this->getUpdateThreshold());
    EXPECT_EQ(sz_f + sz_b, n);
    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, MergeMulti) {
    size_t dim = 4;

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = true,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    auto allocator = tiered_index->getAllocator();

    auto svs_index = tiered_index->GetBackendIndex();
    auto flat_index = tiered_index->GetFlatIndex();

    // Insert vectors with label 0 to SVS only.
    GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, 0, 1);
    GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, 0, 2);
    // Insert vectors with label 1 to flat buffer only.
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 1, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 1, 2);
    // Insert DIFFERENT vectors with label 2 to both SVS and flat buffer.
    GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, 2, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, 2, 1);

    TEST_DATA_T query[dim];
    GenerateVector<TEST_DATA_T>(query, dim, 0);

    // Search in the tiered index for more vectors than it has. Merging the results from the two
    // indexes should result in a list of unique vectors, even if the scores of the duplicates are
    // different.
    runTopKSearchTest(tiered_index, query, 5, [](size_t _, double __, size_t ___) {});
}

TYPED_TEST(SVSTieredIndexTest, testSizeEstimation) {
    size_t dim = 128;
#if HAVE_SVS_LVQ
    // SVS block sizes always rounded to a power of 2
    // This why, in case of quantization, actual block size can be differ than requested
    // In addition, block size to be passed to graph and dataset counted in bytes,
    // converted then to a number of elements.
    // IMHO, would be better to always interpret block size to a number of elements
    // rather than conversion to-from number of bytes
    auto constexpr quantBits = TypeParam::get_quant_bits();
    if constexpr (quantBits != VecSimSvsQuant_NONE) { // constexpr eliminates div-by-zero warning;
                                                      // inner condition is runtime-only
        if (!this->isFallbackToSQ()) {
            // Extra data in LVQ vector
            const auto lvq_vector_extra = sizeof(svs::quantization::lvq::ScalarBundle);
            dim -= (lvq_vector_extra * 8) / quantBits;
        }
    }
#endif
    size_t n = DEFAULT_BLOCK_SIZE;
    size_t graph_degree = 31; // power of 2 - 1
    size_t bs = DEFAULT_BLOCK_SIZE;

    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .blockSize = bs,
        .graph_max_degree = graph_degree,
    };
    VecSimParams vecsim_svs_params = CreateParams(svs_params);

    auto mock_thread_pool = tieredIndexMock();
    // Forcibly trigger first update job after inserting n vectors, then for every one vector.
    auto tiered_params = this->CreateTieredSVSParams(vecsim_svs_params, mock_thread_pool, n, 1);
    VecSimParams params = CreateParams(tiered_params);
    auto *index = VecSimIndex_New(&params);
    mock_thread_pool.ctx->index_strong_ref.reset(index);
    mock_thread_pool.init_threads();

    auto allocator = index->getAllocator();

    size_t initial_size_estimation = VecSimIndex_EstimateInitialSize(&params);

    ASSERT_EQ(initial_size_estimation, index->getAllocationSize());

    // Add vectors up to initial capacity (initial capacity == block size).
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(index, dim, i, i);
    }
    mock_thread_pool.thread_pool_wait();

    const size_t reverse_edges_per_slot = SVSGraphBuilder<uint32_t>::reverse_edges_element_size();
    size_t estimation = (VecSimIndex_EstimateElementSize(&params) - reverse_edges_per_slot) * bs +
                        EstimateReverseEdgesSize(n + 1) - EstimateReverseEdgesSize(n);

    size_t before = index->getAllocationSize();
    GenerateAndAddVector<TEST_DATA_T>(index, dim, bs + n, bs + n);
    mock_thread_pool.thread_pool_join();
    size_t actual = index->getAllocationSize() - before;

    auto tiered_index = this->CastToTieredSVS(index);

    // Flat index should be empty, hence the index size includes only svs size.
    EXPECT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), n + 1);

    // We added n + 1 vectors
    EXPECT_EQ(index->indexSize(), n + 1);

    EXPECT_EQ(index->indexCapacity(), tiered_index->GetBackendIndex()->indexCapacity());

    // We check that the actual size is within 1% of the estimation.
    EXPECT_GE(estimation, actual * 0.99);
    EXPECT_LE(estimation, actual * 1.01);
}

TYPED_TEST(SVSTieredIndexTest, parallelInsertAdHoc) {
    size_t dim = 4;
    size_t n = 1000;

    size_t block_size = n / 100;

    // Create TieredSVS index instance with a mock queue.
    size_t n_labels = n;
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .blockSize = block_size,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();
    std::atomic_int successful_searches(0);

    auto parallel_adhoc_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        auto query = search_job->query;
        size_t element = *(TEST_DATA_T *)query;
        size_t label = element % search_job->n;
        VecSimTieredIndex_AcquireSharedLocks(search_job->index);
        ASSERT_EQ(0, VecSimIndex_GetDistanceFrom_Unsafe(search_job->index, label, query));
        VecSimTieredIndex_ReleaseSharedLocks(search_job->index);
        (*search_job->successful_searches)++;
        delete job;
    };

    // Insert vectors in parallel to search.
    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i % n_labels, i);
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, i);
        auto search_job = new (allocator)
            tieredIndexMock::SearchJobMock(allocator, parallel_adhoc_search, tiered_index, 1, query,
                                           n_labels, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    tiered_index->scheduleSVSIndexInit();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n_labels);
    EXPECT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

// A set of lambdas that determine whether a vector should be inserted to the
// SVS index (returns true) or to the flat index (returns false).
inline constexpr std::array<std::pair<std::string_view, bool (*)(size_t, size_t)>, 11> lambdas = {{
    {"100% SVS,   0% FLAT ", [](size_t idx, size_t n) -> bool { return 1; }},
    {" 50% SVS,  50% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 2; }},
    {"  0% SVS, 100% FLAT ", [](size_t idx, size_t n) -> bool { return 0; }},
    {" 90% SVS,  10% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 10; }},
    {" 10% SVS,  90% FLAT ", [](size_t idx, size_t n) -> bool { return !(idx % 10); }},
    {" 99% SVS,   1% FLAT ", [](size_t idx, size_t n) -> bool { return idx % 100; }},
    {"  1% SVS,  99% FLAT ", [](size_t idx, size_t n) -> bool { return !(idx % 100); }},
    {"first 10% are in SVS", [](size_t idx, size_t n) -> bool { return idx < (n / 10); }},
    {"first 10% are in FLAT", [](size_t idx, size_t n) -> bool { return idx >= (n / 10); }},
    {" last 10% are in FLAT", [](size_t idx, size_t n) -> bool { return idx < (9 * n / 10); }},
    {" last 10% are in SVS", [](size_t idx, size_t n) -> bool { return idx >= (9 * n / 10); }},
}};

TYPED_TEST(SVSTieredIndexTest, BatchIterator) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t d = 4;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
    };
    VecSimParams params = CreateParams(svs_params);

    for (auto &lambda : lambdas) {
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
        ASSERT_INDEX(tiered_index);
        auto allocator = tiered_index->getAllocator();

        auto *svs = tiered_index->GetBackendIndex();
        auto *flat = tiered_index->GetFlatIndex();

        // For every i, add the vector (i,i,i,i) under the label i.
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? svs : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, i % n_labels, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
        // in every iteration. The results order should be sorted by their score (distance from
        // the query vector), which means sorted from the largest id to the lowest.
        size_t n_res = 5;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            std::vector<size_t> expected_ids(n_res);
            for (size_t i = 0; i < n_res; i++) {
                expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id) << decider_name;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num, n_labels / n_res) << decider_name;
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(SVSTieredIndexTest, BatchIteratorReset) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t d = 4;
    size_t M = 8;
    size_t sws = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .construction_window_size = sws,
        .search_window_size = sws,
    };
    VecSimParams params = CreateParams(svs_params);

    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
        ASSERT_INDEX(tiered_index);
        auto allocator = tiered_index->getAllocator();

        auto *svs = tiered_index->GetBackendIndex();
        auto *flat = tiered_index->GetFlatIndex();

        // For every i, add the vector (i,i,i,i) under the label i.
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? svs : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, i % n_labels, i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        ASSERT_NO_FATAL_FAILURE(VecSimBatchIterator_Reset(batchIterator));

        // Get the 100 vectors whose ids are the maximal among those that hasn't been returned yet,
        // in every iteration. Run this flow for 3 times, and reset the iterator.
        size_t n_res = 100;
        size_t re_runs = 3;

        for (size_t take = 0; take < re_runs; take++) {
            size_t iteration_num = 0;
            while (VecSimBatchIterator_HasNext(batchIterator)) {
                std::vector<size_t> expected_ids(n_res);
                for (size_t i = 0; i < n_res; i++) {
                    expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
                }
                auto verify_res = [&](size_t id, double score, size_t index) {
                    ASSERT_EQ(expected_ids[index], id) << decider_name;
                };
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE);
                iteration_num++;
            }
            ASSERT_EQ(iteration_num, n_labels / n_res) << decider_name;
            VecSimBatchIterator_Reset(batchIterator);
        }

        // Try resetting the iterator before it is depleted.
        n_res = 10;
        for (size_t take = 0; take < re_runs; take++) {
            size_t iteration_num = 0;
            do {
                ASSERT_TRUE(VecSimBatchIterator_HasNext(batchIterator)) << decider_name;
                std::vector<size_t> expected_ids(n_res);
                for (size_t i = 0; i < n_res; i++) {
                    expected_ids[i] = (n - iteration_num * n_res - i - 1) % n_labels;
                }
                auto verify_res = [&](size_t id, double score, size_t index) {
                    ASSERT_EQ(expected_ids[index], id) << decider_name;
                };
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE);
            } while (5 > iteration_num++);
            VecSimBatchIterator_Reset(batchIterator);
        }
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(SVSTieredIndexTest, BatchIteratorSize1) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t d = 4;
    size_t M = 8;
    size_t sws = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .construction_window_size = sws,
        .search_window_size = sws,
    };
    VecSimParams params = CreateParams(svs_params);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
        ASSERT_INDEX(tiered_index);
        auto allocator = tiered_index->getAllocator();

        auto *svs = tiered_index->GetBackendIndex();
        auto *flat = tiered_index->GetFlatIndex();

        // For every i, add the vector (i,i,i,i) under the label `n_labels - (i % n_labels)`.
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? svs : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, n_labels - (i % n_labels), i);
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);

        size_t iteration_num = 0;
        size_t n_res = 1, expected_n_res = 1;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            iteration_num++;
            // Expect to get results in the reverse order of labels - which is the order of the
            // distance from the query vector. Get one result in every iteration.
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(id, iteration_num) << decider_name;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE, expected_n_res);
        }

        ASSERT_EQ(iteration_num, n_labels) << decider_name;
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(SVSTieredIndexTest, BatchIteratorAdvanced) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t d = 4;
    size_t M = 8;
    size_t sws = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .construction_window_size = sws,
    };
    VecSimParams params = CreateParams(svs_params);
    SVSRuntimeParams svsRuntimeParams = {.windowSize = sws};
    VecSimQueryParams query_params = CreateQueryParams(svsRuntimeParams);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
        ASSERT_INDEX(tiered_index);
        auto allocator = tiered_index->getAllocator();

        auto *svs = tiered_index->GetBackendIndex();
        auto *flat = tiered_index->GetFlatIndex();

        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator =
            VecSimBatchIterator_New(tiered_index, query, &query_params);

        // Try to get results even though there are no vectors in the index.
        VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 0) << decider_name;
        VecSimQueryReply_Free(res);
        ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator)) << decider_name;

        // Insert one label and query again. The internal id will be 0.
        for (size_t j = 0; j < per_label; j++) {
            GenerateAndAddVector<TEST_DATA_T>(decider(n_labels, n) ? svs : flat, d, n_labels,
                                              n - j);
        }
        VecSimBatchIterator_Reset(batchIterator);
        res = VecSimBatchIterator_Next(batchIterator, 10, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 1) << decider_name;
        VecSimQueryReply_Free(res);
        ASSERT_FALSE(VecSimBatchIterator_HasNext(batchIterator)) << decider_name;
        VecSimBatchIterator_Free(batchIterator);

        // Insert vectors to the index and re-create the batch iterator.
        for (size_t i = 1; i < n_labels; i++) {
            auto cur = decider(i, n) ? svs : flat;
            for (size_t j = 1; j <= per_label; j++) {
                GenerateAndAddVector<TEST_DATA_T>(cur, d, i, (i - 1) * per_label + j);
            }
        }
        ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), n) << decider_name;
        batchIterator = VecSimBatchIterator_New(tiered_index, query, &query_params);

        // Try to get 0 results.
        res = VecSimBatchIterator_Next(batchIterator, 0, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 0) << decider_name;
        VecSimQueryReply_Free(res);

        // n_res does not divide into ef or vice versa - expect leftovers between the graph scans.
        size_t n_res = 7;
        size_t iteration_num = 0;

        while (VecSimBatchIterator_HasNext(batchIterator)) {
            iteration_num++;
            std::vector<size_t> expected_ids;
            // We ask to get the results sorted by ID in a specific batch (in ascending order), but
            // in every iteration the ids should be lower than the previous one, according to the
            // distance from the query.
            for (size_t i = 1; i <= n_res; i++) {
                expected_ids.push_back(n_labels - iteration_num * n_res + i);
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id) << decider_name;
            };
            if (iteration_num <= n_labels / n_res) {
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID);
            } else {
                // In the last iteration there are `n_labels % n_res` results left to return.
                size_t n_left = n_labels % n_res;
                // Remove the first `n_res - n_left` ids from the expected ids.
                while (expected_ids.size() > n_left) {
                    expected_ids.erase(expected_ids.begin());
                }
                runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_ID, n_left);
            }
        }
        ASSERT_EQ(iteration_num, n_labels / n_res + 1) << decider_name;
        // Try to get more results even though there are no.
        res = VecSimBatchIterator_Next(batchIterator, 1, BY_SCORE);
        ASSERT_EQ(VecSimQueryReply_Len(res), 0) << decider_name;
        VecSimQueryReply_Free(res);

        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(SVSTieredIndexTest, BatchIteratorWithOverlaps) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t d = 4;
    size_t M = 8;
    size_t sws = 20;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .construction_window_size = sws,
        .search_window_size = sws,
    };
    VecSimParams params = CreateParams(svs_params);

    // for (auto &[decider_name, decider] : lambdas) { // TODO: not supported by clang < 16
    for (auto &lambda : lambdas) {
        // manually deconstruct the pair to avoid the clang error
        auto &decider_name = lambda.first;
        auto &decider = lambda.second;

        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
        ASSERT_INDEX(tiered_index);
        auto allocator = tiered_index->getAllocator();

        auto *svs = tiered_index->GetBackendIndex();
        auto *flat = tiered_index->GetFlatIndex();

        // For every i, add the vector (i,i,i,i) under the label i.
        size_t flat_count = 0;
        for (size_t i = 0; i < n; i++) {
            auto cur = decider(i, n) ? svs : flat;
            GenerateAndAddVector<TEST_DATA_T>(cur, d, i % n_labels, i);
            if (cur == flat) {
                flat_count++;
                // Add 10% of the vectors in FLAT to SVS as well.
                if (flat_count % 10 == 0) {
                    GenerateAndAddVector<TEST_DATA_T>(svs, d, i % n_labels, i);
                }
            }
        }
        // The index size should be 100-110% of n.
        ASSERT_LE(VecSimIndex_IndexSize(tiered_index), n * 1.1) << decider_name;
        ASSERT_GE(VecSimIndex_IndexSize(tiered_index), n) << decider_name;
        // The number of unique labels should be n_labels.
        // ASSERT_EQ(tiered_index->indexLabelCount(), n_labels) << decider_name;

        // Query for (n,n,n,n) vector (recall that n-1 is the largest id in te index).
        TEST_DATA_T query[d];
        GenerateVector<TEST_DATA_T>(query, d, n);

        VecSimBatchIterator *batchIterator = VecSimBatchIterator_New(tiered_index, query, nullptr);
        size_t iteration_num = 0;

        // Get the 5 vectors whose ids are the maximal among those that hasn't been returned yet
        // in every iteration. The results order should be sorted by their score (distance from
        // the query vector), which means sorted from the largest id to the lowest.
        size_t n_res = 5;
        size_t n_expected = n_res;
        size_t excessive_iterations = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            if (iteration_num * n_res == n_labels) {
                // in some cases, the batch iterator may report that it has more results to return,
                // but it's actually not true and the next call to `VecSimBatchIterator_Next` will
                // return 0 results. This is safe because we don't guarantee how many results the
                // batch iterator will return, and a similar scenario can happen when checking
                // `VecSimBatchIterator_HasNext` on an empty index for the first time (before the
                // first call to `VecSimBatchIterator_Next`). we check that this scenario doesn't
                // happen more than once.
                ASSERT_EQ(excessive_iterations, 0) << decider_name;
                excessive_iterations = 1;
                n_expected = 0;
            }
            std::vector<size_t> expected_ids(n_expected);
            for (size_t i = 0; i < n_expected; i++) {
                expected_ids[i] = (n - iteration_num * n_expected - i - 1) % n_labels;
            }
            auto verify_res = [&](size_t id, double score, size_t index) {
                ASSERT_EQ(expected_ids[index], id) << decider_name;
            };
            runBatchIteratorSearchTest(batchIterator, n_res, verify_res, BY_SCORE, n_expected);
            iteration_num++;
        }
        ASSERT_EQ(iteration_num - excessive_iterations, n_labels / n_res)
            << decider_name << "\nHad excessive iterations: " << (excessive_iterations != 0);
        VecSimBatchIterator_Free(batchIterator);
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, BatchIteratorWithOverlaps_SpacialMultiCases) {
    size_t d = 4;

    std::shared_ptr<VecSimAllocator> allocator;
    VecSimIndex *svs, *flat;
    TEST_DATA_T query[d];
    VecSimBatchIterator *iterator;
    VecSimQueryReply *batch;

    // Create TieredSVS index instance with a mock queue.
    SVSParams svs_params = {
        .type = TypeParam::get_index_type(),
        .dim = d,
        .metric = VecSimMetric_L2,
        .multi = true,
    };
    VecSimParams params = CreateParams(svs_params);
    auto mock_thread_pool = tieredIndexMock();

    auto L2 = [&](size_t element) { return element * element * d; };

    // TEST 1:
    // first batch contains duplicates with different scores.
    auto tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
    allocator = tiered_index->getAllocator();
    svs = tiered_index->GetBackendIndex();
    flat = tiered_index->GetFlatIndex();

    GenerateAndAddVector<TEST_DATA_T>(flat, d, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 2, 2);

    GenerateAndAddVector<TEST_DATA_T>(svs, d, 1, 3);
    GenerateAndAddVector<TEST_DATA_T>(svs, d, 0, 4);
    GenerateAndAddVector<TEST_DATA_T>(svs, d, 3, 5);

    ASSERT_EQ(tiered_index->indexLabelCount(), 4);

    GenerateVector<TEST_DATA_T>(query, d, 0);
    iterator = VecSimBatchIterator_New(tiered_index, query, nullptr);

    // batch size is 3 (the size of each index). Internally the tiered batch iterator will have to
    // handle the duplicates with different scores.
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 3, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 3);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 0);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(0));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 1), 1);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 1), L2(1));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 2), 2);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 2), L2(2));
    VecSimQueryReply_Free(batch);

    // we have 1 more label in the index. we expect the tiered batch iterator to return it only and
    // filter out the duplicates.
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 2, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 1);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 3);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(5));
    ASSERT_FALSE(VecSimBatchIterator_HasNext(iterator));
    VecSimQueryReply_Free(batch);
    // TEST 1 clean up.
    VecSimBatchIterator_Free(iterator);

    // TEST 2:
    // second batch contains duplicates (different scores) from the first batch.
    auto *ctx = new tieredIndexMock::IndexExtCtx(&mock_thread_pool);
    mock_thread_pool.reset_ctx(ctx);
    tiered_index = this->CreateTieredSVSIndex(params, mock_thread_pool);
    allocator = tiered_index->getAllocator();
    svs = tiered_index->GetBackendIndex();
    flat = tiered_index->GetFlatIndex();

    GenerateAndAddVector<TEST_DATA_T>(svs, d, 0, 0);
    GenerateAndAddVector<TEST_DATA_T>(svs, d, 1, 1);
    GenerateAndAddVector<TEST_DATA_T>(svs, d, 2, 2);
    GenerateAndAddVector<TEST_DATA_T>(svs, d, 3, 3);

    GenerateAndAddVector<TEST_DATA_T>(flat, d, 2, 0);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 3, 1);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 0, 2);
    GenerateAndAddVector<TEST_DATA_T>(flat, d, 1, 3);

    ASSERT_EQ(tiered_index->indexLabelCount(), 4);

    iterator = VecSimBatchIterator_New(tiered_index, query, nullptr);

    // ask for 2 results. The internal batch iterators will return 2 results: svs - [0, 1], flat -
    // [2, 3] so there are no duplicates.
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 2, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 2);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 0);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(0));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 1), 2);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 1), L2(0));
    VecSimQueryReply_Free(batch);

    // first batch contained 1 result from each index, so there is one leftover from each iterator.
    // Asking for 3 results will return additional 2 results from each iterator and the tiered batch
    // iterator will have to handle the duplicates that each iterator returned (both labels that
    // were returned in the first batch and duplicates in the current batch).
    ASSERT_TRUE(VecSimBatchIterator_HasNext(iterator));
    batch = VecSimBatchIterator_Next(iterator, 3, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_Len(batch), 2);
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 0), 1);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 0), L2(1));
    ASSERT_EQ(VecSimQueryResult_GetId(batch->results.data() + 1), 3);
    ASSERT_EQ(VecSimQueryResult_GetScore(batch->results.data() + 1), L2(1));
    ASSERT_FALSE(VecSimBatchIterator_HasNext(iterator));
    VecSimQueryReply_Free(batch);
    // TEST 2 clean up.
    VecSimBatchIterator_Free(iterator);
}

TYPED_TEST(SVSTieredIndexTest, parallelBatchIteratorSearch) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t sws = 500;
    size_t n = 1000;
    size_t n_res_min = 3;  // minimum number of results to return per batch
    size_t n_res_max = 15; // maximum number of results to return per batch

    size_t per_label = TypeParam::isMulti() ? 5 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .search_window_size = sws,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    auto *svs = tiered_index->GetBackendIndex();
    auto *flat = tiered_index->GetFlatIndex();

    std::atomic_int successful_searches(0);
    auto parallel_10_batches = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        const size_t res_per_batch = search_job->k;
        const size_t dim = search_job->dim;
        const auto query = search_job->query;

        size_t iteration = 0;
        auto verify_res = [&](size_t id, double score, size_t res_index) {
            TEST_DATA_T element = *(TEST_DATA_T *)query;
            res_index += iteration * res_per_batch;
            ASSERT_EQ(std::abs(id - element), (res_index + 1) / 2);
            ASSERT_EQ(score, dim * (id - element) * (id - element));
        };

        // Run 10 batches of search.
        auto tiered_iterator = VecSimBatchIterator_New(search_job->index, query, nullptr);
        do {
            runBatchIteratorSearchTest(tiered_iterator, res_per_batch, verify_res);
        } while (++iteration < 10 && VecSimBatchIterator_HasNext(tiered_iterator));

        VecSimBatchIterator_Free(tiered_iterator);
        (*search_job->successful_searches)++;
        delete job;
    };

    for (size_t i = 0; i < n; i++) {
        auto cur = i % 2 ? svs : flat;
        GenerateAndAddVector<TEST_DATA_T>(cur, dim, i % n_labels, i);

        // Add a search job.
        size_t cur_res_per_batch = i % (n_res_max - n_res_min) + n_res_min;
        size_t n_res = cur_res_per_batch * 10;
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        // make sure there are `n_res / 2` vectors in the index in each "side" of the query vector.
        GenerateVector<TEST_DATA_T>(query, dim, (i % (n_labels - n_res)) + (n_res / 2));
        auto search_job = new (allocator)
            tieredIndexMock::SearchJobMock(allocator, parallel_10_batches, tiered_index,
                                           cur_res_per_batch, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    EXPECT_EQ(tiered_index->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
    EXPECT_EQ(svs->indexSize(), n / 2);
    EXPECT_EQ(flat->indexSize(), n / 2);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(SVSTieredIndexTest, RangeSearch) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t k = 11;
    size_t per_label = TypeParam::isMulti() ? 5 : 1;

    size_t n_labels = k * 3;
    size_t n = n_labels * per_label;

    auto edge_delta = (k - 0.8) * per_label;
    auto mid_delta = edge_delta / 2;
    // `range` for querying the "edges" of the index and get k results.
    double range = dim * edge_delta * edge_delta; // L2 distance.
    // `half_range` for querying a point in the "middle" of the index and get k results around it.
    double half_range = dim * mid_delta * mid_delta; // L2 distance.

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .epsilon = 3.0 * per_label,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    size_t cur_memory_usage;

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();
    ASSERT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    auto svs_index = tiered_index->GetBackendIndex();
    auto flat_index = tiered_index->GetFlatIndex();

    TEST_DATA_T query_0[dim];
    GenerateVector<TEST_DATA_T>(query_0, dim, 0);
    TEST_DATA_T query_1mid[dim];
    GenerateVector<TEST_DATA_T>(query_1mid, dim, n / 3);
    TEST_DATA_T query_2mid[dim];
    GenerateVector<TEST_DATA_T>(query_2mid, dim, n * 2 / 3);
    TEST_DATA_T query_n[dim];
    GenerateVector<TEST_DATA_T>(query_n, dim, n - 1);

    // Search for vectors when the index is empty.
    runRangeQueryTest(tiered_index, query_0, range, nullptr, 0);

    // Define the verification functions.
    auto ver_res_0 = [&](size_t id, double score, size_t index) {
        EXPECT_EQ(id, index);
        // The expected score is the distance to the first vector of `id` label.
        auto element = id * per_label;
        EXPECT_DOUBLE_EQ(score, dim * element * element);
    };

    auto ver_res_1mid_by_id = [&](size_t id, double score, size_t index) {
        size_t q_id = query_1mid[0] / per_label;
        size_t mod = query_1mid[0] - q_id * per_label;
        // In single value mode, `per_label` is always 1 and `mod` is always 0, so the following
        // branchings is simply `expected_score = abs(id - q_id)`.
        // In multi value mode, for ids higher than the query id, the score is the distance to the
        // first vector of `id` label, and for ids lower than the query id, the score is the
        // distance to the last vector of `id` label. `mod` is the distance to the first vector of
        // `q_id` label.
        double expected_score = 0;
        if (id > q_id) {
            expected_score = (id - q_id) * per_label - mod;
        } else if (id < q_id) {
            expected_score = (q_id - id) * per_label - (per_label - mod - 1);
        }
        expected_score = expected_score * expected_score * dim;
        EXPECT_DOUBLE_EQ(score, expected_score);
    };

    auto ver_res_2mid_by_id = [&](size_t id, double score, size_t index) {
        size_t q_id = query_2mid[0] / per_label;
        size_t mod = query_2mid[0] - q_id * per_label;
        // In single value mode, `per_label` is always 1 and `mod` is always 0, so the following
        // branchings is simply `expected_score = abs(id - q_id)`.
        // In multi value mode, for ids higher than the query id, the score is the distance to the
        // first vector of `id` label, and for ids lower than the query id, the score is the
        // distance to the last vector of `id` label. `mod` is the distance to the first vector of
        // `q_id` label.
        double expected_score = 0;
        if (id > q_id) {
            expected_score = (id - q_id) * per_label - mod;
        } else if (id < q_id) {
            expected_score = (q_id - id) * per_label - (per_label - mod - 1);
        }
        expected_score = expected_score * expected_score * dim;
        EXPECT_DOUBLE_EQ(score, expected_score);
    };

    auto ver_res_1mid_by_score = [&](size_t id, double score, size_t index) {
        size_t q_id = query_1mid[0] / per_label;
        EXPECT_EQ(std::abs(int(id - q_id)), (index + 1) / 2);
        ver_res_1mid_by_id(id, score, index);
    };

    auto ver_res_2mid_by_score = [&](size_t id, double score, size_t index) {
        size_t q_id = query_2mid[0] / per_label;
        EXPECT_EQ(std::abs(int(id - q_id)), (index + 1) / 2);
        ver_res_2mid_by_id(id, score, index);
    };

    auto ver_res_n = [&](size_t id, double score, size_t index) {
        EXPECT_EQ(id, n_labels - 1 - index);
        auto element = index * per_label;
        EXPECT_DOUBLE_EQ(score, dim * element * element);
    };

    // Insert n/2 vectors to the main index.
    for (size_t i = 0; i < (n + 1) / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, i / per_label, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), (n + 1) / 2);
    ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize());

    // Search for `range` with the flat index empty.
    cur_memory_usage = allocator->getAllocationSize();
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Insert n/2 vectors to the flat index.
    for (size_t i = (n + 1) / 2; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i / per_label, i);
    }
    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->indexSize(), svs_index->indexSize() + flat_index->indexSize());

    cur_memory_usage = allocator->getAllocationSize();
    // Search for `range` so all the vectors will be from the SVS index.
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
    // Search for `range` so all the vectors will be from the flat index.
    runRangeQueryTest(tiered_index, query_n, range, ver_res_n, k, BY_SCORE);
    // Search for `range` so some of the results will be from the main and some from the flat index.
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_id, k, BY_ID);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // Add some overlapping vectors to the main and flat index.
    // adding directly to the underlying indexes to avoid jobs logic.
    // The main index will have vectors 0 - 2n/3 and the flat index will have vectors n/3 - n
    for (size_t i = n / 3; i < n / 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat_index, dim, i / per_label, i);
    }
    for (size_t i = n / 2; i < n * 2 / 3; i++) {
        GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, i / per_label, i);
    }

    cur_memory_usage = allocator->getAllocationSize();
    // Search for `range` so all the vectors will be from the main index.
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_ID);
    runRangeQueryTest(tiered_index, query_0, range, ver_res_0, k, BY_SCORE);
    // Search for `range` so all the vectors will be from the flat index.
    runRangeQueryTest(tiered_index, query_n, range, ver_res_n, k, BY_SCORE);
    // Search for `range` so some of the results will be from the main and some from the flat index.
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_score, k, BY_SCORE);
    runRangeQueryTest(tiered_index, query_1mid, half_range, ver_res_1mid_by_id, k, BY_ID);
    runRangeQueryTest(tiered_index, query_2mid, half_range, ver_res_2mid_by_id, k, BY_ID);
    // Memory usage should not change.
    ASSERT_EQ(allocator->getAllocationSize(), cur_memory_usage);

    // // // // // // // // // // // //
    // Check behavior upon timeout.  //
    // // // // // // // // // // // //

    VecSimQueryReply *res;
    // Add a vector to the SVS index so there will be a reason to query it.
    GenerateAndAddVector<TEST_DATA_T>(svs_index, dim, n, n);

    // Set timeout callback to always return 1 (will fail while querying the flat buffer).
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 1; }); // Always times out

    res = VecSimIndex_RangeQuery(tiered_index, query_0, range, nullptr, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);

    // Set timeout callback to return 1 after n checks (will fail while querying the SVS index).
    // Brute-force index checks for timeout after each vector.
    size_t checks_in_flat = flat_index->indexSize();
    VecSimQueryParams qparams = {.timeoutCtx = &checks_in_flat};
    VecSim_SetTimeoutCallbackFunction([](void *ctx) {
        auto count = static_cast<size_t *>(ctx);
        if (*count == 0) {
            return 1;
        }
        (*count)--;
        return 0;
    });
    res = VecSimIndex_RangeQuery(tiered_index, query_0, range, &qparams, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);
    // Make sure we didn't get the timeout in the flat index.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_RangeQuery(flat_index, query_0, range, &qparams, BY_SCORE);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_OK);
    VecSimQueryReply_Free(res);

    // Check again with BY_ID.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_RangeQuery(tiered_index, query_0, range, &qparams, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_TimedOut);
    VecSimQueryReply_Free(res);
    // Make sure we didn't get the timeout in the flat index.
    checks_in_flat = flat_index->indexSize(); // Reset the counter.
    res = VecSimIndex_RangeQuery(flat_index, query_0, range, &qparams, BY_ID);
    ASSERT_EQ(VecSimQueryReply_GetCode(res), VecSim_QueryReply_OK);
    VecSimQueryReply_Free(res);

    // Clean up.
    VecSim_SetTimeoutCallbackFunction([](void *ctx) { return 0; });
}

TYPED_TEST(SVSTieredIndexTest, parallelRangeSearch) {
    // Scalar quantization accuracy is insufficient for this test.
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "SVS Scalar quantization accuracy is insufficient for this test.";
    }
    size_t dim = 4;
    size_t k = 11;
    size_t n = 1000;

    size_t per_label = TypeParam::isMulti() ? 10 : 1;
    size_t n_labels = n / per_label;

    // Create TieredSVS index instance with a mock queue.
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .multi = TypeParam::isMulti(),
        .epsilon = double(dim * k * k),
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();
    EXPECT_EQ(mock_thread_pool.ctx->index_strong_ref.use_count(), 1);

    auto *svs = tiered_index->GetBackendIndex();
    auto *flat = tiered_index->GetFlatIndex();

    std::atomic_int successful_searches(0);
    auto parallel_range_search = [](AsyncJob *job) {
        auto *search_job = reinterpret_cast<tieredIndexMock::SearchJobMock *>(job);
        size_t k = search_job->k;
        size_t dim = search_job->dim;
        // The range that will get us k results.
        double range = dim * ((k - 0.5) / 2) * ((k - 0.5) / 2); // L2 distance.
        auto query = search_job->query;

        auto verify_res = [&](size_t id, double score, size_t res_index) {
            TEST_DATA_T element = *(TEST_DATA_T *)query;
            ASSERT_EQ(std::abs(id - element), (res_index + 1) / 2);
            ASSERT_EQ(score, dim * (id - element) * (id - element));
        };
        runRangeQueryTest(job->index, query, range, verify_res, k, BY_SCORE);
        (*search_job->successful_searches)++;
        delete job;
    };

    for (size_t i = 0; i < n; i++) {
        auto cur = i % 2 ? svs : flat;
        GenerateAndAddVector<TEST_DATA_T>(cur, dim, i % n_labels, i);

        // Add a search job. Make sure the query element is between k and n_labels - k.
        auto query = (TEST_DATA_T *)allocator->allocate(dim * sizeof(TEST_DATA_T));
        GenerateVector<TEST_DATA_T>(query, dim, ((n - i) % (n_labels - (2 * k))) + k);
        auto search_job = new (allocator) tieredIndexMock::SearchJobMock(
            allocator, parallel_range_search, tiered_index, k, query, n, dim, &successful_searches);
        tiered_index->submitSingleJob(search_job);
    }

    EXPECT_EQ(tiered_index->indexSize(), n);
    EXPECT_EQ(tiered_index->indexLabelCount(), n_labels);
    EXPECT_EQ(svs->indexSize(), n / 2);
    EXPECT_EQ(flat->indexSize(), n / 2);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    EXPECT_EQ(successful_searches, n);
    EXPECT_EQ(mock_thread_pool.jobQ.size(), 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, overwriteVectorBasic) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock(1);

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    TEST_DATA_T val = 1.0;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 0, val);
    // Overwrite label 0 (in the flat buffer) with a different value.
    val = 2.0;
    TEST_DATA_T overwritten_vec[] = {val, val, val, val};
    ASSERT_EQ(tiered_index->addVector(overwritten_vec, 0), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(0, overwritten_vec), 0);

    // Validate that jobs were created properly - first job should be invalid after overwrite,
    // the second should be a pending insert job.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->jobType, SVS_BATCH_UPDATE_JOB);
    ASSERT_EQ(mock_thread_pool.jobQ.front().job->isValid, true);
    mock_thread_pool.thread_iteration();

    // Ingest vector into SVS, and then overwrite it.
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    val = 3.0;
    overwritten_vec[0] = overwritten_vec[1] = overwritten_vec[2] = overwritten_vec[3] = val;
    ASSERT_EQ(tiered_index->addVector(overwritten_vec, 0), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    // Overriding vector in tiered index should remove the vector from SVS to avoid duplicates.
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(0, overwritten_vec), 0);

    // Ingest the updated vector to SVS.
    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
    ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(0, overwritten_vec), 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, overwriteVectorAsync) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    for (size_t updateThreshold : {n, size_t{1}}) {
        auto mock_thread_pool = tieredIndexMock();

        auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool,
                                                        updateThreshold, updateThreshold);
        ASSERT_INDEX(tiered_index);
        auto allocator = tiered_index->getAllocator();

        // Launch the BG threads loop that takes jobs from the queue and executes them.

        for (size_t i = 0; i < mock_thread_pool.thread_pool_size; i++) {
            mock_thread_pool.thread_pool.emplace_back(tieredIndexMock::thread_main_loop, i,
                                                      std::ref(mock_thread_pool));
        }

        // Insert vectors and overwrite them multiple times while thread run in the background.
        std::srand(10); // create pseudo random generator with any arbitrary seed.
        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            tiered_index->addVector(vector, i);
        }
        EXPECT_EQ(tiered_index->indexLabelCount(), n);

        size_t num_overwrites = 1000;
        for (size_t i = 0; i < num_overwrites; i++) {
            size_t label_to_overwrite = std::rand() % n;
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            EXPECT_EQ(tiered_index->addVector(vector, label_to_overwrite), 0);
        }

        mock_thread_pool.thread_pool_wait();
        mock_thread_pool.thread_pool_join();

        EXPECT_LE(tiered_index->GetFlatIndex()->indexSize(), updateThreshold);
        EXPECT_EQ(tiered_index->indexLabelCount(), n);
    }
}

TYPED_TEST(SVSTieredIndexTest, testCreateWithDefaultTieredSVSParams) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Build with default specificTieredBackendInfo.svsTieredInfo params
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 0, 0, 0);
    ASSERT_INDEX(tiered_index);

    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    // Validate tiered svs info fields
    constexpr size_t expected_training_threshold =
        TypeParam::get_quant_bits() == VecSimSvsQuant_NONE ? SVS_VAMANA_DEFAULT_UPDATE_THRESHOLD
                                                           : SVS_VAMANA_DEFAULT_TRAINING_THRESHOLD;
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.trainingTriggerThreshold,
              expected_training_threshold);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.updateTriggerThreshold,
              SVS_VAMANA_DEFAULT_UPDATE_THRESHOLD);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.updateJobWaitTime,
              SVS_DEFAULT_UPDATE_JOB_WAIT_TIME);
}

TYPED_TEST(SVSTieredIndexTest, testInfo) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    EXPECT_EQ(info.commonInfo.basicInfo.algo, VecSimAlgo_SVS);
    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, tiered_index->getAllocationSize());
    EXPECT_EQ(info.commonInfo.basicInfo.isMulti, TypeParam::isMulti());
    EXPECT_EQ(info.commonInfo.basicInfo.dim, dim);
    EXPECT_EQ(info.commonInfo.basicInfo.metric, VecSimMetric_L2);
    EXPECT_EQ(info.commonInfo.basicInfo.type, TypeParam::get_index_type());
    EXPECT_EQ(info.commonInfo.basicInfo.blockSize, DEFAULT_BLOCK_SIZE);
    VecSimIndexDebugInfo frontendIndexInfo = tiered_index->GetFlatIndex()->debugInfo();
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();

    compareCommonInfo(info.tieredInfo.frontendCommonInfo, frontendIndexInfo.commonInfo);
    compareFlatInfo(info.tieredInfo.bfInfo, frontendIndexInfo.bfInfo);
    compareCommonInfo(info.tieredInfo.backendCommonInfo, backendIndexInfo.commonInfo);
    compareSVSInfo(info.tieredInfo.backendInfo.svsInfo, backendIndexInfo.svsInfo);

    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          backendIndexInfo.commonInfo.memory +
                                          frontendIndexInfo.commonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
    // Validate tiered svs info fields
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.trainingTriggerThreshold, 1);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.updateTriggerThreshold, 1);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.updateJobWaitTime, 1);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.indexUpdateScheduled, false);

    // Validate that Static info returns the right restricted info as well.
    VecSimIndexBasicInfo s_info = VecSimIndex_BasicInfo(tiered_index);
    ASSERT_EQ(info.commonInfo.basicInfo.algo, s_info.algo);
    ASSERT_EQ(info.commonInfo.basicInfo.dim, s_info.dim);
    ASSERT_EQ(info.commonInfo.basicInfo.blockSize, s_info.blockSize);
    ASSERT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    ASSERT_EQ(info.commonInfo.basicInfo.isMulti, s_info.isMulti);
    ASSERT_EQ(info.commonInfo.basicInfo.type, s_info.type);
    ASSERT_EQ(info.commonInfo.basicInfo.isTiered, s_info.isTiered);

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    info = tiered_index->debugInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 1);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 1);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_TRUE);
    EXPECT_EQ(info.tieredInfo.specificTieredBackendInfo.svsTieredInfo.indexUpdateScheduled, true);

    mock_thread_pool.thread_iteration();
    info = tiered_index->debugInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 1);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 1);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 1);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);

    if (TypeParam::isMulti()) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
        info = tiered_index->debugInfo();

        EXPECT_EQ(info.commonInfo.indexSize, 2);
        EXPECT_EQ(info.commonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 1);
        EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 1);
        EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 1);
        EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                              info.tieredInfo.backendCommonInfo.memory +
                                              info.tieredInfo.frontendCommonInfo.memory);
        EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_TRUE);
    }

    VecSimIndex_DeleteVector(tiered_index, 1);
    VecSimTieredIndex_GC(tiered_index);
    while (mock_thread_pool.jobQ.size() > 0)
        mock_thread_pool.thread_iteration();

    info = tiered_index->debugInfo();

    EXPECT_EQ(info.commonInfo.indexSize, 0);
    EXPECT_EQ(info.commonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.backendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexSize, 0);
    EXPECT_EQ(info.tieredInfo.frontendCommonInfo.indexLabelCount, 0);
    EXPECT_EQ(info.commonInfo.memory, info.tieredInfo.management_layer_memory +
                                          info.tieredInfo.backendCommonInfo.memory +
                                          info.tieredInfo.frontendCommonInfo.memory);
    EXPECT_EQ(info.tieredInfo.backgroundIndexing, VecSimBool_FALSE);
}

TYPED_TEST(SVSTieredIndexTest, testInfoIterator) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t n = 1000;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};

    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 1);
    VecSimIndexDebugInfo info = tiered_index->debugInfo();
    VecSimIndexDebugInfo frontendIndexInfo = tiered_index->GetFlatIndex()->debugInfo();
    VecSimIndexDebugInfo backendIndexInfo = tiered_index->GetBackendIndex()->debugInfo();

    // Use the C API wrapper (as RediSearch does) so the process-wide SHARED_MEMORY
    // field is appended at the top level — compareTieredIndexInfoToIterator expects it.
    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(tiered_index);
    compareTieredIndexInfoToIterator(info, frontendIndexInfo, backendIndexInfo, infoIterator);

    VecSimDebugInfoIterator_Free(infoIterator);
}

TYPED_TEST(SVSTieredIndexTest, debugInfoIteratorFieldOrder) {
    namespace expected_output = test_utils::test_debug_info_iterator_order;

    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    auto mock_thread_pool = tieredIndexMock();

    auto *index = test_utils::CreateNewTieredVecSimIndex(params, mock_thread_pool);
    ASSERT_INDEX(index);

    GenerateAndAddVector<TEST_DATA_T>(index, dim, 1, 1);
    VecSimDebugInfoIterator *infoIterator = VecSimIndex_DebugInfoIterator(index);

    // Test the field order using the common function
    expected_output::testDebugInfoIteratorFieldOrder(infoIterator,
                                                     expected_output::getTieredSVSFields());

    VecSimDebugInfoIterator_Free(infoIterator);
}

TYPED_TEST(SVSTieredIndexTest, writeInPlaceMode) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    const size_t updateThreshold = 2;

    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index =
        this->CreateTieredSVSIndex(svs_params, mock_thread_pool, updateThreshold, updateThreshold);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    VecSim_SetWriteMode(VecSim_WriteInPlace);
    // Validate that the first vector was buffered in flat index.
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 10, 10);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);

    // Validate that the second vector causes moving to the SVS index.
    labelType vec_label = 0;
    GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, vec_label, 0);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 2);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);

    // In write-in-place mode every mutation runs synchronously, including consolidation
    // of soft-deleted slots.
    size_t expected_marked_deleted = 0;
    // Overwrite inplace - only in single-value mode
    if (!TypeParam::isMulti()) {
        TEST_DATA_T overwritten_vec[] = {1, 1, 1, 1};
        tiered_index->addVector(overwritten_vec, vec_label);
        expected_marked_deleted++;
        ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), 3);
        ASSERT_EQ(tiered_index->indexSize(), 3);
        ASSERT_EQ(tiered_index->indexLabelCount(), 2);
        ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
        ASSERT_EQ(tiered_index->getDistanceFrom_Unsafe(vec_label, overwritten_vec), 0);
        ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), expected_marked_deleted);
    }
    // Validate that the vector is deleted and consolidated synchronously (in-place mode),
    // so no marked-deleted entry remains.
    tiered_index->deleteVector(vec_label);
    expected_marked_deleted++;
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), TypeParam::isMulti() ? 2 : 3);
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);

    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), expected_marked_deleted);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, expected_marked_deleted);
}

TYPED_TEST(SVSTieredIndexTest, switchWriteModes) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t n = 500;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .multi = TypeParam::isMulti()};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly trigger the first update job for at least half of the vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n / 2);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();
    VecSim_SetWriteMode(VecSim_WriteAsync);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    // Create and insert vectors one by one async.
    size_t per_label = TypeParam::isMulti() ? 5 : 1;
    size_t n_labels = n / per_label;
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, i % n_labels);
    }

    // Insert another n more vectors INPLACE, while the previous vectors are still being indexed.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    EXPECT_LE(tiered_index->GetBackendIndex()->indexSize(), n);
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, i % n_labels + n_labels);
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();
    EXPECT_GE(tiered_index->GetBackendIndex()->indexSize(), 2 * n - this->getUpdateThreshold());

    // Now delete the last n inserted vectors of the index using async jobs.
    VecSim_SetWriteMode(VecSim_WriteAsync);
    mock_thread_pool.init_threads();
    for (size_t i = 0; i < n_labels; i++) {
        VecSimIndex_DeleteVector(tiered_index, n_labels + i);
    }

    // Insert INPLACE another n vector (instead of the ones that were deleted).
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    auto svs_index = tiered_index->GetBackendIndex();
    // Run twice, at first run we insert non-existing labels, in the second run we overwrite them
    // (for single-value index only).
    for (auto overwrite : {0, 1}) {
        for (size_t i = 0; i < n; i++) {
            TEST_DATA_T vector[dim];
            for (size_t j = 0; j < dim; j++) {
                vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
            }
            labelType cur_label = i % n_labels + n_labels;
            EXPECT_EQ(tiered_index->addVector(vector, cur_label),
                      TypeParam::isMulti() ? 1 : 1 - overwrite);
            // Quantization has limited accuaracy, so we need to check the relative error.
            // If quantization is enabled, we allow a larger relative error.
            double abs_err = TypeParam::get_quant_bits() != VecSimSvsQuant_NONE ? 1e-2 : 1.e-6;
            // Run a query over svs index and see that we only receive ids with label < n_labels+i
            // (the label that we just inserted), and the first result should be this vector
            // (unless it is unreachable)
            auto ver_res = [&](size_t res_label, double score, size_t index) {
                if (index == 0) {
                    if (res_label == cur_label) {
                        EXPECT_NEAR(score, 0, abs_err);
                    } else {
                        tiered_index->acquireSharedLocks();
                        ASSERT_EQ(svs_index->getDistanceFrom_Unsafe(cur_label, vector), 0);
                        tiered_index->releaseSharedLocks();
                    }
                }
                if (!overwrite) {
                    ASSERT_LE(res_label, i + n_labels);
                }
            };
            runTopKSearchTest(svs_index, vector, 10, ver_res);
        }
    }

    mock_thread_pool.thread_pool_join();
    // Verify that vectors were moved to SVS as expected
    auto sz_f = tiered_index->GetFlatIndex()->indexSize();
    auto sz_b = tiered_index->GetBackendIndex()->indexLabelCount();
    EXPECT_LE(sz_f, this->getUpdateThreshold());
    if (TypeParam::isMulti()) {
        ASSERT_EQ(tiered_index->indexLabelCount(), 2 * n_labels);
    } else {
        EXPECT_EQ(sz_f + sz_b, 2 * n_labels);
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, preferAdHocOptimization) {
    size_t dim = 4;

    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Create tiered index with buffer limit set to 0.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    auto svs = tiered_index->GetBackendIndex();
    auto flat = tiered_index->GetFlatIndex();

    // Insert 5 vectors to the main index.
    for (size_t i = 0; i < 5; i++) {
        GenerateAndAddVector<TEST_DATA_T>(svs, dim, i, i);
    }
    // Sanity check. Should choose as SVS.
    ASSERT_EQ(tiered_index->preferAdHocSearch(5, 5, true), svs->preferAdHocSearch(5, 5, true));

    // Insert 6 vectors to the flat index.
    for (size_t i = 0; i < 6; i++) {
        GenerateAndAddVector<TEST_DATA_T>(flat, dim, i, i);
    }
    // Sanity check. Should choose as flat as it has more vectors.
    ASSERT_EQ(tiered_index->preferAdHocSearch(5, 5, true), flat->preferAdHocSearch(5, 5, true));

    // Check for preference of tiered with subset (10) smaller than the tiered index size (11),
    // but larger than any of the underlying indexes.
    ASSERT_NO_THROW(tiered_index->preferAdHocSearch(10, 5, false));
}

TYPED_TEST(SVSTieredIndexTest, runGCBetweenIteratorBatches) {
    if (this->isFallbackToSQ()) {
        GTEST_SKIP() << "Scalar fallback does not preserve the ordering checked by this test.";
    }
    constexpr size_t dim = 4;
    constexpr size_t n_labels = 6;
    for (auto mode : {VecSim_WriteAsync, VecSim_WriteInPlace}) {
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = dim,
                            .metric = VecSimMetric_L2,
                            .multi = TypeParam::isMulti()};
        VecSimParams svs_params = CreateParams(params);
        auto mock_thread_pool = tieredIndexMock(1);
        auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
        ASSERT_INDEX(tiered_index);
        auto *backend = tiered_index->GetBackendIndex();
        for (size_t i = 0; i < n_labels; ++i) {
            GenerateAndAddVector<TEST_DATA_T>(backend, dim, i + 1000, i + 1);
        }

        TEST_DATA_T query[dim] = {};
        VecSimQueryParams query_params{};
        query_params.batchSize = 3;
        auto *iterator = VecSimBatchIterator_New(tiered_index, query, &query_params);
        auto verify_first = [](size_t id, double score, size_t i) {
            EXPECT_EQ(id, i + 1000);
            if constexpr (TypeParam::get_quant_bits() == VecSimSvsQuant_NONE) {
                EXPECT_DOUBLE_EQ(score, dim * double(i + 1) * double(i + 1));
            }
        };
        runBatchIteratorSearchTest(iterator, 3, verify_first);
        for (size_t i = 0; i < 3; ++i) {
            backend->deleteVector(i + 1000);
        }

        VecSim_SetWriteMode(mode);
        std::promise<void> gc_finished;
        auto completion = gc_finished.get_future();
        std::thread gc([&]() {
            VecSimTieredIndex_GC(tiered_index);
            if (mode == VecSim_WriteAsync) {
                mock_thread_pool.thread_iteration();
            }
            gc_finished.set_value();
        });
        const bool completed =
            completion.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
        // If the old lifetime lock is still present, release it on its owning thread
        // so GC can finish and the regression fails without hanging the suite.
        if (!completed) {
            VecSimBatchIterator_Free(iterator);
            iterator = nullptr;
        }
        gc.join();
        VecSim_SetWriteMode(VecSim_WriteAsync);
        ASSERT_TRUE(completed) << "GC waited for an idle iterator to be freed";
        EXPECT_EQ(backend->indexSize(), 3);
        EXPECT_TRUE(VecSimBatchIterator_HasNext(iterator));
        auto verify_remaining = [](size_t id, double score, size_t i) {
            EXPECT_EQ(id, i + 1003);
            if constexpr (TypeParam::get_quant_bits() == VecSimSvsQuant_NONE) {
                EXPECT_DOUBLE_EQ(score, dim * double(i + 4) * double(i + 4));
            }
        };
        runBatchIteratorSearchTest(iterator, 3, verify_remaining);
        EXPECT_FALSE(VecSimBatchIterator_HasNext(iterator));
        VecSimBatchIterator_Free(iterator);
    }
}

namespace {

constexpr size_t svs_gc_test_dim = 4;
constexpr size_t svs_gc_label_base = 1'000'000;
constexpr size_t svs_gc_label_stride = 17;
using SVSReferenceVectors = std::vector<std::vector<std::vector<float>>>;
using SVSTestReply = std::unique_ptr<VecSimQueryReply, decltype(&VecSimQueryReply_Free)>;

size_t svsGCLabel(size_t i) { return svs_gc_label_base + svs_gc_label_stride * i; }

SVSReferenceVectors makeSVSReferenceVectors(size_t n, bool multi, bool distinct = false) {
    SVSReferenceVectors reference(n);
    for (size_t i = 0; i < n; ++i) {
        for (size_t copy = 0; copy < (multi ? 2U : 1U); ++copy) {
            std::vector<float> vector(svs_gc_test_dim);
            for (size_t d = 0; d < vector.size(); ++d) {
                vector[d] = float(i + 1) + float(d) / 4 + (distinct ? float(copy) / 8 : 0);
            }
            reference[i].push_back(std::move(vector));
        }
    }
    return reference;
}

void addSVSReferenceVectors(VecSimIndex *index, const SVSReferenceVectors &reference, size_t begin,
                            size_t end) {
    for (size_t i = begin; i < end; ++i) {
        for (const auto &vector : reference[i]) {
            VecSimIndex_AddVector(index, vector.data(), svsGCLabel(i));
        }
    }
}

double svsReferenceDistance(const std::vector<float> &vector, const std::vector<float> &query) {
    double distance = 0;
    for (size_t d = 0; d < vector.size(); ++d) {
        const double delta = double(vector[d]) - double(query[d]);
        distance += delta * delta;
    }
    return distance;
}

// All references are immutable. Concurrently deleted labels need only remain valid external
// labels; scores are checked for permanent survivors, whose vectors never change.
std::string checkSVSReply(const VecSimQueryReply &reply, const SVSReferenceVectors &reference,
                          const std::vector<float> &query, size_t survivors_begin,
                          size_t &checked_survivors) {
    if (reply.code != VecSim_QueryReply_OK) {
        return "Query did not complete successfully";
    }
    std::unordered_set<size_t> labels;
    for (const auto &result : reply.results) {
        if (result.id < svs_gc_label_base ||
            (result.id - svs_gc_label_base) % svs_gc_label_stride != 0 ||
            (result.id - svs_gc_label_base) / svs_gc_label_stride >= reference.size()) {
            return "Unknown external label: " + std::to_string(result.id);
        }
        if (!std::isfinite(result.score) || !labels.insert(result.id).second) {
            return "Non-finite score or duplicate label: " + std::to_string(result.id);
        }
        const size_t i = (result.id - svs_gc_label_base) / svs_gc_label_stride;
        if (i >= survivors_begin) {
            const double expected = svsReferenceDistance(reference[i][0], query);
            if (std::abs(result.score - expected) > 1e-5 * std::max(1.0, expected)) {
                return "Wrong score for label " + std::to_string(result.id) + ": got " +
                       std::to_string(result.score) + ", expected " + std::to_string(expected);
            }
            ++checked_survivors;
        }
    }
    return {};
}

std::string checkSVSCopies(TieredSVSIndex<float> *index, const SVSReferenceVectors &reference,
                           size_t begin, size_t end) {
    for (size_t i = begin; i < end; ++i) {
        std::vector<std::vector<float>> copied;
        index->getDataByLabel(svsGCLabel(i), copied);
        std::sort(copied.begin(), copied.end());
        // Reference copies are generated in sorted order. Equality also checks multiplicity.
        if (copied != reference[i]) {
            return "Wrong vectors copied for label " + std::to_string(svsGCLabel(i));
        }
    }
    return {};
}

class SVSTestWriteMode {
    VecSimWriteMode previous = TieredSVSIndex<float>::getWriteMode();

public:
    explicit SVSTestWriteMode(VecSimWriteMode mode) { VecSim_SetWriteMode(mode); }
    ~SVSTestWriteMode() { VecSim_SetWriteMode(previous); }
};

// Reader checks run outside the synchronization mutex. Failures are reported after joining,
// and each mutation round waits for every reader to make progress without using sleeps.
class SVSTestReaders {
    std::vector<std::function<std::string()>> checks;
    std::vector<std::thread> threads;
    std::vector<size_t> completed;
    std::mutex mutex;
    std::condition_variable changed;
    std::atomic<bool> stopped{false};
    bool started = false;
    std::string failure;

public:
    explicit SVSTestReaders(std::vector<std::function<std::string()>> checks)
        : checks(std::move(checks)), completed(this->checks.size(), 0) {}

    void start() {
        for (size_t i = 0; i < checks.size(); ++i) {
            threads.emplace_back([this, i] {
                {
                    std::unique_lock lock(mutex);
                    changed.wait(lock, [&] { return started || stopped.load(); });
                }
                while (!stopped.load()) {
                    auto error = checks[i]();
                    {
                        std::lock_guard lock(mutex);
                        ++completed[i];
                        if (!error.empty()) {
                            if (failure.empty()) {
                                failure = "Reader " + std::to_string(i) + ": " + error;
                            }
                            stopped.store(true);
                        }
                    }
                    changed.notify_all();
                }
            });
        }
        {
            std::lock_guard lock(mutex);
            started = true;
        }
        changed.notify_all();
    }

    bool waitForProgress() {
        std::unique_lock lock(mutex);
        const auto before = completed;
        const bool progressed = changed.wait_for(lock, std::chrono::seconds(5), [&] {
            if (!failure.empty()) {
                return true;
            }
            for (size_t i = 0; i < completed.size(); ++i) {
                if (completed[i] == before[i]) {
                    return false;
                }
            }
            return true;
        });
        if (!progressed && failure.empty()) {
            failure = "Timed out waiting for reader progress";
            stopped.store(true);
        }
        return progressed && failure.empty();
    }

    void finish() {
        {
            std::lock_guard lock(mutex);
            stopped.store(true);
        }
        changed.notify_all();
        for (auto &thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

    const std::string &error() const { return failure; } // Read only after finish().
    ~SVSTestReaders() { finish(); }
};

} // namespace

TYPED_TEST(SVSTieredIndexTestBasic, runGCBetweenIteratorBatchesWithStorageShrink) {
    constexpr size_t n_labels = 96;
    constexpr size_t batch_size = 16;
    const auto reference = makeSVSReferenceVectors(n_labels, false);
    const std::vector<float> query(svs_gc_test_dim, 0);
    for (auto mode : {VecSim_WriteAsync, VecSim_WriteInPlace}) {
        for (bool delete_returned : {true, false}) {
            SCOPED_TRACE(::testing::Message()
                         << "mode=" << mode << ", delete_returned=" << delete_returned);
            SVSParams params = {.type = TypeParam::get_index_type(),
                                .dim = svs_gc_test_dim,
                                .metric = VecSimMetric_L2,
                                .blockSize = 8,
                                .search_window_size = n_labels,
                                .search_buffer_capacity = n_labels};
            auto svs_params = CreateParams(params);
            tieredIndexMock mock_thread_pool(1);
            auto *index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
            ASSERT_INDEX(index);
            // Index creation resizes the thread pool, which switches write mode to async.
            SVSTestWriteMode write_mode(mode);
            auto *backend = index->GetBackendIndex();
            addSVSReferenceVectors(backend, reference, 0, n_labels);
            ASSERT_EQ(index->GetFlatIndex()->indexSize(), 0);

            VecSimQueryParams query_params{};
            // Consume the entire native batch, leaving no prefetched external labels.
            query_params.batchSize = batch_size;
            std::unique_ptr<VecSimBatchIterator, decltype(&VecSimBatchIterator_Free)> iterator(
                VecSimBatchIterator_New(index, query.data(), &query_params),
                VecSimBatchIterator_Free);
            std::unordered_set<size_t> seen, deleted;
            auto read_batch = [&] {
                SVSTestReply reply(VecSimBatchIterator_Next(iterator.get(), batch_size, BY_SCORE),
                                   VecSimQueryReply_Free);
                size_t checked = 0;
                EXPECT_EQ(checkSVSReply(*reply, reference, query, 0, checked), "");
                for (const auto &result : reply->results) {
                    EXPECT_EQ(deleted.count(result.id), 0) << result.id;
                    EXPECT_TRUE(seen.insert(result.id).second) << result.id;
                }
                return reply->results.size();
            };
            ASSERT_EQ(read_batch(), batch_size);
            for (size_t round = 0; round < 2; ++round) {
                const size_t begin = (delete_returned ? round : 2 * round + 1) * batch_size;
                for (size_t i = begin; i < begin + batch_size; ++i) {
                    ASSERT_EQ(seen.count(svsGCLabel(i)), delete_returned ? 1 : 0);
                    // As in runGCBetweenIteratorBatches, defer consolidation to GC so the
                    // timeout below can unwind an old iterator holding a lifetime lock.
                    ASSERT_EQ(backend->deleteVector(svsGCLabel(i)), 1);
                    deleted.insert(svsGCLabel(i));
                }
                const auto capacity_before = backend->indexCapacity();
                std::promise<void> gc_finished;
                auto completion = gc_finished.get_future();
                std::thread gc([&] {
                    VecSimTieredIndex_GC(index);
                    if (mode == VecSim_WriteAsync) {
                        mock_thread_pool.thread_iteration();
                    }
                    gc_finished.set_value();
                });
                const bool completed =
                    completion.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
                if (!completed) {
                    iterator.reset();
                }
                gc.join();
                ASSERT_TRUE(completed) << "GC waited for an idle iterator to be freed";
                EXPECT_EQ(index->GetSVSIndex()->indexStorageSize(),
                          n_labels - (round + 1) * batch_size);
                EXPECT_LT(backend->indexCapacity(), capacity_before);
                ASSERT_EQ(read_batch(), batch_size);
            }

            auto drain_iterator = [&] {
                // Bound the loop so failure to reach exhaustion cannot hang the test.
                for (size_t batch = 0;
                     batch <= n_labels / batch_size && VecSimBatchIterator_HasNext(iterator.get());
                     ++batch) {
                    const auto count = read_batch();
                    EXPECT_TRUE(count > 0 || !VecSimBatchIterator_HasNext(iterator.get()));
                }
                EXPECT_FALSE(VecSimBatchIterator_HasNext(iterator.get()));
            };
            drain_iterator();
            for (size_t i = 0; i < n_labels; ++i) {
                EXPECT_EQ(seen.count(svsGCLabel(i)),
                          delete_returned || !deleted.count(svsGCLabel(i)) ? 1 : 0);
            }

            VecSimBatchIterator_Reset(iterator.get());
            seen.clear();
            drain_iterator();
            EXPECT_EQ(seen.size(), n_labels - deleted.size());
            for (size_t i = 0; i < n_labels; ++i) {
                EXPECT_EQ(seen.count(svsGCLabel(i)), deleted.count(svsGCLabel(i)) ? 0 : 1);
            }
        }
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, parallelQueriesDuringGC) {
    constexpr size_t n_labels = 256;
    constexpr size_t rounds = 3;
    constexpr size_t delete_per_round = 64;
    constexpr size_t survivors_begin = rounds * delete_per_round;
    SVSTestWriteMode write_mode(VecSim_WriteAsync);
    for (bool multi : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "multi=" << multi);
        const auto reference = makeSVSReferenceVectors(n_labels, multi);
        const auto &query = reference.back()[0];
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = svs_gc_test_dim,
                            .metric = VecSimMetric_L2,
                            .multi = multi,
                            .blockSize = 16,
                            .search_window_size = n_labels};
        auto svs_params = CreateParams(params);
        tieredIndexMock mock_thread_pool(1);
        auto *index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
        ASSERT_INDEX(index);
        addSVSReferenceVectors(index->GetBackendIndex(), reference, 0, n_labels);
        ASSERT_EQ(index->GetFlatIndex()->indexSize(), 0);

        std::array<size_t, 2> checked_survivors{};
        auto query_once = [&](bool range) -> std::string {
            SVSTestReply reply(
                range ? VecSimIndex_RangeQuery(index, query.data(), 4096, nullptr, BY_SCORE)
                      : VecSimIndex_TopKQuery(index, query.data(), 10, nullptr, BY_SCORE),
                VecSimQueryReply_Free);
            if (reply->results.empty()) {
                return "Empty reply for a query near permanent survivors";
            }
            return checkSVSReply(*reply, reference, query, survivors_begin,
                                 checked_survivors[range ? 1 : 0]);
        };
        SVSTestReaders readers(
            {[&] { return query_once(false); }, [&] { return query_once(true); }});
        bool gc_reader_progress = false;
        index->registerTracingCallback("GCJob::before_run_gc",
                                       [&] { gc_reader_progress = readers.waitForProgress(); });
        readers.start();
        bool progress = readers.waitForProgress();
        for (size_t round = 0; round < rounds && progress; ++round) {
            const auto storage_before = index->GetSVSIndex()->indexStorageSize();
            const auto capacity_before = index->GetBackendIndex()->indexCapacity();
            for (size_t i = round * delete_per_round; i < (round + 1) * delete_per_round; ++i) {
                EXPECT_EQ(index->deleteVector(svsGCLabel(i)), multi ? 2 : 1);
            }
            gc_reader_progress = false;
            VecSimTieredIndex_GC(index);
            // Also drain the consolidation jobs queued by deleteVector.
            mock_thread_pool.init_threads();
            mock_thread_pool.thread_pool_join();
            progress = gc_reader_progress;
            EXPECT_LT(index->GetSVSIndex()->indexStorageSize(), storage_before);
            EXPECT_LT(index->GetBackendIndex()->indexCapacity(), capacity_before);
        }
        readers.finish();
        ASSERT_TRUE(progress) << readers.error();
        ASSERT_EQ(readers.error(), "");
        EXPECT_GT(checked_survivors[0], 0);
        EXPECT_GT(checked_survivors[1], 0);
        EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), n_labels - survivors_begin);
        EXPECT_EQ(index->GetSVSIndex()->getNumMarkedDeleted(), 0);
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, getDataByLabelDuringGC) {
    constexpr size_t n_labels = 256;
    constexpr size_t rounds = 3;
    constexpr size_t delete_per_round = 64;
    constexpr size_t monitored_begin = n_labels - 8;
    SVSTestWriteMode write_mode(VecSim_WriteAsync);
    for (bool multi : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "multi=" << multi);
        const auto reference = makeSVSReferenceVectors(n_labels, multi, true);
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = svs_gc_test_dim,
                            .metric = VecSimMetric_L2,
                            .multi = multi,
                            .blockSize = 16};
        auto svs_params = CreateParams(params);
        tieredIndexMock mock_thread_pool(1);
        auto *index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool);
        ASSERT_INDEX(index);
        addSVSReferenceVectors(index->GetBackendIndex(), reference, 0, n_labels);
        ASSERT_EQ(index->GetFlatIndex()->indexSize(), 0);

        SVSTestReaders readers(
            {[&] { return checkSVSCopies(index, reference, monitored_begin, n_labels); }});
        bool gc_reader_progress = false;
        index->registerTracingCallback("GCJob::before_run_gc",
                                       [&] { gc_reader_progress = readers.waitForProgress(); });
        readers.start();
        bool progress = readers.waitForProgress();
        for (size_t round = 0; round < rounds && progress; ++round) {
            const auto capacity_before = index->GetBackendIndex()->indexCapacity();
            for (size_t i = round * delete_per_round; i < (round + 1) * delete_per_round; ++i) {
                EXPECT_EQ(index->deleteVector(svsGCLabel(i)), multi ? 2 : 1);
            }
            gc_reader_progress = false;
            VecSimTieredIndex_GC(index);
            mock_thread_pool.init_threads();
            mock_thread_pool.thread_pool_join();
            progress = gc_reader_progress;
            EXPECT_EQ(index->GetSVSIndex()->indexStorageSize(),
                      (n_labels - (round + 1) * delete_per_round) * (multi ? 2 : 1));
            EXPECT_LT(index->GetBackendIndex()->indexCapacity(), capacity_before);
        }
        readers.finish();
        ASSERT_TRUE(progress) << readers.error();
        ASSERT_EQ(readers.error(), "");
        EXPECT_EQ(checkSVSCopies(index, reference, monitored_begin, n_labels), "");
        EXPECT_EQ(index->GetSVSIndex()->getNumMarkedDeleted(), 0);
        EXPECT_TRUE(mock_thread_pool.jobQ.empty());
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, getDataByLabelDuringIngestion) {
    constexpr size_t initial_labels = 64;
    constexpr size_t rounds = 4;
    constexpr size_t labels_per_round = 64;
    constexpr size_t n_labels = initial_labels + rounds * labels_per_round;
    SVSTestWriteMode write_mode(VecSim_WriteAsync);
    for (bool multi : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "multi=" << multi);
        const auto reference = makeSVSReferenceVectors(n_labels, multi, true);
        SVSParams params = {.type = TypeParam::get_index_type(),
                            .dim = svs_gc_test_dim,
                            .metric = VecSimMetric_L2,
                            .multi = multi,
                            .blockSize = 16};
        auto svs_params = CreateParams(params);
        tieredIndexMock mock_thread_pool(1);
        auto tiered_params = this->CreateTieredSVSParams(svs_params, mock_thread_pool);
        // Keep each batch on the worker ingestion path instead of bypassing a full buffer.
        tiered_params.flatBufferLimit = 2 * labels_per_round;
        auto *index = this->CreateTieredSVSIndex(tiered_params, mock_thread_pool);
        ASSERT_INDEX(index);
        addSVSReferenceVectors(index->GetBackendIndex(), reference, 0, initial_labels);
        ASSERT_TRUE(index->GetSVSIndex()->ready());

        SVSTestReaders readers(
            {[&] { return checkSVSCopies(index, reference, initial_labels - 8, initial_labels); }});
        readers.start();
        bool progress = readers.waitForProgress();
        for (size_t round = 0; round < rounds && progress; ++round) {
            const size_t begin = initial_labels + round * labels_per_round;
            const auto capacity_before = index->GetBackendIndex()->indexCapacity();
            addSVSReferenceVectors(index, reference, begin, begin + labels_per_round);
            EXPECT_EQ(index->GetFlatIndex()->indexSize(), labels_per_round * (multi ? 2 : 1));
            EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), begin);
            mock_thread_pool.init_threads();
            progress = readers.waitForProgress();
            mock_thread_pool.thread_pool_join();
            EXPECT_EQ(index->GetFlatIndex()->indexSize(), 0);
            EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), begin + labels_per_round);
            EXPECT_EQ(index->GetSVSIndex()->indexStorageSize(),
                      (begin + labels_per_round) * (multi ? 2 : 1));
            EXPECT_GT(index->GetBackendIndex()->indexCapacity(), capacity_before);
            EXPECT_TRUE(mock_thread_pool.jobQ.empty());
        }
        readers.finish();
        ASSERT_TRUE(progress) << readers.error();
        ASSERT_EQ(readers.error(), "");
        EXPECT_EQ(checkSVSCopies(index, reference, initial_labels - 8, initial_labels), "");
        EXPECT_EQ(index->GetBackendIndex()->indexLabelCount(), n_labels);
    }
}

TYPED_TEST(SVSTieredIndexTestBasic, runGCAPI) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t threshold = 1024;
    const size_t n = threshold * 3;
    // svs::data::SimpleData::resize() keeps 1 empty block in case of size reducing, so we need to
    // make sure that we have at least 2 blocks of vectors to be deleted:
    const size_t block_size = threshold / 2;
    SVSParams params = {.type = TypeParam::get_index_type(),
                        .dim = dim,
                        .metric = VecSimMetric_L2,
                        .blockSize = block_size};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, threshold);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Insert three block of vectors directly to SVS.
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, i);
    }

    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);

    for (size_t i = 0; i < threshold; i++) {
        tiered_index->deleteVector(i);
    }
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), threshold);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, threshold);

    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->indexLabelCount(), n - threshold);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n - threshold);
    ASSERT_EQ(tiered_index->GetSVSIndex()->indexStorageSize(), n);
    auto size_before_gc = tiered_index->getAllocationSize();

    auto jobs_before_gc = mock_thread_pool.jobQ.size();
    // Run the GC API call, expect that we will clean up the SVS index.
    VecSimTieredIndex_GC(tiered_index);
    // Expected that a multi-thread GC job was added to the queue.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), jobs_before_gc + mock_thread_pool.thread_pool_size);
    // Run GC twice.
    VecSimTieredIndex_GC(tiered_index);
    // Expected that no new GC jobs were added to the queue (indexGCScheduled is still set until the
    // pending jobs run).
    ASSERT_EQ(mock_thread_pool.jobQ.size(), jobs_before_gc + mock_thread_pool.thread_pool_size);
    // Wait for any pending jobs to complete. As far as SVS GC is done via a job.
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    // Validate sizes after GC.
    ASSERT_EQ(tiered_index->indexSize(), n - threshold);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n - threshold);
    ASSERT_EQ(tiered_index->GetSVSIndex()->indexStorageSize(), n - threshold);
    auto size_after_gc = tiered_index->getAllocationSize();
    // Expect that the size of the index was reduced.
    ASSERT_LT(size_after_gc, size_before_gc);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);

    // The two GC API calls scheduled a single backend GC, which the info accounts for.
    auto svs_info = tiered_index->debugInfo().tieredInfo.specificTieredBackendInfo.svsTieredInfo;
    EXPECT_EQ(svs_info.defragRuns, 1);
    EXPECT_GT(svs_info.defragTimeNs, 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, runGCParallel) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t threshold = 1024;
    const size_t n = threshold * 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Force trigger the first update job for 64 first vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 64);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Run GC on empty index
    VecSimTieredIndex_GC(tiered_index);
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Insert n vectors directly to SVS.
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, i);
    }

    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);

    // Initialize the thread pool to start processing jobs.
    mock_thread_pool.init_threads();

    // Run the mess of add, delete, GC
    for (size_t i = 0; i < threshold; i++) {
        // Run GC for every 64 iterations.
        if (i % 64 == 0) {
            VecSimTieredIndex_GC(tiered_index);
        }
        // Add a new vector
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, n + i);
        // Delete an existing vector
        tiered_index->deleteVector(i + threshold);
    }
    // Final GC after all operations.
    VecSimTieredIndex_GC(tiered_index);
    // Wait for any pending jobs to complete. As far as SVS GC is done via a job.
    mock_thread_pool.thread_pool_join();

    // Validate sizes after GC.
    auto tiered_size = tiered_index->indexSize();
    auto flat_size = tiered_index->GetFlatIndex()->indexSize();
    auto backend_size = tiered_index->GetBackendIndex()->indexSize();
    ASSERT_EQ(tiered_size, n);
    ASSERT_EQ(tiered_size, backend_size + flat_size);
    // Expect that GC cleaned all the deleted vectors.
    ASSERT_EQ(tiered_index->GetSVSIndex()->indexStorageSize(), backend_size);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, runGCInPlaceMode) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 4;
    size_t threshold = 1024;
    const size_t n = threshold * 4;
    SVSParams params = {.type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Force trigger the first update job for 64 first vectors.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 64);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    VecSim_SetWriteMode(VecSim_WriteInPlace);

    // Run GC on empty index
    VecSimTieredIndex_GC(tiered_index);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Insert n vectors directly to SVS.
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index->GetBackendIndex(), vector, i);
    }

    ASSERT_EQ(tiered_index->indexSize(), n);
    ASSERT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);

    // Run the mess of add, delete, GC
    for (size_t i = 0; i < threshold; i++) {
        // Run GC for every 64 iterations.
        if (i % 64 == 0) {
            VecSimTieredIndex_GC(tiered_index);
            ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 0);
            ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
        }
        // Add a new vector
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, n + i);
        // Delete an existing vector
        tiered_index->deleteVector(i + threshold);
    }
    // Final GC after all operations.
    VecSimTieredIndex_GC(tiered_index);

    // Validate if no jobs scheduled to the queue.
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);

    // Validate sizes after GC.
    auto tiered_size = tiered_index->indexSize();
    auto flat_size = tiered_index->GetFlatIndex()->indexSize();
    auto backend_size = tiered_index->GetBackendIndex()->indexSize();
    ASSERT_EQ(tiered_size, n);
    ASSERT_EQ(tiered_size, backend_size + flat_size);
    // Expect that GC cleaned all the deleted vectors.
    ASSERT_EQ(tiered_index->GetSVSIndex()->indexStorageSize(), backend_size);
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);
    EXPECT_EQ(tiered_index->statisticInfo().numberOfMarkedDeleted, 0);

    // One backend GC for every 64th iteration of the loop plus the final one. The GC on the empty
    // index at the start returns early and is not counted.
    auto svs_info = tiered_index->debugInfo().tieredInfo.specificTieredBackendInfo.svsTieredInfo;
    EXPECT_EQ(svs_info.defragRuns, threshold / 64 + 1);
    EXPECT_GT(svs_info.defragTimeNs, 0);
}

TYPED_TEST(SVSTieredIndexTestBasic, switchDeleteModes) {
    // Create TieredSVS index instance with a mock queue.
    size_t dim = 16;
    size_t n = 1000;
    size_t update_threshold = 10;
    SVSParams params = {
        .type = TypeParam::get_index_type(),
        .dim = dim,
        .metric = VecSimMetric_L2,
        .blockSize = 100,
    };
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, update_threshold,
                                                    update_threshold);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    // Create and insert vectors one by one inplace.
    VecSim_SetWriteMode(VecSim_WriteInPlace);
    std::srand(10); // create pseudo random generator with any arbitrary seed.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        VecSimIndex_AddVector(tiered_index, vector, i);
    }
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), n);

    // Update vectors while changing the write mode.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        for (size_t j = 0; j < dim; j++) {
            vector[j] = std::rand() / (TEST_DATA_T)RAND_MAX;
        }
        EXPECT_EQ(tiered_index->addVector(vector, i), 0);
        if (i % 10 == 0) {
            // Change mode every 10 vectors.
            auto next_mode = tiered_index->getWriteMode() == VecSim_WriteInPlace
                                 ? VecSim_WriteAsync
                                 : VecSim_WriteInPlace;
            VecSim_SetWriteMode(next_mode);
        }
    }

    mock_thread_pool.thread_pool_join();
    // Verify that vectors were moved to SVS as expected
    auto sz_f = tiered_index->GetFlatIndex()->indexSize();
    auto sz_b = tiered_index->GetBackendIndex()->indexLabelCount();
    EXPECT_LE(sz_f, update_threshold);
    EXPECT_EQ(sz_f + sz_b, n);
}

TYPED_TEST(SVSTieredIndexTestBasic, testSwapJournalSingle) {
    // Create TieredSVS index instance with a mock queue.
    const size_t dim = 4;
    const size_t n = 15;

    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly set the training threshold to a value that will trigger the update job
    // for first vector only.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, n * 100);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Add n vectors to the index.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        tiered_index->addVector(vector, i);
    }

    // Pause the update job after svs index update
    std::mutex mtx;
    bool added_to_svs = false;
    bool continue_job = false;
    std::condition_variable cv;

    auto tracing_callback = [&]() {
        {
            std::unique_lock lock(mtx);
            added_to_svs = true; // Indicate that we are waiting for the update job to start.
        }
        cv.notify_one(); // Notify that the update job has started.
        {
            std::unique_lock lock(mtx);
            cv.wait(lock, [&] { return continue_job; }); // Wait until we continue.
        }
    };
    tiered_index->registerTracingCallback("UpdateJob::after_add_to_svs", tracing_callback);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    {
        // IMPORTANT!: Do not use ASSERT here, as it will not release the mutex and will cause a
        // deadlock. Use EXPECT instead, so we can continue the test even if the condition is not
        // met.

        // Wait for the update job to start.
        std::unique_lock lock(mtx);
        EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(100), [&] { return added_to_svs; }));

        // update job paused, we have vectors 0-(n-1) in the index, let's do index modifications

        // Remove vector label=n-2, it is copied to backend index.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n - 2), 1);
        // Update vector label=1.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 10), 0);
        // Add a new vector
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n, n), 1);
        // Add another one
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n + 1, n + 1), 1);
        // Remove vector label=0, it is copied to backend index.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 0), 1);
        // Remove the last vector copied to backend index
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n - 1), 1);
        // Update vector label=2.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 2, 20), 0);
        // Remove vector label=2.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 2), 1);
        // Remove vector label=n - in flat only
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n), 1);
        // Add vector (n-1) again
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n - 1, (n - 1) * 10), 1);

        continue_job = true; // Indicate that we can continue the update job.
    }

    // continue the update job
    cv.notify_all(); // Notify that we can continue.

    mock_thread_pool.thread_pool_join();

    // For single-value index, following vectors should be in the index:
    // 0:deleted, 1: 10, 2: deleted, 3:3, ..., n-2:deleted n-1: 10(n-1), n+1: n+1;
    // total: n-2 vectors and labels
    ASSERT_EQ(tiered_index->indexLabelCount(), n - 2);
    // Nothing remains in flat.
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n - 2);

    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), n + 3);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 5);
    // Flat buffer fully drained by the insert jobs.
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), tiered_index->GetBackendIndex()->indexSize() +
                                             tiered_index->GetFlatIndex()->indexSize());

    double abs_err = 1e-2; // Allow a larger relative error for quantization.
    TEST_DATA_T expected_vector[dim];

    // Vector label 0 - deleted
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 0);
    ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(0, expected_vector)));

    // Vector label n-2 - deleted
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 0);
    ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(n - 2, expected_vector)));

    // Vector label=1 was updated to value 10; its latest value is retrievable from the index.
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 10);
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(1, expected_vector), 0, abs_err);

    // Vector label 2 - deleted
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 2);
    ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(2, expected_vector)));

    // Vectors labels [3,n-3] - unchanged
    for (size_t i = 3; i < n - 2; i++) {
        GenerateVector<TEST_DATA_T>(expected_vector, dim, i);
        ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(i, expected_vector), 0, abs_err);
    }

    // Vector label n-1 - deleted and re-added
    GenerateVector<TEST_DATA_T>(expected_vector, dim, (n - 1) * 10);
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(n - 1, expected_vector), 0, abs_err);

    // Vector label n+1 - added
    GenerateVector<TEST_DATA_T>(expected_vector, dim, (n + 1));
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(n + 1, expected_vector), 0, abs_err);
}

TYPED_TEST(SVSTieredIndexTestBasic, testSwapJournalMulti) {
    // Create TieredSVS index instance with a mock queue.
    const size_t dim = 4;
    const size_t n = 15;

    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    // Forcibly set the training threshold to a value that will trigger the update job
    // for first vector only.
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, n * 100);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Add n vectors to the index.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        tiered_index->addVector(vector, i);
    }

    // Pause the update job after svs index update
    std::mutex mtx;
    bool added_to_svs = false;
    bool continue_job = false;
    std::condition_variable cv;

    auto tracing_callback = [&]() {
        {
            std::unique_lock lock(mtx);
            added_to_svs = true; // Indicate that we are waiting for the update job to start.
        }
        cv.notify_one(); // Notify that the update job has started.
        {
            std::unique_lock lock(mtx);
            cv.wait(lock, [&] { return continue_job; }); // Wait until we continue.
        }
    };
    tiered_index->registerTracingCallback("UpdateJob::after_add_to_svs", tracing_callback);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    {
        // IMPORTANT!: Do not use ASSERT here, as it will not release the mutex and will cause a
        // deadlock. Use EXPECT instead, so we can continue the test even if the condition is not
        // met.

        // Wait for the update job to start.
        std::unique_lock lock(mtx);
        EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(100), [&] { return added_to_svs; }));

        // update job paused, we have vectors 0-(n-1) in the index, let's do index modifications

        // Remove vector label=n-2. At the pause it lives only in the backend
        //, so a single vector is removed.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n - 2), 1);
        // Add one more vector label=1.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 10), 1);
        // Add a new vector
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n, n), 1);
        // Add another one
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n + 1, n + 1), 1);
        // Remove vector label=0, only in the backend at the pause.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 0), 1);
        // Remove the last vector, only in the backend at the pause.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n - 1), 1);
        // Add one more vector label=2.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 2, 20), 1);
        // Remove vector label=2: for multi both the original (2) and the new (20) copies sit in
        // the flat buffer at this point, so 2 vectors are removed.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 2), 2);
        // Remove vector label=n - in flat only
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n), 1);
        // Add vector (n-1) again
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n - 1, (n - 1) * 10), 1);

        continue_job = true; // Indicate that we can continue the update job.
    }

    // continue the update job
    cv.notify_all(); // Notify that we can continue.

    mock_thread_pool.thread_pool_join();

    // For multi-value index, following vectors should be in the index:
    // 0: deleted, 1: (1,10), 2: deleted, 3:3, ..., n-2: deleted n-1: 10(n-1), n+1: n+1;
    // total: n-2 labels, n-1 vectors
    ASSERT_EQ(tiered_index->indexLabelCount(), n - 2);
    // In the async-insert design the per-vector insert jobs drain every surviving vector from
    // the flat buffer into the backend, so nothing remains in flat.
    EXPECT_EQ(tiered_index->GetBackendIndex()->indexLabelCount(), n - 2);

    EXPECT_EQ(tiered_index->GetBackendIndex()->indexSize(), n + 3);
    ASSERT_EQ(tiered_index->getNumMarkedDeleted(), 4);
    // Flat buffer fully drained by the insert jobs.
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 0);
    ASSERT_EQ(tiered_index->indexSize(), tiered_index->GetBackendIndex()->indexSize() +
                                             tiered_index->GetFlatIndex()->indexSize());

    double abs_err = 1e-2; // Allow a larger relative error for quantization.
    TEST_DATA_T expected_vector[dim];

    // Vector label 0 - deleted
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 0);
    ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(0, expected_vector)));

    // Vector label n-2 - deleted
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 0);
    ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(n - 2, expected_vector)));

    // Label "1" is multi-valued and keeps both of its vectors (values 1 and 10).
    // The minimal distance for the query 10 should match the second value.
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 10);
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(1, expected_vector), 0, abs_err);
    // And the minimal distance for the query 1.0 should match the first value.
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 1);
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(1, expected_vector), 0, abs_err);

    // Vector label 2 - deleted
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 2);
    ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(2, expected_vector)));

    // Vectors labels [3,n-3] - unchanged
    for (size_t i = 3; i < n - 2; i++) {
        GenerateVector<TEST_DATA_T>(expected_vector, dim, i);
        ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(i, expected_vector), 0, abs_err);
    }

    // Vector label n-1 - deleted and re-added
    GenerateVector<TEST_DATA_T>(expected_vector, dim, (n - 1) * 10);
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(n - 1, expected_vector), 0, abs_err);

    // Vector label n+1 - added
    GenerateVector<TEST_DATA_T>(expected_vector, dim, (n + 1));
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(n + 1, expected_vector), 0, abs_err);
}

TYPED_TEST(SVSTieredIndexTestBasic, testDeletedJournalSingle) {
    // Create TieredSVS index instance with a mock queue.
    const size_t dim = 4;
    const size_t n = 15;

    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n, n * 100);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Add n vectors to the index.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        tiered_index->addVector(vector, i);
    }

    // Pause the update job before svs index update
    std::mutex mtx;
    bool adding_to_svs = false;
    bool continue_job = false;
    std::condition_variable cv;

    auto tracing_callback = [&]() {
        {
            std::unique_lock lock(mtx);
            adding_to_svs = true; // Indicate that we are waiting for the update job to start.
        }
        cv.notify_one(); // Notify that the update job has started.
        {
            std::unique_lock lock(mtx);
            cv.wait(lock, [&] { return continue_job; }); // Wait until we continue.
        }
    };
    tiered_index->registerTracingCallback("UpdateJob::before_add_to_svs", tracing_callback);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    {
        // IMPORTANT!: Do not use ASSERT here, as it will not release the mutex and will cause a
        // deadlock. Use EXPECT instead, so we can continue the test even if the condition is not
        // met.

        // Wait for the update job to start.
        std::unique_lock lock(mtx);
        EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(100), [&] { return adding_to_svs; }));

        // update job paused, we have vectors 0-(n-1) in the index, let's do index modifications

        // Remove vector label=0.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 0), 1);
        // Update vector label=1.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 10), 0);
        // Update vector label=2.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 2, 20), 0);
        // Remove vector label=2.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 2), 1);
        // Remove the last vector
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n - 1), 1);
        // Add a new vector - in flat only
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n, n), 1);
        // Remove vector label=n - in flat only
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n), 1);

        continue_job = true; // Indicate that we can continue the update job.
    }

    // continue the update job
    cv.notify_all(); // Notify that we can continue.

    mock_thread_pool.thread_pool_join();

    // The update job is paused before it transfers vectors to the backend, so at the pause the
    // backend is still empty.
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);

    // Verify that the deleted vectors are not accessible.
    double abs_err = 1e-2; // Allow a larger relative error for quantization.
    TEST_DATA_T expected_vector[dim];
    auto svs_index = tiered_index->GetSVSIndex();
    auto backend_index = tiered_index->GetBackendIndex();
    auto flat_index = tiered_index->GetFlatIndex();

    // labels 0, 2, n-1, n - deleted everywhere
    for (size_t label : {size_t{0}, size_t{2}, n - 1, n}) {
        ASSERT_FALSE(flat_index->isLabelExists(label));
        ASSERT_FALSE(svs_index->isLabelExists(label));
        GenerateVector<TEST_DATA_T>(expected_vector, dim);
        ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(label, expected_vector)));
    }

    // label 1 - updated to 10 during the pause. The insert job drains it from the flat buffer
    // into the backend, so it now lives in the SVS index (not flat) with its updated value.
    ASSERT_FALSE(flat_index->isLabelExists(1));
    ASSERT_TRUE(svs_index->isLabelExists(1));
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 10);
    ASSERT_NEAR(tiered_index->getDistanceFrom_Unsafe(1, expected_vector), 0, abs_err);
}

TYPED_TEST(SVSTieredIndexTestBasic, testDeletedJournalMulti) {
    // Create TieredSVS index instance with a mock queue.
    const size_t dim = 4;
    const size_t n = 15;

    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = true};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();

    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, n, n * 100);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    // Add n vectors to the index.
    for (size_t i = 0; i < n; i++) {
        TEST_DATA_T vector[dim];
        GenerateVector<TEST_DATA_T>(vector, dim, i);
        tiered_index->addVector(vector, i);
    }

    // Pause the update job before svs index update
    std::mutex mtx;
    bool adding_to_svs = false;
    bool continue_job = false;
    std::condition_variable cv;

    auto tracing_callback = [&]() {
        {
            std::unique_lock lock(mtx);
            adding_to_svs = true; // Indicate that we are waiting for the update job to start.
        }
        cv.notify_one(); // Notify that the update job has started.
        {
            std::unique_lock lock(mtx);
            cv.wait(lock, [&] { return continue_job; }); // Wait until we continue.
        }
    };
    tiered_index->registerTracingCallback("UpdateJob::before_add_to_svs", tracing_callback);

    // Launch the BG threads loop that takes jobs from the queue and executes them.
    mock_thread_pool.init_threads();

    {
        // IMPORTANT!: Do not use ASSERT here, as it will not release the mutex and will cause a
        // deadlock. Use EXPECT instead, so we can continue the test even if the condition is not
        // met.

        // Wait for the update job to start.
        std::unique_lock lock(mtx);
        EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(100), [&] { return adding_to_svs; }));

        // update job paused, we have vectors 0-(n-1) in the index, let's do index modifications

        // Remove vector label=0.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 0), 1);
        // Remove the last vector
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n - 1), 1);
        // Update vector label=2.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 2, 20), 1);
        // Remove vector label=2.
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, 2), 2);
        // Update vector label=1.
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, 1, 10), 1);
        // Add a new vector - in flat only
        EXPECT_EQ(GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, n, n), 1);
        // Remove vector label=n - in flat only
        EXPECT_EQ(VecSimIndex_DeleteVector(tiered_index, n), 1);

        continue_job = true; // Indicate that we can continue the update job.
    }

    // continue the update job
    cv.notify_all(); // Notify that we can continue.

    mock_thread_pool.thread_pool_join();

    // The update job is paused before it transfers vectors to the backend, so at the pause the
    // backend is still empty.
    ASSERT_EQ(tiered_index->GetSVSIndex()->getNumMarkedDeleted(), 0);

    // Verify that the deleted vectors are not accessible.
    double abs_err = 1e-2; // Allow a larger relative error for quantization.
    TEST_DATA_T expected_vector[dim];
    auto svs_index = tiered_index->GetSVSIndex();
    auto backend_index = tiered_index->GetBackendIndex();
    auto flat_index = tiered_index->GetFlatIndex();

    // labels 0, 2, n-1, n - deleted everywhere
    for (size_t label : {size_t{0}, size_t{2}, n - 1, n}) {
        ASSERT_FALSE(flat_index->isLabelExists(label));
        ASSERT_FALSE(svs_index->isLabelExists(label));
        GenerateVector<TEST_DATA_T>(expected_vector, dim);
        ASSERT_TRUE(std::isnan(tiered_index->getDistanceFrom_Unsafe(label, expected_vector)));
    }

    // label 1 - multi-value with both vectors (1 and 10). The insert jobs drain the flat buffer
    // into the backend, so both values now live in the SVS index and none remain in flat.
    ASSERT_FALSE(flat_index->isLabelExists(1));
    ASSERT_TRUE(svs_index->isLabelExists(1));
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 1);
    ASSERT_NEAR(backend_index->getDistanceFrom_Unsafe(1, expected_vector), 0, abs_err);
    GenerateVector<TEST_DATA_T>(expected_vector, dim, 10);
    ASSERT_NEAR(backend_index->getDistanceFrom_Unsafe(1, expected_vector), 0, abs_err);
}

// scheduleSVSIndexConsolidate() always registers the label, so a job with an unregistered one
// can only be built by hand. The callback is taken from a real job because the wrapper is
// private.
TYPED_TEST(SVSTieredIndexTestBasic, forgetConsolidateJobIgnoresAnUnregisteredLabel) {
    const size_t dim = 4;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 1, 1);
    ASSERT_INDEX(tiered_index);
    auto allocator = tiered_index->getAllocator();

    mock_thread_pool.init_threads();
    for (size_t i = 0; i < 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    mock_thread_pool.thread_pool_wait();
    mock_thread_pool.thread_pool_join();

    ASSERT_EQ(VecSimIndex_DeleteVector(tiered_index, 1), 1);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    auto *registered_job = mock_thread_pool.jobQ.front().job;
    ASSERT_EQ(registered_job->jobType, SVS_CONSOLIDATE_JOB);

    auto *orphan_job =
        new (allocator) SVSConsolidateJob(allocator, {999}, registered_job->Execute, tiered_index);
    tiered_index->submitSingleJob(orphan_job);
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 2);

    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(0));
    ASSERT_FALSE(tiered_index->GetSVSIndex()->isLabelExists(1));
    ASSERT_EQ(tiered_index->indexLabelCount(), 1);
}

// Every add made while the backend is not ready becomes training data, so the init normally
// deletes the whole buffer and never swaps. Adding straight to the frontend leaves a vector the
// batch does not own, which the init has to swap down and keep.
TYPED_TEST(SVSTieredIndexTestBasic, initSVSIndexKeepsANonTrainingFlatVector) {
    const size_t dim = 4;
    const size_t n = 5;
    const labelType kept_label = 99;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 100, 100);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->ready());

    GenerateAndAddVector<TEST_DATA_T>(tiered_index->GetFlatIndex(), dim, kept_label, kept_label);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), n + 1);

    tiered_index->scheduleSVSIndexInit();
    mock_thread_pool.init_threads();
    mock_thread_pool.thread_pool_join();

    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 1);
    ASSERT_EQ(tiered_index->GetFlatIndex()->getVectorLabel(0), kept_label);
    TEST_DATA_T expected_vector[dim];
    GenerateVector<TEST_DATA_T>(expected_vector, dim, kept_label);
    ASSERT_EQ(tiered_index->GetFlatIndex()->getDistanceFrom_Unsafe(kept_label, expected_vector), 0);
    for (size_t i = 0; i < n; i++) {
        ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(i));
    }
    ASSERT_EQ(tiered_index->indexLabelCount(), n + 1);
}

// ids_to_init_ is only non-empty while the backend is not ready, and insert jobs only exist once
// it is, so the swap bookkeeping of the two together is out of reach of addVector. The hand-made
// job below builds that state. Do not trigger an init after it: initSVSIndex asserts on a ready
// backend.
TYPED_TEST(SVSTieredIndexTestBasic, insertJobFollowsTrainingIdsThroughAFlatSwap) {
    const size_t dim = 4;
    const size_t n = 5;
    const labelType ingested_label = 2;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 100, 100);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < n; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 0);
    ASSERT_FALSE(tiered_index->GetSVSIndex()->ready());

    // Label i sits at flat id i, so the last id gets swapped onto the ingested one.
    tiered_index->submitSingleJob(tiered_index->createInsertJob(ingested_label, ingested_label));
    ASSERT_EQ(mock_thread_pool.jobQ.size(), 1);
    mock_thread_pool.thread_iteration();

    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), n - 1);
    ASSERT_TRUE(tiered_index->GetSVSIndex()->isLabelExists(ingested_label));
    ASSERT_TRUE(tiered_index->labelToInsertJobs.empty());

    TEST_DATA_T expected_vector[dim];
    for (size_t i : {size_t{0}, size_t{1}, size_t{3}, size_t{4}}) {
        GenerateVector<TEST_DATA_T>(expected_vector, dim, i);
        ASSERT_EQ(tiered_index->GetFlatIndex()->getDistanceFrom_Unsafe(i, expected_vector), 0)
            << "label " << i;
    }
    ASSERT_EQ(tiered_index->indexLabelCount(), n);
}

// deleteVector() drops its shared lock before taking the exclusive one, so the label can be gone
// by the time removeLabelFromFlat() runs.
TYPED_TEST(SVSTieredIndexTestBasic, removeLabelFromFlatIgnoresAnAbsentLabel) {
    const size_t dim = 4;
    SVSParams params = {
        .type = TypeParam::get_index_type(), .dim = dim, .metric = VecSimMetric_L2, .multi = false};
    VecSimParams svs_params = CreateParams(params);
    auto mock_thread_pool = tieredIndexMock();
    auto *tiered_index = this->CreateTieredSVSIndex(svs_params, mock_thread_pool, 100, 100);
    ASSERT_INDEX(tiered_index);

    for (size_t i = 0; i < 2; i++) {
        GenerateAndAddVector<TEST_DATA_T>(tiered_index, dim, i, i);
    }

    ASSERT_EQ(tiered_index->removeLabelFromFlat(999), 0);
    ASSERT_EQ(tiered_index->GetFlatIndex()->indexSize(), 2);
    ASSERT_EQ(tiered_index->indexLabelCount(), 2);
}

TEST(SVSTieredIndexTest, testThreadPool) {
    // Test VecSimSVSThreadPool with shared pool
    const size_t num_threads = 4;
    VecSimSVSThreadPool::resize(num_threads);
    auto allocator = VecSimAllocator::newVecsimAllocator();
    VecSimSVSThreadPool pool{allocator};
    ASSERT_EQ(pool.poolSize(), num_threads);
    ASSERT_EQ(pool.size(), 1); // parallelism starts at 1 (calling thread)
    ASSERT_EQ(pool.getParallelism(), 1);

    std::atomic_int counter(0);
    auto task = [&counter](size_t i) { counter += i + 1; };

    // Can use parallel_for immediately with parallelism 1
    pool.parallel_for(task, 1);
    ASSERT_EQ(counter, 1); // 0+1 = 1

    // n > parallelism is allowed — the pool will use as many threads as available.
    // (The old assert was removed to support deferred resize scenarios.)

    counter = 0;
    pool.setParallelism(4);
    pool.parallel_for(task, num_threads);
    ASSERT_EQ(counter, 10); // 1+2+3+4 = 10

    // n < parallelism is valid (SVS may partition into fewer tasks for small problems)
    counter = 0;
    pool.parallel_for(task, 2);
    ASSERT_EQ(counter, 3); // 1+2 = 3

    // setParallelism changes per-index parallelism, not the shared pool
    pool.setParallelism(2);
    ASSERT_EQ(pool.poolSize(), num_threads); // shared pool unchanged
    ASSERT_EQ(pool.size(), 2);               // per-index parallelism
    ASSERT_EQ(pool.getParallelism(), 2);

    counter = 0;
    pool.parallel_for(task, 2);
    ASSERT_EQ(counter, 3); // 1+2 = 3

    // setParallelism boundary checks — asserts in debug mode
#if !defined(RUNNING_ON_VALGRIND) && !defined(NDEBUG)
    ASSERT_DEATH(pool.setParallelism(0), "Parallelism must be at least 1");
    ASSERT_DEATH(pool.setParallelism(10), "Parallelism exceeds shared pool size");
#endif

    // Test write-in-place mode (pool with size 1)
    VecSimSVSThreadPool::resize(1);
    VecSimSVSThreadPool inplace_pool{allocator};
    inplace_pool.setParallelism(1);
    ASSERT_EQ(inplace_pool.size(), 1);
    ASSERT_EQ(inplace_pool.poolSize(), 1);
    counter = 0;
    inplace_pool.parallel_for(task, 1);
    ASSERT_EQ(counter, 1);

    // parallel_for works immediately with default parallelism 1
    VecSimSVSThreadPool::resize(num_threads);
    VecSimSVSThreadPool default_pool{allocator};
    counter = 0;
    default_pool.parallel_for(task, 1);
    ASSERT_EQ(counter, 1); // 0+1 = 1

    // Test exception handling
    auto err_task = [](size_t) { throw std::runtime_error("Test exception"); };
    // n=0 is a no-op (no tasks to run, no error)
    ASSERT_NO_THROW(pool.parallel_for(err_task, 0));
    pool.setParallelism(num_threads);
    ASSERT_THROW(pool.parallel_for(err_task, 1), svs::threads::ThreadingException);
    ASSERT_THROW(pool.parallel_for(err_task, num_threads), svs::threads::ThreadingException);

    // Restore pool to default size so we don't leak state to other tests.
    VecSimSVSThreadPool::resize(1);
}

#else // HAVE_SVS

VecSimIndex *CreateTieredSVSIndex(VecSimParams &svs_params) {
    TieredIndexParams tiered_params{.primaryIndexParams = &svs_params};
    auto *tiered_index = TieredFactory::NewIndex(&tiered_params);
    return tiered_index;
}

TEST(SVSTieredIndexTest, svs_not_supported) {
    SVSParams params = {
        .type = VecSimType_FLOAT32,
        .dim = 16,
        .metric = VecSimMetric_IP,
    };
    auto svs_params = CreateParams(params);

    TieredIndexParams tiered_params{.primaryIndexParams = &svs_params};
    auto index_params = CreateParams(tiered_params);
    auto index = VecSimIndex_New(&index_params);

    ASSERT_EQ(index, nullptr);

    // Although nothing is actually been allocated we calculate a brute force index size to align
    // with the logic of the tiered index function, which currently doesn’t have a verification of
    // the backend index algorithm. This to be changed once a proper verification is introduced.
    auto bf_params = TieredFactory::TieredSVSFactory::NewBFParams(&tiered_params);
    auto expected_size = BruteForceFactory::EstimateInitialSize(&bf_params, false);
    auto size = VecSimIndex_EstimateInitialSize(&index_params);
    ASSERT_EQ(size, expected_size);

    auto size2 = VecSimIndex_EstimateElementSize(&index_params);
    ASSERT_EQ(size2, -1);
}

#endif
