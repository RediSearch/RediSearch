/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Include C++ VecSim headers before any C headers to get full class definitions.
#include "VectorSimilarity/src/VecSim/vec_sim_interface.h"
#include "VectorSimilarity/src/VecSim/vec_sim_adhoc_bf_ctx.h"
#include "VectorSimilarity/src/VecSim/memory/vecsim_malloc.h"

#include "gtest/gtest.h"
#include "index_utils.h"
#include "iterator_util.h"

#include "iterators/hybrid_reader.h"
#include "redisearch.h"
#include "util/timeout.h"
#include "types_rs.h"
#include "rlookup_rs.h"

#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <vector>

// vecsimTimeoutCallback is a global function pointer in hybrid_reader.c, deliberately kept
// non-static so tests can swap it to simulate timeouts.
extern "C" {
extern int (*vecsimTimeoutCallback)(TimeoutCtx *ctx);
}

// operator delete reads obj->allocator after destruction; keep the allocator's shared_ptr alive across delete.
static void freeVecSimObject(VecSimIndexInterface *obj) {
    [[maybe_unused]] auto alloc = obj->getAllocator();
    delete obj;
}


// ============================================================================
// Mocks
// ============================================================================

// Controllable VecSimAdhocBfCtx: returns distances from a pre-loaded map.
// sq8Distances simulates the fast SQ8 approximation pass.
// exactDistances simulates the FP32 reranking pass (getExactDistances).
struct MockAdhocBfCtx : public VecSimAdhocBfCtx {
    std::map<labelType, double> sq8Distances;
    std::map<labelType, double> exactDistances;

    MockAdhocBfCtx(std::shared_ptr<VecSimAllocator> alloc,
                   std::map<labelType, double> sq8,
                   std::map<labelType, double> exact)
        : VecSimAdhocBfCtx(std::move(alloc)),
          sq8Distances(std::move(sq8)),
          exactDistances(std::move(exact)) {}

    double getDistanceFrom(labelType label) const override {
        auto it = sq8Distances.find(label);
        return it != sq8Distances.end() ? it->second
                                        : std::numeric_limits<double>::quiet_NaN();
    }

    void getExactDistances(const labelType *labels, double *out, size_t count) const override {
        for (size_t i = 0; i < count; ++i) {
            auto it = exactDistances.find(labels[i]);
            out[i] = it != exactDistances.end() ? it->second
                                                 : std::numeric_limits<double>::quiet_NaN();
        }
    }
};

// Minimal VecSimIndexInterface implementation for the disk path.
// Only newAdhocBfCtx and indexSize need real implementations; all other methods are stubs.
struct MockDiskVecSimIndex : public VecSimIndexInterface {
    std::map<labelType, double> sq8Distances;
    std::map<labelType, double> exactDistances;

    MockDiskVecSimIndex(std::shared_ptr<VecSimAllocator> alloc,
                        std::map<labelType, double> sq8,
                        std::map<labelType, double> exact)
        : VecSimIndexInterface(std::move(alloc)),
          sq8Distances(std::move(sq8)),
          exactDistances(std::move(exact)) {}

    VecSimAdhocBfCtx *newAdhocBfCtx(const void *) const override {
        auto alloc = VecSimAllocator::newVecsimAllocator();
        return new (alloc) MockAdhocBfCtx(alloc, sq8Distances, exactDistances);
    }

    size_t indexSize() const override { return sq8Distances.size(); }

    // ---- Stubs for pure virtual methods not exercised by these tests ----
    int addVector(const void *, labelType) override { return 0; }
    int deleteVector(labelType) override { return 0; }
    double getDistanceFrom_Unsafe(labelType, const void *) const override { return 0.0; }
    size_t indexCapacity() const override { return 0; }
    size_t indexLabelCount() const override { return 0; }
    VecSimQueryReply *topKQuery(const void *, size_t, VecSimQueryParams *) const override {
        return nullptr;
    }
    VecSimQueryReply *rangeQuery(const void *, double, VecSimQueryParams *,
                                 VecSimQueryReply_Order) const override {
        return nullptr;
    }
    VecSimIndexDebugInfo debugInfo() const override { return VecSimIndexDebugInfo{}; }
    VecSimIndexBasicInfo basicInfo() const override { return VecSimIndexBasicInfo{}; }
    VecSimIndexStatsInfo statisticInfo() const override { return VecSimIndexStatsInfo{}; }
    VecSimDebugInfoIterator *debugInfoIterator() const override { return nullptr; }
    VecSimBatchIterator *newBatchIterator(const void *, VecSimQueryParams *) const override {
        return nullptr;
    }
    bool preferAdHocSearch(size_t, size_t, bool) const override { return true; }
    void setLastSearchMode(VecSearchMode) override {}
    void runGC() override {}
    void acquireSharedLocks() override {}
    void releaseSharedLocks() override {}
};

struct TestHybrid {
    MockDiskVecSimIndex *index;
    QueryIterator *iter;
    TestHybrid(MockDiskVecSimIndex *idx, QueryIterator *it) : index(idx), iter(it) {}
    TestHybrid(TestHybrid &&o) noexcept : index(o.index), iter(o.iter) {
        o.index = nullptr;
        o.iter = nullptr;
    }
    TestHybrid(const TestHybrid &) = delete;
    TestHybrid &operator=(const TestHybrid &) = delete;
    ~TestHybrid() {
        if (iter) iter->Free(iter);
        if (index) freeVecSimObject(index);
    }
};

// ============================================================================
// Test fixture
// ============================================================================

class HybridReaderDiskTest : public ::testing::Test {
    std::unique_ptr<MockQueryEvalCtx> mockCtx;
    std::array<float, 4> queryVec = {1.0f, 2.0f, 3.0f, 4.0f};
    // Stable address used as ownKey sentinel. MetricsVec_UpdateValue compares by
    // pointer identity only and never reads the fields, so zero-init is fine.
    RLookupKey scoreKey = {};
protected:
    void SetUp() override {
        mockCtx = std::make_unique<MockQueryEvalCtx>(100, 10);
        // Sentinel to route the hybrid reader into the disk code path. Safe because:
        //  - hybrid_reader.c only checks diskSpec for nullness, never dereferences it.
        //  - All disk I/O flows through hr->index (MockDiskVecSimIndex), not diskSpec.
        // A real instance is not constructible in unit tests: RedisSearchDiskIndexSpec
        // is an opaque type only the disk backend can produce.
        mockCtx->spec.diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(uintptr_t{1});
    }

    // Creates a HybridIterator forced into ADHOC_BF / disk mode.
    TestHybrid makeIterator(std::map<labelType, double> sq8,
                            std::map<labelType, double> exact,
                            std::vector<t_docId> docIds,
                            size_t k) {
        auto alloc = VecSimAllocator::newVecsimAllocator();
        auto *index = new (alloc) MockDiskVecSimIndex(alloc, std::move(sq8), std::move(exact));

        auto child = new MockIterator(std::move(docIds));

        KNNVectorQuery top_k = {.vector = queryVec.data(), .vecLen = 4, .k = k, .order = BY_SCORE};

        VecSimQueryParams qParams = {};
        qParams.searchMode = HYBRID_ADHOC_BF;

        FieldMaskOrIndex fmi = {.index_tag = FieldMaskOrIndex_Index,
                                .index = RS_INVALID_FIELD_INDEX};
        FieldFilterContext filterCtx = {.field = fmi,
                                        .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};

        HybridIteratorParams hParams = {
            .sctx = &mockCtx->sctx,
            .index = (VecSimIndex *)index,
            .dim = 4,
            .elementType = VecSimType_FLOAT32,
            .spaceMetric = VecSimMetric_L2,
            .query = top_k,
            .qParams = qParams,
            .vectorScoreField = (char *)"__v_score",
            .canTrimDeepResults = true,
            .childIt = &child->base,
            .filterCtx = &filterCtx,
        };

        QueryError err = QueryError_Default();
        QueryIterator *iter = NewHybridVectorIterator(hParams, &err);
        EXPECT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
        return {index, iter};
    }

    TestHybrid makeNormalIterator(std::map<labelType, double> sq8,
                                  std::vector<t_docId> docIds,
                                  size_t k) {
        return makeIterator(std::move(sq8), {}, std::move(docIds), k);
    }

    TestHybrid makeRerankingIterator(std::map<labelType, double> sq8,
                                     std::map<labelType, double> exact,
                                     std::vector<t_docId> docIds,
                                     size_t k) {
        auto h = makeIterator(std::move(sq8), std::move(exact), std::move(docIds), k);
        auto hr = (HybridIterator *)h.iter;
        // Enable reranking before the first Read() triggers prepareResults().
        hr->runtimeParams.hnswDiskRuntimeParams.shouldRerank = VecSimBool_TRUE;
        // Provide a non-null ownKey so MetricsVec_UpdateValue can find and update
        // the score entry. In production this is set by the metrics loader results
        // processor; in tests we supply a stable fixture-member address instead.
        hr->ownKey = &scoreKey;
        return h;
    }

};

// ============================================================================
// Tests
// ============================================================================

// Basic top-k: verify that the k results with the lowest distances are returned in score order.
TEST_F(HybridReaderDiskTest, BasicTopK) {
    std::map<labelType, double> sq8 = {{1, 0.5}, {2, 0.1}, {3, 0.8}};
    auto [index, it] = makeNormalIterator(sq8, {1, 2, 3}, 2);

    ASSERT_NE(it, nullptr);

    // First result: lowest distance = doc 2 (0.1).
    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)2);

    // Second result: next lowest = doc 1 (0.5).
    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)1);

    // Doc 3 (0.8) is outside top-2 and should not appear.
    ASSERT_EQ(it->Read(it), ITERATOR_EOF);
}

// NaN filtering: labels whose distance is NaN must be excluded from results.
TEST_F(HybridReaderDiskTest, NaNFiltering) {
    // Doc 2 has no entry in sq8Distances → getDistanceFrom returns NaN → skipped.
    std::map<labelType, double> sq8 = {{1, 0.5}, {3, 0.8}};
    auto [index, it] = makeNormalIterator(sq8, {1, 2, 3}, 3);

    ASSERT_NE(it, nullptr);

    size_t count = 0;
    while (it->Read(it) == ITERATOR_OK) {
        EXPECT_NE(it->lastDocId, (t_docId)2) << "doc 2 should have been filtered (NaN distance)";
        ++count;
    }
    EXPECT_EQ(count, 2u);
}

// Reranking: when shouldRerank is enabled, getExactDistances results replace SQ8 distances.
TEST_F(HybridReaderDiskTest, RerankingUpdatesScores) {
    // SQ8 approximation makes doc 2 look better than doc 1.
    std::map<labelType, double> sq8 = {{1, 0.9}, {2, 0.8}};
    // Exact FP32 distances reverse the ranking.
    std::map<labelType, double> exact = {{1, 0.1}, {2, 0.7}};
    auto [index, it] = makeRerankingIterator(sq8, exact, {1, 2}, 2);

    ASSERT_NE(it, nullptr);

    // After reranking with exact distances, doc 1 (0.1) should come before doc 2 (0.7).
    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)1);

    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)2);

    ASSERT_EQ(it->Read(it), ITERATOR_EOF);
}

// Timeout: when the timeout callback fires, prepareResults returns TimedOut and Read returns
// ITERATOR_TIMEOUT.
TEST_F(HybridReaderDiskTest, TimeoutReturnsTimedOut) {
    std::map<labelType, double> sq8 = {{1, 0.5}, {2, 0.1}};
    auto [index, it] = makeNormalIterator(sq8, {1, 2}, 2);

    ASSERT_NE(it, nullptr);

    // Swap the global timeout callback to simulate a timeout on every check.
    auto *saved = vecsimTimeoutCallback;
    vecsimTimeoutCallback = [](TimeoutCtx *) -> int { return 1; };

    EXPECT_EQ(it->Read(it), ITERATOR_TIMEOUT);

    vecsimTimeoutCallback = saved;
}
