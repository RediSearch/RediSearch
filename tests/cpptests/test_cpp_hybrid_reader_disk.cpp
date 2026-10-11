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

#include "iterators/vector_top_k.h"
#include "iterators_ffi.h"
#include "redisearch.h"
#include "util/timeout.h"

#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <vector>

// vecsimTimeoutCallback is a global function pointer in vector_top_k.c, deliberately kept
// non-static so tests can swap it to simulate timeouts.
extern "C" {
extern int (*vecsimTimeoutCallback)(QueryRequestTimeout *timeout);
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
// Only newAdhocBfCtx, indexSize and basicInfo need real implementations; all other methods
// are stubs.
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

    // isDisk routes the query onto the disk adhoc-BF path; type and dim must describe the
    // query blob the tests pass, which the iterator validates against this metadata.
    VecSimIndexBasicInfo basicInfo() const override {
        VecSimIndexBasicInfo info = {};
        info.algo = VecSimAlgo_HNSWLIB;
        info.metric = VecSimMetric_L2;
        info.type = VecSimType_FLOAT32;
        info.isDisk = true;
        info.dim = 4;
        return info;
    }

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
    // Observer only; `iter` owns the child and frees it.
    MockIterator *child;
    TestHybrid(MockDiskVecSimIndex *idx, QueryIterator *it, MockIterator *ch)
        : index(idx), iter(it), child(ch) {}
    TestHybrid(TestHybrid &&o) noexcept : index(o.index), iter(o.iter), child(o.child) {
        o.index = nullptr;
        o.iter = nullptr;
        o.child = nullptr;
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
    std::array<float, 4> queryVec = {1.0f, 2.0f, 3.0f, 4.0f};
protected:
    std::unique_ptr<MockQueryEvalCtx> mockCtx;
    void SetUp() override { mockCtx = std::make_unique<MockQueryEvalCtx>(100, 10); }

    // Creates a vector top-k iterator forced into ADHOC_BF / disk mode.
    TestHybrid makeIterator(std::map<labelType, double> sq8,
                            std::map<labelType, double> exact,
                            std::vector<t_docId> docIds,
                            size_t k,
                            t_fieldIndex filterFieldIndex = RS_INVALID_FIELD_INDEX,
                            bool rerank = false) {
        auto alloc = VecSimAllocator::newVecsimAllocator();
        auto *index = new (alloc) MockDiskVecSimIndex(alloc, std::move(sq8), std::move(exact));

        auto child = new MockIterator(std::move(docIds));

        VecSimQueryParams qParams = {};
        qParams.searchMode = HYBRID_ADHOC_BF;
        qParams.hnswDiskRuntimeParams.shouldRerank = rerank ? VecSimBool_TRUE : VecSimBool_UNSET;

        FieldMaskOrIndex fmi = {.index_tag = FieldMaskOrIndex_Index,
                                .index = filterFieldIndex};
        FieldFilterContext filterCtx = {.field = fmi,
                                        .predicate = FIELD_EXPIRATION_PREDICATE_DEFAULT};

        QueryIterator *iter = NewVectorTopKIterator(
            (VecSimIndex *)index, queryVec.data(), sizeof(queryVec), &qParams, k,
            /*can_trim_deep_results*/ true, &child->base, mockCtx->sctx.timeout,
            &mockCtx->sctx, &filterCtx);
        return {index, iter, child};
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
        return makeIterator(std::move(sq8), std::move(exact), std::move(docIds), k,
                            RS_INVALID_FIELD_INDEX, /*rerank*/ true);
    }

    // The distance the iterator ranked and reported the current result on.
    static double scoreOf(const QueryIterator *it) {
        EXPECT_EQ(it->current->data.tag, RSResultData_Metric);
        return it->current->data.metric;
    }
};

// ============================================================================
// Tests
// ============================================================================

// Basic top-k: verify that the k results with the lowest distances are returned in score order.
TEST_F(HybridReaderDiskTest, BasicTopK) {
    std::map<labelType, double> sq8 = {{1, 0.5}, {2, 0.1}, {3, 0.8}};
    auto [index, it, child] = makeNormalIterator(sq8, {1, 2, 3}, 2);

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
    auto [index, it, child] = makeNormalIterator(sq8, {1, 2, 3}, 3);

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
    auto [index, it, child] = makeRerankingIterator(sq8, exact, {1, 2}, 2);

    ASSERT_NE(it, nullptr);

    // After reranking with exact distances, doc 1 (0.1) should come before doc 2 (0.7).
    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)1);

    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)2);

    ASSERT_EQ(it->Read(it), ITERATOR_EOF);
}

// A doc deleted between the scan and the rerank has no exact distance (NaN), and keeps the
// approximate score it was ranked on rather than being scored from an unwritten buffer slot.
TEST_F(HybridReaderDiskTest, RerankingKeepsScoreWithoutExactDistance) {
    std::map<labelType, double> sq8 = {{1, 0.9}, {2, 0.8}};
    std::map<labelType, double> exact = {{1, 0.1}};
    auto [index, it, child] = makeRerankingIterator(sq8, exact, {1, 2}, 2);

    ASSERT_NE(it, nullptr);

    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)1);
    EXPECT_EQ(scoreOf(it), 0.1);

    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)2);
    EXPECT_EQ(scoreOf(it), 0.8);

    ASSERT_EQ(it->Read(it), ITERATOR_EOF);
}

// Reranking is opt-in: with shouldRerank unset the exact distances are never fetched, so the
// SQ8 ranking stands.
TEST_F(HybridReaderDiskTest, ExactDistancesIgnoredWithoutRerank) {
    std::map<labelType, double> sq8 = {{1, 0.9}, {2, 0.8}};
    std::map<labelType, double> exact = {{1, 0.1}, {2, 0.7}};
    auto [index, it, child] = makeIterator(sq8, exact, {1, 2}, 2);

    ASSERT_NE(it, nullptr);

    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)2);

    ASSERT_EQ(it->Read(it), ITERATOR_OK);
    EXPECT_EQ(it->lastDocId, (t_docId)1);

    ASSERT_EQ(it->Read(it), ITERATOR_EOF);
}

// Timeout: when the timeout callback fires, the adhoc scan aborts and Read returns
// ITERATOR_TIMEOUT.
TEST_F(HybridReaderDiskTest, TimeoutReturnsTimedOut) {
    std::map<labelType, double> sq8 = {{1, 0.5}, {2, 0.1}};
    auto [index, it, child] = makeNormalIterator(sq8, {1, 2}, 2);

    ASSERT_NE(it, nullptr);

    // Swap the global timeout callback to simulate a timeout on every check.
    auto *saved = vecsimTimeoutCallback;
    vecsimTimeoutCallback = [](QueryRequestTimeout *) -> int { return 1; };

    EXPECT_EQ(it->Read(it), ITERATOR_TIMEOUT);

    vecsimTimeoutCallback = saved;
}

// The hybrid iterator gives up on an aborted child and on a timed-out one alike, but it has to say
// which: both free the tree, and only a timeout tells the caller the result set is partial. Folding
// the timeout into VALIDATE_ABORTED ends the query as if the index were exhausted.
TEST_F(HybridReaderDiskTest, RevalidateReportsChildTimeoutApartFromAbort) {
    // A fresh iterator per case: VALIDATE_TIMEOUT and VALIDATE_ABORTED both mean the iterator is
    // finished and must be freed, so revalidating the same one again would exercise a sequence the
    // API forbids.
    const std::pair<ValidateStatus, const char *> cases[] = {
        {VALIDATE_TIMEOUT, "a timed-out child must stay a timeout, not degrade to an abort"},
        {VALIDATE_ABORTED, "an aborted child must stay an abort"},
        {VALIDATE_OK, "a child that is still valid leaves the hybrid iterator usable"},
    };

    for (const auto &[childStatus, why] : cases) {
        auto h = makeNormalIterator({{1, 0.5}}, {1}, 1);
        ASSERT_NE(h.iter, nullptr);
        h.child->SetRevalidateResult(childStatus);

        EXPECT_EQ(h.iter->Revalidate(h.iter, &mockCtx->spec), childStatus) << why;
    }
}

// Pins the CURRENT, unresolved hybrid KNN behavior (see expiration-semantics.md):
// expired fields are dropped at yield with no refill, so a live candidate just below
// the top-k is lost and the query under-fills k. Flip the expectation if refill is adopted.
TEST_F(HybridReaderDiskTest, PinsUnderfillKWhenFieldsExpired) {
    const t_expirationTimePoint past = {1, 0};

    // Expire field 0 of docs 1 and 2; this also populates spec.docs.ttl, the third gate condition.
    mockCtx->TTL_Add(1, (t_fieldIndex)0, past);
    mockCtx->TTL_Add(2, (t_fieldIndex)0, past);
    mockCtx->sctx.currentTime = {2, 0};

    // k=3 heap holds docs 2,1,4; live doc 3 (0.8) ranks just below it. Gate on via field 0.
    auto [index, it, child] =
        makeIterator({{1, 0.5}, {2, 0.1}, {3, 0.8}, {4, 0.6}}, {}, {1, 2, 3, 4}, /*k*/ 3, /*field*/ 0);
    ASSERT_NE(it, nullptr);

    std::vector<t_docId> yielded;
    while (it->Read(it) == ITERATOR_OK) {
        yielded.push_back(it->lastDocId);
    }

    // Doc 3 is not pulled in to replace the expired docs: only in-heap doc 4 survives.
    ASSERT_EQ(yielded.size(), 1u);
    EXPECT_EQ(yielded[0], (t_docId)4);
}

// Contrast: with no TTL entries the expiration gate is off (ttl == NULL), so the
// same three candidates all surface in score order. Proves the under-fill above is
// caused by expiration, not by the mock setup.
TEST_F(HybridReaderDiskTest, FillsKWhenNoExpiry) {
    auto [index, it, child] =
        makeIterator({{1, 0.5}, {2, 0.1}, {3, 0.8}}, {}, {1, 2, 3}, /*k*/ 3, /*field*/ 0);
    ASSERT_NE(it, nullptr);

    // Lowest distance first: doc 2 (0.1), doc 1 (0.5), doc 3 (0.8).
    std::vector<t_docId> yielded;
    while (it->Read(it) == ITERATOR_OK) {
        yielded.push_back(it->lastDocId);
    }

    ASSERT_EQ(yielded.size(), 3u);
    EXPECT_EQ(yielded[0], (t_docId)2);
    EXPECT_EQ(yielded[1], (t_docId)1);
    EXPECT_EQ(yielded[2], (t_docId)3);
}
