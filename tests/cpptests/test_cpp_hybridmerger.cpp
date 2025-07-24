/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "result_processor.h"
#include "query.h"
#include "gtest/gtest.h"
#include "config.h"
#include "hybrid/hybrid_scoring.h"
#include <vector>
#include <string>
#include <set>

struct processor1Ctx : public ResultProcessor {
  processor1Ctx() {
    memset(static_cast<ResultProcessor *>(this), 0, sizeof(ResultProcessor));
    counter = 0;
  }
  int counter;
  RLookupKey *kout = NULL;
};

static int numFreed = 0;

static void resultProcessor_GenericFree(ResultProcessor *rp) {
  numFreed++;
  delete static_cast<processor1Ctx *>(rp);
}

// Helper structures and functions to reduce code duplication

// Simple mock upstream with minimal constructor
struct MockUpstream : public ResultProcessor {
  int timeoutAfterCount = 0;
  int errorAfterCount = -1;  // -1 means no error, >= 0 means return error after this many calls
  std::vector<double> scores;
  std::vector<int> docIds;
  int depletionCount = 0;
  int counter = 0;
  std::vector<RSDocumentMetadata> documentMetadata;
  std::vector<std::string> keyStrings;

  // Simplified constructor with just the essentials
  MockUpstream(int timeoutAfterCount = 0,
               const std::vector<double>& Scores = {},
               const std::vector<int>& DocIds = {},
               int depletionCount = 0,
               int errorAfterCount = -1)
    : timeoutAfterCount(timeoutAfterCount), errorAfterCount(errorAfterCount), scores(Scores), docIds(DocIds), depletionCount(depletionCount) {

    this->Next = NextFn;
    documentMetadata.reserve(50);
    keyStrings.reserve(50);

    // If no custom docIds provided, generate sequential ones
    if (docIds.empty() && !scores.empty()) {
      docIds.resize(scores.size());
      for (size_t i = 0; i < scores.size(); i++) {
        docIds[i] = static_cast<int>(i + 1);
      }
    }

    // Pre-create key strings and document metadata for all potential documents
    // We need to account for depletion calls + actual documents
    size_t maxEntries = depletionCount + scores.size();
    if (maxEntries > 0) {
      documentMetadata.resize(maxEntries);
      keyStrings.resize(maxEntries);

      // Pre-create key strings for actual documents (not depletion entries)
      for (size_t i = 0; i < docIds.size(); i++) {
        size_t entryIndex = depletionCount + i;
        keyStrings[entryIndex] = "doc" + std::to_string(docIds[i]);
        documentMetadata[entryIndex].keyPtr = const_cast<char*>(keyStrings[entryIndex].c_str());
      }
    }
  }

  static int NextFn(ResultProcessor *rp, SearchResult *res) {
    MockUpstream *p = static_cast<MockUpstream *>(rp);

    // Handle error (highest precedence)
    if (p->errorAfterCount >= 0 && p->counter >= p->errorAfterCount) {
      return RS_RESULT_ERROR;
    }

    // Handle timeout
    if (p->timeoutAfterCount > 0 && p->counter >= p->timeoutAfterCount) {
      return RS_RESULT_TIMEDOUT;
    }

    // Handle empty upstream (no scores provided)
    if (p->scores.empty()) {
      return RS_RESULT_EOF;
    }

    // Handle depletion
    if (p->counter < p->depletionCount) {
      p->counter++;
      return RS_RESULT_DEPLETING;
    }

    // Handle normal document generation
    int docIndex = p->counter - p->depletionCount;
    if (docIndex >= static_cast<int>(p->scores.size())) {
      return RS_RESULT_EOF;
    }

    // Use docId from array
    res->docId = p->docIds[docIndex];

    // Use score from array
    res->score = p->scores[docIndex];

    // Use pre-created document metadata
    res->dmd = &p->documentMetadata[p->counter];

    p->counter++;
    return RS_RESULT_OK;
  }
};

// Helper function to create hybrid merger with linear scoring
ResultProcessor* CreateLinearHybridMerger(ResultProcessor **upstreams, size_t numUpstreams, double *weights) {
  // Allocate contexts on heap to avoid stack memory issues
  HybridScoringContext *hybridScoringCtx = (HybridScoringContext*)rm_calloc(1, sizeof(HybridScoringContext));

  // Allocate weights on heap and copy values
  double *heapWeights = (double*)rm_calloc(numUpstreams, sizeof(double));
  for (size_t i = 0; i < numUpstreams; i++) {
    heapWeights[i] = weights[i];
  }

  hybridScoringCtx->scoringType = HYBRID_SCORING_LINEAR;
  hybridScoringCtx->linearCtx.linearWeights = heapWeights;
  hybridScoringCtx->linearCtx.numWeights = numUpstreams;

  return RPHybridMerger_New(hybridScoringCtx, upstreams, numUpstreams, NULL);
}

// Helper function to create hybrid merger with RRF scoring
ResultProcessor* CreateRRFHybridMerger(ResultProcessor **upstreams, size_t numUpstreams, size_t k, size_t window) {
  // Allocate contexts on heap to avoid stack memory issues
  HybridScoringContext *hybridScoringCtx = (HybridScoringContext*)rm_calloc(1, sizeof(HybridScoringContext));

  hybridScoringCtx->scoringType = HYBRID_SCORING_RRF;
  hybridScoringCtx->rrfCtx.k = k;
  hybridScoringCtx->rrfCtx.window = window;

  return RPHybridMerger_New(hybridScoringCtx, upstreams, numUpstreams, NULL);
}



class HybridMergerTest : public ::testing::Test {};

/*
 * Test that hybrid merger correctly merges and scores results from two upstreams with the same documents (full intersection)
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: Full intersection (same documents from both upstreams)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Each document gets combined score from both upstreams using linear weights (0.3*2.0 + 0.7*4.0 = 3.4)
 */
TEST_F(HybridMergerTest, testHybridMergerSameDocs) {
  QueryIterator qitr = {0};

  // Create upstreams with same documents (full intersection)
  MockUpstream upstream1(0, {2.0, 2.0, 2.0}, {1, 2, 3});
  MockUpstream upstream2(0, {4.0, 4.0, 4.0}, {1, 2, 3}); // Same docIds

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.3, 0.7};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify hybrid score is applied (should be 3.4 = 0.3*2.0 + 0.7*4.0)
    EXPECT_NEAR(3.4, r.score, 0.0001);

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(3, count); // Should have processed 3 unique documents (full intersection)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger correctly merges and scores results from two upstreams with different documents (no intersection)
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Each document gets weighted score from only its contributing upstream (0.4*1.0=0.4 or 0.6*3.0=1.8)
 */
TEST_F(HybridMergerTest, testHybridMergerDifferentDocuments) {
  QueryIterator qitr = {0};

  // Create upstreams with different documents (no intersection)
  MockUpstream upstream1(0, {1.0, 1.0, 1.0}, {1, 2, 3});
  MockUpstream upstream2(0, {3.0, 3.0, 3.0}, {11, 12, 13}); // Different docIds

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.4, 0.6};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify scores: docs 1-3 (only upstream1) should have score 0.4*1.0=0.4, docs 11-13 (only upstream2) should have score 0.6*3.0=1.8
    if (r.docId <= 3) {
      EXPECT_NEAR(0.4, r.score, 0.0001);  // 0.4 * 1.0 (only upstream1 contributes)
    } else {
      EXPECT_NEAR(1.8, r.score, 0.0001);  // 0.6 * 3.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(6, count); // Should have 6 documents total (3 from each upstream)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with first upstream empty
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: N/A (one upstream empty)
 * Emptiness: First upstream empty, second upstream has documents
 * Timeout: No timeout
 * Expected behavior: Only documents from second upstream with weighted score (0.5*5.0=2.5)
 */
TEST_F(HybridMergerTest, testHybridMergerEmptyUpstream1) {
  QueryIterator qitr = {0};

  // Create upstreams: first empty, second with documents
  MockUpstream upstream1(0, {}, {}); // Empty scores and docIds
  MockUpstream upstream2(0, {5.0, 5.0, 5.0}, {1, 2, 3});

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Should only get results from upstream2 with score 0.5*5.0=2.5 (only upstream2 contributes)
    EXPECT_EQ(2.5, r.score);

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(3, count); // Should have 3 documents (only from upstream2)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with second upstream empty
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: N/A (one upstream empty)
 * Emptiness: First upstream has documents, second upstream empty
 * Timeout: No timeout
 * Expected behavior: Only documents from first upstream with weighted score (0.5*7.0=3.5)
 */
TEST_F(HybridMergerTest, testHybridMergerEmptyUpstream2) {
  QueryIterator qitr = {0};

  // Create upstreams: first with documents, second empty
  MockUpstream upstream1(0, {7.0, 7.0, 7.0}, {1, 2, 3});
  MockUpstream upstream2(0, {}, {}); // Empty scores and docIds

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Should only get results from upstream1 with score 0.5*7.0=3.5 (only upstream1 contributes)
    EXPECT_EQ(3.5, r.score);

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(3, count); // Should have 3 documents (only from upstream1)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with both upstreams empty
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: N/A (both upstreams empty)
 * Emptiness: Both upstreams empty
 * Timeout: No timeout
 * Expected behavior: No documents returned
 */
TEST_F(HybridMergerTest, testHybridMergerBothEmpty) {
  QueryIterator qitr = {0};

  // Create both upstreams empty
  MockUpstream upstream1(0, {}, {}); // Empty scores and docIds
  MockUpstream upstream2(0, {}, {}); // Empty scores and docIds
  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;
    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(0, count); // Should have 0 documents (both upstreams empty)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with RRF scoring and small window size
 *
 * Scoring function: RRF (Reciprocal Rank Fusion)
 * Number of upstreams: 2
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Window size limits results to 2 docs per upstream (4 total), each with RRF score
 */
TEST_F(HybridMergerTest, testRRFScoringSmallWindow) {
  QueryIterator qitr = {0};

  // Create RRF upstreams with custom score arrays (already sorted descending for ranking)
  std::vector<double> scores1 = {0.9, 0.5, 0.1, 0.05, 0.01};
  std::vector<int> docIds1 = {1, 2, 3, 4, 5};
  MockUpstream upstream1(0, scores1, docIds1);

  std::vector<double> scores2 = {0.8, 0.4, 0.2, 0.06, 0.02};
  std::vector<int> docIds2 = {11, 12, 13, 14, 15};
  MockUpstream upstream2(0, scores2, docIds2);

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with RRF scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  ResultProcessor *hybridMerger = CreateRRFHybridMerger(upstreams, 2, 60, 2); // k=60, window=2

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify RRF scores - each document gets score based on its rank
    // With window=2, only top 2 from each upstream are considered
    // Expected RRF scores (k=60):
    // doc1: 1/(60+1) = 1/61 ≈ 0.0164 (rank 1 in upstream1)
    // doc2: 1/(60+2) = 1/62 ≈ 0.0161 (rank 2 in upstream1)
    // doc11: 1/(60+1) = 1/61 ≈ 0.0164 (rank 1 in upstream2)
    // doc12: 1/(60+2) = 1/62 ≈ 0.0161 (rank 2 in upstream2)

    if (r.docId == 1 || r.docId == 11) {
      EXPECT_NEAR(1.0/61.0, r.score, 0.0001);  // Rank 1 in respective upstream
    } else if (r.docId == 2 || r.docId == 12) {
      EXPECT_NEAR(1.0/62.0, r.score, 0.0001);  // Rank 2 in respective upstream
    }

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(4, count); // Should have 4 documents total (2 from each upstream due to window size limit)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with large window size (10) - larger than upstream doc count (3 each)
 *
 * Scoring function: RRF (Reciprocal Rank Fusion)
 * Number of upstreams: 2
 * Intersection: Full intersection (same documents from both upstreams)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: All documents from both upstreams (3 total), each with RRF score combining ranks from both upstreams
 */
TEST_F(HybridMergerTest, testHybridMergerLargeWindow) {
  QueryIterator qitr = {0};

  // Create upstreams with same documents (full intersection) but different rankings
  // Upstream1 yields: doc1=0.9(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
  std::vector<double> scores1 = {0.9, 0.5, 0.1};
  std::vector<int> docIds1 = {1, 2, 3};
  MockUpstream upstream1(0, scores1, docIds1);

  // Upstream2 yields: doc3=0.8(rank1), doc1=0.4(rank2), doc2=0.2(rank3) (same docs, different ranking)
  std::vector<double> scores2 = {0.8, 0.4, 0.2};
  std::vector<int> docIds2 = {3, 1, 2};  // Same docs but in different order
  MockUpstream upstream2(0, scores2, docIds2);

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with RRF scoring - large window (10) larger than document count (3)
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  ResultProcessor *hybridMerger = CreateRRFHybridMerger(upstreams, 2, 60, 10); // k=60, window=10

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify document metadata and key are set
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify RRF scores - each document gets combined score from both upstreams
    // Expected RRF scores (k=60):
    // Upstream1 yields: doc1=0.9(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
    // Upstream2 yields: doc3=0.8(rank1), doc1=0.4(rank2), doc2=0.2(rank3)
    //
    // doc1: 1/(60+1) + 1/(60+2) = 1/61 + 1/62 ≈ 0.0325 (rank1 in upstream1, rank2 in upstream2)
    // doc2: 1/(60+2) + 1/(60+3) = 1/62 + 1/63 ≈ 0.0318 (rank2 in upstream1, rank3 in upstream2)
    // doc3: 1/(60+3) + 1/(60+1) = 1/63 + 1/61 ≈ 0.0323 (rank3 in upstream1, rank1 in upstream2)

    if (r.docId == 1) {
      ASSERT_NEAR(1.0/61.0 + 1.0/62.0, r.score, 0.0001);  // doc1: rank1 + rank2
    } else if (r.docId == 2) {
      ASSERT_NEAR(1.0/62.0 + 1.0/63.0, r.score, 0.0001);  // doc2: rank2 + rank3
    } else if (r.docId == 3) {
      ASSERT_NEAR(1.0/63.0 + 1.0/61.0, r.score, 0.0001);  // doc3: rank3 + rank1
    }

    SearchResult_Clear(&r);
  }

  // Should have 3 documents total (same docs from both upstreams)
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with first upstream depleting longer than second upstream
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: Both upstreams have documents (after depletion)
 * Timeout: No timeout
 * Expected behavior: Handle asymmetric depletion (upstream1 depletes 3 times, upstream2 depletes 1 time), then return all documents with weighted scores
 */
TEST_F(HybridMergerTest, testHybridMergerUpstream1DepletesMore) {
  QueryIterator qitr = {0};

  // Create upstreams with different depletion counts
  MockUpstream upstream1(0, {1.0, 1.0, 1.0}, {1, 2, 3}, 3); // depletionCount = 3
  MockUpstream upstream2(0, {2.0, 2.0, 2.0}, {21, 22, 23}, 1); // depletionCount = 1

  
  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Count results from each upstream - only contributing upstream's weighted score
    if (r.docId >= 1 && r.docId <= 3) {
      EXPECT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else if (r.docId >= 21 && r.docId <= 23) {
      EXPECT_EQ(1.0, r.score);  // 0.5 * 2.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(6, count); // Should have 6 documents total (3 from upstream1 after 3 depletes, 3 from upstream2 after 1 deplete)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with second upstream depleting longer than first upstream
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: Both upstreams have documents (after depletion)
 * Timeout: No timeout
 * Expected behavior: Handle asymmetric depletion (upstream1 depletes 1 time, upstream2 depletes 3 times), then return all documents with weighted scores
 */
TEST_F(HybridMergerTest, testHybridMergerUpstream2DepletesMore) {
  QueryIterator qitr = {0};

  // Create upstreams with different depletion counts
  MockUpstream upstream1(0, {1.0, 1.0, 1.0}, {1, 2, 3}, 1); // depletionCount = 1
  MockUpstream upstream2(0, {2.0, 2.0, 2.0}, {21, 22, 23}, 3); // depletionCount = 3

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Count results from each upstream - only contributing upstream's weighted score
    if (r.docId >= 1 && r.docId <= 3) {
      EXPECT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else if (r.docId >= 21 && r.docId <= 23) {
      EXPECT_EQ(1.0, r.score);  // 0.5 * 2.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(6, count); // Should have 6 documents total (3 from upstream1 after 1 deplete, 3 from upstream2 after 3 depletes)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

// Helper function to setup timeout test environment
void SetupTimeoutTest(QueryIterator* qitr, RSTimeoutPolicy policy, RedisSearchCtx* sctx) {
  memset(sctx, 0, sizeof(RedisSearchCtx));
  sctx->redisCtx = NULL;
  qitr->timeoutPolicy = policy;
}

/*
 * Test that hybrid merger with timeout and return policy - collect anything available
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: Both upstreams have documents
 * Timeout: Yes - first upstream times out after 2 results, return policy
 * Expected behavior: Collect anything available from all upstreams, score based on {1,2,11,12,13,14,15}
 */
TEST_F(HybridMergerTest, testHybridMergerTimeoutReturnPolicy) {
  QueryIterator qitr = {0};

  RedisSearchCtx sctx;
  SetupTimeoutTest(&qitr, TimeoutPolicy_Return, &sctx);

  // Create upstreams: first times out after 2 docs, second has more docs
  MockUpstream upstream1(2, {1.0, 1.0, 1.0}, {1, 2, 3}); // timeoutAfterCount=2
  MockUpstream upstream2(0, {2.0, 2.0, 2.0, 2.0, 2.0}, {11, 12, 13, 14, 15});

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results - should collect anything available
  std::vector<t_docId> receivedDocIds;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int rc;

  // Collect all available results from both upstreams
  while ((rc = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    // Verify document metadata and key are set
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Store the document ID for verification
    receivedDocIds.push_back(r.docId);
    SearchResult_Clear(&r);
  }

  // Should have collected documents from both upstreams: {1,2} from upstream1 and {11,12,13,14,15} from upstream2
  std::vector<t_docId> expectedDocIds = {1, 2, 11, 12, 13, 14, 15};
  ASSERT_EQ(expectedDocIds.size(), receivedDocIds.size());

  // Convert to sets for comparison since order may vary
  std::set<t_docId> expectedDocIdSet(expectedDocIds.begin(), expectedDocIds.end());
  std::set<t_docId> receivedDocIdSet(receivedDocIds.begin(), receivedDocIds.end());
  EXPECT_EQ(expectedDocIdSet, receivedDocIdSet);

  // Final result should be EOF after collecting everything available
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rc);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with timeout and fail policy
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: Both upstreams have documents
 * Timeout: Yes - first upstream times out after 2 results, fail policy
 * Expected behavior: Return no results and immediate timeout (fail fast)
 */
TEST_F(HybridMergerTest, testHybridMergerTimeoutFailPolicy) {
  QueryIterator qitr = {0};

  RedisSearchCtx sctx;
  SetupTimeoutTest(&qitr, TimeoutPolicy_Fail, &sctx);

  // Create upstreams: first times out after 2 docs, second has more docs
  MockUpstream upstream1(2, {1.0, 1.0}, {1, 2}); // timeoutAfterCount=2
  MockUpstream upstream2(0, {2.0, 2.0, 2.0, 2.0, 2.0}, {11, 12, 13, 14, 15});

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int rc;

  // With Fail policy, should return timeout immediately without yielding any results
  while ((rc = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;
    SearchResult_Clear(&r);
  }

  // With Fail policy, should get no results and immediate timeout
  ASSERT_EQ(0, count);
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rc);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with RRF scoring function
 *
 * Scoring function: RRF (Reciprocal Rank Fusion)
 * Number of upstreams: 2
 * Intersection: Full intersection (same documents from both upstreams)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Each document gets RRF score combining ranks from both upstreams: 1/(k+rank1) + 1/(k+rank2)
 */
TEST_F(HybridMergerTest, testRRFScoring) {
  QueryIterator qitr = {0};

  // Create RRF upstreams with custom score arrays for intersection test
  // Upstream1 yields: doc1=0.7(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
  std::vector<double> scores1 = {0.7, 0.5, 0.1};
  std::vector<int> docIds1 = {1, 2, 3};
  MockUpstream upstream1(0, scores1, docIds1);

  // Upstream2 yields: doc2=0.9(rank1), doc1=0.3(rank2), doc3=0.2(rank3) (same docs, different ranking)
  std::vector<double> scores2 = {0.9, 0.3, 0.2};
  std::vector<int> docIds2 = {2, 1, 3};  // Same docs but in different order
  MockUpstream upstream2(0, scores2, docIds2);

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with RRF scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  ResultProcessor *hybridMerger = CreateRRFHybridMerger(upstreams, 2, 60, 4); // k=60, window=4

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;
  size_t count = 0;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Basic verification
    EXPECT_TRUE(r.dmd != nullptr);
    EXPECT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify RRF scores - each document gets combined score from both upstreams
    // Expected RRF scores (k=60):
    // Upstream1 yields: doc1=0.7(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
    // Upstream2 yields: doc2=0.9(rank1), doc1=0.3(rank2), doc3=0.2(rank3)
    //
    // doc1: 1/(60+1) + 1/(60+2) = 1/61 + 1/62 ≈ 0.0325 (rank1 in upstream1, rank2 in upstream2)
    // doc2: 1/(60+2) + 1/(60+1) = 1/62 + 1/61 ≈ 0.0325 (rank2 in upstream1, rank1 in upstream2)
    // doc3: 1/(60+3) + 1/(60+3) = 1/63 + 1/63 ≈ 0.0317 (rank3 in both upstreams)

    if (r.docId == 1) {
      EXPECT_NEAR(1.0/61.0 + 1.0/62.0, r.score, 0.0001);  // doc1: rank1 + rank2
    } else if (r.docId == 2) {
      EXPECT_NEAR(1.0/62.0 + 1.0/61.0, r.score, 0.0001);  // doc2: rank2 + rank1
    } else if (r.docId == 3) {
      EXPECT_NEAR(1.0/63.0 + 1.0/63.0, r.score, 0.0001);  // doc3: rank3 + rank3
    }

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  ASSERT_EQ(3, count); // Should have 3 documents total (full intersection - same docs from both upstreams)
  SearchResult_Destroy(&r);

  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with 3 upstreams using linear scoring
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 3
 * Intersection: No intersection (different documents from each upstream)
 * Emptiness: All upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Each document gets weighted score from only its contributing upstream (0.2*1.0=0.2, 0.3*2.0=0.6, 0.5*3.0=1.5)
 */
TEST_F(HybridMergerTest, testHybridMergerLinear3Upstreams) {
  QueryIterator qitr = {0};

  // Create upstreams with different documents (no intersection)
  MockUpstream upstream1(0, {1.0, 1.0, 1.0}, {1, 2, 3});
  MockUpstream upstream2(0, {2.0, 2.0, 2.0}, {11, 12, 13});
  MockUpstream upstream3(0, {3.0, 3.0, 3.0}, {21, 22, 23});

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;
  ResultProcessor *rp3 = &upstream3;

  // Create hybrid merger with 3 upstreams
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  array_ensure_append_1(upstreams, rp3);
  double weights[] = {0.2, 0.3, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 3, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify document metadata and key are set
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify scores based on docId - only contributing upstream's weighted score
    if (r.docId >= 1 && r.docId <= 3) {
      ASSERT_NEAR(0.2, r.score, 0.0001);  // 0.2 * 1.0 (only upstream1 contributes)
    } else if (r.docId >= 11 && r.docId <= 13) {
      ASSERT_NEAR(0.6, r.score, 0.0001);  // 0.3 * 2.0 (only upstream2 contributes)
    } else if (r.docId >= 21 && r.docId <= 23) {
      ASSERT_NEAR(1.5, r.score, 0.0001);  // 0.5 * 3.0 (only upstream3 contributes)
    }

    SearchResult_Clear(&r);
  }

  // Should have 9 documents total (3 from each upstream)
  ASSERT_EQ(9, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger correctly handles partial intersection with linear scoring
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 2
 * Intersection: Partial intersection (documents 2,3 appear in both upstreams)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Documents 2,3 get combined scores from both upstreams
 */
TEST_F(HybridMergerTest, testHybridMergerPartialIntersection) {
  QueryIterator qitr = {0};

  // Create upstreams with partial intersection: {1,2,3} and {2,3,4,5}, all with score 1
  MockUpstream upstream1(0, {1.0, 1.0, 1.0}, {1, 2, 3});
  MockUpstream upstream2(0, {1.0, 1.0, 1.0, 1.0}, {2, 3, 4, 5});

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  double weights[] = {0.5, 0.5};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 2, weights);

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    if (r.docId == 1 || r.docId == 4 || r.docId == 5) {
      ASSERT_EQ(0.5, r.score); // Single upstream: 0.5 * 1.0 = 0.5
    } else if (r.docId == 2 || r.docId == 3) {
      ASSERT_EQ(1.0, r.score); // Both upstreams: 0.5 * 1.0 + 0.5 * 1.0 = 1.0
    }
    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with RRF scoring function handles partial intersection correctly
 *
 * Scoring function: RRF (Reciprocal Rank Fusion)
 * Number of upstreams: 2
 * Intersection: Partial intersection (documents 2,3 appear in both upstreams)
 * Emptiness: Both upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Documents 2,3 get combined RRF scores from both upstreams, others get single upstream RRF scores
 */
TEST_F(HybridMergerTest, testHybridMergerPartialIntersectionRRF) {
  QueryIterator qitr = {0};

  // Create upstreams with partial intersection: {1,2,3} and {2,3,4,5}
  // Using different scores to create different rankings
  MockUpstream upstream1(0, {0.9, 0.7, 0.5}, {1, 2, 3}); // doc1=rank1, doc2=rank2, doc3=rank3
  MockUpstream upstream2(0, {0.8, 0.6, 0.4, 0.2}, {2, 3, 4, 5}); // doc2=rank1, doc3=rank2, doc4=rank3, doc5=rank4

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;

  // Create hybrid merger with RRF scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  ResultProcessor *hybridMerger = CreateRRFHybridMerger(upstreams, 2, 60, 5); // k=60, window=5

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int lastResult;

  while ((lastResult = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    if (r.docId == 1) {
      // Only in upstream1 at rank 1: RRF = 1/(60+1) = 1/61 ≈ 0.0164
      ASSERT_NEAR(1.0/61.0, r.score, 0.001);
    } else if (r.docId == 2) {
      // In upstream1 at rank 2, upstream2 at rank 1: RRF = 1/(60+2) + 1/(60+1) = 1/62 + 1/61 ≈ 0.0325
      ASSERT_NEAR(1.0/62.0 + 1.0/61.0, r.score, 0.001);
    } else if (r.docId == 3) {
      // In upstream1 at rank 3, upstream2 at rank 2: RRF = 1/(60+3) + 1/(60+2) = 1/63 + 1/62 ≈ 0.0320
      ASSERT_NEAR(1.0/63.0 + 1.0/62.0, r.score, 0.001);
    } else if (r.docId == 4) {
      // Only in upstream2 at rank 3: RRF = 1/(60+3) = 1/63 ≈ 0.0159
      ASSERT_NEAR(1.0/63.0, r.score, 0.001);
    } else if (r.docId == 5) {
      // Only in upstream2 at rank 4: RRF = 1/(60+4) = 1/64 ≈ 0.0156
      ASSERT_NEAR(1.0/64.0, r.score, 0.001);
    }
    SearchResult_Clear(&r);
  }

  ASSERT_EQ(RS_RESULT_EOF, lastResult);
  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger with RRF scoring function with 3 upstreams (full intersection)
 *
 * Scoring function: RRF (Reciprocal Rank Fusion)
 * Number of upstreams: 3
 * Intersection: Full intersection (same documents from all upstreams)
 * Emptiness: All upstreams have documents
 * Timeout: No timeout
 * Expected behavior: Each document gets RRF score combining ranks from all 3 upstreams: 1/(k+rank1) + 1/(k+rank2) + 1/(k+rank3)
 */
TEST_F(HybridMergerTest, testRRFScoring3Upstreams) {
  QueryIterator qitr = {0};

  // Create RRF upstreams with custom score arrays for intersection test
  // Upstream1 yields: doc1=0.9(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
  std::vector<double> scores1 = {0.9, 0.5, 0.1};
  std::vector<int> docIds1 = {1, 2, 3};
  MockUpstream upstream1(0, scores1, docIds1);

  // Upstream2 yields: doc2=0.8(rank1), doc3=0.4(rank2), doc1=0.2(rank3) (same docs, different ranking)
  std::vector<double> scores2 = {0.8, 0.4, 0.2};
  std::vector<int> docIds2 = {2, 3, 1};  // Same docs but in different order
  MockUpstream upstream2(0, scores2, docIds2);

  // Upstream3 yields: doc3=0.7(rank1), doc1=0.6(rank2), doc2=0.3(rank3) (same docs, different ranking)
  std::vector<double> scores3 = {0.7, 0.6, 0.3};
  std::vector<int> docIds3 = {3, 1, 2};  // Same docs but in different order
  MockUpstream upstream3(0, scores3, docIds3);

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;
  ResultProcessor *rp3 = &upstream3;

  // Create hybrid merger with RRF scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  array_ensure_append_1(upstreams, rp3);
  ResultProcessor *hybridMerger = CreateRRFHybridMerger(upstreams, 3, 60, 5); // k=60, window=5

  QITR_PushRP(&qitr, hybridMerger);

  // Process results and verify RRF calculation
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  // Expected RRF scores (k=60):
  // Upstream1 yields: doc1=0.9(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
  // Upstream2 yields: doc2=0.8(rank1), doc3=0.4(rank2), doc1=0.2(rank3)
  // Upstream3 yields: doc3=0.7(rank1), doc1=0.6(rank2), doc2=0.3(rank3)
  //
  // doc1: 1/(60+1) + 1/(60+3) + 1/(60+2) = 1/61 + 1/63 + 1/62
  // doc2: 1/(60+2) + 1/(60+1) + 1/(60+3) = 1/62 + 1/61 + 1/63
  // doc3: 1/(60+3) + 1/(60+2) + 1/(60+1) = 1/63 + 1/62 + 1/61

  double expectedScores[3];
  expectedScores[0] = 1.0/61.0 + 1.0/63.0 + 1.0/62.0; // doc1: upstream1_rank=1, upstream2_rank=3, upstream3_rank=2
  expectedScores[1] = 1.0/62.0 + 1.0/61.0 + 1.0/63.0; // doc2: upstream1_rank=2, upstream2_rank=1, upstream3_rank=3
  expectedScores[2] = 1.0/63.0 + 1.0/62.0 + 1.0/61.0; // doc3: upstream1_rank=3, upstream2_rank=2, upstream3_rank=1

  size_t count = 0;
  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    // Verify document metadata and key are set
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    // Verify RRF score calculation
    int docIndex = r.docId - 1;
    ASSERT_NEAR(expectedScores[docIndex], r.score, 0.0001);

    count++;
    SearchResult_Clear(&r);
  }

  // Should have 3 documents total (same docs from all 3 upstreams)
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

/*
 * Test that hybrid merger correctly handles error precedence with three upstreams returning different states
 *
 * Scoring function: Hybrid linear
 * Number of upstreams: 3
 * Intersection: N/A (error handling test)
 * Emptiness: Mixed (one upstream EOF, one timeout, one error)
 * Timeout: Mixed (one upstream EOF, one timeout, one error)
 * Expected behavior: Return RS_RESULT_ERROR (most severe error) when one upstream returns error, regardless of other upstream states
 */
TEST_F(HybridMergerTest, testHybridMergerErrorPrecedence) {
  // Create upstreams with different behaviors:
  // upstream1: returns RS_RESULT_EOF (empty upstream)
  // upstream2: returns RS_RESULT_TIMEDOUT after 1 call
  // upstream3: returns RS_RESULT_ERROR after 1 call
  MockUpstream upstream1(0, {}, {}); // empty upstream (returns EOF)
  MockUpstream upstream2(1, {1.0, 2.0}, {1, 2}); // timeoutAfterCount=1 (timeout after 1 call)
  MockUpstream upstream3(0, {3.0, 4.0}, {3, 4}, 0, 1); // errorAfterCount=1 (error after 1 call)

  ResultProcessor *rp1 = &upstream1;
  ResultProcessor *rp2 = &upstream2;
  ResultProcessor *rp3 = &upstream3;

  // Create hybrid merger with linear scoring
  arrayof(ResultProcessor*) upstreams = NULL;
  array_ensure_append_1(upstreams, rp1);
  array_ensure_append_1(upstreams, rp2);
  array_ensure_append_1(upstreams, rp3);
  double weights[] = {0.33, 0.33, 0.34};
  ResultProcessor *hybridMerger = CreateLinearHybridMerger(upstreams, 3, weights);

  // Process and verify that the most severe error (RS_RESULT_ERROR) is returned
  SearchResult r = {0};
  int result;
  size_t count = 0;

  // Try to get results - should return error due to upstream3 error
  while ((result = hybridMerger->Next(hybridMerger, &r)) == RS_RESULT_OK) {
    count++;
    SearchResult_Clear(&r);
  }

  // Should return RS_RESULT_ERROR (most severe) even though other upstreams have TIMEOUT and EOF
  ASSERT_EQ(RS_RESULT_ERROR, result);

  SearchResult_Destroy(&r);
  hybridMerger->Free(hybridMerger);
}
