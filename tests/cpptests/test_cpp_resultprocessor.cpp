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
#include "module.h"

struct processor1Ctx : public ResultProcessor {
  processor1Ctx() {
    memset(static_cast<ResultProcessor *>(this), 0, sizeof(ResultProcessor));
    counter = 0;
  }
  int counter;
  RLookupKey *kout = NULL;
};

#define NUM_RESULTS 5

static int p1_Next(ResultProcessor *rp, SearchResult *res) {
  processor1Ctx *p = static_cast<processor1Ctx *>(rp);
  if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;

  res->docId = ++p->counter;
  res->score = (double)res->docId;
  RLookup_WriteOwnKey(p->kout, &res->rowdata, RS_NumVal(res->docId));
  return RS_RESULT_OK;
}

static int p2_Next(ResultProcessor *rp, SearchResult *res) {
  int rc = rp->upstream->Next(rp->upstream, res);
  processor1Ctx *p = static_cast<processor1Ctx *>(rp);
  if (rc == RS_RESULT_EOF) return rc;
  rp->parent->totalResults++;
  return RS_RESULT_OK;
}

static int numFreed = 0;

static void resultProcessor_GenericFree(ResultProcessor *rp) {
  numFreed++;
  delete static_cast<processor1Ctx *>(rp);
}


class ResultProcessorTest : public ::testing::Test {};

TEST_F(ResultProcessorTest, testProcessorChain) {
  QueryIterator qitr = {0};
  RLookup lk = {0};
  processor1Ctx *p = new processor1Ctx();
  p->counter = 0;
  p->Next = p1_Next;
  p->Free = resultProcessor_GenericFree;
  p->kout = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  QITR_PushRP(&qitr, p);

  processor1Ctx *p2 = new processor1Ctx();
  p2->Next = p2_Next;
  p2->Free = resultProcessor_GenericFree;
  QITR_PushRP(&qitr, p2);

  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;
    ASSERT_EQ(count, r.docId);
    ASSERT_EQ(count, r.score);
    RSValue *v = RLookup_GetItem(p->kout, &r.rowdata);
    ASSERT_TRUE(v != NULL);
    ASSERT_EQ(RSValue_Number, v->t);
    ASSERT_EQ(count, v->numval);
    SearchResult_Clear(&r);
  }

  ASSERT_EQ(NUM_RESULTS, count);
  ASSERT_EQ(NUM_RESULTS, qitr.totalResults);
  SearchResult_Destroy(&r);

  numFreed = 0;
  QITR_FreeChain(&qitr);
  ASSERT_EQ(2, numFreed);
  RLookup_Cleanup(&lk);
}

TEST_F(ResultProcessorTest, testHybridMergerSameDocs) {
  QueryIterator qitr = {0};

  // Mock upstream1: generates 3 docs with score 2.0 (e.g., text search results)
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = static_cast<MockUpstream1 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      res->docId = ++p->counter;
      res->score = 2.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: generates same 3 docs with score 4.0 (e.g., vector search results)
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      res->docId = ++p->counter; // Same docIds as upstream1
      res->score = 4.0;

      // Set up document metadata with keys (same docs as upstream1)
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger with window size 4
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    4   // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify hybrid score is applied (should be 3.0 = 0.5*2.0 + 0.5*4.0)
    ASSERT_EQ(3.0, r.score);

    // Verify we get the expected documents
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    SearchResult_Clear(&r);
  }

  // Should have processed 3 unique documents
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerDifferentDocuments) {
  QueryIterator qitr = {0};

  // Mock upstream1: generates 3 docs with score 1.0
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = (MockUpstream1 *)rp;
      if (p->counter >= 3) return RS_RESULT_EOF;

      res->docId = ++p->counter;
      res->score = 1.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: generates 3 different docs with score 3.0
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      res->docId = ++p->counter + 10; // Different docIds (11, 12, 13)
      res->score = 3.0;

      // Set up document metadata with keys - docId matches key number
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc11", "doc12", "doc13"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    3   // window size
  );

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

    // Verify scores: docs 1-3 (only upstream1) should have score 0.5*1.0=0.5, docs 11-13 (only upstream2) should have score 0.5*3.0=1.5
    if (r.docId <= 3) {
      ASSERT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else {
      ASSERT_EQ(1.5, r.score);  // 0.5 * 3.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  // Should have 6 documents total (3 from each upstream)
  ASSERT_EQ(6, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerEmptyUpstream1) {
  QueryIterator qitr = {0};

  // Mock empty upstream1 processor
  struct MockUpstream1 : public ResultProcessor {
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      return RS_RESULT_EOF; // Always empty
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: generates 3 docs with score 5.0
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      res->docId = ++p->counter;
      res->score = 5.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    3   // window size
  );

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

    // Should only get results from upstream2 with score 0.5*5.0=2.5 (only upstream2 contributes)
    ASSERT_EQ(2.5, r.score);

    SearchResult_Clear(&r);
  }

  // Should have 3 documents (only from upstream2)
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerEmptyUpstream2) {
  QueryIterator qitr = {0};

  // Mock upstream1: generates 3 docs with score 7.0
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = static_cast<MockUpstream1 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      res->docId = ++p->counter;
      res->score = 7.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock empty upstream2 processor
  struct MockUpstream2 : public ResultProcessor {
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      return RS_RESULT_EOF; // Always empty
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    3   // window size
  );

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

    // Should only get results from upstream1 with score 0.5*7.0=3.5 (only upstream1 contributes)
    ASSERT_EQ(3.5, r.score);

    SearchResult_Clear(&r);
  }

  // Should have 3 documents (only from upstream1)
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerBothEmpty) {
  QueryIterator qitr = {0};

  // Mock empty upstream1 processor
  struct MockUpstream1 : public ResultProcessor {
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      return RS_RESULT_EOF; // Always empty
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock empty upstream2 processor
  struct MockUpstream2 : public ResultProcessor {
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      return RS_RESULT_EOF; // Always empty
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    3   // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;
    SearchResult_Clear(&r);
  }

  // Should have 0 documents (both upstreams empty)
  ASSERT_EQ(0, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerSmallWindow) {
  QueryIterator qitr = {0};

  // Mock upstream1: generates 5 docs with score 1.0
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = (MockUpstream1 *)rp;
      if (p->counter >= 5) return RS_RESULT_EOF;

      res->docId = ++p->counter;
      res->score = 1.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[5] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3", "doc4", "doc5"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: generates 5 different docs with score 2.0
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 5) return RS_RESULT_EOF;

      res->docId = ++p->counter + 10; // Different docIds (11-15)
      res->score = 2.0;

      // Set up document metadata with keys - docId matches key number
      static RSDocumentMetadata dmd[5] = {0};
      static const char* keys[] = {"doc11", "doc12", "doc13", "doc14", "doc15"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger with small window size (2) - smaller than upstream doc count (5 each)
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    2   // Small window
  );

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
    if (r.docId <= 5) {
      ASSERT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else {
      ASSERT_EQ(1.0, r.score);  // 0.5 * 2.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  // Should have 4 documents total (2 from each upstream due to small window size)
  ASSERT_EQ(4, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerLargeWindow) {
  QueryIterator qitr = {0};

  // Create upstream1: generates 3 docs with score 1.0
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF;

    res->docId = ++p->counter;
    res->score = 1.0;

    // Set up document metadata with keys
    static RSDocumentMetadata dmd[3] = {0};
    static const char* keys[] = {"doc1", "doc2", "doc3"};
    dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
    res->dmd = &dmd[p->counter - 1];

    return RS_RESULT_OK;
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create upstream2: generates 3 different docs with score 2.0
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF;

    res->docId = ++p->counter + 10; // Different docIds (11-13)
    res->score = 2.0;

    // Set up document metadata with keys - docId matches key number
    static RSDocumentMetadata dmd[3] = {0};
    static const char* keys[] = {"doc11", "doc12", "doc13"};
    dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
    res->dmd = &dmd[p->counter - 1];

    return RS_RESULT_OK;
  };
  upstream2->Free = resultProcessor_GenericFree;



  // Create hybrid merger with large window size (10) - larger than upstream doc count (3 each)
  ResultProcessor *upstreams[] = {(ResultProcessor*)upstream1, (ResultProcessor*)upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,   // numUpstreams
    10   // Large window
  );

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
    if (r.docId <= 3) {
      ASSERT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else {
      ASSERT_EQ(1.0, r.score);  // 0.5 * 2.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  // Should have 6 documents total (3 from each upstream)
  ASSERT_EQ(6, count);

  SearchResult_Destroy(&r);
  upstream1->Free(upstream1);
  upstream2->Free(upstream2);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerUpstream1DepletesMore) {
  QueryIterator qitr = {0};

  // Create upstream1: depletes 3 times before returning docs
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);

    // Deplete 3 times, then return 3 docs, then EOF
    if (p->counter < 3) {
      p->counter++;
      return RS_RESULT_DEPLETING; // Don't modify res
    } else if (p->counter >= 3 && p->counter < 6) {
      int docIndex = p->counter - 3;
      p->counter++;
      res->docId = docIndex + 1; // docs 1, 2, 3
      res->score = 1.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc1", "doc2", "doc3"};
      dmd[docIndex].keyPtr = (char*)keys[docIndex];
      res->dmd = &dmd[docIndex];

      return RS_RESULT_OK;
    } else {
      return RS_RESULT_EOF;
    }
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create upstream2: depletes 1 time before returning docs
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);

    // Deplete 1 time, then return 3 docs, then EOF
    if (p->counter < 1) {
      p->counter++;
      return RS_RESULT_DEPLETING; // Don't modify res
    } else if (p->counter >= 1 && p->counter < 4) {
      int docIndex = p->counter - 1;
      p->counter++;
      res->docId = docIndex + 21; // docs 21, 22, 23
      res->score = 2.0;

      // Set up document metadata with keys - docId matches key number
      static RSDocumentMetadata dmd[3] = {0};
      static const char* keys[] = {"doc21", "doc22", "doc23"};
      dmd[docIndex].keyPtr = (char*)keys[docIndex];
      res->dmd = &dmd[docIndex];

      return RS_RESULT_OK;
    } else {
      return RS_RESULT_EOF;
    }
  };
  upstream2->Free = resultProcessor_GenericFree;



  // Create hybrid merger with window size 3
  ResultProcessor *upstreams[] = {(ResultProcessor*)upstream1, (ResultProcessor*)upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    3   // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  size_t upstream1Count = 0;
  size_t upstream2Count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify document metadata and key are set
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    // Count results from each upstream - only contributing upstream's weighted score
    if (r.docId >= 1 && r.docId <= 3) {
      upstream1Count++;
      ASSERT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else if (r.docId >= 21 && r.docId <= 23) {
      upstream2Count++;
      ASSERT_EQ(1.0, r.score);  // 0.5 * 2.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  // Should have 6 documents total (3 from upstream1 after 3 depletes, 3 from upstream2 after 1 deplete)
  ASSERT_EQ(6, count);
  ASSERT_EQ(3, upstream1Count);
  ASSERT_EQ(3, upstream2Count);

  SearchResult_Destroy(&r);
  upstream1->Free(upstream1);
  upstream2->Free(upstream2);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerUpstream2DepletesMore) {
  QueryIterator qitr = {0};

  // Mock upstream1: depletes 1 time before returning docs
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = (MockUpstream1 *)rp;

      // Deplete 1 time, then return 3 docs, then EOF
      if (p->counter < 1) {
        p->counter++;
        return RS_RESULT_DEPLETING; // Don't modify res
      } else if (p->counter >= 1 && p->counter < 4) {
        int docIndex = p->counter - 1;
        p->counter++;
        res->docId = docIndex + 1; // docs 1, 2, 3
        res->score = 1.0;

        // Set up document metadata with keys
        static RSDocumentMetadata dmd[3] = {0};
        static const char* keys[] = {"doc1", "doc2", "doc3"};
        dmd[docIndex].keyPtr = (char*)keys[docIndex];
        res->dmd = &dmd[docIndex];

        return RS_RESULT_OK;
      } else {
        return RS_RESULT_EOF;
      }
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: depletes 3 times before returning docs
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);

      // Deplete 3 times, then return 3 docs, then EOF
      if (p->counter < 3) {
        p->counter++;
        return RS_RESULT_DEPLETING; // Don't modify res
      } else if (p->counter >= 3 && p->counter < 6) {
        int docIndex = p->counter - 3;
        p->counter++;
        res->docId = docIndex + 21; // docs 21, 22, 23
        res->score = 2.0;

        // Set up document metadata with keys - docId matches key number
        static RSDocumentMetadata dmd[3] = {0};
        static const char* keys[] = {"doc21", "doc22", "doc23"};
        dmd[docIndex].keyPtr = (char*)keys[docIndex];
        res->dmd = &dmd[docIndex];

        return RS_RESULT_OK;
      } else {
        return RS_RESULT_EOF;
      }
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;



  // Create hybrid merger with window size 3
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    3   // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  size_t upstream1Count = 0;
  size_t upstream2Count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify document metadata and key are set
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    // Count results from each upstream - only contributing upstream's weighted score
    if (r.docId >= 1 && r.docId <= 3) {
      upstream1Count++;
      ASSERT_EQ(0.5, r.score);  // 0.5 * 1.0 (only upstream1 contributes)
    } else if (r.docId >= 21 && r.docId <= 23) {
      upstream2Count++;
      ASSERT_EQ(1.0, r.score);  // 0.5 * 2.0 (only upstream2 contributes)
    }

    SearchResult_Clear(&r);
  }

  // Should have 6 documents total (3 from upstream1 after 1 deplete, 3 from upstream2 after 3 depletes)
  ASSERT_EQ(6, count);
  ASSERT_EQ(3, upstream1Count);
  ASSERT_EQ(3, upstream2Count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerTimeoutReturnPolicy) {
  QueryIterator qitr = {0};

  // Set up dummy context for timeout functionality
  RedisSearchCtx sctx = {0};
  sctx.redisCtx = RSDummyContext;
  qitr.sctx = &sctx;
  qitr.timeoutPolicy = TimeoutPolicy_Return;

  // Mock upstream1: generates 2 docs then timeout
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = static_cast<MockUpstream1 *>(rp);
      if (p->counter >= 2) return RS_RESULT_TIMEDOUT;

      res->docId = ++p->counter;
      res->score = 1.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[2] = {0};
      static const char* keys[] = {"doc1", "doc2"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: generates 5 different docs
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 5) return RS_RESULT_EOF;

      res->docId = ++p->counter + 10; // docs 11-15
      res->score = 2.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[5] = {0};
      static const char* keys[] = {"doc11", "doc12", "doc13", "doc14", "doc15"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;

  // Create hybrid merger with window size 4
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    4   // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process and verify results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  int rc;

  // Should get some results before timeout
  while ((rc = rpTail->Next(rpTail, &r)) == RS_RESULT_OK) {
    count++;

    // Verify document metadata and key are set
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    SearchResult_Clear(&r);
  }

  ASSERT_EQ(count, 2);
  // Final result should be timeout
  ASSERT_EQ(RS_RESULT_TIMEDOUT, rc);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}

TEST_F(ResultProcessorTest, testHybridMergerTimeoutFailPolicy) {
  QueryIterator qitr = {0};

  // Set up dummy context for timeout functionality
  RedisSearchCtx sctx = {0};
  sctx.redisCtx = RSDummyContext;
  qitr.sctx = &sctx;
  qitr.timeoutPolicy = TimeoutPolicy_Fail;

  // Mock upstream1: generates 2 docs then timeout
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = static_cast<MockUpstream1 *>(rp);
      if (p->counter >= 2) return RS_RESULT_TIMEDOUT;

      res->docId = ++p->counter;
      res->score = 1.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[2] = {0};
      static const char* keys[] = {"doc1", "doc2"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: generates 5 different docs
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 5) return RS_RESULT_EOF;

      res->docId = ++p->counter + 10; // docs 11-15
      res->score = 2.0;

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[5] = {0};
      static const char* keys[] = {"doc11", "doc12", "doc13", "doc14", "doc15"};
      dmd[p->counter - 1].keyPtr = (char*)keys[p->counter - 1];
      res->dmd = &dmd[p->counter - 1];

      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;

  // Create hybrid merger with window size 4
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  // Set up linear weights for simple averaging
  double weights[] = {0.5, 0.5};
  scoringCtx.linearWeights = weights;
  scoringCtx.numScores = 2;
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_LINEAR,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    4   // window size
  );

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

TEST_F(ResultProcessorTest, testRRFScoring) {
  QueryIterator qitr = {0};

  // Mock upstream1: yields docs in descending score order (0.7, 0.5, 0.1)
  struct MockUpstream1 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *p = static_cast<MockUpstream1 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      // Yield in descending score order: rank1=0.7, rank2=0.5, rank3=0.1
      int docIds[] = {1, 2, 3};        // doc1, doc2, doc3
      double scores[] = {0.7, 0.5, 0.1}; // already sorted descending
      const char* keys[] = {"doc1", "doc2", "doc3"};

      res->docId = docIds[p->counter];
      res->score = scores[p->counter];

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      dmd[p->counter].keyPtr = (char*)keys[p->counter];
      res->dmd = &dmd[p->counter];

      p->counter++;
      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream1;

  // Mock upstream2: yields docs in descending score order (0.9, 0.3, 0.2)
  struct MockUpstream2 : public ResultProcessor {
    int counter = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *p = static_cast<MockUpstream2 *>(rp);
      if (p->counter >= 3) return RS_RESULT_EOF;

      // Yield in descending score order: rank1=0.9, rank2=0.3, rank3=0.2
      int docIds[] = {2, 1, 3};        // doc2, doc1, doc3 (sorted by score)
      double scores[] = {0.9, 0.3, 0.2}; // already sorted descending
      const char* keys[] = {"doc2", "doc1", "doc3"};

      res->docId = docIds[p->counter];
      res->score = scores[p->counter];

      // Set up document metadata with keys
      static RSDocumentMetadata dmd[3] = {0};
      dmd[p->counter].keyPtr = (char*)keys[p->counter];
      res->dmd = &dmd[p->counter];

      p->counter++;
      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } upstream2;

  // Create hybrid merger with RRF scoring (upstreams already yield in sorted order)
  ResultProcessor *upstreams[] = {&upstream1, &upstream2};
  ScoringFunctionArgs scoringCtx = {0};
  scoringCtx.RRF_k = 60; // Standard RRF constant
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    HYBRID_SCORING_RRF,
    &scoringCtx,
    upstreams,
    2,  // numUpstreams
    4   // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results and verify RRF calculation
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  // Expected RRF scores (k=60):
  // Upstream1 scores: doc1=0.7(rank1), doc2=0.5(rank2), doc3=0.1(rank3)
  // Upstream2 scores: doc1=0.3(rank2), doc2=0.9(rank1), doc3=0.2(rank3)
  //
  // doc1: 1/(60+1) + 1/(60+2) = 1/61 + 1/62 ≈ 0.0325
  // doc2: 1/(60+2) + 1/(60+1) = 1/62 + 1/61 ≈ 0.0325
  // doc3: 1/(60+3) + 1/(60+3) = 1/63 + 1/63 ≈ 0.0317

  double expectedScores[3];
  expectedScores[0] = 1.0/61.0 + 1.0/62.0; // doc1: upstream1_rank=1, upstream2_rank=2
  expectedScores[1] = 1.0/62.0 + 1.0/61.0; // doc2: upstream1_rank=2, upstream2_rank=1
  expectedScores[2] = 1.0/63.0 + 1.0/63.0; // doc3: upstream1_rank=3, upstream2_rank=3

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

  // Should have 3 documents total
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  QITR_FreeChain(&qitr);
}
