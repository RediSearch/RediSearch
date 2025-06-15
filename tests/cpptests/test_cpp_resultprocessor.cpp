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

TEST_F(ResultProcessorTest, testHybridMerger) {
  QueryIterator qitr = {0};
  RLookup lk = {0};

  // Create first upstream processor that generates results with score 2.0
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF; // Generate 3 results

    res->docId = ++p->counter;
    res->score = 2.0;  // Score from upstream1

    // Mock document metadata with keyPtr
    static RSDocumentMetadata dmd1 = {0};
    static RSDocumentMetadata dmd2 = {0};
    static RSDocumentMetadata dmd3 = {0};

    if (p->counter == 1) {
      dmd1.keyPtr = (char*)"doc1";
      res->dmd = &dmd1;
    } else if (p->counter == 2) {
      dmd2.keyPtr = (char*)"doc2";
      res->dmd = &dmd2;
    } else {
      dmd3.keyPtr = (char*)"doc3";
      res->dmd = &dmd3;
    }

    return RS_RESULT_OK;
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create second upstream processor that generates results with score 3.0
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF; // Generate 3 results

    res->docId = ++p->counter;
    res->score = 4.0;  // Score from upstream2

    // Mock document metadata with keyPtr (same docs as upstream1)
    static RSDocumentMetadata dmd1 = {0};
    static RSDocumentMetadata dmd2 = {0};
    static RSDocumentMetadata dmd3 = {0};

    if (p->counter == 1) {
      dmd1.keyPtr = (char*)"doc1";
      res->dmd = &dmd1;
    } else if (p->counter == 2) {
      dmd2.keyPtr = (char*)"doc2";
      res->dmd = &dmd2;
    } else {
      dmd3.keyPtr = (char*)"doc3";
      res->dmd = &dmd3;
    }

    return RS_RESULT_OK;
  };
  upstream2->Free = resultProcessor_GenericFree;

  // Define hybrid scoring function (simple average)
  auto hybridScoringFunction = [](double score1, double score2, bool hasScore1, bool hasScore2) -> double {
    if (hasScore1 && hasScore2) {
      return (score1 + score2) / 2.0;  // Average: (2.0 + 4.0) / 2 = 3.0
    } else if (hasScore1) {
      return score1;  // Only upstream1 has this document - return as is
    } else if (hasScore2) {
      return score2;  // Only upstream2 has this document - return as is
    } else {
      return 0.0;  // Neither upstream has this document (shouldn't happen)
    }
  };

  // Create hybrid merger with window size 3
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    hybridScoringFunction,
    (ResultProcessor*)upstream1,
    (ResultProcessor*)upstream2,
    3  // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify hybrid score is applied (should be 3.0 = average of 2.0 and 4.0)
    ASSERT_EQ(3.0, r.score);

    // Verify we get the expected documents
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    SearchResult_Clear(&r);
  }

  // Should have processed 3 unique documents
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  numFreed = 0;
  QITR_FreeChain(&qitr);
  //TODO: how to free two upstreams as part of QITR_FreeChain
  // ASSERT_EQ(3, numFreed); // upstream1 + upstream2 + hybridMerger
}

TEST_F(ResultProcessorTest, testHybridMergerDifferentDocuments) {
  QueryIterator qitr = {0};
  RLookup lk = {0};

  // Create first upstream processor that generates doc1, doc2, doc3 with score 1.0
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF; // Generate 3 results

    res->docId = ++p->counter;
    res->score = 1.0;  // Score from upstream1

    // Mock document metadata with keyPtr (doc1, doc2, doc3)
    static RSDocumentMetadata dmd1 = {0};
    static RSDocumentMetadata dmd2 = {0};
    static RSDocumentMetadata dmd3 = {0};

    if (p->counter == 1) {
      dmd1.keyPtr = (char*)"doc1";
      res->dmd = &dmd1;
    } else if (p->counter == 2) {
      dmd2.keyPtr = (char*)"doc2";
      res->dmd = &dmd2;
    } else {
      dmd3.keyPtr = (char*)"doc3";
      res->dmd = &dmd3;
    }

    return RS_RESULT_OK;
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create second upstream processor that generates doc4, doc5, doc6 with score 3.0
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF; // Generate 3 results

    res->docId = p->counter + 10; // Different docIds (11, 12, 13)
    res->score = 3.0;  // Score from upstream2

    // Mock document metadata with keyPtr (doc4, doc5, doc6)
    static RSDocumentMetadata dmd4 = {0};
    static RSDocumentMetadata dmd5 = {0};
    static RSDocumentMetadata dmd6 = {0};

    ++p->counter;
    if (p->counter == 1) {
      dmd4.keyPtr = (char*)"doc4";
      res->dmd = &dmd4;
    } else if (p->counter == 2) {
      dmd5.keyPtr = (char*)"doc5";
      res->dmd = &dmd5;
    } else {
      dmd6.keyPtr = (char*)"doc6";
      res->dmd = &dmd6;
    }

    return RS_RESULT_OK;
  };
  upstream2->Free = resultProcessor_GenericFree;

  // Define hybrid scoring function that handles single-source documents
  auto hybridScoringFunction = [](double score1, double score2, bool hasScore1, bool hasScore2) -> double {
    if (hasScore1 && hasScore2) {
      // Both upstreams have this document (shouldn't happen in this test)
      return (score1 + score2) / 2.0;
    } else if (hasScore1) {
      // Only upstream1 has this document - return score1 as is
      return score1;  // 1.0
    } else if (hasScore2) {
      // Only upstream2 has this document - return score2 as is
      return score2;  // 3.0
    } else {
      return 0.0;  // Neither upstream has this document (shouldn't happen)
    }
  };

  // Create hybrid merger with window size 3
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    hybridScoringFunction,
    (ResultProcessor*)upstream1,
    (ResultProcessor*)upstream2,
    3  // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  std::set<std::string> seenDocs;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Verify we get the expected documents
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    std::string docKey(r.dmd->keyPtr);
    seenDocs.insert(docKey);

    // Verify hybrid score is applied correctly
    if (docKey == "doc1" || docKey == "doc2" || docKey == "doc3") {
      ASSERT_EQ(1.0, r.score); // upstream1 score as is
    } else if (docKey == "doc4" || docKey == "doc5" || docKey == "doc6") {
      ASSERT_EQ(3.0, r.score); // upstream2 score as is
    } else {
      FAIL() << "Unexpected document: " << docKey;
    }

    SearchResult_Clear(&r);
  }

  // Should have processed 6 unique documents (3 from each upstream)
  ASSERT_EQ(6, count);
  ASSERT_EQ(6, seenDocs.size());

  // Verify all expected documents were seen
  ASSERT_TRUE(seenDocs.count("doc1") > 0);
  ASSERT_TRUE(seenDocs.count("doc2") > 0);
  ASSERT_TRUE(seenDocs.count("doc3") > 0);
  ASSERT_TRUE(seenDocs.count("doc4") > 0);
  ASSERT_TRUE(seenDocs.count("doc5") > 0);
  ASSERT_TRUE(seenDocs.count("doc6") > 0);

  SearchResult_Destroy(&r);
  numFreed = 0;
  QITR_FreeChain(&qitr);

  //TODO: how to free two upstreams as part of QITR_FreeChain
  // ASSERT_EQ(3, numFreed); // upstream1 + upstream2 + hybridMerger
}

TEST_F(ResultProcessorTest, testHybridMergerEmptyUpstream1) {
  QueryIterator qitr = {0};
  RLookup lk = {0};

  // Create empty upstream1 processor (returns EOF immediately)
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    return RS_RESULT_EOF; // Always empty
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create upstream2 processor that generates doc1, doc2, doc3 with score 5.0
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF; // Generate 3 results

    res->docId = ++p->counter;
    res->score = 5.0;  // Score from upstream2

    // Mock document metadata with keyPtr
    static RSDocumentMetadata dmd1 = {0};
    static RSDocumentMetadata dmd2 = {0};
    static RSDocumentMetadata dmd3 = {0};

    if (p->counter == 1) {
      dmd1.keyPtr = (char*)"doc1";
      res->dmd = &dmd1;
    } else if (p->counter == 2) {
      dmd2.keyPtr = (char*)"doc2";
      res->dmd = &dmd2;
    } else {
      dmd3.keyPtr = (char*)"doc3";
      res->dmd = &dmd3;
    }

    return RS_RESULT_OK;
  };
  upstream2->Free = resultProcessor_GenericFree;

  // Define hybrid scoring function
  auto hybridScoringFunction = [](double score1, double score2, bool hasScore1, bool hasScore2) -> double {
    if (hasScore1 && hasScore2) {
      return (score1 + score2) / 2.0;
    } else if (hasScore1) {
      return score1;
    } else if (hasScore2) {
      return score2;  // Should be 5.0
    } else {
      return 0.0;
    }
  };

  // Create hybrid merger with window size 3
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    hybridScoringFunction,
    (ResultProcessor*)upstream1,
    (ResultProcessor*)upstream2,
    3  // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Should only get results from upstream2 with original scores
    ASSERT_EQ(5.0, r.score);

    // Verify we get the expected documents
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    SearchResult_Clear(&r);
  }

  // Should have processed 3 documents (only from upstream2)
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  numFreed = 0;
  QITR_FreeChain(&qitr);

  //TODO: how to free two upstreams as part of QITR_FreeChain
  // ASSERT_EQ(3, numFreed); // upstream1 + upstream2 + hybridMerger
}

TEST_F(ResultProcessorTest, testHybridMergerEmptyUpstream2) {
  QueryIterator qitr = {0};
  RLookup lk = {0};

  // Create upstream1 processor that generates doc1, doc2, doc3 with score 7.0
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    processor1Ctx *p = static_cast<processor1Ctx *>(rp);
    if (p->counter >= 3) return RS_RESULT_EOF; // Generate 3 results

    res->docId = ++p->counter;
    res->score = 7.0;  // Score from upstream1

    // Mock document metadata with keyPtr
    static RSDocumentMetadata dmd1 = {0};
    static RSDocumentMetadata dmd2 = {0};
    static RSDocumentMetadata dmd3 = {0};

    if (p->counter == 1) {
      dmd1.keyPtr = (char*)"doc1";
      res->dmd = &dmd1;
    } else if (p->counter == 2) {
      dmd2.keyPtr = (char*)"doc2";
      res->dmd = &dmd2;
    } else {
      dmd3.keyPtr = (char*)"doc3";
      res->dmd = &dmd3;
    }

    return RS_RESULT_OK;
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create empty upstream2 processor (returns EOF immediately)
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    return RS_RESULT_EOF; // Always empty
  };
  upstream2->Free = resultProcessor_GenericFree;

  // Define hybrid scoring function
  auto hybridScoringFunction = [](double score1, double score2, bool hasScore1, bool hasScore2) -> double {
    if (hasScore1 && hasScore2) {
      return (score1 + score2) / 2.0;
    } else if (hasScore1) {
      return score1;  // Should be 7.0
    } else if (hasScore2) {
      return score2;
    } else {
      return 0.0;
    }
  };

  // Create hybrid merger with window size 3
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    hybridScoringFunction,
    (ResultProcessor*)upstream1,
    (ResultProcessor*)upstream2,
    3  // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;

    // Should only get results from upstream1 with original scores
    ASSERT_EQ(7.0, r.score);

    // Verify we get the expected documents
    ASSERT_TRUE(r.dmd != nullptr);
    ASSERT_TRUE(r.dmd->keyPtr != nullptr);

    SearchResult_Clear(&r);
  }

  // Should have processed 3 documents (only from upstream1)
  ASSERT_EQ(3, count);

  SearchResult_Destroy(&r);
  numFreed = 0;
  QITR_FreeChain(&qitr);

  //TODO: how to free two upstreams as part of QITR_FreeChain
  // ASSERT_EQ(3, numFreed); // upstream1 + upstream2 + hybridMerger
}

TEST_F(ResultProcessorTest, testHybridMergerBothEmpty) {
  QueryIterator qitr = {0};
  RLookup lk = {0};

  // Create empty upstream1 processor (returns EOF immediately)
  processor1Ctx *upstream1 = new processor1Ctx();
  upstream1->counter = 0;
  upstream1->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    return RS_RESULT_EOF; // Always empty
  };
  upstream1->Free = resultProcessor_GenericFree;

  // Create empty upstream2 processor (returns EOF immediately)
  processor1Ctx *upstream2 = new processor1Ctx();
  upstream2->counter = 0;
  upstream2->Next = [](ResultProcessor *rp, SearchResult *res) -> int {
    return RS_RESULT_EOF; // Always empty
  };
  upstream2->Free = resultProcessor_GenericFree;

  // Define hybrid scoring function
  auto hybridScoringFunction = [](double score1, double score2, bool hasScore1, bool hasScore2) -> double {
    if (hasScore1 && hasScore2) {
      return (score1 + score2) / 2.0;
    } else if (hasScore1) {
      return score1;
    } else if (hasScore2) {
      return score2;
    } else {
      return 0.0;
    }
  };

  // Create hybrid merger with window size 3
  ResultProcessor *hybridMerger = RPHybridMerger_New(
    hybridScoringFunction,
    (ResultProcessor*)upstream1,
    (ResultProcessor*)upstream2,
    3  // window size
  );

  QITR_PushRP(&qitr, hybridMerger);

  // Process results
  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;

  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;
    SearchResult_Clear(&r);
  }

  // Should have processed 0 documents (both upstreams empty)
  ASSERT_EQ(0, count);

  SearchResult_Destroy(&r);
  numFreed = 0;
  QITR_FreeChain(&qitr);
  //TODO: how to free two upstreams as part of QITR_FreeChain
  // ASSERT_EQ(3, numFreed); // upstream1 + upstream2 + hybridMerger
}

// TODO: Implement timeout tests for hybrid merger
// TEST_F(ResultProcessorTest, testHybridMergerTimeoutUpstream1Return) {
//   // Test timeout on upstream1 with TimeoutPolicy_Return
//   // Should return partial results from upstream1 only
// }

// TEST_F(ResultProcessorTest, testHybridMergerTimeoutUpstream1Fail) {
//   // Test timeout on upstream1 with TimeoutPolicy_Fail
//   // Should return no results and propagate timeout immediately
// }

// TEST_F(ResultProcessorTest, testHybridMergerTimeoutUpstream2Return) {
//   // Test timeout on upstream2 with TimeoutPolicy_Return
//   // Should return all upstream1 results + partial upstream2 results
// }

// TEST_F(ResultProcessorTest, testHybridMergerTimeoutUpstream2Fail) {
//   // Test timeout on upstream2 with TimeoutPolicy_Fail
//   // Should return no results and propagate timeout immediately
// }
