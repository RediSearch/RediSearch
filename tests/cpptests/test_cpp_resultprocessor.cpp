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
#include <thread>
#include <chrono>

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

TEST_F(ResultProcessorTest, RPDepleter_Basic) {
  // Mock upstream processor: yields 3 results, then EOF
  const size_t n_docs = 3;
  QueryIterator qitr = {0};
  struct MockUpstream : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream *self = (MockUpstream *)rp;
      if (self->count >= n_docs) return RS_RESULT_EOF;
      res->docId = ++self->count;
      return RS_RESULT_OK;
    }
    MockUpstream() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } mockUpstream;

  // Create the depleter processor
  ResultProcessor *depleter = RPDepleter_New();
  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;
  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
    // Sleep to let the background thread run
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_GT(depletingCount, 0); // Should have at least one depleting state

  // Now, results should be available
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  // We expect to have received all results from the upstream processor.
  ASSERT_EQ(resultCount, n_docs);
  // The last return code should be RS_RESULT_EOF, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_EOF);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

TEST_F(ResultProcessorTest, RPDepleter_Timeout) {
  // Mock upstream processor: yields 3 results, then timeout.
  const size_t n_docs = 3;
  QueryIterator qitr = {0};
  struct MockUpstream : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream *self = (MockUpstream *)rp;
      if (self->count >= n_docs) return RS_RESULT_TIMEDOUT;
      res->docId = ++self->count;
      return RS_RESULT_OK;
    }
    MockUpstream() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } mockUpstream;

  // Create the depleter processor
  ResultProcessor *depleter = RPDepleter_New();
  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;

  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
    // Sleep to let the background thread run
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  ASSERT_GT(depletingCount, 0);

  // Now, results should be available
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  ASSERT_EQ(resultCount, n_docs);
  // The last return code should be RS_RESULT_TIMEDOUT, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_TIMEDOUT);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

TEST_F(ResultProcessorTest, RPDepleter_LongRun) {
  // Mock upstream processor mimics a long running operation, such that the
  // Next function will be called several times before returning the results.

  const size_t n_docs = 3;
  QueryIterator qitr = {0};

  struct MockUpstream : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream *self = (MockUpstream *)rp;
      if (self->count >= n_docs) return RS_RESULT_EOF;
      // Sleep to simulate a long running operation
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      res->docId = ++self->count;
      return RS_RESULT_OK;
    }
    MockUpstream() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } mockUpstream;

  // Create the depleter processor
  ResultProcessor *depleter = RPDepleter_New();
  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;

  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
  }
  // We now expect to have more than one call to the Next function while the
  // depleter is running in the background.
  ASSERT_GT(depletingCount, 1);

  // Now, results should be available
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  ASSERT_EQ(resultCount, n_docs);
  // The last return code should be RS_RESULT_EOF, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_EOF);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}

TEST_F(ResultProcessorTest, RPDepleter_Error) {
  // Mock upstream processor sends an error on the first call.

  QueryIterator qitr = {0};

  struct MockUpstream : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      return RS_RESULT_ERROR;
    }
    MockUpstream() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } mockUpstream;

  // Create the depleter processor
  ResultProcessor *depleter = RPDepleter_New();
  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;

  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
    // Sleep to let the background thread run
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  // We now expect to have more than one call to the Next function while the
  // depleter is running in the background.
  ASSERT_GT(depletingCount, 0);

  // Now, results should be available (no results will be reached here)
  int resultCount = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_OK);

  // The last return code should be RS_RESULT_ERROR, as the upstream last returned.
  ASSERT_EQ(rc, RS_RESULT_ERROR);

  SearchResult_Destroy(&res);
  depleter->Free(depleter);
}
