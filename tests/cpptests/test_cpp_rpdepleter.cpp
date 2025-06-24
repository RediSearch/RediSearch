/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "result_processor.h"
#include "gtest/gtest.h"
#include <thread>
#include <chrono>
#include <atomic>

class RPDepleterTest : public ::testing::Test {};

TEST_F(RPDepleterTest, RPDepleter_Basic) {
  // Tests basic RPDepleter functionality: background thread depletes upstream results,
  // main thread waits on condition variable, then yields results in order.

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

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New());

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;
  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
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

TEST_F(RPDepleterTest, RPDepleter_Timeout) {
  // Tests RPDepleter handling of upstream timeout: background thread gets timeout,
  // main thread waits on condition variable, then yields results and timeout.

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

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New());

  QITR_PushRP(&qitr, &mockUpstream);
  QITR_PushRP(&qitr, depleter);

  SearchResult res = {0};
  int rc;
  int depletingCount = 0;

  // The first call(s) should return RS_RESULT_DEPLETING until the thread is done
  while ((rc = depleter->Next(depleter, &res)) == RS_RESULT_DEPLETING) {
    depletingCount++;
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

TEST_F(RPDepleterTest, RPDepleter_CrossWakeup) {
  // Tests cross-depleter condition variable signaling: when one depleter finishes,
  // it signals the shared condition variable, waking up other depleters that return
  // `RS_RESULT_DEPLETING` (allowing downstream to try other depleters for results).
  // Test that one depleter can wake up another depleter waiting on the same condition variable.
  // This tests the core mechanism where depleters share sync objects and signal each other.
  // High sleep times are used in order to avoid flakiness.

  const size_t n_docs = 2;
  QueryIterator qitr1 = {0}, qitr2 = {0};

  // Mock upstream that finishes quickly.
  struct FastUpstream : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      FastUpstream *self = (FastUpstream *)rp;
      if (self->count >= n_docs) return RS_RESULT_EOF;
      res->docId = ++self->count;
      // Sleep so it won't finish so fast
      // We use large times in order to reduce flakiness.
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      return RS_RESULT_OK;
    }
    FastUpstream() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } fastUpstream;

  // Mock upstream that takes much longer.
  struct SlowUpstream : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      SlowUpstream *self = (SlowUpstream *)rp;
      if (self->count >= n_docs) return RS_RESULT_EOF;
      // Sleep to simulate much slower operation
      // We use large times in order to reduce flakiness.
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      res->docId = ++self->count + 100;  // Different doc IDs
      return RS_RESULT_OK;
    }
    SlowUpstream() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } slowUpstream;

  // Create shared sync reference and two depleters sharing it
  StrongRef sync_ref = DepleterSync_New();
  ResultProcessor *fastDepleter = RPDepleter_New(StrongRef_Clone(sync_ref));
  ResultProcessor *slowDepleter = RPDepleter_New(StrongRef_Clone(sync_ref));
  StrongRef_Release(sync_ref);  // Release our reference

  // Set up pipelines
  QITR_PushRP(&qitr1, &fastUpstream);
  QITR_PushRP(&qitr1, fastDepleter);
  QITR_PushRP(&qitr2, &slowUpstream);
  QITR_PushRP(&qitr2, slowDepleter);

  SearchResult res = {0};

  // Start both depleters - they should both return DEPLETING initially
  int rc2 = slowDepleter->Next(slowDepleter, &res);
  int rc1 = fastDepleter->Next(fastDepleter, &res);
  ASSERT_EQ(rc1, RS_RESULT_DEPLETING);
  ASSERT_EQ(rc2, RS_RESULT_DEPLETING);

  // Call Next on the slow depleter, and get `RS_RESULT_DEPLETING`, indicating
  // that the fast depleter-thread has finished and woke it up.
  rc2 = slowDepleter->Next(slowDepleter, &res);
  ASSERT_EQ(rc2, RS_RESULT_DEPLETING);

  // Deplete the fast depleter - each result should be available immediately,
  // until we reach the end.
  int resultCount = 0;
  do {
    if (rc1 == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount);
      SearchResult_Clear(&res);
    }
  } while ((rc1 = fastDepleter->Next(fastDepleter, &res)) == RS_RESULT_OK);
  ASSERT_EQ(rc1, RS_RESULT_EOF);
  ASSERT_EQ(resultCount, n_docs);

  // Deplete the slow depleter. There is no other thread to wake it up, so we
  // need to wait for the thread to finish, getting all the results until we
  // reach the end.
  resultCount = 0;
  do {
    if (rc2 == RS_RESULT_OK) {
      ASSERT_EQ(res.docId, ++resultCount + 100);
      SearchResult_Clear(&res);
    }
  } while ((rc2 = slowDepleter->Next(slowDepleter, &res)) == RS_RESULT_OK);
  ASSERT_EQ(rc2, RS_RESULT_EOF);
  ASSERT_EQ(resultCount, n_docs);

  // Clean up
  SearchResult_Destroy(&res);
  fastDepleter->Free(fastDepleter);
  slowDepleter->Free(slowDepleter);
}

TEST_F(RPDepleterTest, RPDepleter_Error) {
  // Tests RPDepleter handling of upstream error: background thread gets error,
  // main thread waits on condition variable, then propagates the error.
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

  // Create depleter processor with new sync reference
  ResultProcessor *depleter = RPDepleter_New(DepleterSync_New());

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
