/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "spec.h"
#include "common.h"
#include "llapi_test_helpers.h"
#include "gc/index_gc.h"
#include "gc/gc.h"
#include "index_gc_ffi.h"
#include "deleted_docs_ffi.h"
#include "config.h"
#include "util/references.h"
#include "tag_index.h"
#include "rules.h"
#include "query_error_ffi.h"
#include "inverted_index.h"
#include "info/global_stats.h"
#include "redis_index.h"
#include "index_utils.h"
#include "notifications.h"
#include "rmalloc.h"
extern "C" {
#include "util/dict.h"
}

#include <iterator>
#include <set>
#include <random>
#include <unordered_set>
#include <thread>
#include <atomic>
#include <chrono>
#include <climits>
#include <vector>

/**
 * The following tests purpose is to make sure the garbage collection is working properly,
 * without causing any data corruption or loss.
 *
 * Main assumption are:
 * 1. New entries are always added to the last block (or to a new block if it reaches its
 * maximum capacity)
 *
 * 2. Old entries can not be modified, but only deleted if the fork process found them as deleted.
 *
 * 3. Last block is defined as the last block as seen by the child.
 * We always prefer the parent process last block. If it was simultaneously modified by both the child and
 * the parent, we take the parent's version.
 *
 * 4. Modifications performed on blocks, other than the last block, are always safe to apply
 * and hence will take place. (relying on (1))
 *
*/

static timespec getTimespecCb(void *) {
  timespec ts = {0};
  ts.tv_nsec = 5000;
  return ts;
}

class IndexGCTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  RefManager *ism;
  IndexGC *fgc;

  void SetUp() override {
    Initialize_KeyspaceNotifications();
    ism = createSpec(ctx);
    RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 0;
    RSGlobalStats.totalStats.logically_deleted = 0;
    fgc = reinterpret_cast<IndexGC *>(get_spec(ism)->gc->gcCtx);
    copyThresholdAtQueueTime();
  }

  void TearDown() override {
    freeSpec(ism);
  }

  // What GCContext_QueueRun does before handing the GC a scheduled run: a scheduled cycle checks
  // the threshold copied then, not the global one.
  void copyThresholdAtQueueTime() { get_spec(ism)->gc->callbacks.onQueueRun(fgc); }

  size_t addDocumentWrapper(const char *docid, const char *field, const char *value) {
    return ::addDocumentWrapper(ctx, ism, docid, field, value);
  }

  size_t totalSpecBlocks() {
    return __atomic_load_n(&get_spec(ism)->stats.totalInvertedIndexBlocks, __ATOMIC_RELAXED);
  }

  void runGC(std::function<void()> beforeApply = {}) {
    IndexGCHook hook{
      [](void *p) { if (auto &f = *static_cast<std::function<void()>*>(p)) f(); },
      &beforeApply
    };
    GCForcedRun forced =
        GCForcedRun_New(GC_FORCED_RUN_DEFAULT_WAIT_MS, GC_FORCED_RUN_RETRY_INTERVAL_MS);
    IndexGC_RunCycle(fgc, &forced, beforeApply ? &hook : nullptr);
  }
};

static InvertedIndex *getTagInvidx(RedisSearchCtx *sctx, const char *field,
                                   const char *value) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, "f1", strlen("f1"));
  auto tix = TagIndex_Ensure(const_cast<FieldSpec *>(fs), NULL, false);
  size_t sz;
  auto iv = TagIndex_OpenIndex(tix, "hello", strlen("hello"), CREATE_INDEX, &sz);
  sctx->spec->stats.invertedSize += sz;
  return iv;
}

class IndexGCTestTag : public IndexGCTest {
protected:
  const char *tag_field_name = "f1";

  void SetUp() override {
    IndexGCTest::SetUp();
    RediSearch_CreateTagField(ism, "f1");
  }
};

class IndexGCTestNumeric : public IndexGCTest {
protected:
  const char *numeric_field_name = "n";

  void SetUp() override {
    IndexGCTest::SetUp();
    RediSearch_CreateNumericField(this->ism, numeric_field_name);
  }
};


/**
 * Runs the fork GC over a numeric index with deleted documents and checks that it reports
 * collected bytes. Whether the reported bytes match what the tree actually freed is checked by
 * `applied_byte_stats_match_the_tree_size_change` in `index_gc/tests/numeric.rs`: the spec's
 * inverted size is updated from the same reported numbers, so it cannot tell here.
 */
TEST_F(IndexGCTestNumeric, testNumeric) {

  size_t total_mem = 0;

  // No inverted indices were created yet
  size_t spec_inv_index_mem_stats = (get_spec(ism))->stats.invertedSize;
  ASSERT_EQ(total_mem, spec_inv_index_mem_stats);

  size_t num_docs = 1000;
  for (size_t i = 0 ; i < num_docs ; i++) {
    std::string val = std::to_string(i);
    total_mem += this->addDocumentWrapper(numToDocStr(i).c_str(), numeric_field_name, val.c_str());
  }

  spec_inv_index_mem_stats = (get_spec(ism))->stats.invertedSize;
  ASSERT_EQ(total_mem, spec_inv_index_mem_stats);

  size_t deleted_docs = num_docs / 4;
  std::mt19937 gen(42);
  std::uniform_int_distribution<size_t> dis(0, num_docs - 1);
  std::unordered_set<size_t> generated_numbers;

  for (size_t i = 0; i < deleted_docs; ++i) {
    size_t random_id = dis(gen);
    while (generated_numbers.find(random_id) != generated_numbers.end())
      random_id = dis(gen);
    generated_numbers.insert(random_id);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, numToDocStr(random_id).c_str()))
        << "Failed to delete doc " << random_id << " at iteration " << i;
  }
  runGC();

  ASSERT_GT(fgc->stats.totalCollected, 0);
}

/** Mark one of the entries in the last block as deleted while the child is running.
 * This means the number of original entries recorded by the child and the current number of
 * entries are equal, and we conclude there weren't any changes in the parent to the block buffer.
 * Make sure the modification take place. */
TEST_F(IndexGCTestTag, testRemoveEntryFromLastBlock) {
  const auto startValue = totalSpecBlocks();

  // Add two documents
  size_t docSize = this->addDocumentWrapper("doc1", "f1", "hello");
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));
  /**
   * To properly test this; we must ensure that the gc is forked AFTER
   * the deletion, but BEFORE the addition.
   */
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  size_t invertedSizeBeforeApply;
  runGC([&]() {
    RS::deleteDocument(ctx, ism, "doc2");
    invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  });

  // gc stats
  ASSERT_EQ(0, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(1, fgc->stats.totalCollected);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  ASSERT_EQ(0, (get_spec(ism))->stats.scoring.numDocuments);
  ASSERT_EQ(1, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply - fgc->stats.totalCollected, (get_spec(ism))->stats.invertedSize);
  ASSERT_EQ(1, totalSpecBlocks() - startValue);
}

/**
 * In this test, the child process needs to delete the only and last block in the index,
 * while the main process adds a document to it.
 * In this case, we discard the changes collected by the child process, so eventually the
 * index contains both documents.
 * */
TEST_F(IndexGCTestTag, testRemoveLastBlockWhileUpdate) {
  const auto startValue = totalSpecBlocks();
  // Add a document
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  /**
   * To properly test this; we must ensure that the gc is forked AFTER
   * the deletion, but BEFORE the addition.
   */
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  size_t invertedSizeBeforeApply;
  runGC([&]() {
    ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));
    invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  });

  // gc stats
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(0, fgc->stats.totalCollected);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  ASSERT_EQ(1, (get_spec(ism))->stats.scoring.numDocuments);
  ASSERT_EQ(2, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply, (get_spec(ism))->stats.invertedSize);
  ASSERT_EQ(1, totalSpecBlocks() - startValue);
}

/**
 * Modify the last block, but don't delete it entirely. While the fork is running,
 * fill up the last block and add more blocks.
 * Make sur eno modifications are applied.
 * */
TEST_F(IndexGCTestTag, testModifyLastBlockWhileAddingNewBlocks) {
  const auto startValue = totalSpecBlocks();
  unsigned curId = 1;


  // populate the first(last) block with two document
  ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(curId++).c_str(), "f1", "hello"));
  ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(curId++).c_str(), "f1", "hello"));

  // Delete one of the documents.
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  auto iv = getTagInvidx(&sctx, "f1", "hello");
  const char *originalData;
  size_t invertedSizeBeforeApply;

  runGC([&]() {
    while (InvertedIndex_NumBlocks(iv) < 3)
      ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(curId++).c_str(), "f1", "hello"));
    ASSERT_EQ(3, totalSpecBlocks() - startValue);
    originalData = IndexBlock_Data(InvertedIndex_BlockRef(iv, 0));
    invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  });

  const char *afterGcData = IndexBlock_Data(InvertedIndex_BlockRef(iv, 0));
  ASSERT_EQ(afterGcData, originalData);

  // gc stats
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(0, fgc->stats.totalCollected);

  size_t addedDocs = curId -1;

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  ASSERT_EQ(addedDocs - 1, (get_spec(ism))->stats.scoring.numDocuments);
  // All other updates are ignored.
  ASSERT_EQ(3, totalSpecBlocks() - startValue);
  ASSERT_EQ(addedDocs, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply, (get_spec(ism))->stats.invertedSize);
}

/** Delete all the blocks, while the main process adds entries to the last block.
 * All the blocks, except the last block, should be removed.
*/
TEST_F(IndexGCTestTag, testRemoveAllBlocksWhileUpdateLast) {
  const auto startValue = totalSpecBlocks();
  unsigned curId = 1;
  char buf[1024];
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;

  // Add documents to the index until it has 2 blocks (1 full block + 1 block with one entry)
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  // Measure the memory added by the last block.
  size_t lastBlockMemory = 0;
  while (InvertedIndex_NumBlocks(iv) < 2) {
    size_t n = snprintf(buf, sizeof(buf), "doc%u", curId++);
    lastBlockMemory = this->addDocumentWrapper(buf, "f1", "hello");
  }

  ASSERT_EQ(2, totalSpecBlocks() - startValue);

  size_t invertedSizeBeforeApply;
  const char *originalData;

  for (unsigned i = 1; i < curId; i++) {
    size_t n = snprintf(buf, sizeof(buf), "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }
  ASSERT_EQ(0, sctx.spec->stats.scoring.numDocuments);
  runGC([&]() {
      invertedSizeBeforeApply = sctx.spec->stats.invertedSize;
      size_t n = snprintf(buf, sizeof(buf), "doc%u", curId);
      lastBlockMemory += this->addDocumentWrapper(buf, "f1", "hello");
      const IndexBlock *lastBlock = InvertedIndex_BlockRef(iv, InvertedIndex_NumBlocks(iv) - 1);
      originalData = IndexBlock_Data(lastBlock);
    });

  // gc stats - make sure we skipped the last block
  const IndexBlock *lastBlock = InvertedIndex_BlockRef(iv, InvertedIndex_NumBlocks(iv) - 1);
  const char *afterGcData = IndexBlock_Data(lastBlock);
  ASSERT_EQ(afterGcData, originalData);
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  // In this case the spec contains only one valid document.
  ASSERT_EQ(1, sctx.spec->stats.scoring.numDocuments);
  // But the last block deletion was skipped.
  ASSERT_EQ(2, sctx.spec->stats.numRecords);
  // 32 bytes is the base size of an inverted index, 8 is the header of the block vector
  ASSERT_EQ(lastBlockMemory + 32 + 8, sctx.spec->stats.invertedSize);
  ASSERT_EQ(1, totalSpecBlocks() - startValue);
}

/**
 * Repair the last block, while adding more documents to it and removing a middle block.
 * This test should be checked with valgrind as it cause index corruption.
 */
TEST_F(IndexGCTestTag, testRepairLastBlockWhileRemovingMiddle) {
  const auto startValue = totalSpecBlocks();
  // Delete the first block:
  char buf[1024];
  unsigned curId = 1;

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  // Add 2 full blocks + 1 block with1 entry.
  unsigned middleBlockFirstId = 0;
  while (InvertedIndex_NumBlocks(iv) < 3) {
    size_t n = snprintf(buf, sizeof(buf), "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
    // A new block had opened
    if (InvertedIndex_NumBlocks(iv) == 2 && !middleBlockFirstId) {
      middleBlockFirstId = curId - 1;
    }
  }

  unsigned lastBlockFirstId = curId - 1;

  ASSERT_EQ(3, totalSpecBlocks() - startValue);

  /**
   * In this case, we want to keep the first entry in the last block,
   * but we want to delete the second entry while appending more documents to it.
   * The block will remain unchanged.
   **/
  snprintf(buf, sizeof(buf), "doc%u", curId++);
  RS::addDocument(ctx, ism, buf, "f1", "hello");

  unsigned total_deletions;
  size_t valid_docs;
  size_t lastBlockEntries;

  ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  total_deletions = 2;
  for (unsigned i = middleBlockFirstId; i < lastBlockFirstId; ++i) {
    snprintf(buf, sizeof(buf), "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
    ++total_deletions;
  }
  valid_docs = curId - 1 - total_deletions;
  ASSERT_EQ(valid_docs, sctx.spec->stats.scoring.numDocuments);
  lastBlockEntries = IndexBlock_NumEntries(InvertedIndex_BlockRef(iv, 2));
  runGC([&]() {
      snprintf(buf, sizeof(buf), "doc%u", curId);
      RS::addDocument(ctx, ism, buf, "f1", "hello");
      ++valid_docs;
    });

  // Since we added entries to the last block after the fork, we ignore the fork updates in the last block
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  // The deletion in the last block was ignored,
  ASSERT_EQ(1 + valid_docs, sctx.spec->stats.numRecords);
  // Other updates should take place.
  ASSERT_EQ(valid_docs, sctx.spec->stats.scoring.numDocuments);
  // We are left with the first + last block.
  ASSERT_EQ(2, InvertedIndex_NumBlocks(iv));
  // The first entry was deleted. first block starts from docId = 2.
  ASSERT_EQ(2, IndexBlock_FirstId(InvertedIndex_BlockRef(iv, 0)));
  // Last block was moved.
  ASSERT_EQ(lastBlockFirstId, IndexBlock_FirstId(InvertedIndex_BlockRef(iv, 1)));
  ASSERT_EQ(3, IndexBlock_NumEntries(InvertedIndex_BlockRef(iv, 1)));
}

/**
 * Repair the last block, while adding more documents to it...
 */
TEST_F(IndexGCTestTag, testRepairLastBlock) {
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  auto iv = getTagInvidx(&sctx, "f1", "hello");
  while (InvertedIndex_NumBlocks(iv) < 2) {
    char buf[1024];
    size_t n = snprintf(buf, sizeof(buf), "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  }
  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/

  //add another document. now the last block has 2 entries.
  char buf[1024];
  snprintf(buf, sizeof(buf), "doc%u", curId++);
  RS::addDocument(ctx, ism, buf, "f1", "hello");

  ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  runGC([&]() {
      snprintf(buf, sizeof(buf), "doc%u", curId);
      RS::addDocument(ctx, ism, buf, "f1", "hello");
    });
  // since the block size in the main process doesn't equal to its original size as seen by the child,
  // we ignore the fork collection - the last block changes should be discarded.
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(2, InvertedIndex_NumBlocks(iv));
}

/**
 * Test repair middle block while last block is removed on child and modified on parent.
 * Make sure there is no datalose.
 */
TEST_F(IndexGCTestTag, testRepairMiddleRemoveLast) {
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  auto iv = getTagInvidx(&sctx, "f1", "hello");
  while (InvertedIndex_NumBlocks(iv) < 3) {
    char buf[1024];
    size_t n = snprintf(buf, sizeof(buf), "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  }

  char buf[1024];
  snprintf(buf, sizeof(buf), "doc%u", curId);
  ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  unsigned next_id = curId + 1;

  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/
  while (curId > 100) {
    snprintf(buf, sizeof(buf), "doc%u", --curId);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }
  runGC([&]() {
      snprintf(buf, sizeof(buf), "doc%u", next_id);
      ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
    });
  ASSERT_EQ(2, InvertedIndex_NumBlocks(iv));
}

/**
 * Ensure that removing a middle block while adding to the parent will maintain
 * the parent's changes
 */
TEST_F(IndexGCTestTag, testRemoveMiddleBlock) {
  const auto startValue = totalSpecBlocks();
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  InvertedIndex *iv = getTagInvidx(&sctx, "f1", "hello");

  while (InvertedIndex_NumBlocks(iv) < 2) {
    RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello");
  }

  unsigned firstMidId = curId;
  while (InvertedIndex_NumBlocks(iv) < 3) {
    RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello");
  }
  unsigned firstLastBlockId = curId;
  unsigned lastMidId = curId - 1;
  ASSERT_EQ(3, totalSpecBlocks() - startValue);

  unsigned newLastBlockId = 0;
  unsigned lastLastBlockId = 0;
  const char *pp = nullptr;

  for (size_t ii = firstMidId; ii < firstLastBlockId; ++ii)
    RS::deleteDocument(ctx, ism, numToDocStr(ii).c_str());
  runGC([&]() {
      newLastBlockId = curId + 1;
      while (InvertedIndex_NumBlocks(iv) < 4)
        ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello"));
      lastLastBlockId = curId - 1;
      const IndexBlock *secondLastBlock = InvertedIndex_BlockRef(iv, InvertedIndex_NumBlocks(iv) - 2);
      pp = IndexBlock_Data(secondLastBlock);
    });

  // We add new documents to the last block after the fork, so we expect the GC to deny it.
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(3, InvertedIndex_NumBlocks(iv));

  // The pointer to the last gc-block, received from the fork
  const IndexBlock *secondLastBlock = InvertedIndex_BlockRef(iv, InvertedIndex_NumBlocks(iv) - 2);
  const char *gcpp = IndexBlock_Data(secondLastBlock);
  ASSERT_EQ(pp, gcpp);

  // Now search for the ID- let's be sure it exists
  auto vv = RS::search(ism, "@f1:{hello}");
  std::set<std::string> ss(vv.begin(), vv.end());
  ASSERT_NE(ss.end(), ss.find(numToDocStr(newLastBlockId)));
  ASSERT_NE(ss.end(), ss.find(numToDocStr(newLastBlockId - 1)));
  ASSERT_NE(ss.end(), ss.find(numToDocStr(lastLastBlockId)));
}

TEST_F(IndexGCTestTag, testDeleteDuringGCCleanup) {
  // Setup.
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  InvertedIndex *iv = getTagInvidx(&sctx, "f1", "hello");

  while (InvertedIndex_NumBlocks(iv) < 2) {
    RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello");
  }
  // Delete one document.
  RS::deleteDocument(ctx, ism, numToDocStr(1).c_str());
  ASSERT_EQ(RSGlobalStats.totalStats.logically_deleted, 1);

  RS::deleteDocument(ctx, ism, numToDocStr(2).c_str());
  ASSERT_EQ(fgc->deletedOrUpdatedDocsFromLastRun, 2);
  runGC();

  ASSERT_EQ(RSGlobalStats.totalStats.logically_deleted, 0);
}

TEST_F(IndexGCTestTag, testDeferredDeletesAreTakenByTheNextRun) {
  RS::addDocument(ctx, ism, "doc1", "f1", "hello");
  RS::addDocument(ctx, ism, "doc2", "f1", "hello");
  IndexSpec *spec = get_spec(ism);
  // Delete off the GC's books, the way slot cleanup does, then hand the count over.
  IndexSpec_LockWrite(spec);
  ASSERT_TRUE(IndexSpec_DeleteDocById_Unsafe(spec, 1));
  ASSERT_TRUE(IndexSpec_DeleteDocById_Unsafe(spec, 2));
  IndexSpec_Unlock(spec);
  IndexSpec_DeferGCDeletes(spec, 2);
  ASSERT_EQ(fgc->deletedOrUpdatedDocsFromLastRun, 0);
  ASSERT_EQ(RSGlobalStats.totalStats.logically_deleted, 0);

  runGC();

  // The run took the count: none is left on the spec, and the stat it raised was settled again.
  EXPECT_EQ(IndexSpec_TakeGCDeferredDeletes(spec), 0);
  EXPECT_EQ(fgc->deletedOrUpdatedDocsFromLastRun, 0);
  EXPECT_EQ(RSGlobalStats.totalStats.logically_deleted, 0);
}

/**
 * Test that simulates a pipe error during GC to trigger the error path.
 * This test verifies that the error handling doesn't cause double-free or other issues.
 */
TEST_F(IndexGCTestTag, testPipeErrorDuringGC) {
  // Add some documents to create work for the GC
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc3", "f1", "hello"));

  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc2"));
  runGC([&]() {
    // Close the read end of the pipe to trigger a poll() error (POLLNVAL),
    // simulating a pipe failure without waiting 3 minutes.
    close(fgc->pipe_read_fd);
    fgc->pipe_read_fd = -1;
  });

  // The GC should have failed, so no bytes should be collected
  // (or at least the operation should complete without crashing)
  ASSERT_EQ(0, fgc->stats.totalCollected);
}

/**
 * Test that closes the pipe while GC is actively applying changes.
 * This test runs multiple iterations to increase the chance of hitting different
 * code paths and timing windows during the apply phase.
 */
TEST_F(IndexGCTestTag, testPipeErrorDuringApply) {
  #ifdef __APPLE__
    GTEST_SKIP() << "Times out quite regularly on macOS";
  #endif
  volatile bool should_close = false;
  volatile bool thread_should_exit = false;
  volatile int delay_usec = 0;

  // Create a single closer thread that will be reused across all iterations
  std::thread closer([this, &should_close, &thread_should_exit, &delay_usec]() {
    while (!thread_should_exit) {
      if (should_close) {
        int fd = fgc->pipe_read_fd;
        usleep(delay_usec);
        fgc->pipe_read_fd = -1;  // Invalidate the fd so it's ok to close it
        close(fd); // Close the read end to simulate pipe error, and to not leak fds
        should_close = false;
      }
    }
  });

  // Run multiple iterations to increase coverage of different timing scenarios
  for (int iteration = 0; iteration < 500; iteration++) {
    // Add documents to create work for the GC
    std::string doc1 = "doc1_" + std::to_string(iteration);
    std::string doc2 = "doc2_" + std::to_string(iteration);
    std::string doc3 = "doc3_" + std::to_string(iteration);

    ASSERT_TRUE(RS::addDocument(ctx, ism, doc1.c_str(), "f1", "hello"));
    ASSERT_TRUE(RS::addDocument(ctx, ism, doc2.c_str(), "f1", "hello"));
    ASSERT_TRUE(RS::addDocument(ctx, ism, doc3.c_str(), "f1", "hello"));

    ASSERT_TRUE(RS::deleteDocument(ctx, ism, doc1.c_str()));
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, doc2.c_str()));
    runGC([&]() {
      delay_usec = iteration * 2;
      should_close = true;
    });

    // Wait for the closer to finish this iteration
    while (should_close) {
      usleep(1);
    }

    // Don't make any assertions about the state - it's timing dependent
    // The important thing is that we don't crash or have memory corruption
  }

  // Clean up the closer thread
  thread_should_exit = true;
  closer.join();
}

// Checks the spec's block accounting when the parent writes while the fork GC child runs. The
// root range's cardinality is covered by the Rust tests in
// `numeric_range_tree/tests/integration/gc.rs`.
TEST_F(IndexGCTestNumeric, testNumericBlocksSinceFork) {
  // The expected block counts assume every document lands in the root range, so the distinct
  // values must stay below the root's split cardinality
  // (`NumericRangeTree::MINIMUM_RANGE_CARDINALITY`).
  constexpr double values[] = {3.1416, 1.4142, 2.718, 1.618};
  constexpr size_t root_split_cardinality = 16;
  static_assert(std::size(values) < root_split_cardinality, "the root range must not split");
  const auto startValue = totalSpecBlocks();
  constexpr size_t docs_per_block = 100;
  size_t cur_id = 1;
  size_t expected_total_blocks = 0;
  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);

  /*
   * Scenario 1: Taking the child last block, and need to address the parent's changes.
   */

  // Add a block worth of documents with the same value
  expected_total_blocks++;
  for (size_t i = 0; i < docs_per_block; i++) {
    this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(values[0]).c_str());
  }

  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  for (size_t i = expected_total_blocks; i < cur_id; i += 10)
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, numToDocStr(i).c_str())) << "Failed to delete doc " << i;
  runGC([&]() {
    expected_total_blocks++;
    for (size_t i = 0; i < docs_per_block / 2; i++)
      this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(values[1]).c_str());
  });

  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);

  /*
   * Scenario 2: Not taking the child last block, and need to address the parent's changes (ignored + last block).
   */

  for (size_t i = expected_total_blocks; i < cur_id; i += 10)
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, numToDocStr(i).c_str())) << "Failed to delete doc " << i;
  runGC([&]() {
    for (size_t i = 0; i < docs_per_block / 2; i++)
      this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(values[2]).c_str());
    EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks) << "Number of blocks should not change";
    expected_total_blocks++;
    for (size_t i = 0; i < docs_per_block / 2; i++)
      this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(values[3]).c_str());
    EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  });

  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);

  /*
   * Scenario 3: Taking the child last block, without any parent changes.
   */

  for (size_t i = docs_per_block + 1; i <= 2 * docs_per_block; i++)
    RS::deleteDocument(ctx, ism, numToDocStr(i).c_str());
  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  runGC();

  expected_total_blocks--;
  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
}

/**
 * Disk consistency windows pause GC scheduling. These cover the state machine directly --
 * the flow tests cannot reach it, since nothing in OSS opens a window.
 */

static unsigned gcSchedFlags(GCContext *gc) {
  return RS_AtomicUintLoadRelaxed(&gc->schedFlags);
}

TEST_F(IndexGCTest, testConsistencyWindowPauseDisarmsAndResumeClears) {
  GCContext *gc = get_spec(ism)->gc;

  GCContext_PauseScheduling(gc);
  ASSERT_TRUE(gcSchedFlags(gc) & GC_SCHED_PAUSED);
  ASSERT_EQ(gc->timerID, 0);  // the window must not leave a timer behind
  ASSERT_TRUE(GCContext_IsEnabled(gc));  // pausing is not stopping

  GCContext_ResumeScheduling(gc);
  ASSERT_FALSE(gcSchedFlags(gc) & GC_SCHED_PAUSED);
}

TEST_F(IndexGCTest, testPauseSurvivesResumeOnAStoppedGC) {
  GCContext *gc = get_spec(ism)->gc;

  // A GC stopped by a debug command must stay stopped across a window.
  GCContext_StopFutureRuns(gc);
  GCContext_PauseScheduling(gc);
  GCContext_ResumeScheduling(gc);

  ASSERT_FALSE(GCContext_IsEnabled(gc));
  ASSERT_EQ(gc->timerID, 0);
}

TEST_F(IndexGCTest, testStartNowDoesNotQueueWhilePaused) {
  GCContext *gc = get_spec(ism)->gc;
  GCContext_StopFutureRuns(gc);  // StartNow requires a stopped GC
  GCContext_PauseScheduling(gc);

  GCContext_StartNow(gc);
  // Enabled, but nothing queued onto the paused pool: the resume path arms instead.
  ASSERT_TRUE(GCContext_IsEnabled(gc));
  ASSERT_FALSE(gcSchedFlags(gc) & GC_SCHED_RUN_PENDING);

  GCContext_ResumeScheduling(gc);
}

TEST_F(IndexGCTest, testWaitForPauseReturnsWhenNothingIsRunning) {
  GC_ThreadPoolPause();
  ASSERT_TRUE(GC_ThreadPoolWaitForPause(10));
  GC_ThreadPoolResume();
}

namespace {
// A job the test can hold on a GC-pool thread, following the MarkAndWait pattern in
// test_cpp_thpool.cpp: spin on an atomic rather than block on the pool's own synchronization.
struct ParkedGCJob {
  std::atomic<bool> running{false};
  std::atomic<bool> hold{true};

  static void Run(void *p) {
    ParkedGCJob *job = static_cast<ParkedGCJob *>(p);
    job->running = true;
    while (job->hold) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
    job->running = false;
  }
};
}  // namespace

// The drain's whole point is the job it cannot stop: pausing the pool keeps new jobs from
// starting, but one already on a thread must be waited out -- or timed out on.
TEST_F(IndexGCTest, testWaitForPauseTimesOutWhileAJobRuns) {
  ParkedGCJob job;
  // Unpark and unpause on every exit path. A failed ASSERT returns immediately, which would
  // otherwise leave a pool thread spinning on a dangling stack pointer and the pool paused
  // for every test after this one.
  struct Cleanup {
    ParkedGCJob &job;
    ~Cleanup() {
      job.hold = false;
      while (job.running) {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
      }
      GC_ThreadPoolResume();
    }
  } cleanup{job};

  GC_ThreadPoolAddJobForTests(ParkedGCJob::Run, &job);
  // Wait for Run itself, not for the in-progress count: the pool increments that when it pulls
  // the job, so a worker descheduled before its first statement would let Cleanup see
  // running == false and destroy `job` under it on an early ASSERT return.
  while (!job.running) {
    std::this_thread::sleep_for(std::chrono::microseconds(1));
  }

  GC_ThreadPoolPause();
  // This count is what the drain-timeout warning reports to the operator.
  ASSERT_EQ(GC_ThreadPoolJobsInProgress(), (size_t)1);
  ASSERT_FALSE(GC_ThreadPoolWaitForPause(50));

  job.hold = false;
  ASSERT_TRUE(GC_ThreadPoolWaitForPause(5000));
  ASSERT_EQ(GC_ThreadPoolJobsInProgress(), (size_t)0);
}

TEST(GCInterval, SecondsToMsClampsSoJitterCannotOverflow) {
  EXPECT_EQ(GC_IntervalSecToMs(0), 0);
  EXPECT_EQ(GC_IntervalSecToMs(1), 1000);
  EXPECT_EQ(GC_IntervalSecToMs(30), 30000);
  for (size_t sec : {(size_t)LLONG_MAX / 1000, (size_t)LLONG_MAX, SIZE_MAX}) {
    long long ms = GC_IntervalSecToMs(sec);
    EXPECT_GT(ms, 0);
    EXPECT_LE(ms, LLONG_MAX / 2);  // the jitter may add up to ms - 1
  }
}

/** A spec made under the THREAD policy, whose doc table tracks its deleted IDs. */
class ThreadGCTest : public IndexGCTestTag {
 protected:
  void SetUp() override {
    RSGlobalConfig.gcConfigParams.gcPolicy = GCPolicy_Thread;
    IndexGCTestTag::SetUp();
  }

  void TearDown() override {
    IndexGCTestTag::TearDown();
    RSGlobalConfig.gcConfigParams.gcPolicy = GCPolicy_Fork;
  }

  uint64_t trackedDeletions() {
    return DeletedDocs_Len(get_spec(ism)->docs.deletedDocs);
  }
};

TEST_F(ThreadGCTest, testRunScannersCollectsTheTrackedDeletions) {
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  ASSERT_EQ(1, trackedDeletions());

  uint64_t pruned = UINT64_MAX;
  ASSERT_EQ(THREAD_GC_DONE, ThreadGC_RunScanners(fgc, &pruned));
  ASSERT_EQ(1, pruned);

  ASSERT_EQ(0, trackedDeletions());
  ASSERT_EQ(1, get_spec(ism)->stats.numRecords);
  ASSERT_GT(fgc->stats.totalCollected, 0);
}

TEST_F(ThreadGCTest, testRunTakesOffWhatItPrunes) {
  for (const char *key : {"doc1", "doc2", "doc3"}) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, key, "f1", "hello"));
  }
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  // Deleted the way slot cleanup does: the count reaches the GC only through the spec.
  IndexSpec *spec = get_spec(ism);
  IndexSpec_LockWrite(spec);
  ASSERT_TRUE(IndexSpec_DeleteDocById_Unsafe(spec, 2));
  IndexSpec_DeferGCDeletes(spec, 1);
  IndexSpec_Unlock(spec);
  ASSERT_EQ(fgc->deletedOrUpdatedDocsFromLastRun, 1);
  ASSERT_EQ(2, trackedDeletions());

  runGC();

  EXPECT_EQ(0, trackedDeletions());
  EXPECT_EQ(fgc->deletedOrUpdatedDocsFromLastRun, 0);
  EXPECT_EQ(IndexSpec_TakeGCDeferredDeletes(spec), 0);
  EXPECT_EQ(RSGlobalStats.totalStats.logically_deleted, 0);
}

TEST_F(ThreadGCTest, testDeletedIdsCountInTheDocTableMemory) {
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, ("doc" + std::to_string(i)).c_str(), "f1", "hello"));
  }
  const DocTable *docs = &get_spec(ism)->docs;
  EXPECT_EQ(DocTable_DeletedDocsMemoryUsage(docs), 0) << "an empty set counts for nothing";
  for (int i = 0; i < 100; i += 3) {
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, ("doc" + std::to_string(i)).c_str()));
  }
  EXPECT_GT(DocTable_DeletedDocsMemoryUsage(docs), 0);

  runGC();

  EXPECT_EQ(DocTable_DeletedDocsMemoryUsage(docs), 0);
}

static void keepObject(void *) {}

TEST_F(ThreadGCTest, testRunScannersReportsADroppedSpec) {
  // Point the GC at an index reference that outlived its object, as after a drop.
  StrongRef strong = StrongRef_New(get_spec(ism), keepObject);
  WeakRef dropped = StrongRef_Demote(strong);
  StrongRef_Invalidate(strong);
  StrongRef_Release(strong);
  WeakRef index = fgc->index;
  fgc->index = dropped;

  uint64_t pruned = UINT64_MAX;
  ThreadGCResult result = ThreadGC_RunScanners(fgc, &pruned);

  fgc->index = index;
  WeakRef_Release(dropped);
  ASSERT_EQ(THREAD_GC_SPEC_DELETED, result);
  ASSERT_EQ(0, pruned);
}

// A THREAD cycle never forks, so whatever it does leaves nothing to retry -- but only a scheduled
// cycle says so; a forced one is its caller's, as under FORK.
TEST_F(ThreadGCTest, testScheduledCycleClearsAFailedFork) {
  fgc->schedState = INDEX_GC_SCHED_FORK_FAILED;
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  ASSERT_TRUE(IndexGC_RunCycle(fgc, nullptr, nullptr));
  ASSERT_EQ(fgc->stats.numCycles, (size_t)1);
  EXPECT_EQ(fgc->schedState, INDEX_GC_SCHED_OK);
}

TEST_F(ThreadGCTest, testForcedCycleLeavesTheSchedulingState) {
  fgc->schedState = INDEX_GC_SCHED_FORK_FAILED;
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  runGC();
  ASSERT_EQ(fgc->stats.numCycles, (size_t)1);
  EXPECT_EQ(fgc->schedState, INDEX_GC_SCHED_FORK_FAILED);
}

TEST(GCTimerDelayTest, testJitterRange) {
  EXPECT_EQ(GC_TimerDelayMs(0, 12345), 0);
  EXPECT_EQ(GC_TimerDelayMs(-1000, 12345), 0);  // not a config value, but never a negative delay
  EXPECT_EQ(GC_TimerDelayMs(10000, 0), 10000);
  EXPECT_EQ(GC_TimerDelayMs(10000, 7), 10007);
  EXPECT_EQ(GC_TimerDelayMs(10000, 19999), 19999);  // almost twice the interval, never twice
  EXPECT_EQ(GC_TimerDelayMs(10000, 20000), 10000);
}

TEST(GCTimerDelayTest, testSaturates) {
  const long long maxIntervalMs = LLONG_MAX / 2;  // GC_IntervalSecToMs's bound
  EXPECT_EQ(GC_TimerDelayMs(GC_IntervalSecToMs(SIZE_MAX), 0), GC_IntervalSecToMs(SIZE_MAX));
  EXPECT_EQ(GC_TimerDelayMs(LLONG_MAX, 0), maxIntervalMs);
  // The largest interval with the largest jitter it can draw still fits.
  EXPECT_EQ(GC_TimerDelayMs(LLONG_MAX, maxIntervalMs - 1), 2 * maxIntervalMs - 1);
  EXPECT_GT(GC_TimerDelayMs(LLONG_MAX, ULONG_MAX), 0);
}

// Past what one module timer can wait, the delay keeps its full value and jitter; the arming
// splits it into steps (GCSchedulerTest.testLongDelayIsArmedInSteps).
TEST(GCTimerDelayTest, testKeepsTheFullDelayPastOneTimer) {
  static_assert(RS_MAX_TIMER_DELAY_MS <= INT_MAX);
  EXPECT_EQ(GC_TimerDelayMs(GC_IntervalSecToMs(2147484), 0), 2147484000LL);
  EXPECT_EQ(GC_TimerDelayMs(GC_IntervalSecToMs(2592000), 2591999999UL), 5183999999LL);
  EXPECT_EQ(GC_TimerDelayMs(GC_IntervalSecToMs(1073742), 1073741999), 2147483999LL);
}

TEST(ModuleTimerTest, testDelayToDeadline) {
  const uint64_t now = 1000;
  EXPECT_EQ(RS_TimerDelayToDeadlineMs(0, now), 0);  // past
  EXPECT_EQ(RS_TimerDelayToDeadlineMs(now, now), 0);
  EXPECT_EQ(RS_TimerDelayToDeadlineMs(now + 1, now), 1);
  EXPECT_EQ(RS_TimerDelayToDeadlineMs(now + RS_MAX_TIMER_DELAY_MS, now), RS_MAX_TIMER_DELAY_MS);
  // Further out, the timer fires at the cap and re-arms for what is left.
  EXPECT_EQ(RS_TimerDelayToDeadlineMs(now + RS_MAX_TIMER_DELAY_MS + 1, now), RS_MAX_TIMER_DELAY_MS);
  EXPECT_EQ(RS_TimerDelayToDeadlineMs(UINT64_MAX, now), RS_MAX_TIMER_DELAY_MS);
}

/**
 * Timer arming and interval restarts. A hand-built GCContext with a fake GC behind its
 * callbacks and fake timers, so each scheduling state is set up directly rather than raced.
 */

namespace {
struct FakeGCTimers {
  static inline std::vector<mstime_t> armed;  // the delay of each arm, in order
  static inline std::vector<RedisModuleTimerID> stopped;
  static inline RedisModuleTimerID lastId = 0;
  static inline RedisModuleTimerProc lastProc = nullptr;
  static inline void *lastData = nullptr;
  static inline uint64_t nowMs = 0;
  static inline GCTimerPlan plan;  // what the fake GC asks for next
  static inline bool indexExists = true;
  static inline std::atomic<bool> terminated{false};

  static RedisModuleTimerID Create(RedisModuleCtx *, mstime_t period, RedisModuleTimerProc proc,
                                   void *data) {
    armed.push_back(period);
    lastProc = proc;
    lastData = data;
    return ++lastId;
  }
  // What Redis does when the last timer armed expires: its delay has passed.
  static void Fire() {
    nowMs += armed.back();
    lastProc(RSDummyContext, lastData);
  }
  static uint64_t Now() {
    return nowMs;
  }
  static int Stop(RedisModuleCtx *, RedisModuleTimerID id, void **) {
    stopped.push_back(id);
    return REDISMODULE_OK;
  }
  static GCTimerPlan NextTimer(void *) {
    return plan;
  }
  static bool IndexExists(void *) {
    return indexExists;
  }
  // The run that finds the index gone: no reschedule, and the context is freed after onTerm.
  static bool DroppedIndexRun(void *, GCForcedRun *) {
    return false;
  }
  static void OnTerm(void *) {
    terminated = true;
  }
};
}  // namespace

class GCSchedulerTest : public ::testing::Test {
 protected:
  GCContext gc{};
  const decltype(RedisModule_CreateTimer) savedCreateTimer = RedisModule_CreateTimer;
  const decltype(RedisModule_StopTimer) savedStopTimer = RedisModule_StopTimer;
  const decltype(GC_NowMs) savedNowMs = GC_NowMs;

  void SetUp() override {
    // Installing a timer API makes RS_IsMock false for the whole process, so a GC job still
    // running from an earlier test would take the real path into an event loop the mock has not.
    ASSERT_EQ(GC_ThreadPoolJobsInProgress(), (size_t)0);
    FakeGCTimers::armed.clear();
    FakeGCTimers::stopped.clear();
    FakeGCTimers::lastId = 0;
    FakeGCTimers::nowMs = 1000;
    GC_NowMs = FakeGCTimers::Now;
    FakeGCTimers::plan = {GC_TIMER_RUN_INTERVAL, 1000000};
    FakeGCTimers::indexExists = true;
    FakeGCTimers::terminated = false;
    // Installing a timer API also turns RS_IsMock off for the scheduler, which is what lets
    // it arm at all.
    RedisModule_CreateTimer = FakeGCTimers::Create;
    RedisModule_StopTimer = FakeGCTimers::Stop;
    gc.callbacks.nextTimer = FakeGCTimers::NextTimer;
    gc.callbacks.indexExists = FakeGCTimers::IndexExists;
  }

  void TearDown() override {
    RedisModule_CreateTimer = savedCreateTimer;
    RedisModule_StopTimer = savedStopTimer;
    GC_NowMs = savedNowMs;
  }

  void expectArmedWith(GCTimerKind kind, long long intervalMs) {
    ASSERT_NE(gc.timerID, 0);
    EXPECT_EQ(gc.timerID, FakeGCTimers::lastId);
    EXPECT_EQ(gc.armedPlan.kind, kind);
    EXPECT_EQ(gc.armedPlan.intervalMs, intervalMs);
    const mstime_t delay = FakeGCTimers::armed.back();
    EXPECT_GE(delay, intervalMs);
    EXPECT_LT(delay, intervalMs * 2);
  }
};

TEST_F(GCSchedulerTest, testArmRecordsThePlan) {
  GCContext_Start(&gc);
  ASSERT_EQ(FakeGCTimers::armed.size(), (size_t)1);
  expectArmedWith(GC_TIMER_RUN_INTERVAL, 1000000);
}

// The CONFIG SET scenario: the effective interval changes after the arm with no call, and the
// later call must still see that the timer is armed with something else.
TEST_F(GCSchedulerTest, testRestartsWhenTheIntervalDiffersFromTheArmedOne) {
  GCContext_Start(&gc);
  const RedisModuleTimerID first = gc.timerID;
  FakeGCTimers::plan.intervalMs = 10000;

  GCContext_OnIntervalChanged(&gc);
  EXPECT_EQ(FakeGCTimers::stopped, std::vector<RedisModuleTimerID>{first});
  ASSERT_EQ(FakeGCTimers::armed.size(), (size_t)2);
  expectArmedWith(GC_TIMER_RUN_INTERVAL, 10000);
}

// Repeated calls with the same value must not keep pushing the run back.
TEST_F(GCSchedulerTest, testNoRestartWhenTheIntervalMatchesTheArmedOne) {
  GCContext_Start(&gc);
  GCContext_OnIntervalChanged(&gc);
  GCContext_OnIntervalChanged(&gc);
  EXPECT_TRUE(FakeGCTimers::stopped.empty());
  EXPECT_EQ(FakeGCTimers::armed.size(), (size_t)1);

  // Nor a repeat of the value a restart just armed.
  FakeGCTimers::plan.intervalMs = 10000;
  GCContext_OnIntervalChanged(&gc);
  GCContext_OnIntervalChanged(&gc);
  EXPECT_EQ(FakeGCTimers::stopped.size(), (size_t)1);
  EXPECT_EQ(FakeGCTimers::armed.size(), (size_t)2);
}

TEST_F(GCSchedulerTest, testNoOpWithoutAGCContext) {
  GCContext_OnIntervalChanged(nullptr);  // an index created with NOGC
  EXPECT_TRUE(FakeGCTimers::armed.empty());
}

TEST_F(GCSchedulerTest, testNoOpWithNoTimer) {
  FakeGCTimers::plan.intervalMs = 10000;
  GCContext_OnIntervalChanged(&gc);  // never started
  EXPECT_TRUE(FakeGCTimers::armed.empty());
  EXPECT_EQ(gc.timerID, 0);
}

TEST_F(GCSchedulerTest, testNoOpWhenStopped) {
  GCContext_Start(&gc);
  GCContext_StopFutureRuns(&gc);
  FakeGCTimers::plan.intervalMs = 10000;
  GCContext_OnIntervalChanged(&gc);
  EXPECT_EQ(FakeGCTimers::armed.size(), (size_t)1);
  EXPECT_EQ(gc.timerID, 0);
}

TEST_F(GCSchedulerTest, testNoOpWhilePausedAndResumeArmsTheNewInterval) {
  GCContext_Start(&gc);
  GCContext_PauseScheduling(&gc);
  FakeGCTimers::plan.intervalMs = 10000;
  GCContext_OnIntervalChanged(&gc);
  EXPECT_EQ(FakeGCTimers::armed.size(), (size_t)1);
  EXPECT_EQ(gc.timerID, 0);

  GCContext_ResumeScheduling(&gc);
  ASSERT_EQ(FakeGCTimers::armed.size(), (size_t)2);
  expectArmedWith(GC_TIMER_RUN_INTERVAL, 10000);
}

TEST_F(GCSchedulerTest, testNoOpWhileARunIsPending) {
  // What a fired timer leaves once its run is queued: enabled, no timer, RUN_PENDING held.
  // That run's re-arm is the one to pick up the new interval; arming here would be a second.
  gc.enabled = true;
  RS_AtomicUintFetchOrRelaxed(&gc.schedFlags, GC_SCHED_RUN_PENDING);
  FakeGCTimers::plan.intervalMs = 10000;

  GCContext_OnIntervalChanged(&gc);
  EXPECT_TRUE(FakeGCTimers::armed.empty());
  EXPECT_EQ(gc.timerID, 0);
}

TEST_F(GCSchedulerTest, testShortDelayIsOneTimer) {
  GCContext_Start(&gc);
  EXPECT_EQ(gc.stepDeadlineMs, (uint64_t)0);  // its own expiry is the deadline
}

// Redis cannot serve a module timer past RS_MAX_TIMER_DELAY_MS (MOD-21060), so a longer delay is
// armed in steps, and the steps add up to the whole interval and jitter.
TEST_F(GCSchedulerTest, testLongDelayIsArmedInSteps) {
  const long long intervalMs = 5000000000LL;  // about 58 days, so at least three steps
  FakeGCTimers::plan.intervalMs = intervalMs;
  GCContext_Start(&gc);
  ASSERT_NE(gc.stepDeadlineMs, (uint64_t)0);

  // Each step fires and arms the next, never a run, until the one that ends at the deadline.
  for (int step = 0; gc.stepDeadlineMs != 0; ++step) {
    ASSERT_LT(step, 10);
    EXPECT_EQ(FakeGCTimers::armed.back(), RS_MAX_TIMER_DELAY_MS);
    FakeGCTimers::Fire();
    ASSERT_NE(gc.timerID, 0);
    EXPECT_FALSE(gcSchedFlags(&gc) & GC_SCHED_RUN_PENDING);
    EXPECT_EQ(gc.armedPlan.intervalMs, intervalMs);
  }
  ASSERT_GE(FakeGCTimers::armed.size(), (size_t)3);
  EXPECT_LE(FakeGCTimers::armed.back(), RS_MAX_TIMER_DELAY_MS);

  mstime_t total = 0;
  for (mstime_t step : FakeGCTimers::armed) {
    total += step;
  }
  EXPECT_GE(total, intervalMs);
  EXPECT_LT(total, intervalMs * 2);
}

// A step wakes the GC like any timer, so an interval changed since the arm takes over there
// rather than at a deadline the old interval set -- forever away for the config's maximum.
TEST_F(GCSchedulerTest, testStepArmsALoweredIntervalAfresh) {
  FakeGCTimers::plan.intervalMs = GC_IntervalSecToMs((size_t)LLONG_MAX);
  GCContext_Start(&gc);
  ASSERT_NE(gc.stepDeadlineMs, (uint64_t)0);
  FakeGCTimers::plan.intervalMs = 1000;

  FakeGCTimers::Fire();
  ASSERT_EQ(FakeGCTimers::armed.size(), (size_t)2);
  expectArmedWith(GC_TIMER_RUN_INTERVAL, 1000);
  EXPECT_EQ(gc.stepDeadlineMs, (uint64_t)0);
  EXPECT_FALSE(gcSchedFlags(&gc) & GC_SCHED_RUN_PENDING);  // a fresh arm, not a run
}

// A drop discovered at a step frees the context now, not at a deadline that may be weeks away.
TEST_F(GCSchedulerTest, testStepHandsADroppedIndexToARun) {
  // Heap-allocated like a real context, since the run that finds the drop frees it.
  GCContext *dropped = static_cast<GCContext *>(rm_calloc(1, sizeof(GCContext)));
  dropped->callbacks = gc.callbacks;
  dropped->callbacks.periodicCallback = FakeGCTimers::DroppedIndexRun;
  dropped->callbacks.onTerm = FakeGCTimers::OnTerm;
  FakeGCTimers::plan.intervalMs = 5000000000LL;
  GCContext_Start(dropped);
  ASSERT_NE(dropped->stepDeadlineMs, (uint64_t)0);

  FakeGCTimers::indexExists = false;
  FakeGCTimers::Fire();
  EXPECT_EQ(FakeGCTimers::armed.size(), (size_t)1);  // no further step
  for (int i = 0; i < 5000 && !FakeGCTimers::terminated; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  EXPECT_TRUE(FakeGCTimers::terminated);
}

// A waiting step is still the run-interval timer the restart is about.
TEST_F(GCSchedulerTest, testRestartDuringStepsArmsTheNewInterval) {
  FakeGCTimers::plan.intervalMs = 5000000000LL;
  GCContext_Start(&gc);
  FakeGCTimers::Fire();
  const RedisModuleTimerID step = gc.timerID;
  FakeGCTimers::plan.intervalMs = 10000;

  GCContext_OnIntervalChanged(&gc);
  EXPECT_EQ(FakeGCTimers::stopped, std::vector<RedisModuleTimerID>{step});
  expectArmedWith(GC_TIMER_RUN_INTERVAL, 10000);
  EXPECT_EQ(gc.stepDeadlineMs, (uint64_t)0);
}

TEST_F(GCSchedulerTest, testNoOpOnARetryTimer) {
  FakeGCTimers::plan = {GC_TIMER_RETRY, 5000};
  GCContext_Start(&gc);
  expectArmedWith(GC_TIMER_RETRY, 5000);

  FakeGCTimers::plan = {GC_TIMER_RUN_INTERVAL, 10000};
  GCContext_OnIntervalChanged(&gc);
  EXPECT_TRUE(FakeGCTimers::stopped.empty());
  EXPECT_EQ(FakeGCTimers::armed.size(), (size_t)1);
}

/**
 * The fork GC's scheduling state, which picks the next timer. Driven through real cycles.
 */

static GCTimerPlan nextForkGCTimer(RefManager *ism) {
  GCContext *gc = get_spec(ism)->gc;
  return gc->callbacks.nextTimer(gc->gcCtx);
}

TEST_F(IndexGCTestTag, testForcedRunLeavesTheSchedulingState) {
  fgc->schedState = INDEX_GC_SCHED_FORK_FAILED;
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  runGC();
  ASSERT_EQ(fgc->stats.numCycles, (size_t)1);  // the fork succeeded
  EXPECT_EQ(fgc->schedState, INDEX_GC_SCHED_FORK_FAILED);
  EXPECT_EQ(nextForkGCTimer(ism).kind, GC_TIMER_RETRY);
}

TEST_F(IndexGCTestTag, testForcedFailedForkLeavesTheSchedulingState) {
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  // A budget spent before the run starts: the forced cycle gives up without forking.
  GCForcedRun forced = GCForcedRun_New(1, GC_FORCED_RUN_RETRY_INTERVAL_MS);
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  ASSERT_TRUE(IndexGC_RunCycle(fgc, &forced, nullptr));
  ASSERT_EQ(forced.outcome, GC_FORCED_RUN_NO_FORK);
  EXPECT_EQ(fgc->schedState, INDEX_GC_SCHED_OK);
  EXPECT_EQ(nextForkGCTimer(ism).kind, GC_TIMER_RUN_INTERVAL);
}

TEST_F(IndexGCTestTag, testScheduledForkClearsAFailedFork) {
  fgc->schedState = INDEX_GC_SCHED_FORK_FAILED;
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  ASSERT_TRUE(IndexGC_RunCycle(fgc, nullptr, nullptr));
  ASSERT_EQ(fgc->stats.numCycles, (size_t)1);
  EXPECT_EQ(fgc->schedState, INDEX_GC_SCHED_OK);
}

TEST_F(IndexGCTest, testThresholdSkipAfterAFailedForkArmsTheRunInterval) {
  const GCSettings *settings = &RSGlobalConfig.gcConfigParams.gcSettings;
  fgc->schedState = INDEX_GC_SCHED_FORK_FAILED;
  GCTimerPlan next = nextForkGCTimer(ism);
  EXPECT_EQ(next.kind, GC_TIMER_RETRY);
  EXPECT_EQ(next.intervalMs, GC_IntervalSecToMs(settings->forkGcRetryInterval));

  RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 100;
  copyThresholdAtQueueTime();
  const bool rescheduled = IndexGC_RunCycle(fgc, nullptr, nullptr);
  RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 0;
  ASSERT_TRUE(rescheduled);
  ASSERT_EQ(fgc->stats.numCycles, (size_t)0);  // skipped, not forked

  next = nextForkGCTimer(ism);
  EXPECT_EQ(next.kind, GC_TIMER_RUN_INTERVAL);
  EXPECT_EQ(next.intervalMs, GC_IntervalSecToMs(settings->forkGcRunIntervalSec));
}

// The interval is read when arming, so a change since the last cycle is what the next timer gets.
TEST_F(IndexGCTest, testNextTimerReadsTheCurrentInterval) {
  GCSettings *settings = &RSGlobalConfig.gcConfigParams.gcSettings;
  const size_t savedRun = settings->forkGcRunIntervalSec;
  const size_t savedRetry = settings->forkGcRetryInterval;
  settings->forkGcRunIntervalSec = savedRun + 7;
  settings->forkGcRetryInterval = savedRetry + 3;

  const GCTimerPlan run = nextForkGCTimer(ism);
  fgc->schedState = INDEX_GC_SCHED_FORK_FAILED;
  const GCTimerPlan retry = nextForkGCTimer(ism);
  settings->forkGcRunIntervalSec = savedRun;
  settings->forkGcRetryInterval = savedRetry;

  EXPECT_EQ(run.intervalMs, GC_IntervalSecToMs(savedRun + 7));
  EXPECT_EQ(retry.intervalMs, GC_IntervalSecToMs(savedRetry + 3));
}

// ---------------------------------------------------------------------------------------------
// Per-index GC settings, and the clean threshold the GC copies when it queues a run.

namespace {
// Returns once every job queued on the GC pool before this call has finished. The pool has one
// worker and runs jobs of one priority in order, so a job queued last finishes last.
void drainGCPool() {
  std::atomic<bool> done{false};
  GC_ThreadPoolAddJobForTests([](void *p) { static_cast<std::atomic<bool> *>(p)->store(true); },
                              &done);
  while (!done) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

// Keeps the GC pool paused for the scope, so a queued run stays queued, and lets every queued
// job finish when the scope ends, however the test exits.
struct PausedGCPool {
  PausedGCPool() { GC_ThreadPoolPause(); }
  ~PausedGCPool() {
    GC_ThreadPoolResume();
    drainGCPool();
  }
};
}  // namespace

class IndexGCSettingsTest : public IndexGCTestTag {
 protected:
  const GCSettings savedGlobals = RSGlobalConfig.gcConfigParams.gcSettings;

  void TearDown() override {
    IndexGCTestTag::TearDown();
    RSGlobalConfig.gcConfigParams.gcSettings = savedGlobals;
  }

  IndexGCSettings &settings() { return get_spec(ism)->gcSettings; }
  size_t &globalThreshold() { return RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold; }
  size_t &globalInterval() { return RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec; }

  // Runs a scheduled cycle, and returns whether it got past the clean threshold.
  bool scheduledCycleCollects() {
    const size_t before = fgc->stats.numCycles;
    EXPECT_TRUE(IndexGC_RunCycle(fgc, nullptr, nullptr));
    return fgc->stats.numCycles != before;
  }

  void addAndDelete(size_t n) {
    for (size_t i = 0; i < n; ++i) {
      const std::string key = "deleted" + std::to_string(i);
      ASSERT_TRUE(RS::addDocument(ctx, ism, key.c_str(), tag_field_name, "hello"));
      ASSERT_TRUE(RS::deleteDocument(ctx, ism, key.c_str()));
    }
  }

  // A second GC for the fixture's spec whose index reference is already dropped, as for a GC
  // whose run discovers the drop. `threshold` is the override the spec had when it was created.
  GCContext *createGCForDroppedIndex(size_t threshold) {
    settings().cleanThreshold = {true, threshold};
    StrongRef strong = StrongRef_New(get_spec(ism), keepObject);
    GCContext *gc = GCContext_CreateGC(strong, GCPolicy_Fork);
    StrongRef_Invalidate(strong);
    StrongRef_Release(strong);
    return gc;
  }

  static void keepObject(void *) {}
};

class IndexGCSettingsPolicyTest : public IndexGCSettingsTest,
                                  public ::testing::WithParamInterface<GCPolicy> {
 protected:
  void SetUp() override {
    RSGlobalConfig.gcConfigParams.gcPolicy = GetParam();
    IndexGCSettingsTest::SetUp();
  }

  void TearDown() override {
    IndexGCSettingsTest::TearDown();
    RSGlobalConfig.gcConfigParams.gcPolicy = GCPolicy_Fork;
  }
};

TEST_P(IndexGCSettingsPolicyTest, testScheduledCycleUsesTheIndexThreshold) {
  globalThreshold() = 0;
  addAndDelete(2);

  settings().cleanThreshold = {true, 3};
  copyThresholdAtQueueTime();
  EXPECT_FALSE(scheduledCycleCollects());

  settings().cleanThreshold = {true, 2};
  copyThresholdAtQueueTime();
  EXPECT_TRUE(scheduledCycleCollects());
  ASSERT_EQ(fgc->deletedOrUpdatedDocsFromLastRun, 0);

  // 0 collects with nothing to collect.
  settings().cleanThreshold = {true, 0};
  globalThreshold() = 100;
  copyThresholdAtQueueTime();
  EXPECT_TRUE(scheduledCycleCollects());
}

TEST_P(IndexGCSettingsPolicyTest, testUnsetThresholdFollowsTheGlobal) {
  globalThreshold() = 5;
  copyThresholdAtQueueTime();
  addAndDelete(1);
  EXPECT_FALSE(scheduledCycleCollects());

  // A change reaches the next queued run, not the one already queued.
  globalThreshold() = 1;
  EXPECT_FALSE(scheduledCycleCollects());
  copyThresholdAtQueueTime();
  EXPECT_TRUE(scheduledCycleCollects());
}

TEST_P(IndexGCSettingsPolicyTest, testQueuedRunKeepsItsThreshold) {
  settings().cleanThreshold = {true, 0};
  copyThresholdAtQueueTime();
  settings().cleanThreshold = {true, 1000};
  EXPECT_TRUE(scheduledCycleCollects());
}

TEST_P(IndexGCSettingsPolicyTest, testForcedCycleIgnoresTheThreshold) {
  settings().cleanThreshold = {true, 1000};
  copyThresholdAtQueueTime();
  const size_t before = fgc->stats.numCycles;
  runGC();
  EXPECT_EQ(fgc->stats.numCycles, before + 1);
}

INSTANTIATE_TEST_SUITE_P(Policies, IndexGCSettingsPolicyTest,
                         ::testing::Values(GCPolicy_Fork, GCPolicy_Thread),
                         [](const ::testing::TestParamInfo<GCPolicy> &info) {
                           return info.param == GCPolicy_Fork ? "Fork" : "Thread";
                         });

TEST_F(IndexGCSettingsTest, testCreatedWithTheIndexThreshold) {
  settings().cleanThreshold = {true, 42};
  StrongRef strong = StrongRef_New(get_spec(ism), keepObject);
  GCContext *gc = GCContext_CreateGC(strong, GCPolicy_Fork);
  EXPECT_EQ(static_cast<IndexGC *>(gc->gcCtx)->cleanThreshold, 42);
  GCContext_StopMock(gc);
  StrongRef_Release(strong);
}

TEST_F(IndexGCSettingsTest, testQueueRunCopiesTheThreshold) {
  GCContext *gc = get_spec(ism)->gc;
  GCContext_StopFutureRuns(gc);  // StartNow requires a stopped GC
  settings().cleanThreshold = {true, 1000};
  {
    PausedGCPool paused;
    GCContext_StartNow(gc);
    ASSERT_TRUE(gcSchedFlags(gc) & GC_SCHED_RUN_PENDING);
    EXPECT_EQ(fgc->cleanThreshold, 1000);

    // While that run holds the context, queueing again changes nothing: the run may already be
    // reading the copy.
    settings().cleanThreshold = {true, 7};
    GCContext_StopFutureRuns(gc);
    GCContext_StartNow(gc);
    EXPECT_EQ(fgc->cleanThreshold, 1000);
  }
  EXPECT_FALSE(gcSchedFlags(gc) & GC_SCHED_RUN_PENDING);  // the run finished
}

// GCContext_FinishDrop and a timer that fires after the drop both queue the run through
// GCContext_QueueRun, which finds the index gone; the run then frees the GC.
TEST_F(IndexGCSettingsTest, testQueueRunAfterTheDropKeepsTheCopy) {
  GCContext *gc = createGCForDroppedIndex(42);
  settings().cleanThreshold = {true, 9};
  PausedGCPool paused;
  GCContext_FinishDrop(gc);
  EXPECT_TRUE(gcSchedFlags(gc) & GC_SCHED_RUN_PENDING);
  EXPECT_EQ(static_cast<IndexGC *>(gc->gcCtx)->cleanThreshold, 42);
}

TEST_F(IndexGCSettingsTest, testRunIntervalHelper) {
  globalInterval() = 30;
  EXPECT_EQ(IndexGC_RunIntervalSec(fgc), 30);

  settings().runIntervalSec = {true, 5};
  globalInterval() = 60;
  EXPECT_EQ(IndexGC_RunIntervalSec(fgc), 5);

  settings().runIntervalSec = {false, 0};
  EXPECT_EQ(IndexGC_RunIntervalSec(fgc), 60);
}

TEST_F(IndexGCSettingsTest, testRunIntervalHelperAfterTheDrop) {
  settings().runIntervalSec = {true, 5};
  globalInterval() = 60;
  GCContext *gc = createGCForDroppedIndex(0);
  // An unlinked index still arms, with the global interval.
  EXPECT_EQ(IndexGC_RunIntervalSec(static_cast<IndexGC *>(gc->gcCtx)), 60);
  GCContext_StopMock(gc);
}

TEST(IndexGCSettingsHelpers, testEffectiveValues) {
  const GCSettings saved = RSGlobalConfig.gcConfigParams.gcSettings;
  RSGlobalConfig.gcConfigParams.gcSettings.forkGcRunIntervalSec = 30;
  RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 100;

  IndexGCSettings s;
  IndexGCSettings_Init(&s);
  EXPECT_EQ(IndexGCSettings_RunIntervalSec(&s), 30);
  EXPECT_EQ(IndexGCSettings_CleanThreshold(&s), 100);

  s.runIntervalSec = {true, 1};
  s.cleanThreshold = {true, 0};
  EXPECT_EQ(IndexGCSettings_RunIntervalSec(&s), 1);
  EXPECT_EQ(IndexGCSettings_CleanThreshold(&s), 0);

  RSGlobalConfig.gcConfigParams.gcSettings = saved;
}

TEST(IndexGCSettingsHelpers, testValueRanges) {
  EXPECT_FALSE(IndexGCSettings_IsValid(INDEX_GC_SETTING_RUN_INTERVAL, 0));
  EXPECT_TRUE(IndexGCSettings_IsValid(INDEX_GC_SETTING_RUN_INTERVAL, 1));
  EXPECT_TRUE(IndexGCSettings_IsValid(INDEX_GC_SETTING_RUN_INTERVAL, LLONG_MAX));
  EXPECT_FALSE(IndexGCSettings_IsValid(INDEX_GC_SETTING_RUN_INTERVAL, (size_t)LLONG_MAX + 1));
  EXPECT_TRUE(IndexGCSettings_IsValid(INDEX_GC_SETTING_CLEAN_THRESHOLD, 0));
  EXPECT_TRUE(IndexGCSettings_IsValid(INDEX_GC_SETTING_CLEAN_THRESHOLD, LLONG_MAX));
  EXPECT_FALSE(IndexGCSettings_IsValid(INDEX_GC_SETTING_CLEAN_THRESHOLD, (size_t)LLONG_MAX + 1));
  EXPECT_FALSE(IndexGCSettings_IsValid((IndexGCSettingId)99, 1));
}
