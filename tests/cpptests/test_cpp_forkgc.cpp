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
#include "fork_gc.h"
#include "tag_index.h"
#include "rules.h"
#include "query_error_ffi.h"
#include "inverted_index.h"
#include "numeric_range_tree.h"
#include "numeric_range_tree_ffi.h"
#include "info/global_stats.h"
#include "redis_index.h"
#include "index_utils.h"
#include "notifications.h"
extern "C" {
#include "util/dict.h"
}

#include <set>
#include <random>
#include <unordered_set>
#include <thread>

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

class FGCTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  RefManager *ism;
  ForkGC *fgc;

  void SetUp() override {
    Initialize_KeyspaceNotifications();
    ism = createSpec(ctx);
    RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 0;
    RSGlobalStats.totalStats.logically_deleted = 0;
    fgc = reinterpret_cast<ForkGC *>(get_spec(ism)->gc->gcCtx);
  }

  void TearDown() override {
    freeSpec(ism);
  }

  size_t addDocumentWrapper(const char *docid, const char *field, const char *value) {
    return ::addDocumentWrapper(ctx, ism, docid, field, value);
  }

  size_t totalSpecBlocks() {
    return __atomic_load_n(&get_spec(ism)->stats.totalInvertedIndexBlocks, __ATOMIC_RELAXED);
  }

  void runGC(std::function<void()> beforeApply = {}) {
    FGCHook hook{
      [](void *p) { if (auto &f = *static_cast<std::function<void()>*>(p)) f(); },
      &beforeApply
    };
    FGC_RunCycle(fgc, true, beforeApply ? &hook : nullptr);
  }
};

static InvertedIndex *getTagInvidx(RedisSearchCtx *sctx, const char *field,
                                   const char *value) {
  const FieldSpec *fs = IndexSpec_GetFieldWithLength(sctx->spec, "f1", strlen("f1"));
  auto tix = TagIndex_Ensure(const_cast<FieldSpec *>(fs), NULL);
  size_t sz;
  auto iv = TagIndex_OpenIndex(tix, "hello", strlen("hello"), CREATE_INDEX, &sz);
  sctx->spec->stats.invertedSize += sz;
  return iv;
}

class FGCTestTag : public FGCTest {
protected:
  const char *tag_field_name = "f1";

  void SetUp() override {
    FGCTest::SetUp();
    RediSearch_CreateTagField(ism, "f1");
  }
};

class FGCTestNumeric : public FGCTest {
protected:
  const char *numeric_field_name = "n";

  void SetUp() override {
    FGCTest::SetUp();
    RediSearch_CreateNumericField(this->ism, numeric_field_name);
  }
};


/**
 * This test purpose is to validate inverted indexes size statistics are updated correctly by the gc.
 * Since The numeric tree inverted index size directly affect the spec statistics updates,
 * this test ensure they are aligned.
 */
TEST_F(FGCTestNumeric, testNumeric) {

  size_t total_mem = 0;

  // No inverted indices were created yet
  size_t spec_inv_index_mem_stats = (get_spec(ism))->stats.invertedSize;
  ASSERT_EQ(total_mem, spec_inv_index_mem_stats);

  size_t num_docs = 1000;
  for (size_t i = 0 ; i < num_docs ; i++) {
    std::string val = std::to_string(i);
    total_mem += this->addDocumentWrapper(numToDocStr(i).c_str(), numeric_field_name, val.c_str());
  }

  NumericRangeTree *rt = getNumericTree(get_spec(ism), numeric_field_name);
  spec_inv_index_mem_stats = (get_spec(ism))->stats.invertedSize;
  size_t numeric_tree_mem = NumericRangeTree_GetInvertedIndexesSize(rt);
  ASSERT_EQ(total_mem, numeric_tree_mem);
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

  size_t spec_inv_index_mem_stats_after_delete = (get_spec(ism))->stats.invertedSize;
  size_t numeric_tree_mem_after_delete = NumericRangeTree_GetInvertedIndexesSize(rt);
  ASSERT_EQ(spec_inv_index_mem_stats_after_delete, numeric_tree_mem_after_delete);

  size_t collected_bytes = numeric_tree_mem - numeric_tree_mem_after_delete;
  // gc stats
  ASSERT_EQ(collected_bytes, fgc->stats.totalCollected);
}

/** Mark one of the entries in the last block as deleted while the child is running.
 * This means the number of original entries recorded by the child and the current number of
 * entries are equal, and we conclude there weren't any changes in the parent to the block buffer.
 * Make sure the modification take place. */
TEST_F(FGCTestTag, testRemoveEntryFromLastBlock) {
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
TEST_F(FGCTestTag, testRemoveLastBlockWhileUpdate) {
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
TEST_F(FGCTestTag, testModifyLastBlockWhileAddingNewBlocks) {
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
    // Block 0 is sealed (Arc-shared with the live index), so its buffer pointer
    // stays valid after we drop the snapshot — the live `iv` still holds the
    // sealed Arc.
    InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
    originalData = IndexBlock_Data(InvertedIndexSnapshot_BlockRef(snap, 0));
    InvertedIndexSnapshot_Free(snap);
    invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  });

  InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
  const char *afterGcData = IndexBlock_Data(InvertedIndexSnapshot_BlockRef(snap, 0));
  InvertedIndexSnapshot_Free(snap);
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
TEST_F(FGCTestTag, testRemoveAllBlocksWhileUpdateLast) {
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
  // The last block is the `in_progress` slot, which the snapshot deep-clones —
  // pointer identity across snapshots is meaningless. Instead, capture the
  // block's metadata pre- and post-GC and assert it didn't change. That matches
  // the test's intent ("verify GC was denied on the last block").
  uint16_t origLastNumEntries = 0;
  t_docId origLastFirstId = 0;
  t_docId origLastLastId = 0;

  for (unsigned i = 1; i < curId; i++) {
    size_t n = snprintf(buf, sizeof(buf), "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }
  ASSERT_EQ(0, sctx.spec->stats.scoring.numDocuments);
  runGC([&]() {
      invertedSizeBeforeApply = sctx.spec->stats.invertedSize;
      size_t n = snprintf(buf, sizeof(buf), "doc%u", curId);
      lastBlockMemory += this->addDocumentWrapper(buf, "f1", "hello");
      InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
      const IndexBlock *lastBlock = InvertedIndexSnapshot_BlockRef(
        snap, InvertedIndexSnapshot_NumBlocks(snap) - 1);
      origLastNumEntries = IndexBlock_NumEntries(lastBlock);
      origLastFirstId = IndexBlock_FirstId(lastBlock);
      origLastLastId = IndexBlock_LastId(lastBlock);
      InvertedIndexSnapshot_Free(snap);
    });

  // gc stats - make sure we skipped the last block
  {
    InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
    const IndexBlock *lastBlock = InvertedIndexSnapshot_BlockRef(
      snap, InvertedIndexSnapshot_NumBlocks(snap) - 1);
    ASSERT_EQ(origLastNumEntries, IndexBlock_NumEntries(lastBlock));
    ASSERT_EQ(origLastFirstId, IndexBlock_FirstId(lastBlock));
    ASSERT_EQ(origLastLastId, IndexBlock_LastId(lastBlock));
    InvertedIndexSnapshot_Free(snap);
  }
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  // In this case the spec contains only one valid document.
  ASSERT_EQ(1, sctx.spec->stats.scoring.numDocuments);
  // But the last block deletion was skipped.
  ASSERT_EQ(2, sctx.spec->stats.numRecords);
  // After GC, the surviving last block lives in `in_progress` (the slot that
  // got the in-runGC write) — `pending` is empty, `sealed` is empty. The
  // invariant residual is:
  //   96  sizeof InvertedIndex (sealed Arc + pending Vec + Option<IndexBlock>
  //                             + small fields)
  //   24  Arc<ThinVec> heap for the empty `sealed`
  //    2  encoded buffer in `in_progress` (2 entries: 1 from the pre-runGC
  //       tail write + 1 from the in-runGC add)
  // The Option<IndexBlock> slot itself is counted in `size_of::<Self>()` —
  // `lastBlockMemory` (the per-add `mem_growth` for an in_progress write) is
  // only buffer growth and is not part of the residual.
  ASSERT_EQ(96 + 24 + 2, sctx.spec->stats.invertedSize);
  ASSERT_EQ(1, totalSpecBlocks() - startValue);
}

/**
 * Repair the last block, while adding more documents to it and removing a middle block.
 * This test should be checked with valgrind as it cause index corruption.
 */
TEST_F(FGCTestTag, testRepairLastBlockWhileRemovingMiddle) {
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
  {
    InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
    lastBlockEntries = IndexBlock_NumEntries(InvertedIndexSnapshot_BlockRef(snap, 2));
    InvertedIndexSnapshot_Free(snap);
  }
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
  {
    InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
    // The first entry was deleted. first block starts from docId = 2.
    ASSERT_EQ(2, IndexBlock_FirstId(InvertedIndexSnapshot_BlockRef(snap, 0)));
    // Last block was moved.
    ASSERT_EQ(lastBlockFirstId, IndexBlock_FirstId(InvertedIndexSnapshot_BlockRef(snap, 1)));
    ASSERT_EQ(3, IndexBlock_NumEntries(InvertedIndexSnapshot_BlockRef(snap, 1)));
    InvertedIndexSnapshot_Free(snap);
  }
}

/**
 * Repair the last block, while adding more documents to it...
 */
TEST_F(FGCTestTag, testRepairLastBlock) {
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
TEST_F(FGCTestTag, testRepairMiddleRemoveLast) {
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
TEST_F(FGCTestTag, testRemoveMiddleBlock) {
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
      // Second-last block is sealed (Arc-shared with the live index), so the
      // buffer pointer survives dropping the snapshot.
      InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
      const IndexBlock *secondLastBlock = InvertedIndexSnapshot_BlockRef(
        snap, InvertedIndexSnapshot_NumBlocks(snap) - 2);
      pp = IndexBlock_Data(secondLastBlock);
      InvertedIndexSnapshot_Free(snap);
    });

  // We add new documents to the last block after the fork, so we expect the GC to deny it.
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(3, InvertedIndex_NumBlocks(iv));

  // The pointer to the last gc-block, received from the fork
  const char *gcpp = nullptr;
  {
    InvertedIndexSnapshot *snap = InvertedIndex_Snapshot(iv);
    const IndexBlock *secondLastBlock = InvertedIndexSnapshot_BlockRef(
      snap, InvertedIndexSnapshot_NumBlocks(snap) - 2);
    gcpp = IndexBlock_Data(secondLastBlock);
    InvertedIndexSnapshot_Free(snap);
  }
  ASSERT_EQ(pp, gcpp);

  // Now search for the ID- let's be sure it exists
  auto vv = RS::search(ism, "@f1:{hello}");
  std::set<std::string> ss(vv.begin(), vv.end());
  ASSERT_NE(ss.end(), ss.find(numToDocStr(newLastBlockId)));
  ASSERT_NE(ss.end(), ss.find(numToDocStr(newLastBlockId - 1)));
  ASSERT_NE(ss.end(), ss.find(numToDocStr(lastLastBlockId)));
}

TEST_F(FGCTestTag, testDeleteDuringGCCleanup) {
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

/**
 * Test that simulates a pipe error during GC to trigger the error path.
 * This test verifies that the error handling doesn't cause double-free or other issues.
 */
TEST_F(FGCTestTag, testPipeErrorDuringGC) {
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
TEST_F(FGCTestTag, testPipeErrorDuringApply) {
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

TEST_F(FGCTestNumeric, testNumericBlocksSinceFork) {
  const auto startValue = totalSpecBlocks();
  constexpr size_t docs_per_block = 100;
  constexpr size_t first_split_card = 16; // from `numeric_index.c`
  size_t cur_cardinality = 0;
  size_t cur_id = 1;
  size_t expected_total_blocks = 0;
  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);

  /*
   * Scenario 1: Taking the child last block, and need to address the parent's changes.
   */

  // Add a block worth of documents with the same value
  ASSERT_LT(++cur_cardinality, first_split_card);
  expected_total_blocks++;
  for (size_t i = 0; i < docs_per_block; i++) {
    this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(3.1416).c_str());
  }
  NumericRangeTree *rt = getNumericTree(get_spec(ism), numeric_field_name);
  const NumericRangeNode *root = NumericRangeTree_GetRoot(rt);
  const NumericRange *rootRange = NumericRangeNode_GetRange(root);

  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  ASSERT_TRUE(rootRange);
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rootRange));
  for (size_t i = expected_total_blocks; i < cur_id; i += 10)
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, numToDocStr(i).c_str())) << "Failed to delete doc " << i;
  runGC([&]() {
    ASSERT_LT(++cur_cardinality, first_split_card);
    expected_total_blocks++;
    for (size_t i = 0; i < docs_per_block / 2; i++)
      this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(1.4142).c_str());
  });

  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  // Refresh root/range references as tree may have changed
  root = NumericRangeTree_GetRoot(rt);
  rootRange = NumericRangeNode_GetRange(root);
  ASSERT_TRUE(rootRange);
  // The fork is not aware of the new value added after the fork, but the parent should update the
  // cardinality after applying the fork's changes.
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rootRange));

  /*
   * Scenario 2: Not taking the child last block, and need to address the parent's changes (ignored + last block).
   */

  for (size_t i = expected_total_blocks; i < cur_id; i += 10)
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, numToDocStr(i).c_str())) << "Failed to delete doc " << i;
  runGC([&]() {
    ASSERT_LT(++cur_cardinality, first_split_card);
    for (size_t i = 0; i < docs_per_block / 2; i++)
      this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(2.718).c_str());
    EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks) << "Number of blocks should not change";
    ASSERT_LT(++cur_cardinality, first_split_card);
    expected_total_blocks++;
    for (size_t i = 0; i < docs_per_block / 2; i++)
      this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(1.618).c_str());
    EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  });

  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  root = NumericRangeTree_GetRoot(rt);
  rootRange = NumericRangeNode_GetRange(root);
  ASSERT_TRUE(rootRange);
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rootRange));

  /*
   * Scenario 3: Taking the child last block, without any parent changes.
   */

  for (size_t i = docs_per_block + 1; i <= 2 * docs_per_block; i++)
    RS::deleteDocument(ctx, ism, numToDocStr(i).c_str());
  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  runGC();

  expected_total_blocks--;
  EXPECT_EQ(totalSpecBlocks() - startValue, expected_total_blocks);
  // Refresh root/range references as tree may have changed
  root = NumericRangeTree_GetRoot(rt);
  rootRange = NumericRangeNode_GetRange(root);
  ASSERT_TRUE(rootRange);
  // We had 2 values in the second block and in it only. We expect the cardinality to decrease by 2.
  cur_cardinality -= 2;
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rootRange));
}
