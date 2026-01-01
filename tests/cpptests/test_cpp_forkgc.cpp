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
#include "redisearch_api.h"
#include "fork_gc.h"
#include "tag_index.h"
#include "rules.h"
#include "query_error.h"
#include "inverted_index.h"
#include "numeric_index.h"
#include "rwlock.h"
#include "info/global_stats.h"
#include "redis_index.h"
#include "index_utils.h"
extern "C" {
#include "util/dict.h"
}

#include <set>
#include <random>
#include <unordered_set>
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

typedef struct {
  void *fgc;
  RefManager *ism;
  volatile bool runGc;
} args_t;

void *cbWrapper(void *args) {
  args_t *fgcArgs = (args_t *)args;
  GCContext *gc = get_spec(fgcArgs->ism)->gc;
  ForkGC *fgc = reinterpret_cast<ForkGC *>(gc->gcCtx);

  while (true) {
    // sync thread
    while (fgcArgs->runGc && fgc->pauseState != FGC_PAUSED_CHILD) {
      usleep(500);
    }
    if (!fgcArgs->runGc) {
      break;
    }

    // run ForkGC
    gc->callbacks.periodicCallback(fgc);
  }
  return NULL;
}

class FGCTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  RefManager *ism;
  ForkGC *fgc;
  args_t args;
  pthread_t thread;

  void SetUp() override {
    ism = createSpec(ctx);
    RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold = 0;
    RSGlobalStats.totalStats.logically_deleted = 0;
    runGcThread();
  }

  void runGcThread() {
    fgc = reinterpret_cast<ForkGC *>(get_spec(ism)->gc->gcCtx);
    thread = {0};
    args = {.fgc = fgc, .ism = ism, .runGc = true};

    pthread_create(&thread, NULL, cbWrapper, &args);
  }

  void TearDown() override {
    args.runGc = false;
    // wait for the gc thread to finish current loop and exit the thread
    pthread_join(thread, NULL);
    freeSpec(ism);
  }

  size_t addDocumentWrapper(const char *docid, const char *field, const char *value) {
    return ::addDocumentWrapper(ctx, ism, docid, field, value);
  }
};

static InvertedIndex *getTagInvidx(RedisSearchCtx *sctx, const char *field,
                                   const char *value) {
  RedisModuleString *fmtkey = IndexSpec_GetFormattedKeyByName(sctx->spec, "f1", INDEXFLD_T_TAG);
  auto tix = TagIndex_Open(sctx->spec, fmtkey, CREATE_INDEX);
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
  size_t numeric_tree_mem = rt->invertedIndexesSize;
  ASSERT_EQ(total_mem, numeric_tree_mem);
  ASSERT_EQ(total_mem, spec_inv_index_mem_stats);

  // Delete some docs
  FGC_WaitBeforeFork(fgc);
  size_t deleted_docs = num_docs / 4;
  std::mt19937 gen(42);
  std::uniform_int_distribution<size_t> dis(0, num_docs - 1);
  std::unordered_set<size_t> generated_numbers;
  for (size_t i = 0; i < deleted_docs; ++i) {
    size_t random_id = dis(gen);
    while (generated_numbers.find(random_id) != generated_numbers.end()) {
        random_id = dis(gen);
    }
    generated_numbers.insert(random_id);
    auto rv = RS::deleteDocument(ctx, ism, numToDocStr(random_id).c_str());
    ASSERT_TRUE(rv) << "Failed to delete doc " << random_id << " at iteration " << i;
  }
  FGC_ForkAndWaitBeforeApply(fgc);
  FGC_Apply(fgc);

  size_t spec_inv_index_mem_stats_after_delete = (get_spec(ism))->stats.invertedSize;
  size_t numeric_tree_mem_after_delete = rt->invertedIndexesSize;
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
  const auto startValue = TotalIIBlocks;

  // Add two documents
  size_t docSize = this->addDocumentWrapper("doc1", "f1", "hello");
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));
  /**
   * To properly test this; we must ensure that the gc is forked AFTER
   * the deletion, but BEFORE the addition.
   */
  FGC_WaitBeforeFork(fgc);
  auto rv = RS::deleteDocument(ctx, ism, "doc1");
  ASSERT_TRUE(rv);

  /**
   * This function allows the GC to perform fork(2), but makes it wait
   * before it begins receiving results.
   */
  FGC_ForkAndWaitBeforeApply(fgc);
  rv = RS::deleteDocument(ctx, ism, "doc2");

  size_t invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  /** This function allows the gc to receive the results */
  FGC_Apply(fgc);

  // gc stats
  ASSERT_EQ(0, fgc->stats.gcBlocksDenied);
  // The buffer's initial capacity is INDEX_BLOCK_INITIAL_CAP, the function
  // IndexBlock_Repair() shrinks the buffer to the number of valid entries in
  // the block, collecting the remaining memory.
  ASSERT_EQ(INDEX_BLOCK_INITIAL_CAP - 1, fgc->stats.totalCollected);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  ASSERT_EQ(0, (get_spec(ism))->stats.numDocuments);
  ASSERT_EQ(1, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply - fgc->stats.totalCollected, (get_spec(ism))->stats.invertedSize);
  ASSERT_EQ(1, TotalIIBlocks - startValue);
}

/**
 * In this test, the child process needs to delete the only and last block in the index,
 * while the main process adds a document to it.
 * In this case, we discard the changes collected by the child process, so eventually the
 * index contains both documents.
 * */
TEST_F(FGCTestTag, testRemoveLastBlockWhileUpdate) {
  const auto startValue = TotalIIBlocks;
  // Add a document
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  /**
   * To properly test this; we must ensure that the gc is forked AFTER
   * the deletion, but BEFORE the addition.
   */
  FGC_WaitBeforeFork(fgc);
  auto rv = RS::deleteDocument(ctx, ism, "doc1");
  ASSERT_TRUE(rv);

  /**
   * This function allows the GC to perform fork(2), but makes it wait
   * before it begins receiving results.
   */
  FGC_ForkAndWaitBeforeApply(fgc);
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));

  size_t invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  /** This function allows the gc to receive the results */
  FGC_Apply(fgc);

  // gc stats
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(0, fgc->stats.totalCollected);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  ASSERT_EQ(1, (get_spec(ism))->stats.numDocuments);
  ASSERT_EQ(2, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply, (get_spec(ism))->stats.invertedSize);
  ASSERT_EQ(1, TotalIIBlocks - startValue);
}

/**
 * Modify the last block, but don't delete it entirely. While the fork is running,
 * fill up the last block and add more blocks.
 * Make sur eno modifications are applied.
 * */
TEST_F(FGCTestTag, testModifyLastBlockWhileAddingNewBlocks) {
  const auto startValue = TotalIIBlocks;
  unsigned curId = 1;


  // populate the first(last) block with two document
  ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(curId++).c_str(), "f1", "hello"));
  ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(curId++).c_str(), "f1", "hello"));

  // Delete one of the documents.
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  FGC_WaitBeforeFork(fgc);

  // The fork will see one block of 2 docs with 1 deleted doc.
  FGC_ForkAndWaitBeforeApply(fgc);

  // Now add documents until we have new blocks added.
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  while (iv->size < 3) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(curId++).c_str(), "f1", "hello"));
  }
  ASSERT_EQ(3, TotalIIBlocks - startValue);

  // Save the pointer to the original block data.
  const char *originalData = iv->blocks[0].buf.data;
  // The fork will return an array of one block with one entry, but we will ignore it.
  size_t invertedSizeBeforeApply = (get_spec(ism))->stats.invertedSize;
  FGC_Apply(fgc);

  const char *afterGcData = iv->blocks[0].buf.data;
  ASSERT_EQ(afterGcData, originalData);

  // gc stats
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(0, fgc->stats.totalCollected);

  size_t addedDocs = curId -1;

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  ASSERT_EQ(addedDocs - 1, (get_spec(ism))->stats.numDocuments);
  // All other updates are ignored.
  ASSERT_EQ(3, TotalIIBlocks - startValue);
  ASSERT_EQ(addedDocs, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply, (get_spec(ism))->stats.invertedSize);
}

/** Delete all the blocks, while the main process adds entries to the last block.
 * All the blocks, except the last block, should be removed.
*/
TEST_F(FGCTestTag, testRemoveAllBlocksWhileUpdateLast) {
  const auto startValue = TotalIIBlocks;
  unsigned curId = 1;
  char buf[1024];
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
sctx.spec->monitorDocumentExpiration = false;

  // Add documents to the index until it has 2 blocks (1 full block + 1 block with one entry)
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  // Measure the memory added by the last block.
  size_t lastBlockMemory = 0;
  while (iv->size < 2) {
    size_t n = sprintf(buf, "doc%u", curId++);
    lastBlockMemory = this->addDocumentWrapper(buf, "f1", "hello");
  }

  ASSERT_EQ(2, TotalIIBlocks - startValue);

  FGC_WaitBeforeFork(fgc);
  // Delete all.
  for (unsigned i = 1; i < curId; i++) {
    size_t n = sprintf(buf, "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }

  ASSERT_EQ(0, sctx.spec->stats.numDocuments);

  /**
   * This function allows the GC to perform fork(2), but makes it wait
   * before it begins receiving results. From this point any changes made by the
   * main process are not part of the forked process.
   */
  FGC_ForkAndWaitBeforeApply(fgc);

  size_t invertedSizeBeforeApply = sctx.spec->stats.invertedSize;
  // Add a new document so the last block's is different from the the one copied to the fork.
  size_t n = sprintf(buf, "doc%u", curId);
  lastBlockMemory += this->addDocumentWrapper(buf, "f1", "hello");

  // Save the pointer to the original last block data.
  const char *originalData = iv->blocks[iv->size - 1].buf.data;

  /** Apply the child changes. All the entries the child has seen are marked as deleted,
   * but since the last block was modified by the main the process, we keep it, assuming it
   * will be deleted in the next gc run (where the fork is not running during modifications,
   * or the we opened a new block and this block is no longer the last)
   */
  FGC_Apply(fgc);

  // gc stats - make sure we skipped the last block
  const char *afterGcData = iv->blocks[iv->size - 1].buf.data;
  ASSERT_EQ(afterGcData, originalData);
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);

  // numDocuments is updated in the indexing process, while all other fields are only updated if
  // their memory was cleaned by the gc.
  // In this case the spec contains only one valid document.
  ASSERT_EQ(1, sctx.spec->stats.numDocuments);
  // But the last block deletion was skipped.
  ASSERT_EQ(2, sctx.spec->stats.numRecords);
  ASSERT_EQ(lastBlockMemory + sizeof_InvertedIndex(iv->flags), sctx.spec->stats.invertedSize);
  ASSERT_EQ(1, TotalIIBlocks - startValue);
}

/**
 * Repair the last block, while adding more documents to it and removing a middle block.
 * This test should be checked with valgrind as it cause index corruption.
 */
TEST_F(FGCTestTag, testRepairLastBlockWhileRemovingMiddle) {
  const auto startValue = TotalIIBlocks;
  // Delete the first block:
  char buf[1024];
  unsigned curId = 1;

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  // Add 2 full blocks + 1 block with1 entry.
  unsigned middleBlockFirstId = 0;
  while (iv->size < 3) {
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
    // A new block had opened
    if (iv->size == 2 && !middleBlockFirstId) {
      middleBlockFirstId = curId - 1;
    }
  }

  unsigned lastBlockFirstId = curId - 1;

  ASSERT_EQ(3, TotalIIBlocks - startValue);

  /**
   * In this case, we want to keep the first entry in the last block,
   * but we want to delete the second entry while appending more documents to it.
   * The block will remain unchanged.
   **/
  sprintf(buf, "doc%u", curId++);
  RS::addDocument(ctx, ism, buf, "f1", "hello");

  // Wait before we fork so the next updates will copied to the child memory.
  FGC_WaitBeforeFork(fgc);

  // Delete the second entry of the last block
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  // Delete first entry in the index
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));
  unsigned total_deletions = 2;

  // Delete the second block (out of 3 blocks)
  for (unsigned i = middleBlockFirstId; i < lastBlockFirstId; ++i) {
    sprintf(buf, "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
    ++total_deletions;
  }

  // curId - 1 = total added documents
  size_t valid_docs = curId - 1 - total_deletions;
  ASSERT_EQ(valid_docs, sctx.spec->stats.numDocuments);

  size_t lastBlockEntries = iv->blocks[2].numEntries;
  FGC_ForkAndWaitBeforeApply(fgc);

  // Add a document -- this one is to keep
  sprintf(buf, "doc%u", curId);
  RS::addDocument(ctx, ism, buf, "f1", "hello");
  ++valid_docs;
  FGC_Apply(fgc);

  // Since we added entries to the last block after the fork, we ignore the fork updates in the last block
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  // The deletion in the last block was ignored,
  ASSERT_EQ(1 + valid_docs, sctx.spec->stats.numRecords);
  // Other updates should take place.
  ASSERT_EQ(valid_docs, sctx.spec->stats.numDocuments);
  // We are left with the first + last block.
  ASSERT_EQ(2, iv->size);
  // The first entry was deleted. first block starts from docId = 2.
  ASSERT_EQ(2, iv->blocks[0].firstId);
  // Last block was moved.
  ASSERT_EQ(lastBlockFirstId, iv->blocks[1].firstId);
  ASSERT_EQ(3, iv->blocks[1].numEntries);
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
  while (iv->size < 2) {
    char buf[1024];
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  }
  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/

  //add another document. now the last block has 2 entries.
  char buf[1024];
  sprintf(buf, "doc%u", curId++);
  RS::addDocument(ctx, ism, buf, "f1", "hello");

  FGC_WaitBeforeFork(fgc);

  // Delete the doc we have just added.
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));

  FGC_ForkAndWaitBeforeApply(fgc);

  // Add a document to the last block. This change is not known to the child.
  sprintf(buf, "doc%u", curId);
  RS::addDocument(ctx, ism, buf, "f1", "hello");
  FGC_Apply(fgc);
  // since the block size in the main process doesn't equal to its original size as seen by the child,
  // we ignore the fork collection - the last block changes should be discarded.
  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(2, iv->size);
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
  while (iv->size < 3) {
    char buf[1024];
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  }

  char buf[1024];
  sprintf(buf, "doc%u", curId);
  ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  unsigned next_id = curId + 1;

  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/
  FGC_WaitBeforeFork(fgc);

  while (curId > 100) {
    sprintf(buf, "doc%u", --curId);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }

  FGC_ForkAndWaitBeforeApply(fgc);

  sprintf(buf, "doc%u", next_id);
  ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));

  FGC_Apply(fgc);
  ASSERT_EQ(2, iv->size);
}

/**
 * Ensure that removing a middle block while adding to the parent will maintain
 * the parent's changes
 */
TEST_F(FGCTestTag, testRemoveMiddleBlock) {
  const auto startValue = TotalIIBlocks;
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  sctx.spec->monitorDocumentExpiration = false;
  InvertedIndex *iv = getTagInvidx(&sctx, "f1", "hello");

  while (iv->size < 2) {
    RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello");
  }

  unsigned firstMidId = curId;
  while (iv->size < 3) {
    RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello");
  }
  unsigned firstLastBlockId = curId;
  unsigned lastMidId = curId - 1;
  ASSERT_EQ(3, TotalIIBlocks - startValue);

  FGC_WaitBeforeFork(fgc);

  // Delete the middle block
  for (size_t ii = firstMidId; ii < firstLastBlockId; ++ii) {
    RS::deleteDocument(ctx, ism, numToDocStr(ii).c_str());
  }

  FGC_ForkAndWaitBeforeApply(fgc);

  // While the child is running, fill the last block and add another block.
  unsigned newLastBlockId = curId + 1;
  while (iv->size < 4) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello"));
  }
  unsigned lastLastBlockId = curId - 1;

  // Get the previous pointer, i.e. the one we expect to have the updated
  // info. We do -2 and not -1 because we have one new document in the
  // fourth block (as a sentinel)
  const char *pp = iv->blocks[iv->size - 2].buf.data;
  FGC_Apply(fgc);

  // We hadn't performed any changes to the last block prior to the fork.
  ASSERT_EQ(0, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(3, iv->size);

  // The pointer to the last gc-block, received from the fork
  const char *gcpp = iv->blocks[iv->size - 2].buf.data;
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
  InvertedIndex *iv = getTagInvidx(&sctx, "f1", "hello");

  while (iv->size < 2) {
    RS::addDocument(ctx, ism, numToDocStr(++curId).c_str(), "f1", "hello");
  }
  // Delete one document.
  RS::deleteDocument(ctx, ism, numToDocStr(1).c_str());
  ASSERT_EQ(RSGlobalStats.totalStats.logically_deleted, 1);

  FGC_WaitBeforeFork(fgc);

  // Delete the second document while fGC is waiting before the fork. If we were storing the number
  // of document to delete at this point, we wouldn't have accounted for this deletion later on
  // after the GC is done.
  RS::deleteDocument(ctx, ism, numToDocStr(2).c_str());
  ASSERT_EQ(fgc->deletedDocsFromLastRun, 2);

  FGC_Apply(fgc);

  ASSERT_EQ(RSGlobalStats.totalStats.logically_deleted, 0);
}

TEST_F(FGCTestNumeric, testNumericBlocksSinceFork) {
  const auto startValue = TotalIIBlocks;
  constexpr size_t docs_per_block = INDEX_BLOCK_SIZE;
  constexpr size_t first_split_card = 16; // from `numeric_index.c`
  size_t cur_cardinality = 0;
  size_t cur_id = 1;
  size_t expected_total_blocks = 0;
  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);

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

  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);
  ASSERT_TRUE(rt->root->range);
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rt->root->range));
  FGC_WaitBeforeFork(fgc);

  // Delete some docs from the blocks
  for (size_t i = expected_total_blocks; i < cur_id; i += 10) {
    auto rv = RS::deleteDocument(ctx, ism, numToDocStr(i).c_str());
    ASSERT_TRUE(rv) << "Failed to delete doc " << i;
  }

  FGC_ForkAndWaitBeforeApply(fgc);

  // Add a half block worth of documents to the index with a different value. The fork is not aware of these changes.
  ASSERT_LT(++cur_cardinality, first_split_card);
  expected_total_blocks++;
  for (size_t i = 0; i < docs_per_block / 2; i++) {
    this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(1.4142).c_str());
  }

  FGC_Apply(fgc);

  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);
  ASSERT_TRUE(rt->root->range);
  // The fork is not aware of the new value added after the fork, but the parent should update the
  // cardinality after applying the fork's changes.
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rt->root->range));

  /*
   * Scenario 2: Not taking the child last block, and need to address the parent's changes (ignored + last block).
   */

  FGC_WaitBeforeFork(fgc);

  // Delete some docs from the blocks
  for (size_t i = expected_total_blocks; i < cur_id; i += 10) {
    auto rv = RS::deleteDocument(ctx, ism, numToDocStr(i).c_str());
    ASSERT_TRUE(rv) << "Failed to delete doc " << i;
  }

  FGC_ForkAndWaitBeforeApply(fgc);

  // Add a half block worth of documents to the index with a different value. The fork is not aware of these changes.
  ASSERT_LT(++cur_cardinality, first_split_card);
  for (size_t i = 0; i < docs_per_block / 2; i++) {
    this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(2.718).c_str());
  }
  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks) << "Number of blocks should not change";
  // Add another half block worth of documents to the index with a different value.
  ASSERT_LT(++cur_cardinality, first_split_card);
  expected_total_blocks++;
  for (size_t i = 0; i < docs_per_block / 2; i++) {
    this->addDocumentWrapper(numToDocStr(cur_id++).c_str(), numeric_field_name, std::to_string(1.618).c_str());
  }
  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);

  FGC_Apply(fgc);

  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);
  ASSERT_TRUE(rt->root->range);
  // The child is aware of 1 value in the first block and one in the second,
  // while the parent is aware of a third value in the second block and a fourth in the third.
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rt->root->range));

  /*
   * Scenario 3: Taking the child last block, without any parent changes.
   */

  FGC_WaitBeforeFork(fgc);

  // Delete the entire second block
  for (size_t i = rt->root->range->entries->blocks[1].firstId; i <= rt->root->range->entries->blocks[1].lastId; i++) {
    RS::deleteDocument(ctx, ism, numToDocStr(i).c_str());
  }
  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);

  FGC_ForkAndWaitBeforeApply(fgc);
  FGC_Apply(fgc);

  expected_total_blocks--;
  EXPECT_EQ(TotalIIBlocks - startValue, expected_total_blocks);
  ASSERT_TRUE(rt->root->range);
  // We had 2 values in the second block and in it only. We expect the cardinality to decrease by 2.
  cur_cardinality -= 2;
  EXPECT_EQ(cur_cardinality, NumericRange_GetCardinality(rt->root->range));
}
