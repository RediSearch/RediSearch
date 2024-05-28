#include "gtest/gtest.h"
#include "spec.h"
#include "common.h"
#include "redisearch_api.h"
#include "fork_gc.h"
#include "tag_index.h"
#include "rules.h"
#include "query_error.h"
#include "inverted_index.h"
#include "rwlock.h"
extern "C" {
#include "util/dict.h"
}

#include <set>

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
  RedisModuleCtx *ctx;
  void *fgc;
  RefManager *ism;
} args_t;

static pthread_t thread;

void *cbWrapper(void *args) {
  args_t *fgcArgs = (args_t *)args;
  ForkGC *fgc = reinterpret_cast<ForkGC *>(get_spec(fgcArgs->ism)->gc->gcCtx);

  // sync thread
  while (fgc->pauseState != FGC_PAUSED_CHILD) {
    usleep(500);
  }

  // run ForkGC
  get_spec(fgcArgs->ism)->gc->callbacks.periodicCallback(fgcArgs->fgc);
  rm_free(args);
  return NULL;
}



class FGCTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  RefManager *ism;
  ForkGC *fgc;

  void SetUp() override {
    ism = createIndex(ctx);
    RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold = 0;
    runGcThread();
  }

  void runGcThread() {
    Spec_AddToDict(ism);
    fgc = reinterpret_cast<ForkGC *>(get_spec(ism)->gc->gcCtx);
    thread = {0};
    args_t *args = (args_t *)rm_calloc(1, sizeof(*args));
    *args = {.ctx = ctx, .fgc = fgc, .ism = ism};

    pthread_create(&thread, NULL, cbWrapper, args);
  }
  void TearDown() override {
    // Return the reference
    IndexSpec_RemoveFromGlobals({ism});
    // Detach from the gc to make sure we are not stuck on waiting
    // for the pauseState to be changed.
    pthread_detach(thread);
  }

  RefManager *createIndex(RedisModuleCtx *ctx) {
    RSIndexOptions opts = {0};
    opts.gcPolicy = GC_POLICY_FORK;
    auto ism = RediSearch_CreateIndex("idx", &opts);
    EXPECT_FALSE(ism == NULL);
    EXPECT_FALSE(get_spec(ism)->gc == NULL);

    // Let's use a tag field, so that there's only one entry in the tag index
    RediSearch_CreateField(ism, "f1", RSFLDTYPE_TAG, 0);

    const char *pref = "";
    SchemaRuleArgs args = {0};
    args.type = "HASH";
    args.prefixes = &pref;
    args.nprefixes = 1;

    QueryError status = {};

    get_spec(ism)->rule = SchemaRule_Create(&args, {ism}, &status);

    return ism;
  }

  size_t addDocumentWrapper(const char *docid, const char *field, const char *value) {
    size_t beforAddMem = (get_spec(ism))->stats.invertedSize;
    assert(RS::addDocument(ctx, ism, docid, field, value));
    return (get_spec(ism))->stats.invertedSize - beforAddMem;

  }
};

static InvertedIndex *getTagInvidx(RedisSearchCtx* sctx, const char *field,
                                   const char *value) {
  RedisModuleKey *keyp = NULL;
  RedisModuleString *fmtkey = IndexSpec_GetFormattedKeyByName(sctx->spec, "f1", INDEXFLD_T_TAG);
  auto tix = TagIndex_Open(sctx, fmtkey, 1, &keyp);
  size_t sz;
  auto iv = TagIndex_OpenIndex(tix, "hello", strlen("hello"), 1, &sz);
  sctx->spec->stats.invertedSize += sz;
  return iv;
}

static std::string numToDocid(unsigned id) {
  char buf[1024];
  sprintf(buf, "doc%u", id);
  return std::string(buf);
}

/** Mark one of the entries in the last block as deleted while the child is running.
 * This means the number of original entries recorded by the child and the current number of
 * entries are equal, and we conclude there weren't any changes in the parent to the block buffer.
 * Make sure the modification take place. */
TEST_F(FGCTest, testRemoveEntryFromLastBlock) {

  // Add two documents
  size_t docSize = addDocumentWrapper("doc1", "f1", "hello");
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
  ASSERT_EQ(1, TotalIIBlocks);
}

/**
 * In this test, the child process needs to delete the only and last block in the index,
 * while the main process adds a document to it.
 * In this case, we discard the changes collected by the child process, so eventually the
 * index contains both documents.
 * */
TEST_F(FGCTest, testRemoveLastBlockWhileUpdate) {

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
  ASSERT_EQ(1, TotalIIBlocks);
}

/**
 * Modify the last block, but don't delete it entirly. While the fork is running,
 * fill up the last block and add more blocks.
 * Make sur eno modifications are applied.
 * */
TEST_F(FGCTest, testModifyLastBlockWhileAddingNewBlocks) {

  unsigned curId = 1;


  // populate the first(last) block with two document
  ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocid(curId++).c_str(), "f1", "hello"));
  ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocid(curId++).c_str(), "f1", "hello"));

  // Delete one of the documents.
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc1"));

  FGC_WaitBeforeFork(fgc);

  // The fork will see one block of 2 docs with 1 deleted doc.
  FGC_ForkAndWaitBeforeApply(fgc);

  // Now add documents until we have new blocks added.
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  while (iv->size < 3) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocid(curId++).c_str(), "f1", "hello"));
  }
  ASSERT_EQ(3, TotalIIBlocks);

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
  ASSERT_EQ(3, TotalIIBlocks);
  ASSERT_EQ(addedDocs, (get_spec(ism))->stats.numRecords);
  ASSERT_EQ(invertedSizeBeforeApply, (get_spec(ism))->stats.invertedSize);
}

/** Delete all the blocks, while the main process adds entries to the last block.
 * All the blocks, except the last block, should be removed.
*/
TEST_F(FGCTest, testRemoveAllBlocksWhileUpdateLast) {

  unsigned curId = 1;
  char buf[1024];
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  
  // Add documents to the index until it has 2 blocks (1 full block + 1 block with one entry)
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  // Measure the memory added by the last block.
  size_t lastBlockMemory = 0;
  while (iv->size < 2) {
    size_t n = sprintf(buf, "doc%u", curId++);
    lastBlockMemory = addDocumentWrapper(buf, "f1", "hello");
  }

  ASSERT_EQ(2, TotalIIBlocks);

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
  lastBlockMemory += addDocumentWrapper(buf, "f1", "hello");

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
  ASSERT_EQ(1, TotalIIBlocks);
}

/**
 * Repair the last block, while adding more documents to it and removing a middle block.
 * This test should be checked with valgrind as it cause index corruption.
 */
TEST_F(FGCTest, testRepairLastBlockWhileRemovingMiddle) {
  // Delete the first block:
  char buf[1024];
  unsigned curId = 1;

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
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

  ASSERT_EQ(3, TotalIIBlocks);

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
TEST_F(FGCTest, testRepairLastBlock) {
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
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
TEST_F(FGCTest, testRepairMiddleRemoveLast) {
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
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
TEST_F(FGCTest, testRemoveMiddleBlock) {
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  InvertedIndex *iv = getTagInvidx(&sctx, "f1", "hello");

  while (iv->size < 2) {
    RS::addDocument(ctx, ism, numToDocid(++curId).c_str(), "f1", "hello");
  }

  unsigned firstMidId = curId;
  while (iv->size < 3) {
    RS::addDocument(ctx, ism, numToDocid(++curId).c_str(), "f1", "hello");
  }
  unsigned firstLastBlockId = curId;
  unsigned lastMidId = curId - 1;
  ASSERT_EQ(3, TotalIIBlocks);

  FGC_WaitBeforeFork(fgc);

  // Delete the middle block
  for (size_t ii = firstMidId; ii < firstLastBlockId; ++ii) {
    RS::deleteDocument(ctx, ism, numToDocid(ii).c_str());
  }

  FGC_ForkAndWaitBeforeApply(fgc);

  // While the child is running, fill the last block and add another block.
  unsigned newLastBlockId = curId + 1;
  while (iv->size < 4) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocid(++curId).c_str(), "f1", "hello"));
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
  ASSERT_NE(ss.end(), ss.find(numToDocid(newLastBlockId)));
  ASSERT_NE(ss.end(), ss.find(numToDocid(newLastBlockId - 1)));
  ASSERT_NE(ss.end(), ss.find(numToDocid(lastLastBlockId)));
}
