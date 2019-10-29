#include <gtest/gtest.h>
#include "spec.h"
#include "common.h"
#include "redisearch_api.h"
#include "fork_gc.h"
#include "tag_index.h"
#include "inverted_index.h"
#include "rwlock.h"
#include <set>

static timespec getTimespecCb(void *) {
  timespec ts = {0};
  ts.tv_nsec = 5000;
  return ts;
}

class FGCTest : public ::testing::Test {
 protected:
  IndexSpec *createIndex(RedisModuleCtx *ctx) {
    RSIndexOptions opts = {0};
    opts.gcPolicy = GC_POLICY_FORK;
    auto sp = RediSearch_CreateIndex("idx", &opts);
    EXPECT_FALSE(sp == NULL);
    EXPECT_FALSE(sp->gc == NULL);

    // Let's use a tag field, so that there's only one entry in the tag index
    RediSearch_CreateField(sp, "f1", RSFLDTYPE_TAG, 0);

    // Set the interval timer to something lower, so we don't wait too
    // long
    timespec ts = {0};
    ts.tv_nsec = 5000;  // 500us
    sp->gc->callbacks.getInterval = getTimespecCb;
    RMUtilTimer_SetInterval(sp->gc->timer, ts);
    RMUtilTimer_Signal(sp->gc->timer);
    return sp;
  }
};

TEST_F(FGCTest, testRemoveLastBlock) {
  RMCK::Context ctx;
  auto sp = createIndex(ctx);

  // Add a document
  ASSERT_TRUE(RS::addDocument(ctx, sp, "doc1", "f1", "hello"));

  auto fgc = reinterpret_cast<ForkGC *>(sp->gc->gcCtx);

  /**
   * To properly test this; we must ensure that the gc is forked AFTER
   * the deletion, but BEFORE the addition.
   */
  FGC_WaitAtFork(fgc);
  auto rv = RS::deleteDocument(ctx, sp, "doc1");
  ASSERT_TRUE(rv);

  /**
   * This function allows the GC to perform fork(2), but makes it wait
   * before it begins receiving results.
   */
  FGC_WaitAtApply(fgc);

  ASSERT_TRUE(RS::addDocument(ctx, sp, "doc2", "f1", "hello"));

  /** This function allows the gc to receive the results */
  FGC_WaitClear(fgc);

  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);

  // By now, the gc should be resumed.
  RediSearch_DropIndex(sp);
}

static InvertedIndex *getTagInvidx(RedisModuleCtx *ctx, IndexSpec *sp, const char *field,
                                   const char *value) {
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RedisModuleKey *keyp = NULL;
  RedisModuleString *fmtkey = IndexSpec_GetFormattedKeyByName(sp, "f1", INDEXFLD_T_TAG);
  auto tix = TagIndex_Open(&sctx, fmtkey, 1, &keyp);
  auto iv = TagIndex_OpenIndex(tix, "hello", strlen("hello"), 1);
  return iv;
}

static std::string numToDocid(unsigned id) {
  char buf[1024];
  sprintf(buf, "doc%u", id);
  return std::string(buf);
}

/**
 * Repair the last block, while adding more documents to it and removing a midle block.
 * This test should be checked with valgrind as it cause index corruption.
 */
TEST_F(FGCTest, testRepairLastBlockWhileRemovingMidle) {
  RMCK::Context ctx;
  auto sp = createIndex(ctx);
  // Delete the first block:
  unsigned curId = 0;
  auto iv = getTagInvidx(ctx, sp, "f1", "hello");
  while (iv->size < 3) {
    char buf[1024];
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, sp, buf, "f1", "hello"));
  }

  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/
  char buf[1024];
  sprintf(buf, "doc%u", curId++);
  std::string toDel(buf);
  RS::addDocument(ctx, sp, buf, "f1", "hello");

  auto fgc = reinterpret_cast<ForkGC *>(sp->gc->gcCtx);
  FGC_WaitAtFork(fgc);

  ASSERT_TRUE(RS::deleteDocument(ctx, sp, buf));
  ASSERT_TRUE(RS::deleteDocument(ctx, sp, "doc0"));

  // delete an entire block
  for (int i = 100; i < 200; ++i) {
    sprintf(buf, "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, sp, buf));
  }
  FGC_WaitAtApply(fgc);

  // Add a document -- this one is to keep
  sprintf(buf, "doc%u", curId);
  RS::addDocument(ctx, sp, buf, "f1", "hello");
  FGC_WaitClear(fgc);

  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(2, iv->size);
  RediSearch_DropIndex(sp);
}

/**
 * Repair the last block, while adding more documents to it...
 */
TEST_F(FGCTest, testRepairLastBlock) {
  RMCK::Context ctx;
  auto sp = createIndex(ctx);
  // Delete the first block:
  unsigned curId = 0;
  auto iv = getTagInvidx(ctx, sp, "f1", "hello");
  while (iv->size < 2) {
    char buf[1024];
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, sp, buf, "f1", "hello"));
  }

  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/
  char buf[1024];
  sprintf(buf, "doc%u", curId++);
  std::string toDel(buf);
  RS::addDocument(ctx, sp, buf, "f1", "hello");

  auto fgc = reinterpret_cast<ForkGC *>(sp->gc->gcCtx);
  FGC_WaitAtFork(fgc);

  ASSERT_TRUE(RS::deleteDocument(ctx, sp, buf));
  FGC_WaitAtApply(fgc);

  // Add a document -- this one is to keep
  sprintf(buf, "doc%u", curId);
  RS::addDocument(ctx, sp, buf, "f1", "hello");
  FGC_WaitClear(fgc);

  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(2, iv->size);
  RediSearch_DropIndex(sp);
}

/**
 * Test repair midle block while last block is removed on child and modified on parent.
 * Make sure there is no datalose.
 */
TEST_F(FGCTest, testRepairMidleRemoveLast) {
  RMCK::Context ctx;
  auto sp = createIndex(ctx);
  // Delete the first block:
  unsigned curId = 0;
  auto iv = getTagInvidx(ctx, sp, "f1", "hello");
  while (iv->size < 3) {
    char buf[1024];
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, sp, buf, "f1", "hello"));
  }

  char buf[1024];
  sprintf(buf, "doc%u", curId);
  ASSERT_TRUE(RS::addDocument(ctx, sp, buf, "f1", "hello"));
  unsigned next_id = curId + 1;

  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/
  auto fgc = reinterpret_cast<ForkGC *>(sp->gc->gcCtx);
  FGC_WaitAtFork(fgc);

  while (curId > 100) {
    sprintf(buf, "doc%u", --curId);
    ASSERT_TRUE(RS::deleteDocument(ctx, sp, buf));
  }

  FGC_WaitAtApply(fgc);

  sprintf(buf, "doc%u", next_id);
  ASSERT_TRUE(RS::addDocument(ctx, sp, buf, "f1", "hello"));

  FGC_WaitClear(fgc);

  ASSERT_EQ(2, iv->size);
  RediSearch_DropIndex(sp);
}

/**
 * Ensure that removing a middle block while adding to the parent will maintain
 * the parent's changes
 */
TEST_F(FGCTest, testRemoveMiddleBlock) {
  RMCK::Context ctx;
  auto sp = createIndex(ctx);
  // Delete the first block:
  unsigned curId = 0;
  InvertedIndex *iv = getTagInvidx(ctx, sp, "f1", "hello");

  while (iv->size < 2) {
    RS::addDocument(ctx, sp, numToDocid(++curId).c_str(), "f1", "hello");
  }

  unsigned firstMidId = curId;
  while (iv->size < 3) {
    RS::addDocument(ctx, sp, numToDocid(++curId).c_str(), "f1", "hello");
  }
  unsigned firstLastBlockId = curId;
  unsigned lastMidId = curId - 1;
  ASSERT_EQ(3, iv->size);

  auto fgc = reinterpret_cast<ForkGC *>(sp->gc->gcCtx);
  FGC_WaitAtFork(fgc);
  for (size_t ii = firstMidId; ii < lastMidId + 1; ++ii) {
    RS::deleteDocument(ctx, sp, numToDocid(ii).c_str());
  }

  FGC_WaitAtApply(fgc);
  // Add a new document
  unsigned newLastBlockId = curId + 1;
  while (iv->size < 4) {
    ASSERT_TRUE(RS::addDocument(ctx, sp, numToDocid(++curId).c_str(), "f1", "hello"));
  }
  unsigned lastLastBlockId = curId - 1;

  // Get the previous pointer, i.e. the one we expect to have the updated
  // info. We do -2 and not -1 because we have one new document in the
  // fourth block (as a sentinel)
  const char *pp = iv->blocks[iv->size - 2].buf.data;
  FGC_WaitClear(fgc);
  ASSERT_EQ(3, iv->size);

  // The pointer to the last gc-block, received from the fork
  const char *gcpp = iv->blocks[iv->size - 2].buf.data;
  ASSERT_EQ(pp, gcpp);

  // Now search for the ID- let's be sure it exists
  auto vv = RS::search(sp, "@f1:{hello}");
  std::set<std::string> ss(vv.begin(), vv.end());
  ASSERT_NE(ss.end(), ss.find(numToDocid(newLastBlockId)));
  ASSERT_NE(ss.end(), ss.find(numToDocid(newLastBlockId - 1)));
  ASSERT_NE(ss.end(), ss.find(numToDocid(lastLastBlockId)));
  ASSERT_EQ(0, fgc->stats.gcBlocksDenied);

  RediSearch_DropIndex(sp);
}
