#include <gtest/gtest.h>
#include "spec.h"
#include "common.h"
#include "redisearch_api.h"
#include "fork_gc.h"
#include "tag_index.h"
#include "inverted_index.h"

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

TEST_F(FGCTest, testRemoveSingle) {
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
  ASSERT_TRUE(RS::deleteDocument(ctx, sp, "doc1"));

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

/**
 * Repair the last block, while adding more documents to it...
 */
TEST_F(FGCTest, testRepairBlock) {
  RMCK::Context ctx;
  auto sp = createIndex(ctx);
  // Delete the first block:
  unsigned curId = 0;

  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, sp);
  RedisModuleKey *keyp = NULL;
  RedisModuleString *fmtkey = IndexSpec_GetFormattedKeyByName(sp, "f1", INDEXFLD_T_TAG);
  auto tix = TagIndex_Open(&sctx, fmtkey, 1, &keyp);
  auto iv = TagIndex_OpenIndex(tix, "hello", strlen("hello"), 1);

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
}
