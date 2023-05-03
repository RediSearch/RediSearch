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
  get_spec(fgcArgs->ism)->gc->callbacks.periodicCallback(fgcArgs->ctx, fgcArgs->fgc);
  rm_free(args);
  return NULL;
}

void runGcThread(RedisModuleCtx *ctx, void *fgc, RefManager *ism) {
  thread = {0};
  args_t *args = (args_t *)rm_calloc(1, sizeof(*args));
  *args = {.ctx = ctx, .fgc = fgc, .ism = ism};

  pthread_create(&thread, NULL, cbWrapper, args);
}

class FGCTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  RefManager *ism;
  ForkGC *fgc;

  void SetUp() override {
    ism = createIndex(ctx);
    RSGlobalConfig.gcConfigParams.forkGc.forkGcCleanThreshold = 0;
    Spec_AddToDict(ism);
    fgc = reinterpret_cast<ForkGC *>(get_spec(ism)->gc->gcCtx);
    runGcThread(ctx, fgc, ism);
  }

  void TearDown() override {
    // Return the reference
    IndexSpec_RemoveFromGlobals({ism});
    pthread_join(thread, NULL);
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
};

TEST_F(FGCTest, testRemoveLastBlock) {

  // Add a document
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc1", "f1", "hello"));
  /**
   * To properly test this; we must ensure that the gc is forked AFTER
   * the deletion, but BEFORE the addition.
   */
  FGC_WaitAtFork(fgc);
  auto rv = RS::deleteDocument(ctx, ism, "doc1");
  ASSERT_TRUE(rv);

  /**
   * This function allows the GC to perform fork(2), but makes it wait
   * before it begins receiving results.
   */
  FGC_WaitAtApply(fgc);
  ASSERT_TRUE(RS::addDocument(ctx, ism, "doc2", "f1", "hello"));

  /** This function allows the gc to receive the results */
  FGC_WaitClear(fgc);

  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
}

static InvertedIndex *getTagInvidx(RedisSearchCtx* sctx, const char *field,
                                   const char *value) {
  RedisModuleKey *keyp = NULL;
  RedisModuleString *fmtkey = IndexSpec_GetFormattedKeyByName(sctx->spec, "f1", INDEXFLD_T_TAG);
  auto tix = TagIndex_Open(sctx, fmtkey, 1, &keyp);
  auto iv = TagIndex_OpenIndex(tix, "hello", strlen("hello"), 1);
  return iv;
}

static std::string numToDocid(unsigned id) {
  char buf[1024];
  sprintf(buf, "doc%u", id);
  return std::string(buf);
}

/**
 * Repair the last block, while adding more documents to it and removing a middle block.
 * This test should be checked with valgrind as it cause index corruption.
 */
TEST_F(FGCTest, testRepairLastBlockWhileRemovingMiddle) {
  // Delete the first block:
  unsigned curId = 0;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, get_spec(ism));
  auto iv = getTagInvidx(&sctx,  "f1", "hello");
  while (iv->size < 3) {
    char buf[1024];
    size_t n = sprintf(buf, "doc%u", curId++);
    ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));
  }

  /**
   * In this case, we want to keep `curId`, but we want to delete a 'middle' entry
   * while appending documents to it..
   **/
  char buf[1024];
  sprintf(buf, "doc%u", curId++);
  std::string toDel(buf);
  RS::addDocument(ctx, ism, buf, "f1", "hello");

  FGC_WaitAtFork(fgc);

  ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  ASSERT_TRUE(RS::deleteDocument(ctx, ism, "doc0"));

  // delete an entire block
  for (int i = 1000; i < 2000; ++i) {
    sprintf(buf, "doc%u", i);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }
  FGC_WaitAtApply(fgc);

  // Add a document -- this one is to keep
  sprintf(buf, "doc%u", curId);
  RS::addDocument(ctx, ism, buf, "f1", "hello");
  FGC_WaitClear(fgc);

  ASSERT_EQ(1, fgc->stats.gcBlocksDenied);
  ASSERT_EQ(2, iv->size);
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
  char buf[1024];
  sprintf(buf, "doc%u", curId++);
  std::string toDel(buf);
  RS::addDocument(ctx, ism, buf, "f1", "hello");

  FGC_WaitAtFork(fgc);

  ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  FGC_WaitAtApply(fgc);

  // Add a document -- this one is to keep
  sprintf(buf, "doc%u", curId);
  RS::addDocument(ctx, ism, buf, "f1", "hello");
  FGC_WaitClear(fgc);

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
  FGC_WaitAtFork(fgc);

  while (curId > 100) {
    sprintf(buf, "doc%u", --curId);
    ASSERT_TRUE(RS::deleteDocument(ctx, ism, buf));
  }

  FGC_WaitAtApply(fgc);

  sprintf(buf, "doc%u", next_id);
  ASSERT_TRUE(RS::addDocument(ctx, ism, buf, "f1", "hello"));

  FGC_WaitClear(fgc);
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
  ASSERT_EQ(3, iv->size);

  FGC_WaitAtFork(fgc);

  for (size_t ii = firstMidId; ii < lastMidId + 1; ++ii) {
    RS::deleteDocument(ctx, ism, numToDocid(ii).c_str());
  }

  FGC_WaitAtApply(fgc);
  // Add a new document
  unsigned newLastBlockId = curId + 1;
  while (iv->size < 4) {
    ASSERT_TRUE(RS::addDocument(ctx, ism, numToDocid(++curId).c_str(), "f1", "hello"));
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
  auto vv = RS::search(ism, "@f1:{hello}");
  std::set<std::string> ss(vv.begin(), vv.end());
  ASSERT_NE(ss.end(), ss.find(numToDocid(newLastBlockId)));
  ASSERT_NE(ss.end(), ss.find(numToDocid(newLastBlockId - 1)));
  ASSERT_NE(ss.end(), ss.find(numToDocid(lastLastBlockId)));
  ASSERT_EQ(0, fgc->stats.gcBlocksDenied);
}
