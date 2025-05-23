
/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "redismodule.h"
#include "module.h"
#include "version.h"
#include "common.h"
#include "redismock/util.h"
#include "redismock/internal.h"

#include "gtest/gtest.h"

extern "C" {

static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, REDISEARCH_MODULE_NAME, REDISEARCH_MODULE_VERSION,
                       REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  return RediSearch_InitModuleInternal(ctx);
}

}

class MyEnvironment : public ::testing::Environment {
  virtual void SetUp() {
    const char *arguments[] = {"NOGC"};
    // No arguments..
    RMCK_Bootstrap(my_OnLoad, arguments, 1);
    RSGlobalConfig.freeResourcesThread = false;
  }

  virtual void TearDown() {
    RMCK_Shutdown();
    RediSearch_CleanupModule();
  }
};

bool RS::deleteDocument(RedisModuleCtx *ctx, RSIndex *index, const char *docid) {
  return RediSearch_DeleteDocument(index, docid, strlen(docid)) == REDISMODULE_OK;
}

static std::vector<std::string> getResultsCommon(RSIndex *index, RSResultsIterator *it) {
  std::vector<std::string> ret;
  EXPECT_FALSE(it == NULL);

  if (!it) {
    goto done;
  }

  while (true) {
    size_t n = 0;
    auto cur = RediSearch_ResultsIteratorNext(it, index, &n);
    if (cur == NULL) {
      break;
    }
    ret.push_back(std::string((const char *)cur, n));
  }

done:
  if (it) {
    RediSearch_ResultsIteratorFree(it);
  }
  return ret;
}

std::vector<std::string> RS::search(RSIndex *index, RSQueryNode *qn) {
  auto it = RediSearch_GetResultsIterator(qn, index);
  return getResultsCommon(index, it);
}

std::vector<std::string> RS::search(RSIndex *index, const char *s) {
  auto it = RediSearch_IterateQuery(index, s, strlen(s), NULL);
  return getResultsCommon(index, it);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new MyEnvironment());
  return RUN_ALL_TESTS();
}
