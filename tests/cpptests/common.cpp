
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
  RSGlobalConfig.defaultScorer = rm_strdup(DEFAULT_SCORER_NAME);
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
    RediSearch_CleanupModule(NULL);
  }
};

int main(int argc, char **argv) {
  RS::InstallSegvStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new MyEnvironment());
  return RUN_ALL_TESTS();
}
