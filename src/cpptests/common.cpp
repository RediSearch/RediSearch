#include <redismodule.h>
#include <gtest/gtest.h>
#include <module.h>
#include <version.h>
#include "redismock/util.h"
#include "redismock/internal.h"

extern "C" {
static int my_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (RedisModule_Init(ctx, "ft", REDISEARCH_MODULE_VERSION, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR)
    return REDISMODULE_ERR;
  return RediSearch_InitModuleInternal(ctx, argv, argc);
}
}

class MyEnvironment : public ::testing::Environment {
  virtual void SetUp() {
    const char *arguments[] = {"SAFEMODE", "NOGC"};
    // No arguments..
    RMCK_Bootstrap(my_OnLoad, arguments, 2);
  }

  virtual void TearDown() {
    RMCK_Shutdown();
    RediSearch_CleanupModule();
  }
};

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new MyEnvironment());
  RUN_ALL_TESTS();
}
