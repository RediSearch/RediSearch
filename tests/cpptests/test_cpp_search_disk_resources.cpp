/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "notifications.h"
#include "search_disk.h"

#include "gtest/gtest.h"

#include <cstring>

extern "C" {
extern RedisSearchDiskAPI *disk;
extern bool isFlex;
void ConfigChangedCallback(RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t event, void *data);
void SearchDisk_SetTestAPI(RedisSearchDiskAPI *api);
}

namespace {

template <typename T>
struct Restore {
  T &target;
  T saved;

  explicit Restore(T &target) : target(target), saved(target) {
  }

  ~Restore() {
    target = saved;
  }
};

class SearchDiskResourcesTest : public ::testing::Test {
 protected:
  Restore<decltype(RedisModule_ConfigGetNumeric)> configGetNumeric{RedisModule_ConfigGetNumeric};
  Restore<decltype(RedisModule_BigModuleRegister)> bigModuleRegister{RedisModule_BigModuleRegister};
  Restore<decltype(disk)> diskApi{disk};
  Restore<decltype(disk_db)> diskDb{disk_db};
  Restore<decltype(isFlex)> flex{isFlex};
  Restore<decltype(RSGlobalConfig.diskMaxMemoryPercentage)> maxMemoryPercentage{
      RSGlobalConfig.diskMaxMemoryPercentage};
  Restore<decltype(RSGlobalConfig.diskWbmBudgetPerIndexMB)> wbmBudgetPerIndex{
      RSGlobalConfig.diskWbmBudgetPerIndexMB};
  Restore<decltype(RSGlobalConfig.diskMaxOpenFiles)> maxOpenFiles{
      RSGlobalConfig.diskMaxOpenFiles};

  RedisSearchDiskAPI api{};
  RedisModuleCtx *ctx = nullptr;

  static inline long long configuredShardMemory = 0;
  static inline SearchDiskResourceConfig openedConfig{};
  static inline bool openCalled = false;
  static inline bool openIndexCalled = false;
  static inline bool openIndexSucceeds = true;
  static inline bool restoreOpen = false;
  static inline size_t updatedShardMemory = 0;
  static inline size_t shardMemoryUpdates = 0;

  static int configNumeric(RedisModuleCtx *, const char *name, long long *value) {
    if (std::strcmp(name, "bigredis-max-ram") != 0) {
      return REDISMODULE_ERR;
    }
    *value = configuredShardMemory;
    return REDISMODULE_OK;
  }

  static RedisSearchDisk *open(RedisModuleCtx *, const SearchDiskResourceConfig *config,
                               bool, bool, bool) {
    openCalled = true;
    openedConfig = *config;
    return reinterpret_cast<RedisSearchDisk *>(&openedConfig);
  }

  static RedisSearchDiskIndexSpec *openIndex(
      RedisModuleCtx *, RedisSearchDisk *, const HiddenString *, const char *, size_t,
      DocumentType, bool, bool isRestore, const SearchDiskCompactionCallbacks *, void *privateData) {
    openIndexCalled = true;
    restoreOpen = isRestore;
    return openIndexSucceeds ? reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData) : nullptr;
  }

  static void updateShardMemory(RedisSearchDisk *, size_t shardMemoryBytes) {
    updatedShardMemory = shardMemoryBytes;
    ++shardMemoryUpdates;
  }

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    ASSERT_NE(ctx, nullptr);

    configuredShardMemory = 987654321;
    openedConfig = {};
    openCalled = false;
    openIndexCalled = false;
    openIndexSucceeds = true;
    restoreOpen = false;
    updatedShardMemory = 0;
    shardMemoryUpdates = 0;

    api.basic.open = open;
    api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) {};
    api.basic.setThrottleCallbacks = [](ThrottleCB, ThrottleCB) {};
    api.basic.openIndexSpec = openIndex;
    api.basic.closeIndexSpec = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {};
    api.basic.closeIndexOnMainThread = [](RedisModuleCtx *, RedisSearchDiskIndexSpec *) {};
    api.basic.updateShardMemory = updateShardMemory;

    SearchDisk_SetTestAPI(&api);
    disk = nullptr;
    disk_db = nullptr;
    isFlex = false;
    RedisModule_ConfigGetNumeric = configNumeric;
    RedisModule_BigModuleRegister = [](RedisModuleCtx *, RedisModuleBigCallbacks *) {
      return REDISMODULE_OK;
    };
  }

  void TearDown() override {
    SearchDisk_SetTestAPI(nullptr);
    RedisModule_FreeThreadSafeContext(ctx);
  }

  void initializeDisk() {
    ASSERT_TRUE(SearchDisk_Initialize(ctx));
    ASSERT_TRUE(openCalled);
  }
};

TEST_F(SearchDiskResourcesTest, InitializeForwardsEveryConfiguredResource) {
  RSGlobalConfig.diskMaxMemoryPercentage = 73;
  RSGlobalConfig.diskWbmBudgetPerIndexMB = 37;
  RSGlobalConfig.diskMaxOpenFiles = 4093;

  initializeDisk();

  EXPECT_EQ(openedConfig.shardMemoryBytes, 987654321u);
  EXPECT_EQ(openedConfig.maxMemoryPercentage, 73u);
  EXPECT_EQ(openedConfig.wbmBudgetPerIndexMB, 37u);
  EXPECT_EQ(openedConfig.maxOpenFiles, 4093);
}

TEST_F(SearchDiskResourcesTest, ConfigNotificationUpdatesValidatedShardMemory) {
  configuredShardMemory = 123456789;
  const char *changed[] = {"bigredis-max-ram"};
  RedisModuleConfigChangeV1 change{};
  change.version = REDISMODULE_CONFIGCHANGE_VERSION;
  change.num_changes = 1;
  change.config_names = changed;

  ConfigChangedCallback(ctx, RedisModuleEvent_Config, REDISMODULE_SUBEVENT_CONFIG_CHANGE, &change);
  EXPECT_EQ(shardMemoryUpdates, 0u);

  initializeDisk();

  ConfigChangedCallback(ctx, RedisModuleEvent_Config, REDISMODULE_SUBEVENT_CONFIG_CHANGE, &change);

  EXPECT_EQ(shardMemoryUpdates, 1u);
  EXPECT_EQ(updatedShardMemory, 123456789u);

  configuredShardMemory = 0;
  ConfigChangedCallback(ctx, RedisModuleEvent_Config, REDISMODULE_SUBEVENT_CONFIG_CHANGE, &change);
  EXPECT_EQ(shardMemoryUpdates, 1u);
}

TEST_F(SearchDiskResourcesTest, CreateAndRestoreUseDistinctAdmissionModes) {
  disk = &api;
  disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
  isFlex = true;
  const char *args[] = {"SCHEMA", "title", "TEXT"};
  QueryError status = QueryError_Default();

  StrongRef ref = IndexSpec_ParseC(ctx, "resource_modes", args, 3, &status);
  IndexSpec *spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
  ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&status);
  EXPECT_TRUE(openIndexCalled);
  EXPECT_FALSE(restoreOpen);

  spec->diskSpec = nullptr;
  spec->diskRegistered = false;
  openIndexCalled = false;
  ASSERT_EQ(IndexSpec_RdbLoadOpenDisk(ctx, spec, false, &status), REDISMODULE_OK);
  EXPECT_TRUE(openIndexCalled);
  EXPECT_TRUE(restoreOpen);

  spec->diskSpec = nullptr;
  spec->diskRegistered = false;
  StrongRef_Release(ref);
  QueryError_ClearError(&status);
}

TEST_F(SearchDiskResourcesTest, CreateReportsDiskAdmissionFailure) {
  disk = &api;
  disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
  isFlex = true;
  openIndexSucceeds = false;
  const char *args[] = {"SCHEMA", "title", "TEXT"};
  QueryError status = QueryError_Default();

  StrongRef ref = IndexSpec_ParseC(ctx, "resource_rejected", args, 3, &status);

  EXPECT_EQ(StrongRef_Get(ref), nullptr);
  EXPECT_EQ(QueryError_GetCode(&status), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&status);
}

}  // namespace
