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
int RediSearch_InitModuleConfigForTests(RedisModuleCtx *ctx);
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

struct NumericRegistration {
  bool seen = false;
  long long defaultValue = 0;
  unsigned int flags = 0;
  long long min = 0;
  long long max = 0;
  RedisModuleConfigGetNumericFunc get = nullptr;
  RedisModuleConfigSetNumericFunc set = nullptr;
  void *privateData = nullptr;
};

class SearchDiskResourcesTest : public ::testing::Test {
 protected:
  Restore<decltype(RedisModule_ConfigGetNumeric)> configGetNumeric{RedisModule_ConfigGetNumeric};
  Restore<decltype(RedisModule_BigModuleRegister)> bigModuleRegister{RedisModule_BigModuleRegister};
  Restore<decltype(RedisModule_RegisterNumericConfig)> registerNumericConfig{
      RedisModule_RegisterNumericConfig};
  Restore<decltype(RedisModule_RegisterBoolConfig)> registerBoolConfig{
      RedisModule_RegisterBoolConfig};
  Restore<decltype(RedisModule_RegisterStringConfig)> registerStringConfig{
      RedisModule_RegisterStringConfig};
  Restore<decltype(RedisModule_RegisterEnumConfig)> registerEnumConfig{
      RedisModule_RegisterEnumConfig};
  Restore<decltype(RedisModule_LoadDefaultConfigs)> loadDefaultConfigs{
      RedisModule_LoadDefaultConfigs};
  Restore<decltype(RedisModule_LoadConfigs)> loadConfigs{RedisModule_LoadConfigs};
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

  static inline NumericRegistration maxMemoryConfig{};
  static inline NumericRegistration wbmBudgetConfig{};
  static inline NumericRegistration maxOpenFilesConfig{};
  static inline long long configuredShardMemory = 0;
  static inline SearchDiskResourceConfig openedConfig{};
  static inline bool openCalled = false;
  static inline bool openIndexCalled = false;
  static inline bool openIndexSucceeds = true;
  static inline bool restoreOpen = false;
  static inline size_t updatedShardMemory = 0;
  static inline size_t shardMemoryUpdates = 0;
  static inline bool openRdbStateCalled = false;
  static inline RedisSearchDiskRdbState *openedRdbState = nullptr;
  static inline size_t openedObfuscatedNameLength = 0;

  static int configNumeric(RedisModuleCtx *, const char *name, long long *value) {
    if (std::strcmp(name, "bigredis-max-ram") != 0) {
      return REDISMODULE_ERR;
    }
    *value = configuredShardMemory;
    return REDISMODULE_OK;
  }

  static int registerNumeric(RedisModuleCtx *, const char *name, long long defaultValue,
                             unsigned int flags, long long min, long long max,
                             RedisModuleConfigGetNumericFunc get,
                             RedisModuleConfigSetNumericFunc set, RedisModuleConfigApplyFunc,
                             void *privateData) {
    NumericRegistration *registration = nullptr;
    if (std::strcmp(name, "search-disk-max-memory-percentage") == 0) {
      registration = &maxMemoryConfig;
    } else if (std::strcmp(name, "search-disk-wbm-budget-per-index-mb") == 0) {
      registration = &wbmBudgetConfig;
    } else if (std::strcmp(name, "search-disk-max-open-files") == 0) {
      registration = &maxOpenFilesConfig;
    }
    if (registration) {
      *registration = {true, defaultValue, flags, min, max, get, set, privateData};
    }
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

  static RedisSearchDiskIndexSpec *openIndexWithRdbState(
      RedisModuleCtx *, RedisSearchDisk *, const HiddenString *, const char *, size_t nameLength,
      DocumentType, RedisSearchDiskRdbState *rdbState, const SearchDiskCompactionCallbacks *,
      void *privateData) {
    openRdbStateCalled = true;
    openedRdbState = rdbState;
    openedObfuscatedNameLength = nameLength;
    return reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
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
    maxMemoryConfig = NumericRegistration{};
    wbmBudgetConfig = NumericRegistration{};
    maxOpenFilesConfig = NumericRegistration{};
    openRdbStateCalled = false;
    openedRdbState = nullptr;
    openedObfuscatedNameLength = 0;

    api.basic.open = open;
    api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) {};
    api.basic.setThrottleCallbacks = [](ThrottleCB, ThrottleCB) {};
    api.basic.openIndexSpec = openIndex;
    api.basic.openIndexSpecWithRdbState = openIndexWithRdbState;
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

TEST_F(SearchDiskResourcesTest, InitializeRejectsInvalidShardMemory) {
  configuredShardMemory = 0;

  EXPECT_FALSE(SearchDisk_Initialize(ctx));
  EXPECT_FALSE(openCalled);
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

TEST_F(SearchDiskResourcesTest, RegistersResourceConfig) {
  RedisModule_RegisterNumericConfig = registerNumeric;
  RedisModule_RegisterBoolConfig =
      [](RedisModuleCtx *, const char *, int, unsigned int, RedisModuleConfigGetBoolFunc,
         RedisModuleConfigSetBoolFunc, RedisModuleConfigApplyFunc, void *) {
        return REDISMODULE_OK;
      };
  RedisModule_RegisterStringConfig =
      [](RedisModuleCtx *, const char *, const char *, unsigned int,
         RedisModuleConfigGetStringFunc, RedisModuleConfigSetStringFunc,
         RedisModuleConfigApplyFunc, void *) {
        return REDISMODULE_OK;
      };
  RedisModule_RegisterEnumConfig =
      [](RedisModuleCtx *, const char *, int, unsigned int, const char **, const int *, int,
         RedisModuleConfigGetEnumFunc, RedisModuleConfigSetEnumFunc,
         RedisModuleConfigApplyFunc, void *) {
        return REDISMODULE_OK;
      };
  RedisModule_LoadDefaultConfigs = [](RedisModuleCtx *) { return REDISMODULE_OK; };
  RedisModule_LoadConfigs = [](RedisModuleCtx *) { return REDISMODULE_OK; };
  isFlex = true;

  ASSERT_EQ(RediSearch_InitModuleConfigForTests(ctx), REDISMODULE_OK);

  const unsigned int expectedFlags =
      REDISMODULE_CONFIG_HIDDEN | REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_UNPREFIXED;
  ASSERT_TRUE(maxMemoryConfig.seen);
  EXPECT_EQ(maxMemoryConfig.defaultValue, DEFAULT_DISK_MAX_MEMORY_PERCENTAGE);
  EXPECT_EQ(maxMemoryConfig.flags, expectedFlags);
  EXPECT_EQ(maxMemoryConfig.min, DISK_MAX_MEMORY_PERCENTAGE_MIN);
  EXPECT_EQ(maxMemoryConfig.max, DISK_MAX_MEMORY_PERCENTAGE_MAX);
  ASSERT_TRUE(wbmBudgetConfig.seen);
  EXPECT_EQ(wbmBudgetConfig.defaultValue, DEFAULT_DISK_WBM_BUDGET_PER_INDEX_MB);
  EXPECT_EQ(wbmBudgetConfig.flags, expectedFlags);
  EXPECT_EQ(wbmBudgetConfig.min, 1);
  EXPECT_EQ(wbmBudgetConfig.max, DISK_WBM_BUDGET_PER_INDEX_MAX_MB);
  ASSERT_TRUE(maxOpenFilesConfig.seen);
  EXPECT_EQ(maxOpenFilesConfig.defaultValue, DEFAULT_DISK_MAX_OPEN_FILES);
  EXPECT_EQ(maxOpenFilesConfig.flags, expectedFlags);
  EXPECT_EQ(maxOpenFilesConfig.min, DISK_MAX_OPEN_FILES_MIN);
  EXPECT_EQ(maxOpenFilesConfig.max, INT_MAX);

  RedisModuleString *error = nullptr;
  ASSERT_EQ(maxMemoryConfig.set("search-disk-max-memory-percentage", 73,
                                maxMemoryConfig.privateData, &error),
            REDISMODULE_OK);
  EXPECT_EQ(maxMemoryConfig.get("search-disk-max-memory-percentage",
                                maxMemoryConfig.privateData),
            73);
  ASSERT_EQ(wbmBudgetConfig.set("search-disk-wbm-budget-per-index-mb", 37,
                               wbmBudgetConfig.privateData, &error),
            REDISMODULE_OK);
  EXPECT_EQ(wbmBudgetConfig.get("search-disk-wbm-budget-per-index-mb",
                               wbmBudgetConfig.privateData),
            37);
  ASSERT_EQ(maxOpenFilesConfig.set("search-disk-max-open-files", 4093,
                                  maxOpenFilesConfig.privateData, &error),
            REDISMODULE_OK);
  EXPECT_EQ(maxOpenFilesConfig.get("search-disk-max-open-files",
                                  maxOpenFilesConfig.privateData),
            4093);
  EXPECT_EQ(error, nullptr);
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

TEST_F(SearchDiskResourcesTest, RestoreStateOpenUsesDiskBoundary) {
  disk = &api;
  disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
  IndexSpec spec{};
  HiddenString *name = NewHiddenString("restore", 7, false);
  auto *rdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&spec);

  RedisSearchDiskIndexSpec *result = SearchDisk_OpenIndexWithRdbState(
      ctx, name, "obfuscated", DocumentType_Hash, rdbState, &spec);

  EXPECT_TRUE(openRdbStateCalled);
  EXPECT_EQ(openedRdbState, rdbState);
  EXPECT_EQ(openedObfuscatedNameLength, std::strlen("obfuscated"));
  EXPECT_EQ(result, reinterpret_cast<RedisSearchDiskIndexSpec *>(&spec));
  EXPECT_TRUE(spec.diskRegistered);
  HiddenString_Free(name, false);
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
