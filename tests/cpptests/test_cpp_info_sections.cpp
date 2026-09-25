/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "common.h"
#include "concurrent_ctx.h"

extern "C" {
#include "info/info_redis/info_redis.h"
#include "search_disk.h"
extern RedisSearchDiskAPI *disk;
extern bool isFlex;
}

#include <map>
#include <set>
#include <string>

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

struct InfoCapture {
  std::set<std::string> requested;
  std::vector<std::string> sections;
  std::map<std::string, std::string> fields;
  bool selected = false;

  static InfoCapture &get(RedisModuleInfoCtx *ctx) {
    return *reinterpret_cast<InfoCapture *>(ctx);
  }
  static int section(RedisModuleInfoCtx *ctx, const char *name) {
    auto &info = get(ctx);
    info.selected = info.requested.empty() || info.requested.count(name);
    if (info.selected) info.sections.emplace_back(name);
    return info.selected ? REDISMODULE_OK : REDISMODULE_ERR;
  }
  static int field(RedisModuleInfoCtx *ctx, const char *name, const char *value) {
    auto &info = get(ctx);
    if (info.selected) info.fields[name] = value;
    return info.selected ? REDISMODULE_OK : REDISMODULE_ERR;
  }
  template <typename T>
  static int number(RedisModuleInfoCtx *ctx, const char *name, T value) {
    return field(ctx, name, std::to_string(value).c_str());
  }
  static int beginDict(RedisModuleInfoCtx *, const char *) {
    return REDISMODULE_OK;
  }
  static int endDict(RedisModuleInfoCtx *) {
    return REDISMODULE_OK;
  }
};

class InfoSectionsTest : public ::testing::Test {
 protected:
  Restore<decltype(RedisModule_InfoAddSection)> section{RedisModule_InfoAddSection};
  Restore<decltype(RedisModule_InfoAddFieldCString)> string{RedisModule_InfoAddFieldCString};
  Restore<decltype(RedisModule_InfoAddFieldULongLong)> unsignedNumber{
      RedisModule_InfoAddFieldULongLong};
  Restore<decltype(RedisModule_InfoAddFieldLongLong)> signedNumber{
      RedisModule_InfoAddFieldLongLong};
  Restore<decltype(RedisModule_InfoAddFieldDouble)> realNumber{RedisModule_InfoAddFieldDouble};
  Restore<decltype(RedisModule_InfoBeginDictField)> beginDict{RedisModule_InfoBeginDictField};
  Restore<decltype(RedisModule_InfoEndDictField)> endDict{RedisModule_InfoEndDictField};
  Restore<decltype(RedisModule_BigModuleRegister)> bigModuleRegister{RedisModule_BigModuleRegister};
  Restore<decltype(RedisModule_SubscribeToServerEvent)> subscribe{
      RedisModule_SubscribeToServerEvent};
  using LogCallback = void (*)(RedisModuleCtx *, const char *, const char *, ...);
  LogCallback savedLog = RedisModule_Log;
  Restore<decltype(disk)> diskApi{disk};
  Restore<decltype(disk_db)> diskDb{disk_db};
  Restore<decltype(isFlex)> flex{isFlex};
  Restore<decltype(RSGlobalConfig.infoEmitOnZeroIndexes)> emit{
      RSGlobalConfig.infoEmitOnZeroIndexes};
  RedisSearchDiskAPI api{};
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  static inline int collections = 0;
  static inline int cachedCollections = 0;
  static inline int diskOutputs = 0;
  static inline bool acceptV2 = false;
  static inline std::vector<uint64_t> registeredVersions;
  static inline std::vector<unsigned int> controls;
  static inline int targetRegistrations = 0;
  static inline RedisModuleEventCallback cronCallback = nullptr;
  static inline RedisModuleBigCallbacksV2 callbacks{};

  static int registerBigModule(RedisModuleCtx *, RedisModuleBigCallbacks *candidate) {
    registeredVersions.push_back(candidate->version);
    if (candidate->version == REDISMODULE_BIG_CALLBACKS_VERSION && acceptV2) {
      callbacks = *candidate;
      return REDISMODULE_OK;
    }
    return candidate->version == 1 ? REDISMODULE_OK : REDISMODULE_ERR;
  }

  static int subscribeEvent(RedisModuleCtx *, RedisModuleEvent event,
                            RedisModuleEventCallback callback) {
    EXPECT_EQ(event.id, RedisModuleEvent_CronLoop.id);
    cronCallback = callback;
    return REDISMODULE_OK;
  }

  static void discardLog(RedisModuleCtx *, const char *, const char *, ...) {
  }

  void SetUp() override {
    ConcurrentSearch_CreatePool(1);
    RedisModule_InfoAddSection = InfoCapture::section;
    RedisModule_InfoAddFieldCString = InfoCapture::field;
    RedisModule_InfoAddFieldULongLong = InfoCapture::number<unsigned long long>;
    RedisModule_InfoAddFieldLongLong = InfoCapture::number<long long>;
    RedisModule_InfoAddFieldDouble = InfoCapture::number<double>;
    RedisModule_InfoBeginDictField = InfoCapture::beginDict;
    RedisModule_InfoEndDictField = InfoCapture::endDict;
    RedisModule_BigModuleRegister = registerBigModule;
    RedisModule_SubscribeToServerEvent = subscribeEvent;
    RedisModule_Log = discardLog;
    const char *args[] = {"SCHEMA", "title", "TEXT"};
    QueryError err = QueryError_Default();
    ref = IndexSpec_ParseC(nullptr, "info_sections", args, 3, &err);
    spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    ASSERT_NE(spec, nullptr);
    Spec_AddToDict(ref.rm);
    // The disk API only sees this as an opaque identity; no storage is needed.
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
    api.metrics.collectIndexMetrics = [](RedisSearchDisk *,
                                         RedisSearchDiskIndexSpec *) -> uint64_t {
      ++collections;
      return 1234;
    };
    api.metrics.collectCachedIndexMetrics = [](RedisSearchDisk *,
                                               RedisSearchDiskIndexSpec *) -> uint64_t {
      ++cachedCollections;
      return 4321;
    };
    api.metrics.getCachedDiskUsage = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) -> uint64_t {
      return 55;
    };
    api.metrics.getCachedBlockCount = [](RedisSearchDisk *,
                                         RedisSearchDiskIndexSpec *) -> uint64_t { return 9; };
    api.metrics.control = [](RedisSearchDisk *, unsigned int action) {
      controls.push_back(action);
    };
    api.metrics.registerTarget = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *, bool retire) {
      EXPECT_FALSE(retire);
      ++targetRegistrations;
    };
    api.metrics.getInvertedIndexTotalBlocks = [](RedisSearchDiskIndexSpec *) -> uint64_t {
      return 7;
    };
    api.metrics.outputInfoMetrics = [](RedisSearchDisk *, RedisModuleInfoCtx *ctx) {
      ++diskOutputs;
      EXPECT_EQ(collections + cachedCollections, 1);
      EXPECT_EQ(InfoCapture::get(ctx).sections.back(), "disk");
      RedisModule_InfoAddFieldULongLong(ctx, "disk_usage", 1234);
    };
    api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) {};
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    isFlex = true;
    RSGlobalConfig.infoEmitOnZeroIndexes = true;
    collections = cachedCollections = diskOutputs = 0;
    acceptV2 = false;
    registeredVersions.clear();
    controls.clear();
    targetRegistrations = 0;
    cronCallback = nullptr;
    callbacks = {};
  }

  void TearDown() override {
    if (SearchDisk_InfoCacheEnabled()) SearchDisk_Close(nullptr);
    RedisModule_Log = savedLog;
    ConcurrentSearch_ThreadPoolDestroy();
    if (spec) {
      spec->diskSpec = nullptr;
      Indexes_RemoveSpecFromGlobals(ref, false);
    }
  }

  InfoCapture run(std::set<std::string> requested) {
    InfoCapture info;
    info.requested = std::move(requested);
    RS_moduleInfoFunc(reinterpret_cast<RedisModuleInfoCtx *>(&info), 0);
    return info;
  }
};

TEST_F(InfoSectionsTest, CheapSectionsDoNotCollectDiskMetrics) {
  for (const char *name :
       {"version", "fields_statistics", "runtime_configurations", "multi_threading",
        "coordinator_warnings_and_errors", "dialect_statistics", "cursors"}) {
    auto info = run({name});
    EXPECT_EQ(info.sections, std::vector<std::string>{name});
    EXPECT_FALSE(info.fields.empty()) << name;
    EXPECT_EQ(collections, 0) << name;
    EXPECT_EQ(diskOutputs, 0) << name;
  }
}

TEST_F(InfoSectionsTest, UnknownSectionDoesNotCollect) {
  auto info = run({"unknown"});
  EXPECT_TRUE(info.sections.empty());
  EXPECT_TRUE(info.fields.empty());
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(diskOutputs, 0);
}

TEST_F(InfoSectionsTest, MultipleAggregateSectionsCollectOnce) {
  auto info = run({"indexes", "memory", "disk"});
  EXPECT_EQ(info.sections, (std::vector<std::string>{"indexes", "memory", "disk"}));
  EXPECT_EQ(collections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(info.fields.at("total_inverted_index_blocks"), "7");
  EXPECT_EQ(info.fields.at("disk_usage"), "1234");
}

TEST_F(InfoSectionsTest, DiskOnlyStillCollectsBeforeOutput) {
  auto info = run({"disk"});
  EXPECT_EQ(info.sections, std::vector<std::string>{"disk"});
  EXPECT_EQ(collections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(info.fields.at("disk_usage"), "1234");
}

TEST_F(InfoSectionsTest, AllSectionsCollectOnce) {
  auto info = run({});
  EXPECT_EQ(collections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(info.sections.size(),
            std::set<std::string>(info.sections.begin(), info.sections.end()).size());
}

TEST_F(InfoSectionsTest, ZeroIndexSuppressionPreservesConfigAndSelection) {
  spec->diskSpec = nullptr;
  Indexes_RemoveSpecFromGlobals(ref, false);
  spec = nullptr;
  RSGlobalConfig.infoEmitOnZeroIndexes = false;
  auto info = run({"runtime_configurations"});
  EXPECT_EQ(info.sections, std::vector<std::string>{"runtime_configurations"});
  EXPECT_EQ(info.fields.at("info_on_zero_indexes"), "OFF");
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(diskOutputs, 0);
}

TEST_F(InfoSectionsTest, V2CallbacksUseCachedInfoMetrics) {
  acceptV2 = true;
  ASSERT_TRUE(SearchDisk_RegisterBigModuleCallbacks(nullptr));
  EXPECT_EQ(registeredVersions, std::vector<uint64_t>{REDISMODULE_BIG_CALLBACKS_VERSION});
  ASSERT_NE(callbacks.getCachedDiskUsage, nullptr);
  ASSERT_NE(callbacks.pauseMetrics, nullptr);
  ASSERT_NE(callbacks.resumeMetrics, nullptr);
  ASSERT_NE(callbacks.forkChildMetrics, nullptr);
  EXPECT_TRUE(SearchDisk_InfoCacheEnabled());
  EXPECT_EQ(controls, std::vector<unsigned int>{2});
  ASSERT_NE(cronCallback, nullptr);

  spec->diskRegistered = true;
  cronCallback(nullptr, RedisModuleEvent_CronLoop, 0, nullptr);
  EXPECT_EQ(controls, (std::vector<unsigned int>{2, 0}));
  EXPECT_EQ(targetRegistrations, 1);

  auto info = run({"indexes", "memory", "disk"});
  EXPECT_EQ(info.fields.at("total_inverted_index_blocks"), "9");
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(cachedCollections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(callbacks.getCachedDiskUsage(), 55);

  callbacks.pauseMetrics();
  callbacks.resumeMetrics();
  callbacks.forkChildMetrics();
  EXPECT_EQ(controls, (std::vector<unsigned int>{2, 0, 1, 2, 3}));
}

TEST_F(InfoSectionsTest, V1FallbackKeepsLiveInfoMetrics) {
  ASSERT_TRUE(SearchDisk_RegisterBigModuleCallbacks(nullptr));
  EXPECT_EQ(registeredVersions, (std::vector<uint64_t>{REDISMODULE_BIG_CALLBACKS_VERSION, 1}));
  EXPECT_FALSE(SearchDisk_InfoCacheEnabled());
  EXPECT_TRUE(controls.empty());

  auto info = run({"indexes", "memory", "disk"});
  EXPECT_EQ(info.fields.at("total_inverted_index_blocks"), "7");
  EXPECT_EQ(collections, 1);
  EXPECT_EQ(cachedCollections, 0);
  EXPECT_EQ(diskOutputs, 1);
}

}  // namespace
