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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
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
  static inline std::vector<uint64_t> registeredVersions;
  static inline unsigned totalReads = 0;
  static inline unsigned indexUsageReads = 0;
  static inline int targetRegistrations = 0;
  static inline RedisModuleBigCallbacksV1 callbacks{};

  static int registerBigModule(RedisModuleCtx *, RedisModuleBigCallbacks *candidate) {
    registeredVersions.push_back(candidate->version);
    callbacks = *candidate;
    return candidate->version == 1 ? REDISMODULE_OK : REDISMODULE_ERR;
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
    api.metrics.readCachedIndexMetrics = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {
      ++cachedCollections;
      return CachedIndexMetrics{4321, 55, 9};
    };
    api.metrics.stopMetrics = [](RedisSearchDisk *) {};
    api.metrics.activateTarget = [](RedisSearchDiskIndexSpec *) {};
    api.metrics.getCachedTotalDiskUsage = [](RedisSearchDisk *) -> uint64_t {
      ++totalReads;
      return 55;
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
    registeredVersions.clear();
    totalReads = indexUsageReads = 0;
    targetRegistrations = 0;
    callbacks = {};
  }

  void TearDown() override {
    if (disk_db) SearchDisk_Close(nullptr);
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

TEST_F(InfoSectionsTest, MultipleAggregateSectionsReadCacheOnce) {
  auto info = run({"indexes", "memory", "disk"});
  EXPECT_EQ(info.sections, (std::vector<std::string>{"indexes", "memory", "disk"}));
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(cachedCollections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(info.fields.at("total_inverted_index_blocks"), "9");
  EXPECT_EQ(info.fields.at("disk_usage"), "1234");
}

TEST_F(InfoSectionsTest, DiskOnlyReadsCacheBeforeOutput) {
  auto info = run({"disk"});
  EXPECT_EQ(info.sections, std::vector<std::string>{"disk"});
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(cachedCollections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(info.fields.at("disk_usage"), "1234");
}

TEST_F(InfoSectionsTest, AllSectionsReadCacheOnce) {
  auto info = run({});
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(cachedCollections, 1);
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

TEST_F(InfoSectionsTest, V1CallbackAndModuleCacheUsePublishedMetrics) {
  ASSERT_TRUE(SearchDisk_RegisterBigModuleCallbacks(nullptr));
  EXPECT_EQ(registeredVersions, std::vector<uint64_t>{REDISMODULE_BIG_CALLBACKS_VERSION});
  ASSERT_NE(callbacks.getDiskUsage, nullptr);
  EXPECT_EQ(targetRegistrations, 0);

  auto info = run({"indexes", "memory", "disk"});
  EXPECT_EQ(info.fields.at("total_inverted_index_blocks"), "9");
  EXPECT_EQ(collections, 0);
  EXPECT_EQ(cachedCollections, 1);
  EXPECT_EQ(diskOutputs, 1);
  EXPECT_EQ(callbacks.getDiskUsage(), 55);

  EXPECT_EQ(totalReads, 1);
  EXPECT_EQ(indexUsageReads, 0);
}

TEST_F(InfoSectionsTest, CachedTotalDoesNotReadIndexes) {
  ASSERT_TRUE(SearchDisk_RegisterBigModuleCallbacks(nullptr));
  std::vector<StrongRef> indexes;
  Restore<decltype(isFlex)> flexMode{isFlex};
  isFlex = false;
  for (unsigned i = 0; i < 1000; ++i) {
    const char *args[] = {"SCHEMA", "title", "TEXT"};
    QueryError err = QueryError_Default();
    auto name = std::string("cached_total_") + std::to_string(i);
    auto index = IndexSpec_ParseC(nullptr, name.c_str(), args, 3, &err);
    auto *sp = static_cast<IndexSpec *>(StrongRef_Get(index));
    ASSERT_NE(sp, nullptr);
    Spec_AddToDict(index.rm);
    sp->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(sp);
    indexes.push_back(index);
  }
  isFlex = true;
  EXPECT_EQ(callbacks.getDiskUsage(), 55);
  EXPECT_EQ(totalReads, 1);
  EXPECT_EQ(indexUsageReads, 0);
  EXPECT_EQ(collections, 0);
  for (auto index : indexes) {
    static_cast<IndexSpec *>(StrongRef_Get(index))->diskSpec = nullptr;
    Indexes_RemoveSpecFromGlobals(index, false);
  }
}

TEST_F(InfoSectionsTest, ProductionCreateAndDropPublishCachedUsage) {
  Restore<decltype(RSGlobalConfig.gcConfigParams.enableGC)> gcEnabled{
      RSGlobalConfig.gcConfigParams.enableGC};
  RSGlobalConfig.gcConfigParams.enableGC = false;
  static uint64_t total;
  total = 0;
  api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return true; };
  api.basic.reserveOpenFiles = [](RedisSearchDisk *) { return true; };
  api.basic.releaseOpenFiles = [](RedisSearchDisk *) {};
  api.basic.openIndexSpec = [](RedisModuleCtx *, RedisSearchDisk *, const HiddenString *,
                               const char *, size_t, DocumentType, bool,
                               const SearchDiskCompactionCallbacks *, void *privateData) {
    EXPECT_EQ(total, 0u);
    return reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
  };
  api.metrics.activateTarget = [](RedisSearchDiskIndexSpec *index) {
    auto *created = reinterpret_cast<IndexSpec *>(index);
    EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("cached_usage_lifecycle")), created);
    ++targetRegistrations;
    total += 17;
  };
  api.metrics.getCachedTotalDiskUsage = [](RedisSearchDisk *) { return total; };
  api.basic.closeIndexOnMainThread = [](RedisModuleCtx *, RedisSearchDisk *,
                                        RedisSearchDiskIndexSpec *) { total -= 17; };
  api.basic.closeIndexSpec = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {};
  api.index.markToBeDeleted = [](RedisSearchDiskIndexSpec *) {};
  SearchDisk_UpdateMemoryLimit(size_t{1} << 40);
  ASSERT_TRUE(SearchDisk_RegisterBigModuleCallbacks(RSDummyContext));
  EXPECT_EQ(callbacks.getDiskUsage(), 0u);
  RMCK::ArgvList args(RSDummyContext, "FT.CREATE", "cached_usage_lifecycle", "SKIPINITIALSCAN", "SCHEMA", "title", "TEXT");
  QueryError error = QueryError_Default();
  auto *created = Indexes_CreateNewSpec(RSDummyContext, args, args.size(), &error);
  ASSERT_NE(created, nullptr);
  EXPECT_EQ(targetRegistrations, 1);
  EXPECT_EQ(callbacks.getDiskUsage(), 17u);
  Indexes_RemoveSpecFromGlobals(IndexSpec_GetStrongRefUnsafe(created), false);
  EXPECT_EQ(callbacks.getDiskUsage(), 0u);
}

TEST_F(InfoSectionsTest, RegistrationFailureIsReported) {
  RedisModule_BigModuleRegister = [](RedisModuleCtx *, RedisModuleBigCallbacks *) {
    return REDISMODULE_ERR;
  };
  EXPECT_FALSE(SearchDisk_RegisterBigModuleCallbacks(nullptr));
}

}  // namespace
