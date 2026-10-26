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

#include <cmath>
#include <map>
#include <string>
#include <thread>

extern "C" {
#include "indexes.h"
#include "info/indexes_info.h"
#include "info/info_command.h"
#include "search_disk.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
extern bool isFlex;
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

class ImportInfoTest : public ::testing::Test {
 protected:
  Restore<decltype(disk)> diskApi{disk};
  Restore<decltype(disk_db)> diskDb{disk_db};
  Restore<decltype(isFlex)> flex{isFlex};
  Restore<decltype(RSGlobalConfig.gcConfigParams.enableGC)> gc{
      RSGlobalConfig.gcConfigParams.enableGC};
  Restore<decltype(RedisModule_ReplyWithSimpleString)> replyString{
      RedisModule_ReplyWithSimpleString};
  Restore<decltype(RedisModule_ReplyWithLongLong)> replyInteger{RedisModule_ReplyWithLongLong};
  Restore<decltype(RedisModule_ReplyWithDouble)> replyDouble{RedisModule_ReplyWithDouble};
  Restore<decltype(RedisModule_InfoAddSection)> infoSection{RedisModule_InfoAddSection};
  Restore<decltype(RedisModule_InfoBeginDictField)> infoBegin{RedisModule_InfoBeginDictField};
  Restore<decltype(RedisModule_InfoEndDictField)> infoEnd{RedisModule_InfoEndDictField};
  Restore<decltype(RedisModule_InfoAddFieldCString)> infoString{RedisModule_InfoAddFieldCString};
  Restore<decltype(RedisModule_InfoAddFieldLongLong)> infoInteger{RedisModule_InfoAddFieldLongLong};
  Restore<decltype(RedisModule_InfoAddFieldULongLong)> infoUnsigned{
      RedisModule_InfoAddFieldULongLong};
  Restore<decltype(RedisModule_InfoAddFieldDouble)> infoDouble{RedisModule_InfoAddFieldDouble};
  RedisSearchDiskAPI api{};
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  std::string lastKey;
  std::map<std::string, double> numbers;
  size_t visibilityReads = 0;
  static inline ImportInfoTest *active = nullptr;

  template <typename T>
  static int recordNumber(RedisModuleInfoCtx *, const char *name, T value) {
    active->numbers[name] = static_cast<double>(value);
    return REDISMODULE_OK;
  }

  void SetUp() override {
    active = this;
    RSGlobalConfig.gcConfigParams.enableGC = false;
    isFlex = false;
    const char *args[] = {"SCHEMA", "t", "TEXT"};
    QueryError error = QueryError_Default();
    ref = IndexSpec_ParseC(nullptr, "import_info", args, 3, &error);
    spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&error);
    Spec_AddToDict(ref.rm);

    RedisModule_ReplyWithSimpleString = [](RedisModuleCtx *ctx, const char *value) {
      active->lastKey = value;
      return active->replyString.saved(ctx, value);
    };
    RedisModule_ReplyWithLongLong = [](RedisModuleCtx *ctx, long long value) {
      active->numbers[active->lastKey] = static_cast<double>(value);
      return active->replyInteger.saved(ctx, value);
    };
    RedisModule_ReplyWithDouble = [](RedisModuleCtx *ctx, double value) {
      active->numbers[active->lastKey] = value;
      return active->replyDouble.saved(ctx, value);
    };
    RedisModule_InfoAddSection = [](RedisModuleInfoCtx *, const char *) { return REDISMODULE_OK; };
    RedisModule_InfoBeginDictField = RedisModule_InfoAddSection;
    RedisModule_InfoEndDictField = [](RedisModuleInfoCtx *) { return REDISMODULE_OK; };
    RedisModule_InfoAddFieldCString = [](RedisModuleInfoCtx *, const char *, const char *) {
      return REDISMODULE_OK;
    };
    RedisModule_InfoAddFieldLongLong = recordNumber<long long>;
    RedisModule_InfoAddFieldULongLong = recordNumber<unsigned long long>;
    RedisModule_InfoAddFieldDouble = recordNumber<double>;
  }

  void TearDown() override {
    spec->diskSpec = nullptr;
    spec->diskRegistered = false;
    Indexes_RemoveSpecFromGlobals(ref, false);
    active = nullptr;
  }

  void makeDisk() {
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
    spec->stats.scoring.numDocuments = 5;
    spec->stats.scoring.totalDocsLen = 40;
    api.slotMigration.getHiddenImportStats = [](RedisSearchDiskIndexSpec *index, uint64_t *docs,
                                                uint64_t *length) {
      auto *spec = reinterpret_cast<IndexSpec *>(index);
      ++active->visibilityReads;
      EXPECT_TRUE(IndexSpec_IsReadLocked(spec));
      int writer = 0;
      std::thread probe([&] {
        writer = pthread_rwlock_trywrlock(&spec->rwlock);
        if (writer == 0) pthread_rwlock_unlock(&spec->rwlock);
      });
      probe.join();
      EXPECT_NE(writer, 0) << "visibility and physical counters must share the spec lock";
      *docs = 3;
      *length = 21;
    };
    api.docTable.getMaxDocId = [](RedisSearchDiskIndexSpec *) -> t_docId { return 5; };
    api.metrics.getNumRecords = [](RedisSearchDiskIndexSpec *) -> uint64_t { return 6; };
    api.metrics.getTextFieldMetrics = [](const RedisSearchDiskIndexSpec *, t_fieldId) {
      return PerFieldTextDiskMetrics{};
    };
    api.metrics.getInvertedIndexTotalBlocks = [](RedisSearchDiskIndexSpec *) -> uint64_t {
      return 0;
    };
    api.metrics.collectIndexMetrics = [](RedisSearchDisk *,
                                         RedisSearchDiskIndexSpec *) -> uint64_t { return 0; };
    api.metrics.getInvertedIndexTotalMemory = api.metrics.collectIndexMetrics;
    api.metrics.getDocTableTotalMemory = api.metrics.collectIndexMetrics;
    api.metrics.readCachedIndexMetrics = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {
      return CachedIndexMetrics{};
    };
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    isFlex = true;
  }

  void ftInfo() {
    RMCK::ArgvList args(RSDummyContext, "FT.INFO", "import_info");
    ASSERT_EQ(IndexInfoCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
  }
};

TEST_F(ImportInfoTest, EmptyFtInfoPreservesNaNAverage) {
  ftInfo();
  ASSERT_TRUE(numbers.count("records_per_doc_avg"));
  EXPECT_TRUE(std::isnan(numbers.at("records_per_doc_avg")));
}

TEST_F(ImportInfoTest, EmptyModuleInfoPreservesNaNAverage) {
  RedisModuleInfoCtx ctx;
  IndexSpec_AddToInfo(&ctx, spec, false, false, false);
  ASSERT_TRUE(numbers.count("records_per_doc_avg"));
  EXPECT_TRUE(std::isnan(numbers.at("records_per_doc_avg")));
}

TEST_F(ImportInfoTest, FtInfoLocksItsVisibleSnapshot) {
  makeDisk();
  ftInfo();
  EXPECT_EQ(visibilityReads, 1u);
  EXPECT_EQ(numbers.at("num_docs"), 2);
  EXPECT_EQ(numbers.at("records_per_doc_avg"), 3);
}

TEST_F(ImportInfoTest, TotalInfoLocksItsVisibleSnapshot) {
  makeDisk();
  const auto info = IndexesInfo_TotalInfo();
  EXPECT_EQ(visibilityReads, 1u);
  EXPECT_EQ(info.total_num_docs_in_indexes, 2u);
}

TEST_F(ImportInfoTest, ModuleInfoLocksItsVisibleSnapshot) {
  makeDisk();
  RedisModuleInfoCtx ctx;
  IndexSpec_AddToInfo(&ctx, spec, false, false, false);
  EXPECT_EQ(visibilityReads, 1u);
  EXPECT_EQ(numbers.at("number_of_docs"), 2);
  EXPECT_EQ(numbers.at("records_per_doc_avg"), 3);
}

TEST_F(ImportInfoTest, ModuleInfoPreservesItsCallersReadLock) {
  makeDisk();
  RedisModuleInfoCtx ctx;
  IndexSpec_LockRead(spec);
  IndexSpec_AddToInfo(&ctx, spec, false, false, false);
  EXPECT_TRUE(IndexSpec_IsReadLocked(spec));
  IndexSpec_Unlock(spec);
  EXPECT_EQ(visibilityReads, 1u);
  EXPECT_EQ(numbers.at("number_of_docs"), 2);
}

TEST_F(ImportInfoTest, CrashInfoSkipsVisibilityTrackingWhileSpecIsWriteLocked) {
  makeDisk();
  RedisModuleInfoCtx ctx;
  IndexSpec_LockWrite(spec);
  IndexSpec_AddToInfo(&ctx, spec, false, true, false);
  IndexSpec_Unlock(spec);
  EXPECT_EQ(visibilityReads, 0u);
  EXPECT_EQ(numbers.at("number_of_docs"), 5);
}
}  // namespace
