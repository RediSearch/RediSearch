/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "search_disk.h"

extern "C" {
extern RedisSearchDiskAPI *disk;
extern bool isFlex;
}

#include "gtest/gtest.h"

#include <cstring>

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
  Restore<decltype(disk)> diskApi{disk};
  Restore<decltype(disk_db)> diskDb{disk_db};
  Restore<decltype(isFlex)> flex{isFlex};

  RedisSearchDiskAPI api{};
  RedisModuleCtx *ctx = nullptr;

  static inline bool openIndexCalled = false;
  static inline bool openIndexSucceeds = true;
  static inline bool restoreOpen = false;
  static inline bool openRdbStateCalled = false;
  static inline RedisSearchDiskRdbState *openedRdbState = nullptr;
  static inline size_t openedObfuscatedNameLength = 0;

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

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    ASSERT_NE(ctx, nullptr);

    openIndexCalled = false;
    openIndexSucceeds = true;
    restoreOpen = false;
    openRdbStateCalled = false;
    openedRdbState = nullptr;
    openedObfuscatedNameLength = 0;

    api.basic.openIndexSpec = openIndex;
    api.basic.openIndexSpecWithRdbState = openIndexWithRdbState;

    disk = nullptr;
    disk_db = nullptr;
    isFlex = false;
  }

  void TearDown() override {
    RedisModule_FreeThreadSafeContext(ctx);
  }

};

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
