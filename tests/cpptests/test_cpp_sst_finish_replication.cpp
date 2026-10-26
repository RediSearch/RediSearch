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

extern "C" {
#include "indexes.h"
#include "search_disk.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
extern bool isFlex;
}

#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

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

// Counts are per spec, keyed by the IndexSpec the disk layer receives as
// private data / returns as its handle.
std::map<void *, int> opens;
std::map<void *, int> activations;

class SSTFinishReplicationTestBase : public ::testing::Test {
 protected:
  Restore<decltype(disk)> diskApi{disk};
  Restore<decltype(disk_db)> diskDb{disk_db};
  Restore<decltype(isFlex)> flex{isFlex};
  Restore<decltype(RSGlobalConfig.gcConfigParams.enableGC)> gc{
      RSGlobalConfig.gcConfigParams.enableGC};
  RedisSearchDiskAPI api{};
  std::vector<StrongRef> refs;

  void SetUp() override {
    opens.clear();
    activations.clear();
    RSGlobalConfig.gcConfigParams.enableGC = false;
    api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return true; };
    api.basic.openIndexSpecWithRdbState =
        [](RedisModuleCtx *, RedisSearchDisk *, const HiddenString *, const char *, size_t,
           DocumentType, RedisSearchDiskRdbState *, const SearchDiskFieldOpen *, size_t,
           const SearchDiskCompactionCallbacks *, void *privateData) {
          ++opens[privateData];
          return reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
        };
    api.metrics.activateTarget = [](RedisSearchDiskIndexSpec *index) { ++activations[index]; };
  }

  void TearDown() override {
    isFlex = false;
    for (StrongRef ref : refs) {
      IndexSpec *sp = static_cast<IndexSpec *>(StrongRef_Get(ref));
      sp->diskSpec = nullptr;
      sp->diskRegistered = false;
      // Pending states are borrowed fixture tokens, including in the death-test parent.
      sp->pendingDiskRdbState = nullptr;
      Indexes_RemoveSpecFromGlobals(ref, false);
    }
  }

  // Publishes a spec in the state an SST load leaves it in until LOADING_ENDED.
  IndexSpec *stageSpec(const std::string &name) {
    const char *args[] = {"SCHEMA", "t", "TEXT"};
    QueryError err = QueryError_Default();
    StrongRef ref = IndexSpec_ParseC(nullptr, name.c_str(), args, 3, &err);
    IndexSpec *sp = static_cast<IndexSpec *>(StrongRef_Get(ref));
    EXPECT_NE(sp, nullptr) << QueryError_GetUserError(&err);
    Spec_AddToDict(ref.rm);
    refs.push_back(ref);
    // Opaque to the mocked disk layer, which never dereferences it.
    sp->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(sp);
    return sp;
  }
};

class SSTFinishReplicationTest : public SSTFinishReplicationTestBase,
                                 public ::testing::WithParamInterface<size_t> {};

TEST_F(SSTFinishReplicationTestBase, FailedOpenAbortsCompletion) {
  stageSpec("sst_finish_open_failure");
  api.basic.openIndexSpecWithRdbState = [](RedisModuleCtx *, RedisSearchDisk *,
                                           const HiddenString *, const char *, size_t, DocumentType,
                                           RedisSearchDiskRdbState *, const SearchDiskFieldOpen *,
                                           size_t, const SearchDiskCompactionCallbacks *, void *) {
    return static_cast<RedisSearchDiskIndexSpec *>(nullptr);
  };
  Restore<decltype(RedisModule_GetContextFlags)> flags{RedisModule_GetContextFlags};
  RedisModule_GetContextFlags = [](RedisModuleCtx *) { return 0; };
  // The mock normally throws; reproduce Redis's terminating assertion for this test.
  Restore<decltype(RedisModule__Assert)> assertion{RedisModule__Assert};
  RedisModule__Assert = [](const char *condition, const char *file, int) {
    std::fprintf(stderr, "%s: %s\n", file, condition);
    std::abort();
  };
  disk = &api;
  disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
  isFlex = true;

  EXPECT_DEATH(Indexes_FinishSSTReplication(RSDummyContext), "indexes\\.c: ok");
}

// The registry is often still resizing when the load ends; the walk must still
// open every staged spec exactly once.
TEST_P(SSTFinishReplicationTest, OpensEveryStagedSpecExactlyOnce) {
  std::vector<IndexSpec *> specs;
  for (size_t i = 0; i < GetParam(); ++i) {
    specs.push_back(stageSpec("sst_finish_" + std::to_string(i)));
  }
  disk = &api;
  disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
  isFlex = true;

  Indexes_FinishSSTReplication(RSDummyContext);

  for (IndexSpec *sp : specs) {
    EXPECT_EQ(opens[sp], 1) << HiddenString_GetUnsafe(sp->specName, nullptr);
    EXPECT_EQ(activations[sp], 1) << HiddenString_GetUnsafe(sp->specName, nullptr);
    EXPECT_EQ(sp->diskSpec, reinterpret_cast<RedisSearchDiskIndexSpec *>(sp));
  }
}

INSTANTIATE_TEST_SUITE_P(IndexCounts, SSTFinishReplicationTest,
                         ::testing::Values(1, 4, 5, 8, 9, 10, 16, 17, 33));

}  // namespace
