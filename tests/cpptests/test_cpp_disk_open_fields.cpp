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
#include "rules.h"
#include "search_disk.h"
extern RedisSearchDiskAPI *disk;
extern bool isFlex;
}

#include <string>
#include <tuple>
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

using Desc = std::tuple<t_fieldIndex, FieldType>;

const char *const kVectorF32Dim4[] = {"VECTOR",  "HNSW",       "14", "TYPE",
                                      "FLOAT32", "DIM",        "4",  "DISTANCE_METRIC",
                                      "L2",      "M",          "16", "EF_CONSTRUCTION",
                                      "200",     "EF_RUNTIME", "10", "RERANK",
                                      "TRUE"};

// Drives the real create/alter/load code against a recording mock of the disk API.
class DiskSchemaOpenTest : public ::testing::Test {
 protected:
  Restore<decltype(disk)> diskApi{disk};
  Restore<decltype(disk_db)> diskDb{disk_db};
  Restore<decltype(isFlex)> flex{isFlex};
  Restore<decltype(RSGlobalConfig.gcConfigParams.enableGC)> gcEnabled{
      RSGlobalConfig.gcConfigParams.enableGC};
  RedisSearchDiskAPI api{};
  std::vector<StrongRef> created;

  static inline std::vector<std::string> events;
  static inline std::vector<std::vector<Desc>> openCalls;
  // The schema must be complete before the database is opened.
  static inline std::vector<size_t> numFieldsAtOpen;
  static inline bool openResult = true;
  static inline bool initMissingResult = true;
  static inline int closeOnMainThreadCalls = 0;
  static inline int closeIndexSpecCalls = 0;
  static inline int rdbStateToken = 0;

  static bool recordSchema(RedisModuleCtx *ctx, void *privateData,
                           const SearchDiskFieldOpen *fields, size_t n) {
    auto *index = reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
    EXPECT_EQ(ctx, RSDummyContext);
    numFieldsAtOpen.push_back(reinterpret_cast<IndexSpec *>(index)->numFields);
    std::vector<Desc> descs;
    bool indexMissing = false;
    for (size_t i = 0; i < n; ++i) {
      descs.emplace_back(fields[i].fieldIndex, fields[i].types);
      const auto *sp = reinterpret_cast<const IndexSpec *>(index);
      const auto *fs = sp->fields + fields[i].fieldIndex;
      EXPECT_EQ(fields[i].indexMissing, FieldSpec_IndexesMissing(fs) != 0);
      indexMissing |= fields[i].indexMissing;
      if (fields[i].types & INDEXFLD_T_VECTOR) {
        size_t nameLen;
        const char *name = HiddenString_GetUnsafe(fs->fieldName, &nameLen);
        EXPECT_NE(fields[i].fieldName, nullptr);
        EXPECT_EQ(fields[i].fieldNameLen, nameLen);
        if (fields[i].fieldName) {
          EXPECT_EQ(std::string(fields[i].fieldName, fields[i].fieldNameLen),
                    std::string(name, nameLen));
        }
        EXPECT_EQ(fields[i].vectorParams, &fs->vectorOpts.vecSimParams);
      } else {
        EXPECT_EQ(fields[i].fieldName, nullptr);
        EXPECT_EQ(fields[i].fieldNameLen, 0u);
        EXPECT_EQ(fields[i].vectorParams, nullptr);
      }
    }
    openCalls.push_back(std::move(descs));
    return openResult && (!indexMissing || initMissingResult);
  }

  void SetUp() override {
    ConcurrentSearch_CreatePool(1);
    RSGlobalConfig.gcConfigParams.enableGC = false;
    events.clear();
    openCalls.clear();
    numFieldsAtOpen.clear();
    openResult = true;
    initMissingResult = true;
    closeOnMainThreadCalls = closeIndexSpecCalls = 0;

    api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return true; };
    api.basic.reserveOpenFiles = [](RedisSearchDisk *) { return true; };
    api.basic.releaseOpenFiles = [](RedisSearchDisk *) {};
    // The mock uses the C spec as an opaque disk handle.
    api.basic.openIndexSpec = [](RedisModuleCtx *ctx, RedisSearchDisk *, const HiddenString *,
                                 const char *, size_t, DocumentType, bool,
                                 const SearchDiskFieldOpen *fields, size_t n,
                                 const SearchDiskCompactionCallbacks *, void *privateData) {
      events.emplace_back("openIndex");
      return recordSchema(ctx, privateData, fields, n)
                 ? reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData)
                 : nullptr;
    };
    api.basic.openIndexSpecWithRdbState =
        [](RedisModuleCtx *ctx, RedisSearchDisk *, const HiddenString *, const char *, size_t,
           DocumentType, RedisSearchDiskRdbState *state, const SearchDiskFieldOpen *fields,
           size_t n, const SearchDiskCompactionCallbacks *, void *privateData) {
          EXPECT_EQ(state, reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken));
          events.emplace_back("openIndexWithRdbState");
          return recordSchema(ctx, privateData, fields, n)
                     ? reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData)
                     : nullptr;
        };
    api.basic.freeRdbState = [](RedisSearchDiskRdbState *) {};
    api.basic.closeIndexOnMainThread = [](RedisModuleCtx *, RedisSearchDisk *,
                                          RedisSearchDiskIndexSpec *) { ++closeOnMainThreadCalls; };
    api.basic.closeIndexSpec = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {
      ++closeIndexSpecCalls;
    };
    api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) {};
    api.index.markToBeDeleted = [](RedisSearchDiskIndexSpec *) {};

    api.vector.createVectorIndex = [](RedisModuleCtx *, RedisSearchDiskIndexSpec *index,
                                      const VecSimParamsDisk *params) -> void * {
      EXPECT_EQ(params->diskContext->storage, index);
      events.emplace_back("createVector");
      return VecSimIndex_New(params->indexParams->algoParams.tieredParams.primaryIndexParams);
    };
    api.vector.bindVectorIndexStorage = [](RedisModuleCtx *, RedisSearchDiskIndexSpec *index,
                                           void *, const VecSimParamsDisk *params) {
      EXPECT_EQ(params->diskContext->storage, index);
      events.emplace_back("bindVector");
      return true;
    };
    api.vector.freeVectorIndex = [](void *index) {
      VecSimIndex_Free(static_cast<VecSimIndex *>(index));
    };
    api.metrics.activateTarget = [](RedisSearchDiskIndexSpec *) {};
    api.metrics.stopMetrics = [](RedisSearchDisk *) {};
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    isFlex = true;
    SearchDisk_UpdateMemoryLimit(size_t{1} << 40);
  }

  void TearDown() override {
    for (auto ref : created) {
      Indexes_RemoveSpecFromGlobals(ref, false);
    }
    if (disk_db) SearchDisk_Close(nullptr);
    ConcurrentSearch_ThreadPoolDestroy();
  }

  // FT.CREATE <name> SKIPINITIALSCAN SCHEMA <schema...>
  IndexSpec *create(const char *name, std::vector<const char *> schema, QueryError *err) {
    std::vector<const char *> argv = {"FT.CREATE", name, "SKIPINITIALSCAN", "SCHEMA"};
    argv.insert(argv.end(), schema.begin(), schema.end());
    RMCK::ArgvList args(RSDummyContext, argv.data(), argv.size());
    IndexSpec *sp = Indexes_CreateNewSpec(RSDummyContext, args, args.size(), err);
    if (sp) created.push_back(IndexSpec_GetStrongRefUnsafe(sp));
    return sp;
  }

  static std::vector<const char *> withVector(std::vector<const char *> schema, const char *name) {
    schema.push_back(name);
    schema.insert(schema.end(), std::begin(kVectorF32Dim4), std::end(kVectorF32Dim4));
    return schema;
  }
};

TEST_F(DiskSchemaOpenTest, CreateOpensAllFieldsOnceAfterParsing) {
  auto schema =
      withVector({"title", "TEXT", "tags", "TAG", "price", "NUMERIC", "loc", "GEO"}, "v32");
  schema.insert(schema.end(), {"v16", "VECTOR", "HNSW", "14", "TYPE", "FLOAT16", "DIM", "3",
                               "DISTANCE_METRIC", "COSINE", "M", "16", "EF_CONSTRUCTION", "200",
                               "EF_RUNTIME", "10", "RERANK", "FALSE"});
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_create", schema, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);

  EXPECT_EQ(events, (std::vector<std::string>{"openIndex"}));
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(numFieldsAtOpen, std::vector<size_t>{6});
  EXPECT_EQ(openCalls[0], (std::vector<Desc>{
                              {0, INDEXFLD_T_FULLTEXT},
                              {1, INDEXFLD_T_TAG},
                              {2, INDEXFLD_T_NUMERIC},
                              {3, INDEXFLD_T_GEO},
                              {4, INDEXFLD_T_VECTOR},
                              {5, INDEXFLD_T_VECTOR},
                          }));
  // C-side field handles are bound after a successful database open.
  EXPECT_EQ(sp->fields[4].vectorOpts.diskCtx.storage, sp->diskSpec);
  EXPECT_TRUE(sp->fields[4].vectorOpts.diskCtx.rerank);
  EXPECT_FALSE(sp->fields[5].vectorOpts.diskCtx.rerank);
  EXPECT_NE(sp->fields[1].tagOpts.tagIndex, nullptr);
}

TEST_F(DiskSchemaOpenTest, MemoryIndexDoesNotOpenDiskFieldStorage) {
  isFlex = false;
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_memory", {"title", "TEXT", "tags", "TAG"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  EXPECT_EQ(sp->diskSpec, nullptr);
  EXPECT_TRUE(events.empty());
  EXPECT_TRUE(openCalls.empty());
}

TEST_F(DiskSchemaOpenTest, CreateWithSchemaErrorOpensNoField) {
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_bad_schema", {"title", "TEXT", "bad", "NOSUCHTYPE"}, &err),
            nullptr);
  QueryError_ClearError(&err);
  EXPECT_TRUE(openCalls.empty());
}

// Exercises a schema with many declarations.
TEST_F(DiskSchemaOpenTest, CreateWithWideSchemaPassesEveryField) {
  std::vector<std::string> names;
  for (int i = 0; i < 20; ++i) names.push_back("f" + std::to_string(i));
  std::vector<const char *> schema;
  for (int i = 0; i < 20; ++i) {
    schema.push_back(names[i].c_str());
    schema.push_back(i % 2 ? "TAG" : "NUMERIC");
  }
  QueryError err = QueryError_Default();
  ASSERT_NE(create("open_fields_wide", schema, &err), nullptr) << QueryError_GetUserError(&err);

  ASSERT_EQ(openCalls.size(), 1u);
  std::vector<Desc> expected;
  for (t_fieldIndex i = 0; i < 20; ++i) {
    expected.emplace_back(i, i % 2 ? INDEXFLD_T_TAG : INDEXFLD_T_NUMERIC);
  }
  EXPECT_EQ(openCalls[0], expected);
}

TEST_F(DiskSchemaOpenTest, FailedMissingStorageInitFailsCreate) {
  initMissingResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_create_missing_fail", {"tags", "TAG", "INDEXMISSING"}, &err),
            nullptr);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("open_fields_create_missing_fail")), nullptr);
  EXPECT_EQ(closeOnMainThreadCalls, 0);
}

TEST_F(DiskSchemaOpenTest, FailedCreateRegistersNothing) {
  openResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_create_fail", {"title", "TEXT", "tags", "TAG"}, &err), nullptr);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("open_fields_create_fail")), nullptr);
  EXPECT_EQ(closeOnMainThreadCalls, 0);
  EXPECT_EQ(closeIndexSpecCalls, 0);
}

TEST_F(DiskSchemaOpenTest, ReplicatedCreateStorageFailureAborts) {
  Restore<decltype(RedisModule_GetContextFlags)> flags{RedisModule_GetContextFlags};
  RedisModule_GetContextFlags = [](RedisModuleCtx *) { return REDISMODULE_CTX_FLAGS_REPLICATED; };
  openResult = false;
  EXPECT_DEATH(
      {
        QueryError err = QueryError_Default();
        create("replicated_create_fail", {"title", "TEXT"}, &err);
      },
      "");
}

TEST_F(DiskSchemaOpenTest, ReplicatedMissingStorageFailureAborts) {
  Restore<decltype(RedisModule_GetContextFlags)> flags{RedisModule_GetContextFlags};
  RedisModule_GetContextFlags = [](RedisModuleCtx *) { return REDISMODULE_CTX_FLAGS_REPLICATED; };
  initMissingResult = false;
  EXPECT_DEATH(
      {
        QueryError err = QueryError_Default();
        create("replicated_missing_fail", {"tags", "TAG", "INDEXMISSING"}, &err);
      },
      "");
}

class DiskSchemaOpenLoadTest : public DiskSchemaOpenTest {
 protected:
  RedisModuleIO *io = nullptr;
  IndexSpec *loaded = nullptr;

  // Saves a disk index created from `schema` and loads it back without opening its disk index.
  void saveAndLoad(const char *name, std::vector<const char *> schema) {
    QueryError err = QueryError_Default();
    IndexSpec *sp = create(name, schema, &err);
    ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
    io = RMCK_CreateRdbIO();
    IndexSpec_RdbSave(io, sp, 0);
    ASSERT_EQ(RMCK_IsIOError(io), 0);
    Indexes_RemoveSpecFromGlobals(created.back(), false);
    created.pop_back();
    io->read_pos = 0;

    loaded = IndexSpec_RdbLoad(io, INDEX_CURRENT_VERSION, false, &err);
    ASSERT_NE(loaded, nullptr) << QueryError_GetUserError(&err);
    events.clear();
    openCalls.clear();
    closeOnMainThreadCalls = closeIndexSpecCalls = 0;
  }

  void TearDown() override {
    if (loaded) {
      StrongRef ref = IndexSpec_GetStrongRefUnsafe(loaded);
      if (loaded->diskSpec) SearchDisk_CloseIndexOnMainThread(RSDummyContext, loaded);
      SchemaPrefixes_RemoveSpec(ref);
      StrongRef_Release(ref);
    }
    if (io) RMCK_FreeRdbIO(io);
    DiskSchemaOpenTest::TearDown();
  }
};

const std::vector<Desc> kLoadedFields = {
    {0, INDEXFLD_T_FULLTEXT},
    {1, INDEXFLD_T_TAG},
    {2, INDEXFLD_T_VECTOR},
};

TEST_F(DiskSchemaOpenLoadTest, RdbLoadOpensAllFieldsOnce) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad(
      "open_fields_rdb", withVector({"title", "TEXT", "tags", "TAG", "INDEXMISSING"}, "v")));

  QueryError err = QueryError_Default();
  ASSERT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_OK)
      << QueryError_GetUserError(&err);

  EXPECT_EQ(events, (std::vector<std::string>{"openIndex"}));
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openCalls[0], kLoadedFields);
  EXPECT_EQ(loaded->fields[2].vectorOpts.diskCtx.storage, loaded->diskSpec);
}

TEST_F(DiskSchemaOpenLoadTest, RdbLoadFailsWhenOpeningFieldsFails) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_rdb_fail", {"title", "TEXT", "tags", "TAG"}));

  openResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_ERR);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(closeOnMainThreadCalls, 0);
}

// Loads through the non-SST path and plants the pending state, so vector fields, which SST load
// creates up front, are left out.

TEST_F(DiskSchemaOpenLoadTest, SstOpenOpensNonVectorFieldsOnce) {
  ASSERT_NO_FATAL_FAILURE(
      saveAndLoad("open_fields_sst", {"title", "TEXT", "tags", "TAG", "INDEXMISSING"}));
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);

  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));

  EXPECT_EQ(events, (std::vector<std::string>{"openIndexWithRdbState"}));
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openCalls[0], (std::vector<Desc>(kLoadedFields.begin(), kLoadedFields.end() - 1)));
}

TEST_F(DiskSchemaOpenLoadTest, SstOpenPassesVectorSchemaBeforeCreatingVector) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("schema_sst_vector", withVector({"tags", "TAG"}, "v")));
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  EXPECT_EQ(events, (std::vector<std::string>{"openIndexWithRdbState", "createVector"}));
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openCalls[0], (std::vector<Desc>{{0, INDEXFLD_T_TAG}, {1, INDEXFLD_T_VECTOR}}));
  EXPECT_EQ(loaded->pendingDiskRdbState, nullptr);
}

TEST_F(DiskSchemaOpenLoadTest, SstOpenPassesVectorSchemaBeforeBindingVector) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("schema_sst_bind", withVector({"tags", "TAG"}, "v")));
  auto *fs = &loaded->fields[1];
  fs->vectorOpts.vecSimIndex =
      VecSimIndex_New(fs->vectorOpts.vecSimParams.algoParams.tieredParams.primaryIndexParams);
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  EXPECT_EQ(events, (std::vector<std::string>{"openIndexWithRdbState", "bindVector"}));
  EXPECT_EQ(loaded->pendingDiskRdbState, nullptr);
}

TEST_F(DiskSchemaOpenLoadTest, SstOpenFailureConsumesPendingState) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("schema_sst_fail", {"tags", "TAG", "INDEXMISSING"}));
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
  openResult = false;
  EXPECT_FALSE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  EXPECT_EQ(loaded->pendingDiskRdbState, nullptr);
  EXPECT_EQ(loaded->diskSpec, nullptr);
  EXPECT_FALSE(loaded->diskRegistered);
  EXPECT_EQ(closeOnMainThreadCalls, 0);
}

}  // namespace
