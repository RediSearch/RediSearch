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
#include "query_test_utils.h"

extern "C" {
#include "rules.h"
#include "search_disk.h"
#include "tag_index.h"
#include "triemap_ffi.h"
int RegisterDebugCommands(RedisModuleCommand *debugCommand);
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
  static inline std::vector<IndexFlags> flagsAtOpen;
  static inline bool openResult = true;
  // When set, the mock refuses the open and reports this reason through `status`.
  static inline const char *openError = nullptr;
  static inline bool initMissingResult = true;
  static inline int closeOnMainThreadCalls = 0;
  static inline int closeIndexSpecCalls = 0;
  static inline int rdbStateToken = 0;
  static inline SearchDiskCompactionCallbacks compactionCallbacks{};
  static inline bool restoredTagCounts = false;

  static bool recordSchema(RedisModuleCtx *ctx, void *privateData, IndexFlags flags,
                           const SearchDiskField *fields, size_t n) {
    auto *index = reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
    EXPECT_EQ(ctx, RSDummyContext);
    flagsAtOpen.push_back(flags);
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
    flagsAtOpen.clear();
    openResult = true;
    openError = nullptr;
    initMissingResult = true;
    closeOnMainThreadCalls = closeIndexSpecCalls = 0;

    api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return true; };
    api.basic.reserveOpenFiles = [](RedisSearchDisk *) { return true; };
    api.basic.releaseOpenFiles = [](RedisSearchDisk *) {};
    // The mock uses the C spec as an opaque disk handle.
    api.basic.openIndexSpec = [](RedisModuleCtx *ctx, RedisSearchDisk *, const HiddenString *,
                                 const char *, size_t, DocumentType, bool, IndexFlags flags,
                                 const SearchDiskField *fields, size_t n,
                                 const SearchDiskCompactionCallbacks *callbacks, void *privateData,
                                 QueryError *status) {
      compactionCallbacks = *callbacks;
      events.emplace_back("openIndex");
      if (openError) {
        QueryError_SetError(status, QUERY_ERROR_CODE_DISK_CREATION, openError);
        return static_cast<RedisSearchDiskIndexSpec *>(nullptr);
      }
      return recordSchema(ctx, privateData, flags, fields, n)
                 ? reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData)
                 : nullptr;
    };
    api.basic.openIndexSpecWithRdbState =
        [](RedisModuleCtx *ctx, RedisSearchDisk *, const HiddenString *, const char *, size_t,
           DocumentType, RedisSearchDiskRdbState *state, bool tagCountsRestored, IndexFlags flags,
           const SearchDiskField *fields, size_t n, const SearchDiskCompactionCallbacks *,
           void *privateData) {
          restoredTagCounts = tagCountsRestored;
          EXPECT_EQ(state, reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken));
          events.emplace_back("openIndexWithRdbState");
          return recordSchema(ctx, privateData, flags, fields, n)
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

  // FT.CREATE <name> SKIPINITIALSCAN <options...> SCHEMA <schema...>
  IndexSpec *create(const char *name, std::vector<const char *> schema, QueryError *err,
                    std::vector<const char *> options = {}) {
    std::vector<const char *> argv = {"FT.CREATE", name, "SKIPINITIALSCAN"};
    argv.insert(argv.end(), options.begin(), options.end());
    argv.push_back("SCHEMA");
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

TEST_F(DiskSchemaOpenTest, CreateForwardsIndexFlags) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_flags", {"title", "TEXT"}, &err, {"NOOFFSETS", "NOFIELDS"});
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);

  EXPECT_EQ(flagsAtOpen, std::vector<IndexFlags>{sp->flags});
  EXPECT_FALSE(sp->flags & (Index_StoreTermOffsets | Index_StoreByteOffsets));
  EXPECT_FALSE(sp->flags & Index_StoreFieldFlags);
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

TEST_F(DiskSchemaOpenTest, FailedCreateReportsTheDiskLayersReason) {
  openError = "index name contains a NUL byte";
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_create_reason", {"title", "TEXT"}, &err), nullptr);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  std::string message = QueryError_GetUserError(&err);
  EXPECT_NE(message.find(openError), std::string::npos) << message;
  QueryError_ClearError(&err);
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("open_fields_create_reason")), nullptr);
}

TEST_F(DiskSchemaOpenTest, FailedCreateWithoutReasonReportsGenericError) {
  openResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_create_generic", {"title", "TEXT"}, &err), nullptr);
  std::string message = QueryError_GetUserError(&err);
  EXPECT_NE(message.find("Could not open disk index"), std::string::npos) << message;
  QueryError_ClearError(&err);
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

TEST_F(DiskSchemaOpenTest, ReplicatedResourcePreparationFailureAborts) {
  Restore<decltype(RedisModule_GetContextFlags)> flags{RedisModule_GetContextFlags};
  RedisModule_GetContextFlags = [](RedisModuleCtx *) { return REDISMODULE_CTX_FLAGS_REPLICATED; };
  api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return false; };
  EXPECT_DEATH(
      {
        QueryError err = QueryError_Default();
        create("replicated_prepare_fail", {"title", "TEXT"}, &err);
      },
      "");
}

class DiskSchemaOpenLoadTest : public DiskSchemaOpenTest {
 protected:
  RedisModuleIO *io = nullptr;
  IndexSpec *loaded = nullptr;

  // Saves a disk index created from `schema` and loads it back without opening its disk index.
  void saveAndLoad(const char *name, std::vector<const char *> schema,
                   std::vector<const char *> options = {}) {
    QueryError err = QueryError_Default();
    IndexSpec *sp = create(name, schema, &err, options);
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
    flagsAtOpen.clear();
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

TEST_F(DiskSchemaOpenLoadTest, RdbLoadForwardsIndexFlags) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_rdb_flags", {"title", "TEXT"}, {"NOOFFSETS"}));

  QueryError err = QueryError_Default();
  ASSERT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_OK)
      << QueryError_GetUserError(&err);

  EXPECT_EQ(flagsAtOpen, std::vector<IndexFlags>{loaded->flags});
  EXPECT_FALSE(loaded->flags & Index_StoreTermOffsets);
}

TEST_F(DiskSchemaOpenLoadTest, RdbLoadReportsTheDiskLayersReason) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_rdb_reason", {"title", "TEXT"}));

  openError = "index name contains a NUL byte";
  QueryError err = QueryError_Default();
  EXPECT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_ERR);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  std::string message = QueryError_GetUserError(&err);
  EXPECT_NE(message.find(openError), std::string::npos) << message;
  QueryError_ClearError(&err);
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

TEST_F(DiskSchemaOpenLoadTest, SstOpenForwardsIndexFlags) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_sst_flags", {"title", "TEXT"}, {"NOFIELDS"}));
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);

  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));

  EXPECT_EQ(flagsAtOpen, std::vector<IndexFlags>{loaded->flags});
  EXPECT_FALSE(loaded->flags & Index_StoreFieldFlags);
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

TEST_F(DiskSchemaOpenTest, TagCompactionCallbackUpdatesOnlyTheSelectedField) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("tag_compaction_counts", {"first", "TAG", "second", "TAG"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  const char *values[] = {"hello"};
  for (size_t field = 0; field < 2; ++field) {
    TagIndex_Commit(TagIndex_Open(&sp->fields[field]), values, 1, &sp->stats);
  }
  TagIndex_Commit(TagIndex_Open(&sp->fields[1]), values, 1, &sp->stats);
  ASSERT_NE(compactionCallbacks.decrementTagValueCount, nullptr);
  void *update = compactionCallbacks.beginUpdate(sp);
  EXPECT_FALSE(compactionCallbacks.decrementTagValueCount(update, 1, "hello", 5, 1));
  EXPECT_TRUE(compactionCallbacks.decrementTagValueCount(update, 1, "hello", 5, 1));
  compactionCallbacks.endUpdate(update);
  EXPECT_EQ(TrieMap_Find(TagIndex_Open(&sp->fields[1])->values, "hello", 5), TRIEMAP_NOTFOUND);
  EXPECT_EQ(
      reinterpret_cast<uintptr_t>(TrieMap_Find(TagIndex_Open(&sp->fields[0])->values, "hello", 5)),
      1u);
}

TEST_F(DiskSchemaOpenTest, TagDebugCommandsReportDiskCountsAndRejectPostingDumps) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("tag_debug_counts", {"tags", "TAG"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  const char *values[] = {"hello"};
  auto *idx = TagIndex_Open(&sp->fields[0]);
  TagIndex_Commit(idx, values, 1, &sp->stats);
  TagIndex_Commit(idx, values, 1, &sp->stats);

  Restore createHook{RedisModule_CreateSubcommand};
  Restore flagsHook{RedisModule_GetContextFlagsAll};
  Restore arrayHook{RedisModule_ReplyWithArray};
  Restore lengthHook{RedisModule_ReplySetArrayLength};
  Restore stringHook{RedisModule_ReplyWithStringBuffer};
  Restore integerHook{RedisModule_ReplyWithLongLong};
  static RedisModuleCmdFunc dump;
  static RedisModuleCmdFunc info;
  static std::vector<std::string> replies;
  dump = info = nullptr;
  RedisModule_GetContextFlagsAll = [] { return 0; };
  RedisModule_CreateSubcommand = [](RedisModuleCommand *, const char *name,
                                    RedisModuleCmdFunc callback, const char *, int, int, int) {
    if (strcmp(name, "DUMP_TAGIDX") == 0) dump = callback;
    if (strcmp(name, "INFO_TAGIDX") == 0) info = callback;
    return REDISMODULE_OK;
  };
  ASSERT_EQ(RegisterDebugCommands(nullptr), REDISMODULE_OK);
  ASSERT_NE(dump, nullptr);
  ASSERT_NE(info, nullptr);
  RedisModule_ReplyWithArray = [](RedisModuleCtx *, long length) {
    replies.push_back("array:" + std::to_string(length));
    return REDISMODULE_OK;
  };
  RedisModule_ReplySetArrayLength = [](RedisModuleCtx *, long length) {
    replies.push_back("length:" + std::to_string(length));
  };
  RedisModule_ReplyWithStringBuffer = [](RedisModuleCtx *, const char *value, size_t len) {
    replies.emplace_back(value, len);
    return REDISMODULE_OK;
  };
  RedisModule_ReplyWithLongLong = [](RedisModuleCtx *, long long value) {
    replies.push_back(std::to_string(value));
    return REDISMODULE_OK;
  };

  RMCK::ArgvList dumpArgs(RSDummyContext, "FT.DEBUG", "DUMP_TAGIDX", "tag_debug_counts", "tags");
  replies.clear();
  EXPECT_EQ(dump(RSDummyContext, dumpArgs.data(), dumpArgs.size()), REDISMODULE_OK);
  EXPECT_EQ(replies, (std::vector<std::string>{"array:-1", "array:2", "hello", "2", "length:1"}));

  RMCK::ArgvList infoArgs(RSDummyContext, "FT.DEBUG", "INFO_TAGIDX", "tag_debug_counts", "tags",
                          "COUNT_VALUE_ENTRIES", "LIMIT", "10");
  replies.clear();
  EXPECT_EQ(info(RSDummyContext, infoArgs.data(), infoArgs.size()), REDISMODULE_OK);
  EXPECT_EQ(replies, (std::vector<std::string>{"array:-1", "num_values", "1", "values", "array:-1",
                                               "array:-1", "value", "hello", "num_entries", "2",
                                               "length:4", "length:1", "length:4"}));

  RMCK::ArgvList postingArgs(RSDummyContext, "FT.DEBUG", "INFO_TAGIDX", "tag_debug_counts", "tags",
                             "DUMP_ID_ENTRIES");
  RMCK_GetLastError(RSDummyContext).clear();
  EXPECT_EQ(info(RSDummyContext, postingArgs.data(), postingArgs.size()), REDISMODULE_OK);
  EXPECT_EQ(RMCK_GetLastError(RSDummyContext),
            "dump_id_entries not supported for disk-mode indexes");
  RMCK_GetLastError(RSDummyContext).clear();
}

class DiskTagPersistenceTest : public DiskSchemaOpenTest {
 protected:
  RedisModuleIO *io = nullptr;
  IndexSpec *loaded = nullptr;

  void SetUp() override {
    DiskSchemaOpenTest::SetUp();
    api.basic.indexSpecRdbSave = [](RedisModuleIO *rdb, RedisSearchDiskIndexSpec *) {
      RMCK_SaveUnsigned(rdb, 123);
    };
    api.basic.loadRdbToTempObject = [](RedisModuleIO *rdb) {
      if (RMCK_LoadUnsigned(rdb) != 123 || RMCK_IsIOError(rdb)) {
        return static_cast<RedisSearchDiskRdbState *>(nullptr);
      }
      return reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
    };
    io = RMCK_CreateRdbIO();
  }

  IndexSpec *saveSource(int contextFlags = REDISMODULE_CTX_FLAGS_SST_RDB) {
    QueryError err = QueryError_Default();
    IndexSpec *sp = create("tag_sst_counts", {"first", "TAG", "second", "TAG"}, &err);
    EXPECT_NE(sp, nullptr) << QueryError_GetUserError(&err);
    if (!sp) return nullptr;
    const char *tags[] = {"hello", "world", ""};
    TagIndex_Commit(TagIndex_Open(&sp->fields[0]), tags, 3, &sp->stats);
    TagIndex_Commit(TagIndex_Open(&sp->fields[0]), tags, 1, &sp->stats);
    IndexSpec_RdbSave(io, sp, contextFlags);
    return sp;
  }

  void skipRdbString() {
    RedisModule_Free(RMCK_LoadStringBuffer(io, nullptr));
  }

  size_t tagDictionaryMarkerOffset() {
    io->read_pos = 0;
    skipRdbString();        // Index name
    RMCK_LoadUnsigned(io);  // Index flags
    RMCK_LoadUnsigned(io);  // Number of fields
    return io->read_pos;
  }

  // Advance over the schema for one TAG field in this fixture.
  void skipTagFieldSchema() {
    skipRdbString();  // Field name
    if (RMCK_LoadUnsigned(io)) {
      skipRdbString();  // Field path
    }
    RMCK_LoadUnsigned(io);  // Field types
    RMCK_LoadUnsigned(io);  // Field options
    RMCK_LoadSigned(io);    // Sort index
    RMCK_LoadUnsigned(io);  // TAG flags
    skipRdbString();        // TAG separator
  }

  void discardSource() {
    Indexes_RemoveSpecFromGlobals(created.back(), false);
    created.pop_back();
    io->read_pos = 0;
  }

  void load(int version) {
    QueryError err = QueryError_Default();
    loaded = IndexSpec_RdbLoad(io, version, true, &err);
    ASSERT_NE(loaded, nullptr) << QueryError_GetUserError(&err);
    EXPECT_EQ(io->read_pos, io->buffer.size());
  }

  void expectLoadError(int version, const char *message) {
    QueryError err = QueryError_Default();
    loaded = IndexSpec_RdbLoad(io, version, true, &err);
    EXPECT_EQ(loaded, nullptr);
    EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_PARSE_ARGS);
    EXPECT_NE(std::string(QueryError_GetUserError(&err)).find(message), std::string::npos);
    QueryError_ClearError(&err);
  }

  void TearDown() override {
    if (loaded) {
      if (loaded->diskSpec) SearchDisk_CloseIndexOnMainThread(RSDummyContext, loaded);
      IndexSpec_RdbLoadDiscard(IndexSpec_GetStrongRefUnsafe(loaded));
    }
    RMCK_FreeRdbIO(io);
    DiskSchemaOpenTest::TearDown();
  }
};

TEST_F(DiskTagPersistenceTest, CountsSurviveLoadingAndStorageBinding) {
  ASSERT_NE(saveSource(), nullptr);
  discardSource();
  load(INDEX_CURRENT_VERSION);
  ASSERT_NE(loaded, nullptr);
  auto *idx = TagIndex_Open(&loaded->fields[0]);
  ASSERT_NE(idx, nullptr);
  EXPECT_TRUE(idx->valuesAreCounts);
  EXPECT_EQ(idx->diskSpec, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(TrieMap_Find(idx->values, "hello", 5)), 2u);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(TrieMap_Find(idx->values, "world", 5)), 1u);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(TrieMap_Find(idx->values, "", 0)), 1u);
  EXPECT_EQ(TagIndex_NUniqueValues(TagIndex_Open(&loaded->fields[1])), 0u);
  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  EXPECT_TRUE(restoredTagCounts);
  EXPECT_EQ(TagIndex_Open(&loaded->fields[0]), idx);
  EXPECT_EQ(idx->diskSpec, loaded->diskSpec);
}

TEST_F(DiskTagPersistenceTest, UnboundCountDictionariesCanBeDiscarded) {
  ASSERT_NE(saveSource(), nullptr);
  discardSource();
  load(INDEX_CURRENT_VERSION);
  ASSERT_NE(loaded, nullptr);
  EXPECT_EQ(loaded->diskSpec, nullptr);
}

TEST_F(DiskTagPersistenceTest, RestoredTagQueriesRequireExactValues) {
  ASSERT_NE(saveSource(), nullptr);
  discardSource();
  load(INDEX_CURRENT_VERSION);
  ASSERT_NE(loaded, nullptr);
  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  Restore unstable{RSGlobalConfig.enableUnstableFeatures};
  RSGlobalConfig.enableUnstableFeatures = true;
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(RSDummyContext, loaded);
  QASTCXX ast(sctx);
  for (const char *query :
       {"@first:{hel*}", "@first:{*llo}", "@first:{*ell*}", "@first:{w'hel*'}", "@first:>{a}"}) {
    EXPECT_FALSE(ast.isValidQuery(query, static_cast<QASTValidationFlagsSet>(0))) << query;
    EXPECT_NE(std::string(ast.getError()).find("queries are not supported on Flex indexes"),
              std::string::npos)
        << query;
  }
  EXPECT_TRUE(ast.isValidQuery("@first:{hello}", static_cast<QASTValidationFlagsSet>(0)));
  EXPECT_TRUE(ast.isValidQuery("@first:{hello|world}", static_cast<QASTValidationFlagsSet>(0)));
}

TEST_F(DiskTagPersistenceTest, SstTagIndexWithoutDictionariesFailsTheSpecLoad) {
  // A plain RDB has the same field records as the SST encoding before TAG dictionaries.
  ASSERT_NE(saveSource(0), nullptr);
  discardSource();
  expectLoadError(INDEX_SST_TAG_VALUES_VERSION - 1, "SST RDB is missing the TAG dictionary");
}

TEST_F(DiskTagPersistenceTest, MissingDictionaryMarkerFailsTheTagSpecLoad) {
  ASSERT_NE(saveSource(), nullptr);
  io->buffer.at(tagDictionaryMarkerOffset()) = 0;
  discardSource();
  expectLoadError(INDEX_SST_TAG_VALUES_VERSION, "SST RDB is missing the TAG dictionary");
}

TEST_F(DiskTagPersistenceTest, InvalidDictionaryMarkerFailsTheSpecLoad) {
  ASSERT_NE(saveSource(), nullptr);
  io->buffer.at(tagDictionaryMarkerOffset()) = 2;
  discardSource();
  expectLoadError(INDEX_CURRENT_VERSION, "Invalid SST RDB TAG dictionary marker");
}

TEST_F(DiskTagPersistenceTest, LegacySstWithoutTagFieldsCanLoad) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("legacy_sst_without_tag", {"number", "NUMERIC"}, &err);
  ASSERT_NE(sp, nullptr);
  IndexSpec_RdbSave(io, sp, REDISMODULE_CTX_FLAGS_SST_RDB);
  const size_t offset = tagDictionaryMarkerOffset();
  ASSERT_EQ(RMCK_LoadUnsigned(io), 1u);
  io->buffer.erase(io->buffer.begin() + offset, io->buffer.begin() + io->read_pos);
  discardSource();
  load(INDEX_SST_TAG_VALUES_VERSION - 1);
  ASSERT_NE(loaded, nullptr);
  EXPECT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
}

TEST_F(DiskTagPersistenceTest, LegacyPlainRdbTagIndexCanLoadWithoutDictionaries) {
  ASSERT_NE(saveSource(0), nullptr);
  discardSource();
  QueryError err = QueryError_Default();
  loaded = IndexSpec_RdbLoad(io, INDEX_SST_TAG_VALUES_VERSION - 1, false, &err);
  ASSERT_NE(loaded, nullptr) << QueryError_GetUserError(&err);
  EXPECT_EQ(TagIndex_Open(&loaded->fields[0]), nullptr);
  EXPECT_EQ(io->read_pos, io->buffer.size());
}

TEST_F(DiskTagPersistenceTest, InvalidSecondDictionaryDiscardsLoadedCountValues) {
  ASSERT_NE(saveSource(), nullptr);
  tagDictionaryMarkerOffset();
  ASSERT_EQ(RMCK_LoadUnsigned(io), 1u);
  skipTagFieldSchema();
  auto *firstDictionary = TagIndex_RdbLoad(io, 0, false);
  ASSERT_NE(firstDictionary, nullptr);
  TagIndex_Free(firstDictionary);
  skipTagFieldSchema();
  ASSERT_EQ(RMCK_LoadUnsigned(io), 0u);
  io->buffer.resize(io->read_pos - sizeof(uint64_t));
  RMCK_SaveUnsigned(io, 1);  // The second dictionary now claims an entry that is missing.
  discardSource();
  QueryError err = QueryError_Default();
  loaded = IndexSpec_RdbLoad(io, INDEX_CURRENT_VERSION, true, &err);
  EXPECT_EQ(loaded, nullptr);
  QueryError_ClearError(&err);
}
