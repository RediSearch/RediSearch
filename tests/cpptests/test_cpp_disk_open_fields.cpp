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
int CreateIndexCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int CreateIndexIfNotExistsCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
}

#include <cstdarg>
#include <cstring>
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
  Restore<decltype(RSGlobalConfig.diskSharedFieldCFs)> sharedFieldCFs{
      RSGlobalConfig.diskSharedFieldCFs};
  Restore<decltype(RSGlobalConfig.simulateInFlex)> simulatedFlex{RSGlobalConfig.simulateInFlex};
  Restore<decltype(RSGlobalConfig.gcConfigParams.enableGC)> gcEnabled{
      RSGlobalConfig.gcConfigParams.enableGC};
  RedisSearchDiskAPI api{};
  std::vector<StrongRef> created;

  static inline std::vector<std::string> events;
  static inline std::vector<std::vector<Desc>> openCalls;
  // The schema must be complete before the database is opened.
  static inline std::vector<size_t> numFieldsAtOpen;
  static inline bool openResult = true;
  static inline bool supportsSharedLayout = true;
  static inline std::vector<bool> openedLayouts;
  static inline bool initMissingResult = true;
  static inline int closeOnMainThreadCalls = 0;
  static inline int closeIndexSpecCalls = 0;
  static inline int rdbStateToken = 0;
  static inline int freedRdbStates = 0;
  static inline std::vector<std::string> replicatedArgs;
  static inline std::string replicatedFormat;
  static inline int wrongArityCalls = 0;

  static int recordReplication(RedisModuleCtx *, const char *, const char *format, ...) {
    replicatedFormat = format;
    va_list args;
    va_start(args, format);
    if (replicatedFormat == "sclv") {
      RedisModuleString *name = va_arg(args, RedisModuleString *);
      size_t len;
      const char *value = RedisModule_StringPtrLen(name, &len);
      replicatedArgs.emplace_back(value, len);
      replicatedArgs.emplace_back(va_arg(args, const char *));
      replicatedArgs.push_back(std::to_string(va_arg(args, long long)));
    } else if (replicatedFormat != "v") {
      ADD_FAILURE() << "unexpected replication format: " << replicatedFormat;
      va_end(args);
      return REDISMODULE_ERR;
    }
    RedisModuleString **argv = va_arg(args, RedisModuleString **);
    size_t argc = va_arg(args, size_t);
    va_end(args);
    for (size_t i = 0; i < argc; ++i) {
      size_t len;
      const char *value = RedisModule_StringPtrLen(argv[i], &len);
      replicatedArgs.emplace_back(value, len);
    }
    return REDISMODULE_OK;
  }

  static bool recordSchema(RedisModuleCtx *ctx, void *privateData, const SearchDiskField *fields,
                           size_t n, bool sharedLayout) {
    auto *index = reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
    EXPECT_EQ(ctx, RSDummyContext);
    numFieldsAtOpen.push_back(reinterpret_cast<IndexSpec *>(index)->numFields);
    openedLayouts.push_back(sharedLayout);
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
    return openResult && (!sharedLayout || supportsSharedLayout) &&
           (!indexMissing || initMissingResult);
  }

  void SetUp() override {
    ConcurrentSearch_CreatePool(1);
    RSGlobalConfig.gcConfigParams.enableGC = false;
    RSGlobalConfig.diskSharedFieldCFs = false;
    RSGlobalConfig.simulateInFlex = false;
    events.clear();
    openCalls.clear();
    numFieldsAtOpen.clear();
    replicatedArgs.clear();
    replicatedFormat.clear();
    openedLayouts.clear();
    openResult = true;
    supportsSharedLayout = true;
    initMissingResult = true;
    closeOnMainThreadCalls = closeIndexSpecCalls = freedRdbStates = 0;

    api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return true; };
    api.basic.reserveOpenFiles = [](RedisSearchDisk *) { return true; };
    api.basic.releaseOpenFiles = [](RedisSearchDisk *) {};
    // The mock uses the C spec as an opaque disk handle.
    api.basic.openIndexSpec = [](RedisModuleCtx *ctx, RedisSearchDisk *, const HiddenString *,
                                 const char *, size_t, DocumentType, bool sharedLayout, bool,
                                 const SearchDiskField *fields, size_t n,
                                 const SearchDiskCompactionCallbacks *, void *privateData) {
      events.emplace_back("openIndex");
      return recordSchema(ctx, privateData, fields, n, sharedLayout)
                 ? reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData)
                 : nullptr;
    };
    api.basic.openIndexSpecWithRdbState =
        [](RedisModuleCtx *ctx, RedisSearchDisk *, const HiddenString *, const char *, size_t,
           DocumentType, bool sharedLayout, RedisSearchDiskRdbState *state,
           const SearchDiskField *fields, size_t n, const SearchDiskCompactionCallbacks *,
           void *privateData) {
          EXPECT_EQ(state, reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken));
          ++freedRdbStates;
          events.emplace_back("openIndexWithRdbState");
          return recordSchema(ctx, privateData, fields, n, sharedLayout)
                     ? reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData)
                     : nullptr;
        };
    api.basic.indexSpecRdbSave = [](RedisModuleIO *, RedisSearchDiskIndexSpec *) {};
    api.basic.loadRdbToTempObject = [](RedisModuleIO *) {
      return reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
    };
    api.basic.freeRdbState = [](RedisSearchDiskRdbState *) { ++freedRdbStates; };
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
  EXPECT_FALSE(sp->flags & Index_SharedFieldCFs);
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

TEST_F(DiskSchemaOpenTest, BackendRejectsUnsupportedSharedLayoutWithoutPublishingIndex) {
  RSGlobalConfig.diskSharedFieldCFs = true;
  supportsSharedLayout = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("unsupported_shared_layout", {"tags", "TAG", "price", "NUMERIC"}, &err),
            nullptr);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);
  EXPECT_EQ(events, (std::vector<std::string>{"openIndex"}));
  EXPECT_EQ(openedLayouts, (std::vector<bool>{true}));
  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("unsupported_shared_layout")), nullptr);
  EXPECT_EQ(closeOnMainThreadCalls, 0);
  EXPECT_EQ(closeIndexSpecCalls, 0);
}

TEST_F(DiskSchemaOpenTest, SharedDefaultDoesNotAffectMemoryOrValidationOnlyIndexes) {
  RSGlobalConfig.diskSharedFieldCFs = true;
  isFlex = false;
  for (bool simulate : {false, true}) {
    SCOPED_TRACE(simulate);
    RSGlobalConfig.simulateInFlex = simulate;
    QueryError err = QueryError_Default();
    IndexSpec *sp = create(simulate ? "layout_validation_only" : "layout_memory",
                           {"tags", "TAG", "price", "NUMERIC"}, &err);
    ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
    EXPECT_FALSE(sp->flags & Index_SharedFieldCFs);
    EXPECT_EQ(sp->diskSpec, nullptr);
    EXPECT_TRUE(events.empty());
    EXPECT_TRUE(openCalls.empty());
  }
}

TEST_F(DiskSchemaOpenTest, PerFieldCreateKeepsLegacyReplicationArguments) {
  Restore<decltype(RedisModule_Replicate)> replicate{RedisModule_Replicate};
  RedisModule_Replicate = recordReplication;
  const char *argv[] = {
      "FT.CREATE", "legacy_replication_arguments", "SKIPINITIALSCAN", "SCHEMA", "tags", "TAG"};
  RMCK::ArgvList args(RSDummyContext, argv, std::size(argv));
  EXPECT_EQ(CreateIndexCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
  auto *sp = static_cast<IndexSpec *>(
      StrongRef_Get(Indexes_LoadIndexSpecUnsafe("legacy_replication_arguments")));
  ASSERT_NE(sp, nullptr) << RMCK_GetLastError(RSDummyContext);
  created.push_back(IndexSpec_GetStrongRefUnsafe(sp));
  EXPECT_FALSE(sp->flags & Index_SharedFieldCFs);
  EXPECT_EQ(replicatedFormat, "v");
  EXPECT_EQ(replicatedArgs, (std::vector<std::string>{"legacy_replication_arguments",
                                                      "SKIPINITIALSCAN", "SCHEMA", "tags", "TAG"}));
}

TEST_F(DiskSchemaOpenTest, ReplicatedPerFieldLayoutIgnoresChangedDefault) {
  RSGlobalConfig.diskSharedFieldCFs = true;
  for (bool metadata : {false, true}) {
    SCOPED_TRACE(metadata);
    const char *name = metadata ? "replicated_per_field_layout" : "legacy_per_field_layout";
    std::vector<const char *> argv = {"FT._CREATEIFNX", name};
    if (metadata) argv.insert(argv.end(), {"_DISK_LAYOUT", "0"});
    argv.insert(argv.end(), {"SKIPINITIALSCAN", "SCHEMA", "tags", "TAG"});
    RMCK::ArgvList args(RSDummyContext, argv.data(), argv.size());
    EXPECT_EQ(CreateIndexIfNotExistsCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
    StrongRef ref = Indexes_LoadIndexSpecUnsafe(name);
    auto *sp = static_cast<IndexSpec *>(StrongRef_Get(ref));
    ASSERT_NE(sp, nullptr) << RMCK_GetLastError(RSDummyContext);
    created.push_back(IndexSpec_GetStrongRefUnsafe(sp));
    EXPECT_FALSE(sp->flags & Index_SharedFieldCFs);
    EXPECT_NE(sp->diskSpec, nullptr);
  }
  EXPECT_EQ(openCalls.size(), 2u);
}

TEST_F(DiskSchemaOpenTest, ReplicatedSharedLayoutDoesNotFallBackToPerFieldDefault) {
  const char *argv[] = {"FT._CREATEIFNX",
                        "replicated_shared_layout",
                        "_DISK_LAYOUT",
                        "1",
                        "SKIPINITIALSCAN",
                        "SCHEMA",
                        "tags",
                        "TAG"};
  RMCK::ArgvList args(RSDummyContext, argv, std::size(argv));
  EXPECT_EQ(CreateIndexIfNotExistsCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
  auto *sp = static_cast<IndexSpec *>(
      StrongRef_Get(Indexes_LoadIndexSpecUnsafe("replicated_shared_layout")));
  ASSERT_NE(sp, nullptr) << RMCK_GetLastError(RSDummyContext);
  created.push_back(IndexSpec_GetStrongRefUnsafe(sp));
  EXPECT_TRUE(sp->flags & Index_SharedFieldCFs);
  EXPECT_EQ(openedLayouts, (std::vector<bool>{true}));
}

TEST_F(DiskSchemaOpenTest, SharedCreateReplicatesItsResolvedLayoutAcrossDifferentDefaults) {
  Restore<decltype(RedisModule_Replicate)> replicate{RedisModule_Replicate};
  RedisModule_Replicate = recordReplication;
  RSGlobalConfig.diskSharedFieldCFs = true;
  const char *argv[] = {
      "FT.CREATE", "replicated_chosen_layout", "SKIPINITIALSCAN", "SCHEMA", "tags", "TAG"};
  RMCK::ArgvList args(RSDummyContext, argv, std::size(argv));
  EXPECT_EQ(CreateIndexCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
  auto *sp = static_cast<IndexSpec *>(
      StrongRef_Get(Indexes_LoadIndexSpecUnsafe("replicated_chosen_layout")));
  ASSERT_NE(sp, nullptr) << RMCK_GetLastError(RSDummyContext);
  EXPECT_TRUE(sp->flags & Index_SharedFieldCFs);
  EXPECT_EQ(replicatedFormat, "sclv");
  EXPECT_EQ(replicatedArgs,
            (std::vector<std::string>{"replicated_chosen_layout", "_DISK_LAYOUT", "1",
                                      "SKIPINITIALSCAN", "SCHEMA", "tags", "TAG"}));
  Indexes_RemoveSpecFromGlobals(IndexSpec_GetStrongRefUnsafe(sp), false);
  RedisModule_Replicate = replicate.saved;
  RSGlobalConfig.diskSharedFieldCFs = false;
  openedLayouts.clear();
  std::vector<const char *> replay = {"FT._CREATEIFNX"};
  for (const auto &arg : replicatedArgs) replay.push_back(arg.c_str());
  RMCK::ArgvList replayArgs(RSDummyContext, replay.data(), replay.size());
  EXPECT_EQ(CreateIndexIfNotExistsCommand(RSDummyContext, replayArgs, replayArgs.size()),
            REDISMODULE_OK);
  auto *replica = static_cast<IndexSpec *>(
      StrongRef_Get(Indexes_LoadIndexSpecUnsafe("replicated_chosen_layout")));
  ASSERT_NE(replica, nullptr) << RMCK_GetLastError(RSDummyContext);
  created.push_back(IndexSpec_GetStrongRefUnsafe(replica));
  EXPECT_TRUE(replica->flags & Index_SharedFieldCFs);
  EXPECT_EQ(openedLayouts, (std::vector<bool>{true}));
}

TEST_F(DiskSchemaOpenTest, ReplicatedExistingIndexRetainsItsLayoutAndSchema) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("existing_replicated_layout", {"tags", "TAG"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  const auto fingerprint = IndexSpec_SchemaFingerprint(sp);
  events.clear();
  openCalls.clear();
  RSGlobalConfig.diskSharedFieldCFs = true;
  const char *argv[] = {"FT._CREATEIFNX",  "existing_replicated_layout",
                        "_DISK_LAYOUT",    "1",
                        "SKIPINITIALSCAN", "SCHEMA",
                        "price",           "NUMERIC"};
  RMCK::ArgvList args(RSDummyContext, argv, std::size(argv));
  EXPECT_EQ(CreateIndexIfNotExistsCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
  EXPECT_EQ(IndexSpec_SchemaFingerprint(sp), fingerprint);
  EXPECT_FALSE(sp->flags & Index_SharedFieldCFs);
  EXPECT_EQ(sp->numFields, 1u);
  EXPECT_TRUE(events.empty());
  EXPECT_TRUE(openCalls.empty());
}

TEST_F(DiskSchemaOpenTest, PublicCreateDoesNotAcceptPrivateLayoutMetadata) {
  const char *argv[] = {"FT.CREATE",
                        "private_layout_in_public_create",
                        "_DISK_LAYOUT",
                        "1",
                        "SKIPINITIALSCAN",
                        "SCHEMA",
                        "tags",
                        "TAG"};
  RMCK::ArgvList args(RSDummyContext, argv, std::size(argv));
  RMCK_GetLastError(RSDummyContext).clear();
  EXPECT_EQ(CreateIndexCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
  EXPECT_FALSE(RMCK_GetLastError(RSDummyContext).empty());
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("private_layout_in_public_create")), nullptr);
  EXPECT_TRUE(events.empty());
  EXPECT_TRUE(openCalls.empty());
}

TEST_F(DiskSchemaOpenTest, TruncatedPrivateLayoutMetadataUsesWrongArityWithoutCreatingStorage) {
  Restore<decltype(RedisModule_WrongArity)> wrongArity{RedisModule_WrongArity};
  wrongArityCalls = 0;
  RedisModule_WrongArity = [](RedisModuleCtx *) {
    ++wrongArityCalls;
    return REDISMODULE_ERR;
  };
  const char *argv[] = {
      "FT._CREATEIFNX", "truncated_private_layout", "_DISK_LAYOUT", "0", "SCHEMA", "tags"};
  for (size_t argc : {5u, 6u}) {
    SCOPED_TRACE(argc);
    RMCK::ArgvList args(RSDummyContext, argv, argc);
    EXPECT_EQ(CreateIndexIfNotExistsCommand(RSDummyContext, args, args.size()), REDISMODULE_ERR);
    EXPECT_EQ(wrongArityCalls, argc - 4);
    EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("truncated_private_layout")), nullptr);
    EXPECT_TRUE(events.empty());
    EXPECT_TRUE(openCalls.empty());
  }
}

TEST_F(DiskSchemaOpenTest, InvalidReplicatedLayoutDoesNotCreateStorage) {
  for (const char *layout : {"2", "-1", "invalid"}) {
    SCOPED_TRACE(layout);
    const char *argv[] = {"FT._CREATEIFNX",
                          "invalid_replicated_layout",
                          "_DISK_LAYOUT",
                          layout,
                          "SKIPINITIALSCAN",
                          "SCHEMA",
                          "tags",
                          "TAG"};
    RMCK::ArgvList args(RSDummyContext, argv, std::size(argv));
    EXPECT_EQ(CreateIndexIfNotExistsCommand(RSDummyContext, args, args.size()), REDISMODULE_OK);
    EXPECT_EQ(RMCK_GetLastError(RSDummyContext), "Invalid replicated disk CF layout");
    EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("invalid_replicated_layout")), nullptr);
    EXPECT_TRUE(events.empty());
    EXPECT_TRUE(openCalls.empty());
  }
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
  void saveAndLoad(const char *name, std::vector<const char *> schema) {
    QueryError err = QueryError_Default();
    IndexSpec *sp = create(name, schema, &err);
    ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
    saveAndLoadSpec(sp, false);
  }

  void saveAndLoadSpec(IndexSpec *sp, bool useSst) {
    io = RMCK_CreateRdbIO();
    IndexSpec_RdbSave(io, sp, useSst ? REDISMODULE_CTX_FLAGS_SST_RDB : 0);
    ASSERT_EQ(RMCK_IsIOError(io), 0);
    Indexes_RemoveSpecFromGlobals(created.back(), false);
    created.pop_back();
    io->read_pos = 0;

    QueryError err = QueryError_Default();
    loaded = IndexSpec_RdbLoad(io, INDEX_CURRENT_VERSION, useSst, &err);
    ASSERT_NE(loaded, nullptr) << QueryError_GetUserError(&err);
    events.clear();
    openCalls.clear();
    openedLayouts.clear();
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

class DiskLayoutRdbTest : public DiskSchemaOpenLoadTest,
                          public ::testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(DiskLayoutRdbTest, SavedLayoutSurvivesReloadWithAnOppositeDefault) {
  const auto [sharedLayout, useSst] = GetParam();
  RSGlobalConfig.diskSharedFieldCFs = sharedLayout;
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("persisted_disk_layout", {"tags", "TAG", "price", "NUMERIC"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  EXPECT_EQ(openedLayouts, (std::vector<bool>{sharedLayout}));
  const auto fingerprint = IndexSpec_SchemaFingerprint(sp);
  RSGlobalConfig.diskSharedFieldCFs = !sharedLayout;
  ASSERT_NO_FATAL_FAILURE(saveAndLoadSpec(sp, useSst));
  ASSERT_NE(loaded, nullptr);
  EXPECT_EQ(bool(loaded->flags & Index_SharedFieldCFs), sharedLayout);
  EXPECT_EQ(IndexSpec_SchemaFingerprint(loaded), fingerprint);
  EXPECT_EQ(loaded->pendingDiskRdbState != nullptr, useSst);
  EXPECT_EQ(RMCK_IsIOError(io), 0);
  if (useSst) {
    ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  } else {
    ASSERT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_OK)
        << QueryError_GetUserError(&err);
  }
  EXPECT_EQ(openedLayouts, (std::vector<bool>{sharedLayout}));
  EXPECT_EQ(loaded->pendingDiskRdbState, nullptr);
}

INSTANTIATE_TEST_SUITE_P(OrdinaryAndSst, DiskLayoutRdbTest,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()));

TEST_F(DiskSchemaOpenLoadTest, UnsupportedSharedRdbLayoutFailsWithoutPublishingNativeHandle) {
  RSGlobalConfig.diskSharedFieldCFs = true;
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("unsupported_rdb_layout", {"tags", "TAG"}));
  RSGlobalConfig.diskSharedFieldCFs = false;
  supportsSharedLayout = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_ERR);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);
  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openedLayouts, (std::vector<bool>{true}));
  EXPECT_EQ(loaded->diskSpec, nullptr);
}

TEST_F(DiskSchemaOpenLoadTest, UnsupportedSharedSstLayoutConsumesStateOnBackendFailure) {
  RSGlobalConfig.diskSharedFieldCFs = true;
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("unsupported_sst_layout", {"tags", "TAG"}));
  RSGlobalConfig.diskSharedFieldCFs = false;
  supportsSharedLayout = false;
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
  EXPECT_FALSE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openedLayouts, (std::vector<bool>{true}));
  EXPECT_EQ(loaded->pendingDiskRdbState, nullptr);
  EXPECT_EQ(loaded->diskSpec, nullptr);
  EXPECT_EQ(freedRdbStates, 1);
}

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
