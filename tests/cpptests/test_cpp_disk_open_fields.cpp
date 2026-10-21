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
class DiskOpenFieldsTest : public ::testing::Test {
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
  // The spec's field count seen by each openFields call.
  static inline std::vector<size_t> numFieldsAtOpen;
  static inline bool openFieldsResult = true;
  static inline bool initMissingResult = true;
  static inline int closeOnMainThreadCalls = 0;
  static inline int closeIndexSpecCalls = 0;
  static inline int rdbStateToken = 0;

  void SetUp() override {
    ConcurrentSearch_CreatePool(1);
    RSGlobalConfig.gcConfigParams.enableGC = false;
    events.clear();
    openCalls.clear();
    numFieldsAtOpen.clear();
    openFieldsResult = true;
    initMissingResult = true;
    closeOnMainThreadCalls = closeIndexSpecCalls = 0;

    api.basic.updateMemoryLimit = [](RedisSearchDisk *, size_t, size_t) { return true; };
    api.basic.reserveOpenFiles = [](RedisSearchDisk *) { return true; };
    api.basic.releaseOpenFiles = [](RedisSearchDisk *) {};
    // The mock hands back the C spec as the disk handle so openFields can inspect it.
    api.basic.openIndexSpec = [](RedisModuleCtx *, RedisSearchDisk *, const HiddenString *,
                                 const char *, size_t, DocumentType, bool,
                                 const SearchDiskCompactionCallbacks *, void *privateData) {
      events.emplace_back("openIndex");
      return reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
    };
    api.basic.openIndexSpecWithRdbState =
        [](RedisModuleCtx *, RedisSearchDisk *, const HiddenString *, const char *, size_t,
           DocumentType, RedisSearchDiskRdbState *state, const SearchDiskCompactionCallbacks *,
           void *privateData) {
          EXPECT_EQ(state, reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken));
          events.emplace_back("openIndexWithRdbState");
          return reinterpret_cast<RedisSearchDiskIndexSpec *>(privateData);
        };
    api.basic.freeRdbState = [](RedisSearchDiskRdbState *) {};
    api.basic.closeIndexOnMainThread = [](RedisModuleCtx *, RedisSearchDisk *,
                                          RedisSearchDiskIndexSpec *) { ++closeOnMainThreadCalls; };
    api.basic.closeIndexSpec = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {
      ++closeIndexSpecCalls;
    };
    api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) {};
    api.index.markToBeDeleted = [](RedisSearchDiskIndexSpec *) {};
    api.index.initializeMissingStorage = [](RedisModuleCtx *, RedisSearchDiskIndexSpec *) {
      events.emplace_back("initializeMissingStorage");
      return initMissingResult;
    };
    api.index.openFields = [](RedisModuleCtx *ctx, RedisSearchDiskIndexSpec *index,
                              const SearchDiskFieldOpen *fields, size_t n) {
      EXPECT_EQ(ctx, RSDummyContext);
      events.emplace_back("openFields");
      numFieldsAtOpen.push_back(reinterpret_cast<IndexSpec *>(index)->numFields);
      std::vector<Desc> descs;
      for (size_t i = 0; i < n; ++i) {
        descs.emplace_back(fields[i].fieldIndex, fields[i].types);
      }
      openCalls.push_back(std::move(descs));
      return openFieldsResult;
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

  // FT.ALTER <sp> SCHEMA ADD <fields...>
  static bool alter(IndexSpec *sp, std::vector<const char *> fields, QueryError *err) {
    ArgsCursor ac;
    ArgsCursor_InitCString(&ac, fields.data(), fields.size());
    return IndexSpec_AddFields(IndexSpec_GetStrongRefUnsafe(sp), sp, RSDummyContext, &ac, err);
  }

  static const FieldSpec *field(const IndexSpec *sp, const char *name) {
    return IndexSpec_GetFieldWithLength(sp, name, strlen(name));
  }

  static std::vector<const char *> withVector(std::vector<const char *> schema, const char *name) {
    schema.push_back(name);
    schema.insert(schema.end(), std::begin(kVectorF32Dim4), std::end(kVectorF32Dim4));
    return schema;
  }
};

TEST_F(DiskOpenFieldsTest, CreateOpensAllFieldsOnceAfterParsing) {
  auto schema =
      withVector({"title", "TEXT", "tags", "TAG", "price", "NUMERIC", "loc", "GEO"}, "v32");
  schema.insert(schema.end(), {"v16", "VECTOR", "HNSW", "14", "TYPE", "FLOAT16", "DIM", "3",
                               "DISTANCE_METRIC", "COSINE", "M", "16", "EF_CONSTRUCTION", "200",
                               "EF_RUNTIME", "10", "RERANK", "FALSE"});
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_create", schema, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);

  EXPECT_EQ(events, (std::vector<std::string>{"openIndex", "openFields"}));
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
  // What the open step builds on top of a successful openFields.
  EXPECT_EQ(sp->fields[4].vectorOpts.diskCtx.storage, sp->diskSpec);
  EXPECT_TRUE(sp->fields[4].vectorOpts.diskCtx.rerank);
  EXPECT_FALSE(sp->fields[5].vectorOpts.diskCtx.rerank);
  EXPECT_NE(sp->fields[1].tagOpts.tagIndex, nullptr);
}

TEST_F(DiskOpenFieldsTest, CreateWithoutOptionalCallbackStillInitializesStorage) {
  api.index.openFields = nullptr;
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_optional", withVector({"tags", "TAG", "INDEXMISSING"}, "v"), &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  EXPECT_EQ(events, (std::vector<std::string>{"openIndex", "initializeMissingStorage"}));
  EXPECT_TRUE(openCalls.empty());
  EXPECT_NE(sp->fields[0].tagOpts.tagIndex, nullptr);
  EXPECT_EQ(sp->fields[1].vectorOpts.diskCtx.storage, sp->diskSpec);
}

TEST_F(DiskOpenFieldsTest, MemoryIndexDoesNotOpenDiskFieldStorage) {
  isFlex = false;
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_memory", {"title", "TEXT", "tags", "TAG"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  EXPECT_EQ(sp->diskSpec, nullptr);
  EXPECT_TRUE(events.empty());
  EXPECT_TRUE(openCalls.empty());
}

TEST_F(DiskOpenFieldsTest, CreateWithSchemaErrorOpensNoField) {
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_bad_schema", {"title", "TEXT", "bad", "NOSUCHTYPE"}, &err),
            nullptr);
  QueryError_ClearError(&err);
  EXPECT_TRUE(openCalls.empty());
}

// Wide enough to outgrow the hook's on-stack descriptor buffer.
TEST_F(DiskOpenFieldsTest, CreateWithWideSchemaPassesEveryField) {
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

TEST_F(DiskOpenFieldsTest, AlterOpensOnlyAddedFields) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_alter", {"title", "TEXT", "tags", "TAG"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  openCalls.clear();
  events.clear();
  numFieldsAtOpen.clear();

  ASSERT_TRUE(alter(sp, withVector({"price", "NUMERIC", "more", "TAG"}, "v"), &err))
      << QueryError_GetUserError(&err);

  EXPECT_EQ(events, std::vector<std::string>{"openFields"});
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(numFieldsAtOpen, std::vector<size_t>{5});
  EXPECT_EQ(openCalls[0], (std::vector<Desc>{
                              {2, INDEXFLD_T_NUMERIC},
                              {3, INDEXFLD_T_TAG},
                              {4, INDEXFLD_T_VECTOR},
                          }));
  EXPECT_NE(field(sp, "more")->tagOpts.tagIndex, nullptr);
}

TEST_F(DiskOpenFieldsTest, FailedMissingStorageInitFailsAlterBeforeOpeningFields) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_alter_missing_fail", {"title", "TEXT"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  events.clear();
  openCalls.clear();

  initMissingResult = false;
  EXPECT_FALSE(alter(sp, {"tags", "TAG", "INDEXMISSING"}, &err));
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(events, std::vector<std::string>{"initializeMissingStorage"});
  EXPECT_TRUE(openCalls.empty());
  EXPECT_EQ(sp->numFields, 1);
  EXPECT_FALSE(IndexSpec_HasIndexMissing(sp));
}

TEST_F(DiskOpenFieldsTest, FailedMissingStorageInitFailsCreate) {
  initMissingResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_create_missing_fail", {"tags", "TAG", "INDEXMISSING"}, &err),
            nullptr);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_TRUE(openCalls.empty());
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("open_fields_create_missing_fail")), nullptr);
  EXPECT_EQ(closeOnMainThreadCalls, 1);
}

TEST_F(DiskOpenFieldsTest, AlterAddingIndexMissingInitializesMissingStorageFirst) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_alter_missing", {"title", "TEXT"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  ASSERT_FALSE(IndexSpec_HasIndexMissing(sp));
  events.clear();

  ASSERT_TRUE(alter(sp, {"price", "NUMERIC", "tags", "TAG", "INDEXMISSING"}, &err))
      << QueryError_GetUserError(&err);

  EXPECT_EQ(events, (std::vector<std::string>{"initializeMissingStorage", "openFields"}));
  EXPECT_TRUE(IndexSpec_HasIndexMissing(sp));
}

TEST_F(DiskOpenFieldsTest, FailedAlterLeavesSchemaUnchangedAndIdsReusable) {
  QueryError err = QueryError_Default();
  IndexSpec *sp = create("open_fields_alter_fail", {"title", "TEXT"}, &err);
  ASSERT_NE(sp, nullptr) << QueryError_GetUserError(&err);
  IndexSpecCache *cacheBefore = sp->spcache;
  const size_t cacheFieldsBefore = sp->spcache->nfields;

  openFieldsResult = false;
  EXPECT_FALSE(alter(sp, {"tags", "TAG", "INDEXMISSING", "body", "TEXT"}, &err));
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(sp->numFields, 1);
  EXPECT_EQ(field(sp, "tags"), nullptr);
  EXPECT_EQ(field(sp, "body"), nullptr);
  EXPECT_EQ(sp->spcache, cacheBefore);
  EXPECT_EQ(sp->spcache->nfields, cacheFieldsBefore);
  EXPECT_FALSE(IndexSpec_HasIndexMissing(sp));
  EXPECT_EQ(array_len(sp->missing.fields), 0u);

  // The rolled-back field ids are handed out again, here to fields of other types.
  openFieldsResult = true;
  ASSERT_TRUE(alter(sp, {"tags", "NUMERIC", "body", "TAG"}, &err)) << QueryError_GetUserError(&err);
  ASSERT_EQ(openCalls.size(), 3u);
  EXPECT_EQ(openCalls[1], (std::vector<Desc>{{1, INDEXFLD_T_TAG}, {2, INDEXFLD_T_FULLTEXT}}));
  EXPECT_EQ(openCalls[2], (std::vector<Desc>{{1, INDEXFLD_T_NUMERIC}, {2, INDEXFLD_T_TAG}}));
  EXPECT_EQ(sp->numFields, 3);
  ASSERT_NE(field(sp, "tags"), nullptr);
  EXPECT_TRUE(FIELD_IS(field(sp, "tags"), INDEXFLD_T_NUMERIC));
}

TEST_F(DiskOpenFieldsTest, FailedCreateClosesDiskIndexAndRegistersNothing) {
  openFieldsResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(create("open_fields_create_fail", {"title", "TEXT", "tags", "TAG"}, &err), nullptr);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("open_fields_create_fail")), nullptr);
  EXPECT_EQ(closeOnMainThreadCalls, 1);
  EXPECT_EQ(closeIndexSpecCalls, 1);
}

class DiskOpenFieldsLoadTest : public DiskOpenFieldsTest {
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
    DiskOpenFieldsTest::TearDown();
  }
};

const std::vector<Desc> kLoadedFields = {
    {0, INDEXFLD_T_FULLTEXT},
    {1, INDEXFLD_T_TAG},
    {2, INDEXFLD_T_VECTOR},
};

TEST_F(DiskOpenFieldsLoadTest, RdbLoadOpensAllFieldsOnce) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad(
      "open_fields_rdb", withVector({"title", "TEXT", "tags", "TAG", "INDEXMISSING"}, "v")));

  QueryError err = QueryError_Default();
  ASSERT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_OK)
      << QueryError_GetUserError(&err);

  EXPECT_EQ(events,
            (std::vector<std::string>{"openIndex", "initializeMissingStorage", "openFields"}));
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openCalls[0], kLoadedFields);
  EXPECT_EQ(loaded->fields[2].vectorOpts.diskCtx.storage, loaded->diskSpec);
}

TEST_F(DiskOpenFieldsLoadTest, RdbLoadWithoutOptionalCallbackSucceeds) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_optional_rdb", {"tags", "TAG", "INDEXMISSING"}));
  api.index.openFields = nullptr;
  QueryError err = QueryError_Default();
  EXPECT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_OK)
      << QueryError_GetUserError(&err);
  EXPECT_EQ(events, (std::vector<std::string>{"openIndex", "initializeMissingStorage"}));
  EXPECT_TRUE(openCalls.empty());
  EXPECT_NE(loaded->fields[0].tagOpts.tagIndex, nullptr);
}

TEST_F(DiskOpenFieldsLoadTest, RdbLoadFailsWhenOpeningFieldsFails) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_rdb_fail", {"title", "TEXT", "tags", "TAG"}));

  openFieldsResult = false;
  QueryError err = QueryError_Default();
  EXPECT_EQ(IndexSpec_RdbLoadOpenDisk(RSDummyContext, loaded, false, &err), REDISMODULE_ERR);
  EXPECT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_DISK_CREATION);
  QueryError_ClearError(&err);

  EXPECT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(closeOnMainThreadCalls, 1);
}

// Loads through the non-SST path and plants the pending state, so vector fields, which SST load
// creates up front, are left out.
TEST_F(DiskOpenFieldsLoadTest, SstOpenWithoutOptionalCallbackSucceeds) {
  ASSERT_NO_FATAL_FAILURE(saveAndLoad("open_fields_optional_sst", {"tags", "TAG", "INDEXMISSING"}));
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);
  api.index.openFields = nullptr;

  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));
  EXPECT_EQ(events,
            (std::vector<std::string>{"openIndexWithRdbState", "initializeMissingStorage"}));
  EXPECT_TRUE(openCalls.empty());
  EXPECT_NE(loaded->fields[0].tagOpts.tagIndex, nullptr);
}

TEST_F(DiskOpenFieldsLoadTest, SstOpenOpensNonVectorFieldsOnce) {
  ASSERT_NO_FATAL_FAILURE(
      saveAndLoad("open_fields_sst", {"title", "TEXT", "tags", "TAG", "INDEXMISSING"}));
  loaded->pendingDiskRdbState = reinterpret_cast<RedisSearchDiskRdbState *>(&rdbStateToken);

  ASSERT_TRUE(IndexSpec_SSTRdbOpenAndApply(RSDummyContext, loaded));

  EXPECT_EQ(events, (std::vector<std::string>{"openIndexWithRdbState", "initializeMissingStorage",
                                              "openFields"}));
  ASSERT_EQ(openCalls.size(), 1u);
  EXPECT_EQ(openCalls[0], (std::vector<Desc>(kLoadedFields.begin(), kLoadedFields.end() - 1)));
}

}  // namespace
