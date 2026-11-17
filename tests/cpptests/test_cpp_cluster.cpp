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
#include "indexes.h"
#include "redismock/redismock.h"
#include "redismock/util.h"

#include "trie/trie.h"
extern "C" {
#include "dictionary.h"
}

class ClusterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize Redis mock
        ctx = RedisModule_GetThreadSafeContext(NULL);
        ASSERT_TRUE(ctx != nullptr);
    }

    void TearDown() override {
        Dictionary_Clear();
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
            ctx = nullptr;
        }
        for (auto &spec_ref : specs) {
            Indexes_RemoveSpecFromGlobals(spec_ref, false);
        }
        specs.clear();
    }

    RedisModuleCtx *ctx = nullptr;
    std::vector<StrongRef> specs; // To hold references to created IndexSpecs
};


TEST_F(ClusterTest, SchemaPropagation) {

    QueryError err = QueryError_Default();

    // Create an IndexSpec
    const char *args[] = {"SCHEMA", "title", "TEXT", "WEIGHT", "2.0", "body", "TEXT", "price", "NUMERIC"};
    StrongRef original_spec_ref = IndexSpec_ParseC(NULL, "idx1", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    Spec_AddToDict(original_spec_ref.rm);
    specs.push_back(original_spec_ref); // Keep track of created spec for cleanup

    // Create a second IndexSpec
    const char *args2[] = {"SCHEMA", "name", "TEXT", "age", "NUMERIC", "city", "TAG"};
    StrongRef second_spec_ref = IndexSpec_ParseC(NULL, "idx2", args2, sizeof(args2) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    Spec_AddToDict(second_spec_ref.rm);
    specs.push_back(second_spec_ref); // Keep track of created spec for cleanup

    // Collect serialized specs for verification
    std::set<std::string> serialized_specs;
    for (const auto &spec_ref : specs) {
        IndexSpec *spec = (IndexSpec *)StrongRef_Get(spec_ref);
        ASSERT_TRUE(spec != nullptr);
        RedisModuleString *serialized = IndexSpec_Serialize(spec);
        ASSERT_TRUE(serialized != nullptr);
        size_t len;
        const char *str = RedisModule_StringPtrLen(serialized, &len);
        serialized_specs.insert(std::string(str, len));
        RedisModule_FreeString(NULL, serialized);
    }

    // Test propagation of schemas
    Indexes_Propagate(ctx);
    auto &propagated_commands = RMCK_GetPropagatedCommands(ctx);
    ASSERT_EQ(specs.size(), propagated_commands.size());
    ASSERT_EQ(specs.size(), Indexes_Count());

    // Expected commands: _FT._RESTOREIFNX SCHEMA <encode version> <serialized schema>
    // We will check that the serialized schema matches what we expect
    for (auto &cmd : propagated_commands) {
        ASSERT_EQ(cmd.size(), 4);
        ASSERT_EQ(cmd[0], "_FT._RESTOREIFNX");
        ASSERT_EQ(cmd[1], "SCHEMA");
        int encver = std::stoi(cmd[2]);
        ASSERT_EQ(encver, INDEX_CURRENT_VERSION);
        ASSERT_TRUE(serialized_specs.find(cmd[3]) != serialized_specs.end()) << "Serialized schema not found: " << cmd[3];
    }
}

TEST_F(ClusterTest, DictionaryPropagation) {
    std::map<std::string, std::set<std::string>> dicts;
    // Add entries to the dictionary
    auto dict_add = [&](const char *dictName, const std::vector<const char *> &words) {
        // Add words to the dictionary using `Dictionary_Add`
        std::vector<RedisModuleString*> rwords;
        for (const char *word : words) {
            rwords.push_back(RedisModule_CreateString(ctx, word, strlen(word)));
        }
        int added = Dictionary_Add(ctx, dictName, rwords.data(), rwords.size());
        ASSERT_EQ(added, words.size());
        for (auto rstr : rwords) {
            RedisModule_FreeString(ctx, rstr);
        }

        // Add to local map for verification
        for (const char *word : words) {
            dicts[dictName].insert(word);
        }
    };
    dict_add("dict1", {"apple", "banana", "cherry"});
    dict_add("dict2", {"dog", "elephant", "frog", "giraffe"});

    // Propagate dictionaries
    Dictionary_Propagate(ctx);
    auto &propagated_commands = RMCK_GetPropagatedCommands(ctx);

    // We expect two commands, one for each dictionary
    ASSERT_EQ(propagated_commands.size(), Dictionary_Size());
    ASSERT_EQ(propagated_commands.size(), 2);

    // Expected command format: _FT.DICTADD <dictName> <word1> <word2> ...
    for (auto &cmd : propagated_commands) {
        ASSERT_GT(cmd.size(), 2); // At least command, dictName, and one word
        ASSERT_EQ(cmd[0], "_FT.DICTADD");
        const std::string &dictName = cmd[1];
        ASSERT_TRUE(dicts.find(dictName) != dicts.end()) << "Unexpected dictionary name: " << dictName;
        const auto &expected_words = dicts[dictName];
        std::set<std::string> cmd_words(cmd.begin() + 2, cmd.end());
        ASSERT_EQ(cmd_words, expected_words) << "Words in dictionary command do not match expected words";
        ASSERT_EQ(cmd.size() - 2, expected_words.size()) << "Word count mismatch for dictionary: " << dictName;
    }
}

extern "C" {
#include "slots_tracker_ffi.h"
#include "alias.h"
int DropIndexCommand(RedisModuleCtx *, RedisModuleString **, int);
int DropIfExistsIndexCommand(RedisModuleCtx *, RedisModuleString **, int);
int RestoreSchema(RedisModuleCtx *, RedisModuleString **, int);
void ClusterSlotMigrationEvent(RedisModuleCtx *, RedisModuleEvent, uint64_t, void *);
}

class AsmSchemaImportTest : public ::testing::Test {
 protected:
  RedisModuleCtx *ctx = nullptr;
  bool importing = false;

  void importEvent(uint64_t subevent) {
    RedisModuleSlotRangeArray slots = {.num_ranges = 0};
    RedisModuleClusterSlotMigrationInfo info = {};
    info.version = REDISMODULE_CLUSTER_SLOT_MIGRATION_INFO_VERSION;
    info.task_id = "task";
    info.slots = &slots;
    ClusterSlotMigrationEvent(ctx, RedisModuleEvent_ClusterSlotMigration, subevent, &info);
  }

  void startImport() {
    importEvent(REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_STARTED);
    importing = true;
  }

  void finishImport(
      uint64_t subevent = REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_COMPLETED) {
    importEvent(subevent);
    importing = false;
  }

  void SetUp() override {
    ctx = RedisModule_GetThreadSafeContext(nullptr);
    slots_tracker_reset();
  }

  void TearDown() override {
    if (importing) finishImport(REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_FAILED);
    slots_tracker_reset();
    Indexes_Free(ctx, specDict_g, false);
    RMCK::flushdb(ctx);
    RedisModule_FreeThreadSafeContext(ctx);
  }

  IndexSpec *create(const char *field = "old") {
    RMCK::ArgvList args(ctx, "FT.CREATE", "asm_idx", "SKIPINITIALSCAN", "SCHEMA", field, "TEXT");
    QueryError status = QueryError_Default();
    auto *spec = Indexes_CreateNewSpec(ctx, args, args.size(), &status);
    EXPECT_NE(spec, nullptr) << QueryError_GetDisplayableError(&status, true);
    QueryError_ClearError(&status);
    return spec;
  }

  std::string snapshot(IndexSpec *spec) {
    if (!spec) return {};
    auto *serialized = IndexSpec_Serialize(spec);
    size_t len;
    const char *data = RedisModule_StringPtrLen(serialized, &len);
    std::string result(data, len);
    RedisModule_FreeString(ctx, serialized);
    return result;
  }

  void drop() {
    RMCK::ArgvList args(ctx, "FT.DROPINDEX", "asm_idx");
    EXPECT_EQ(DropIndexCommand(ctx, args, args.size()), REDISMODULE_OK);
  }

  void restore(const std::string &serialized) {
    RMCK::ArgvList args(
        ctx, std::vector<std::string>{"_FT._RESTOREIFNX", "SCHEMA",
                                      std::to_string(INDEX_CURRENT_VERSION), serialized});
    EXPECT_EQ(RestoreSchema(ctx, args, args.size()), REDISMODULE_OK);
  }

  IndexSpec *lookup() {
    return static_cast<IndexSpec *>(StrongRef_Get(Indexes_LoadIndexSpecUnsafe("asm_idx")));
  }
};

TEST_F(AsmSchemaImportTest, DelayedSchemaCannotRestoreDroppedIndex) {
  auto serialized = snapshot(create());
  startImport();
  drop();
  restore(serialized);
  EXPECT_EQ(lookup(), nullptr);
  finishImport();
  EXPECT_EQ(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, RecreateSurvivesDelayedSchemaAndImportCompletion) {
  auto serialized = snapshot(create());
  startImport();
  drop();
  auto *replacement = create("new");
  auto replacementId = replacement->specId;
  restore(serialized);
  ASSERT_EQ(lookup(), replacement);
  EXPECT_EQ(lookup()->specId, replacementId);
  EXPECT_STREQ(HiddenString_GetUnsafe(lookup()->fields[0].fieldName, nullptr), "new");
  finishImport();
  EXPECT_EQ(lookup(), replacement);
}

TEST_F(AsmSchemaImportTest, RepeatedDropDoesNotAllowOldSchemaBack) {
  auto serialized = snapshot(create());
  startImport();
  drop();
  create("new");
  drop();
  restore(serialized);
  EXPECT_EQ(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, IdempotentDropBeforeSchemaArrivalIsRemembered) {
  auto serialized = snapshot(create());
  drop();
  startImport();
  RMCK_GetPropagatedCommands(ctx).clear();
  RMCK::ArgvList args(ctx, "_FT._DROPINDEXIFX", "asm_idx");
  EXPECT_EQ(DropIfExistsIndexCommand(ctx, args, args.size()), REDISMODULE_OK);
  const auto &propagated = RMCK_GetPropagatedCommands(ctx);
  ASSERT_EQ(propagated.size(), 1u);
  EXPECT_EQ(propagated[0],
            (std::vector<std::string>{"_FT._DROPINDEXIFX", "asm_idx", "_FORCEKEEPDOCS"}));
  restore(serialized);
  EXPECT_EQ(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, RetryDoesNotInheritFinishedImportsDrops) {
  auto serialized = snapshot(create());
  startImport();
  drop();
  finishImport(REDISMODULE_SUBEVENT_CLUSTER_SLOT_MIGRATION_IMPORT_FAILED);
  startImport();
  restore(serialized);
  ASSERT_NE(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, RejectedSchemaDoesNotLeaveAliasBehind) {
  auto *spec = create();
  HiddenString *alias = NewHiddenString("asm_alias", 9, false);
  QueryError status = QueryError_Default();
  ASSERT_EQ(IndexAlias_Add(alias, spec->own_ref, 0, &status), REDISMODULE_OK);
  auto serialized = snapshot(spec);
  startImport();
  drop();
  restore(serialized);
  EXPECT_EQ(StrongRef_Get(IndexAlias_Get(alias)), nullptr);
  HiddenString_Free(alias, false);
  EXPECT_EQ(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, InvalidDropDoesNotSuppressSchema) {
  auto serialized = snapshot(create());
  startImport();
  RMCK::ArgvList args(ctx, "FT.DROPINDEX", "asm_idx", "invalid");
  DropIndexCommand(ctx, args, args.size());
  EXPECT_FALSE(slots_tracker_was_index_dropped("asm_idx"));
  ASSERT_NE(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, SchemaRestoreOutsideImportStillWorks) {
  auto serialized = snapshot(create());
  drop();
  restore(serialized);
  ASSERT_NE(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, AliasDropRecordsAndReplicatesCanonicalName) {
  auto *spec = create();
  HiddenString *alias = NewHiddenString("asm_alias", 9, false);
  QueryError status = QueryError_Default();
  ASSERT_EQ(IndexAlias_Add(alias, spec->own_ref, 0, &status), REDISMODULE_OK);
  auto serialized = snapshot(spec);
  startImport();
  RMCK_GetPropagatedCommands(ctx).clear();
  RMCK::ArgvList args(ctx, "FT.DROPINDEX", "asm_alias");
  ASSERT_EQ(DropIndexCommand(ctx, args, args.size()), REDISMODULE_OK);
  const auto &propagated = RMCK_GetPropagatedCommands(ctx);
  ASSERT_EQ(propagated.size(), 1u);
  EXPECT_EQ(propagated[0],
            (std::vector<std::string>{"_FT._DROPINDEXIFX", "asm_idx", "_FORCEKEEPDOCS"}));
  restore(serialized);
  EXPECT_EQ(lookup(), nullptr);
  EXPECT_EQ(StrongRef_Get(IndexAlias_Get(alias)), nullptr);
  HiddenString_Free(alias, false);
}

TEST_F(AsmSchemaImportTest, InvalidAbsentDropDoesNotSuppressSchemaOrReplicate) {
  auto serialized = snapshot(create());
  drop();
  startImport();
  RMCK_GetPropagatedCommands(ctx).clear();
  RMCK::ArgvList args(ctx, "_FT._DROPINDEXIFX", "asm_idx", "invalid");
  EXPECT_EQ(DropIfExistsIndexCommand(ctx, args, args.size()), REDISMODULE_OK);
  EXPECT_TRUE(RMCK_GetPropagatedCommands(ctx).empty());
  restore(serialized);
  EXPECT_NE(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, FullSyncWithoutIndexesPreservesDropBeforeDelayedSchema) {
  auto serialized = snapshot(create());
  startImport();
  drop();
  auto *io = RMCK_CreateRdbIO();
  RMCK_SaveAux(IndexSpecType, io);
  ASSERT_FALSE(io->buffer.empty());
  slots_tracker_reset();
  slots_tracker_begin_loading();
  slots_tracker_start_import("task");
  ASSERT_EQ(RMCK_LoadAux(IndexSpecType, io, INDEX_CURRENT_VERSION), REDISMODULE_OK);
  EXPECT_EQ(io->read_pos, io->buffer.size());
  RMCK_FreeRdbIO(io);
  restore(serialized);
  EXPECT_EQ(lookup(), nullptr);
}

TEST_F(AsmSchemaImportTest, FullSyncLoadsReplacementDespiteDeletionRecord) {
  auto serialized = snapshot(create());
  startImport();
  drop();
  create("new");
  auto *io = RMCK_CreateRdbIO();
  RMCK_SaveAux(IndexSpecType, io);
  Indexes_Free(ctx, specDict_g, false);
  slots_tracker_begin_loading();
  slots_tracker_start_import("task");
  ASSERT_EQ(RMCK_LoadAux(IndexSpecType, io, INDEX_CURRENT_VERSION), REDISMODULE_OK);
  RMCK_FreeRdbIO(io);
  ASSERT_NE(lookup(), nullptr);
  EXPECT_STREQ(HiddenString_GetUnsafe(lookup()->fields[0].fieldName, nullptr), "new");
  EXPECT_TRUE(slots_tracker_was_index_dropped("asm_idx"));
  restore(serialized);
  EXPECT_STREQ(HiddenString_GetUnsafe(lookup()->fields[0].fieldName, nullptr), "new");
}

TEST_F(AsmSchemaImportTest, PreviousRdbVersionLoadsWithoutImportRecords) {
  auto *io = RMCK_CreateRdbIO();
  RedisModule_SaveUnsigned(io, 0);
  startImport();
  slots_tracker_record_index_drop("asm_idx");
  slots_tracker_begin_loading();
  ASSERT_EQ(RMCK_LoadAux(IndexSpecType, io, INDEX_IMPORT_DROPS_VERSION - 1), REDISMODULE_OK);
  EXPECT_EQ(io->read_pos, io->buffer.size());
  EXPECT_FALSE(slots_tracker_was_index_dropped("asm_idx"));
  RMCK_FreeRdbIO(io);
}
