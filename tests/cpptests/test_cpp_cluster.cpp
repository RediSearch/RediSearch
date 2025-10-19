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
#include "redismock/redismock.h"

#include "trie/trie_type.h"
extern "C" {
#include "dictionary.h"
#include "slot_ranges.h"

// Definition of SharedSlotRangeArray for use in the test (as it's opaque in the header)
struct SharedSlotRangeArray {
  uint32_t refcount;
  RedisModuleSlotRangeArray array;
};
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
            IndexSpec_RemoveFromGlobals(spec_ref, false);
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
    StrongRef original_spec_ref = IndexSpec_ParseC("idx1", args, sizeof(args) / sizeof(const char *), &err);
    ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);
    Spec_AddToDict(original_spec_ref.rm);
    specs.push_back(original_spec_ref); // Keep track of created spec for cleanup

    // Create a second IndexSpec
    const char *args2[] = {"SCHEMA", "name", "TEXT", "age", "NUMERIC", "city", "TAG"};
    StrongRef second_spec_ref = IndexSpec_ParseC("idx2", args2, sizeof(args2) / sizeof(const char *), &err);
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

TEST_F(ClusterTest, SlotRangesManagement) {
    // Get local slot ranges
    auto *ranges = Slots_GetLocalSlots();

    ASSERT_EQ(ranges->refcount, 2) << "Initial refcount should be 2 after first get - caller and cache";
    // Sanity - expect the mock ranges
    ASSERT_EQ(ranges->array.num_ranges, 2);
    ASSERT_EQ(ranges->array.ranges[0].start, 0);
    ASSERT_EQ(ranges->array.ranges[0].end, 5460);
    ASSERT_EQ(ranges->array.ranges[1].start, 10923);
    ASSERT_EQ(ranges->array.ranges[1].end, 16383);

    // Get again - should increase refcount
    auto *ranges2 = Slots_GetLocalSlots();
    ASSERT_EQ(ranges2, ranges) << "Subsequent get should return same pointer";
    ASSERT_EQ(ranges->refcount, 3) << "Refcount should be 3 after second get";

    // Drop cache reference
    Slots_DropCachedLocalSlots();
    ASSERT_EQ(ranges->refcount, 2) << "Refcount should be 2 after dropping cache reference";

    // Drop one reference
    Slots_FreeLocalSlots(ranges2);
    ASSERT_EQ(ranges->refcount, 1) << "Refcount should be 1 after dropping one reference";

    // Get again - should create new ranges since cache was dropped
    ranges2 = Slots_GetLocalSlots();
    ASSERT_NE(ranges2, ranges) << "After dropping cache, new get should return different pointer";
    ASSERT_EQ(ranges2->refcount, 2) << "New ranges refcount should be 2 after get";

    // Check slot access
    ASSERT_TRUE(Slots_CanAccessKeysInSlot(ranges2, 0));
    ASSERT_TRUE(Slots_CanAccessKeysInSlot(ranges2, 5000));
    ASSERT_FALSE(Slots_CanAccessKeysInSlot(ranges2, 6000));
    ASSERT_TRUE(Slots_CanAccessKeysInSlot(ranges2, 11000));
    ASSERT_FALSE(Slots_CanAccessKeysInSlot(ranges2, 9000));
    ASSERT_TRUE(Slots_CanAccessKeysInSlot(ranges2, 16383));

    // Cleanup
    Slots_FreeLocalSlots(ranges);
    Slots_FreeLocalSlots(ranges2);
    Slots_DropCachedLocalSlots();
}
