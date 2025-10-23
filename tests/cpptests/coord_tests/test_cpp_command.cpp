/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "rmr/command.h"
#include "slot_ranges.h"
#include "rmalloc.h"

#include <vector>
#include <string>

#include "hiredis/hiredis.h"
#include "hiredis/read.h"
#include "rmutil/args.h"

// Helper functions for testing
void printMRCommand(const MRCommand* cmd) {
    printf("MRCommand (%d args): ", cmd->num);
    for (int i = 0; i < cmd->num; i++) {
        // Check if this argument contains binary data (non-printable characters or null bytes)
        bool is_binary = false;
        for (size_t j = 0; j < cmd->lens[i]; j++) {
            unsigned char c = (unsigned char)cmd->strs[i][j];
            if (c == 0 || (c < 32 && c != '\t' && c != '\n' && c != '\r') || c > 126) {
                is_binary = true;
                break;
            }
        }

        if (is_binary) {
            printf("[%d]=<binary:%zu bytes:", i, cmd->lens[i]);
            for (size_t j = 0; j < cmd->lens[i]; j++) {
                printf("%02x", (unsigned char)cmd->strs[i][j]);
            }
            printf("> ");
        } else {
            printf("[%d]='%.*s' ", i, (int)cmd->lens[i], cmd->strs[i]);
        }
    }
    printf("\n");
}

bool verifyCommandArgs(const MRCommand* cmd, const std::vector<std::string>& expected) {
    if (cmd->num != (int)expected.size()) {
        return false;
    }

    for (int i = 0; i < cmd->num; i++) {
        if (cmd->lens[i] != expected[i].length() ||
            memcmp(cmd->strs[i], expected[i].c_str(), cmd->lens[i]) != 0) {
            return false;
        }
    }
    return true;
}

bool verifyCommandArgsPrefix(const MRCommand* cmd, const std::vector<std::string>& expected) {
    if (cmd->num < (int)expected.size()) {
        return false;
    }

    for (int i = 0; i < (int)expected.size(); i++) {
        if (cmd->lens[i] != expected[i].length() ||
            memcmp(cmd->strs[i], expected[i].c_str(), cmd->lens[i]) != 0) {
            return false;
        }
    }
    return true;
}

bool hasSlotRangeInfo(const MRCommand* cmd) {
    for (int i = 0; i < cmd->num - 2; i++) {
        if (cmd->lens[i] == strlen("RANGE_SLOTS_BINARY") &&
            memcmp(cmd->strs[i], "RANGE_SLOTS_BINARY", strlen("RANGE_SLOTS_BINARY")) == 0) {
            return true;
        }
    }
    return false;
}

int findArgPosition(const MRCommand* cmd, const char* arg) {
    for (int i = 0; i < cmd->num; i++) {
        if (cmd->lens[i] == strlen(arg) &&
            memcmp(cmd->strs[i], arg, strlen(arg)) == 0) {
            return i;
        }
    }
    return -1;
}

// Base test class for non-parameterized tests
class MRCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);

        // Create a test slot range array using the same pattern as test_cpp_slot_ranges.cpp
        testSlotArray = createSlotRangeArray({{0, 8191}, {8192, 16383}});
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
        }
        if (testSlotArray) {
            rm_free(testSlotArray);
        }
    }

    // Helper function to create a RedisModuleSlotRangeArray for testing
    RedisModuleSlotRangeArray* createSlotRangeArray(const std::vector<std::pair<uint16_t, uint16_t>>& ranges) {
        // Allocate memory for the struct plus the flexible array member
        size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges.size();
        RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
        array->num_ranges = ranges.size();

        for (size_t i = 0; i < ranges.size(); i++) {
            array->ranges[i].start = ranges[i].first;
            array->ranges[i].end = ranges[i].second;
        }

        return array;
    }

    RedisModuleCtx *ctx = nullptr;
    RedisModuleSlotRangeArray* testSlotArray;
};

// Parameterized test class for slot range tests
class MRCommandSlotRangeTest : public ::testing::TestWithParam<std::vector<std::pair<uint16_t, uint16_t>>> {
protected:
    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);

        // Create slot range array from the parameter
        auto ranges = GetParam();
        testSlotArray = createSlotRangeArray(ranges);

        // Create a description for logging
        testDescription = "SlotRanges[";
        for (size_t i = 0; i < ranges.size(); i++) {
            if (i > 0) testDescription += ",";
            testDescription += std::to_string(ranges[i].first) + "-" + std::to_string(ranges[i].second);
        }
        testDescription += "]";
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
        }
        if (testSlotArray) {
            rm_free(testSlotArray);
        }
    }

    // Helper function to create a RedisModuleSlotRangeArray for testing
    RedisModuleSlotRangeArray* createSlotRangeArray(const std::vector<std::pair<uint16_t, uint16_t>>& ranges) {
        // Allocate memory for the struct plus the flexible array member
        size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges.size();
        RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
        array->num_ranges = ranges.size();

        for (size_t i = 0; i < ranges.size(); i++) {
            array->ranges[i].start = ranges[i].first;
            array->ranges[i].end = ranges[i].second;
        }

        return array;
    }

    RedisModuleCtx *ctx = nullptr;
    RedisModuleSlotRangeArray* testSlotArray;
    std::string testDescription;
};

// ============================================================================
// Command Building Tests
// ============================================================================

// Test basic command creation with MR_NewCommand
TEST_F(MRCommandTest, testBasicCommandCreation) {
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");

    EXPECT_EQ(cmd.num, 3);
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"FT.SEARCH", "test_index", "hello"}));
    EXPECT_EQ(cmd.targetShard, INVALID_SHARD);
    EXPECT_FALSE(cmd.forCursor);
    EXPECT_FALSE(cmd.forProfiling);
    EXPECT_FALSE(cmd.depleted);

    MRCommand_Free(&cmd);
}

// Test command creation from argv
TEST_F(MRCommandTest, testCommandCreationFromArgv) {
    const char* argv[] = {"FT.AGGREGATE", "myindex", "*", "GROUPBY", "1", "@category"};
    MRCommand cmd = MR_NewCommandArgv(6, argv);

    EXPECT_EQ(cmd.num, 6);
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"FT.AGGREGATE", "myindex", "*", "GROUPBY", "1", "@category"}));

    MRCommand_Free(&cmd);
}

// Test command copying
TEST_F(MRCommandTest, testCommandCopy) {
    MRCommand original = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");
    original.targetShard = 5;
    original.forCursor = true;
    original.protocol = 3;

    MRCommand copy = MRCommand_Copy(&original);

    EXPECT_EQ(copy.num, original.num);
    EXPECT_TRUE(verifyCommandArgs(&copy, {"FT.SEARCH", "test_index", "hello"}));
    EXPECT_EQ(copy.targetShard, original.targetShard);
    EXPECT_EQ(copy.forCursor, original.forCursor);
    EXPECT_EQ(copy.protocol, original.protocol);

    MRCommand_Free(&original);
    MRCommand_Free(&copy);
}

// Test appending arguments to a command
TEST_F(MRCommandTest, testCommandAppend) {
    MRCommand cmd = MR_NewCommand(2, "FT.SEARCH", "myindex");

    MRCommand_Append(&cmd, "hello", 5);
    MRCommand_Append(&cmd, "LIMIT", 5);
    MRCommand_Append(&cmd, "0", 1);
    MRCommand_Append(&cmd, "10", 2);

    EXPECT_EQ(cmd.num, 6);
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"FT.SEARCH", "myindex", "hello", "LIMIT", "0", "10"}));

    MRCommand_Free(&cmd);
}

// Test inserting arguments at specific positions
TEST_F(MRCommandTest, testCommandInsert) {
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "myindex", "hello");

    // Insert LIMIT arguments at position 3
    MRCommand_Insert(&cmd, 3, "LIMIT", 5);
    MRCommand_Insert(&cmd, 4, "0", 1);
    MRCommand_Insert(&cmd, 5, "10", 2);

    EXPECT_EQ(cmd.num, 6);
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"FT.SEARCH", "myindex", "hello", "LIMIT", "0", "10"}));
    MRCommand_Free(&cmd);
}

// Test replacing arguments in a command
TEST_F(MRCommandTest, testCommandReplaceArg) {
    MRCommand cmd = MR_NewCommand(4, "FT.SEARCH", "myindex", "hello", "world");
    // Replace the query
    MRCommand_ReplaceArg(&cmd, 2, "goodbye", 7);
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"FT.SEARCH", "myindex", "goodbye", "world"}));
    MRCommand_Free(&cmd);
}

// Test setting command prefix
TEST_F(MRCommandTest, testCommandSetPrefix) {
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "myindex", "hello");
    MRCommand_SetPrefix(&cmd, "_FT");
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"_FT.SEARCH", "myindex", "hello"}));
    MRCommand_Free(&cmd);
}

// Test replacing command prefix when one already exists
TEST_F(MRCommandTest, testCommandReplacePrefixExisting) {
    MRCommand cmd = MR_NewCommand(3, "_FT.SEARCH", "myindex", "hello");
    MRCommand_SetPrefix(&cmd, "NEW");
    EXPECT_TRUE(verifyCommandArgs(&cmd, {"NEW.SEARCH", "myindex", "hello"}));
    MRCommand_Free(&cmd);
}

// ============================================================================
// Slot Range Tests
// ============================================================================

// Test that slot range info is added to different types of commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToHybridCommand) {
    // Create a hybrid command
    MRCommand cmd = MR_NewCommand(7, "_FT.HYBRID", "test_index", "SEARCH", "hello", "VSIM", "@vector", "data");
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to cursor command should succeed";
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "Cursor command should contain slot range information";
    MRCommand_Free(&cmd);
}

// Test that slot range info is added to cursor commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToCursorCommand) {
    // Create a cursor command
    MRCommand cmd = MR_NewCommand(4, "_FT.CURSOR", "READ", "test_index", "12345");
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to cursor command should succeed";
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "Cursor command should contain slot range information";

    MRCommand_Free(&cmd);
}

// Test that slot range info is added to FT.SEARCH commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToSearchCommand) {
    // Create a FT.SEARCH command
    MRCommand cmd = MR_NewCommand(5, "FT.SEARCH", "myindex", "hello", "LIMIT", "10");

    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to FT.SEARCH command should succeed";
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "FT.SEARCH command should contain slot range information";

    // Verify the original command arguments are preserved and slot range info is added
    EXPECT_EQ(cmd.num, 8) << "Command should have 8 arguments after adding slot range info";
    EXPECT_TRUE(verifyCommandArgsPrefix(&cmd, {"FT.SEARCH", "myindex", "hello", "LIMIT", "10"})) << "Original arguments should be preserved";

    // Verify slot range arguments are at the end
    int rangePos = findArgPosition(&cmd, "RANGE_SLOTS_BINARY");
    EXPECT_EQ(rangePos, 5) << "RANGE_SLOTS_BINARY should be at position 5";
    std::string sizeArg(cmd.strs[rangePos + 1], cmd.lens[rangePos + 1]);
    EXPECT_EQ(sizeArg, "12") << "Size argument should be '12'";
    EXPECT_GT(cmd.lens[rangePos + 2], 0) << "Binary data should be present";

    MRCommand_Free(&cmd);
}

// Test that slot range info is added to FT.AGGREGATE commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToAggregateCommand) {
    // Create a FT.AGGREGATE command
    MRCommand cmd = MR_NewCommand(6, "FT.AGGREGATE", "myindex", "*", "GROUPBY", "1", "@category");

    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to FT.AGGREGATE command should succeed";
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "FT.AGGREGATE command should contain slot range information";

    // Verify the original command arguments are preserved and slot range info is added
    EXPECT_EQ(cmd.num, 9) << "Command should have 9 arguments after adding slot range info";
    EXPECT_TRUE(verifyCommandArgsPrefix(&cmd, {"FT.AGGREGATE", "myindex", "*", "GROUPBY", "1", "@category"})) << "Original arguments should be preserved";

    // Verify slot range arguments are at the end
    int rangePos = findArgPosition(&cmd, "RANGE_SLOTS_BINARY");
    EXPECT_EQ(rangePos, 6) << "RANGE_SLOTS_BINARY should be at position 6";
    std::string sizeArg(cmd.strs[rangePos + 1], cmd.lens[rangePos + 1]);
    EXPECT_EQ(sizeArg, "12") << "Size argument should be '12'";
    EXPECT_GT(cmd.lens[rangePos + 2], 0) << "Binary data should be present";

    MRCommand_Free(&cmd);
}

// Helper function to extract slot range data from argc/argv using ArgsCursor
// This demonstrates how to find and deserialize slot range data in real code
RedisModuleSlotRangeArray* extractSlotRangeFromArgs(RedisModuleString **argv, int argc) {
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv, argc);

    // Search for RANGE_SLOTS_BINARY token
    while (!AC_IsAtEnd(&ac)) {
        const char *current_arg;
        size_t arg_len;

        if (AC_GetString(&ac, &current_arg, &arg_len, AC_F_NOADVANCE) == AC_OK) {
            if (arg_len == strlen("RANGE_SLOTS_BINARY") &&
                strncmp(current_arg, "RANGE_SLOTS_BINARY", arg_len) == 0) {

                // Found the token, advance past it
                AC_Advance(&ac);

                // Get the size argument
                const char *size_str;
                if (AC_GetString(&ac, &size_str, NULL, 0) == AC_OK) {
                    size_t expected_size = strtoul(size_str, NULL, 10);

                    // Get the binary data argument
                    RedisModuleString *binary_rms;
                    if (AC_GetRString(&ac, &binary_rms, 0) == AC_OK) {
                        size_t binary_len;
                        const char *binary_data = RedisModule_StringPtrLen(binary_rms, &binary_len);

                        if (binary_len == expected_size) {
                            // Allocate space for the slot range array
                            // Note: In real code, you'd need to know the max possible ranges
                            // or parse the binary data first to get the count
                            RedisModuleSlotRangeArray *slot_array = (RedisModuleSlotRangeArray*)rm_malloc(
                                sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * 16); // Max 16 ranges

                            // Deserialize the binary data
                            if (RedisModuleSlotRangeArray_DeserializeBinary(
                                    (const unsigned char*)binary_data, binary_len, slot_array)) {
                                return slot_array;
                            } else {
                                rm_free(slot_array);
                            }
                        }
                    }
                }
                break;
            }
        }
        AC_Advance(&ac);
    }

    return NULL; // Not found or error
}

// Helper function to extract slot range data from argc/argv using ArgsCursor (Human-Readable format)
// This demonstrates how to find and deserialize human-readable slot range data in real code
RedisModuleSlotRangeArray* extractSlotRangeFromArgs_HumanReadable(RedisModuleString **argv, int argc) {
    ArgsCursor ac;
    ArgsCursor_InitRString(&ac, argv, argc);

    // Search for RANGE_SLOTS_HR token
    while (!AC_IsAtEnd(&ac)) {
        const char *current_arg;
        size_t arg_len;

        if (AC_GetString(&ac, &current_arg, &arg_len, AC_F_NOADVANCE) == AC_OK) {
            if (arg_len == strlen("RANGE_SLOTS_HR") &&
                strncmp(current_arg, "RANGE_SLOTS_HR", arg_len) == 0) {

                // Found RANGE_SLOTS_HR, advance to next argument (num_ranges)
                AC_Advance(&ac);
                const char *num_ranges_str;
                if (AC_GetString(&ac, &num_ranges_str, NULL, 0) == AC_OK) {
                    int32_t num_ranges = strtol(num_ranges_str, NULL, 10);

                    if (num_ranges > 0 && num_ranges <= 16384) { // Sanity check
                        // Allocate slot range array
                        RedisModuleSlotRangeArray *slot_array = (RedisModuleSlotRangeArray*)rm_malloc(
                            sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * num_ranges);

                        if (slot_array) {
                            slot_array->num_ranges = num_ranges;

                            // Read start_slot and end_slot pairs
                            for (int i = 0; i < num_ranges; i++) {
                                const char *start_slot_str, *end_slot_str;

                                // Get start slot
                                if (AC_GetString(&ac, &start_slot_str, NULL, 0) != AC_OK) {
                                    rm_free(slot_array);
                                    return NULL;
                                }

                                // Get end slot
                                if (AC_GetString(&ac, &end_slot_str, NULL, 0) != AC_OK) {
                                    rm_free(slot_array);
                                    return NULL;
                                }

                                // Parse and store the range
                                slot_array->ranges[i].start = (uint16_t)strtoul(start_slot_str, NULL, 10);
                                slot_array->ranges[i].end = (uint16_t)strtoul(end_slot_str, NULL, 10);
                            }

                            return slot_array;
                        }
                    }
                }
                break;
            }
        }
        AC_Advance(&ac);
    }

    return NULL; // Not found or error
}

// Define the parameter values for slot range tests
INSTANTIATE_TEST_SUITE_P(
    SlotRangeVariations,
    MRCommandSlotRangeTest,
    ::testing::Values(
        // Single range (full cluster)
        std::vector<std::pair<uint16_t, uint16_t>>{{0, 16383}},

        // Two ranges (original test case)
        std::vector<std::pair<uint16_t, uint16_t>>{{0, 8191}, {8192, 16383}},

        // Three ranges
        std::vector<std::pair<uint16_t, uint16_t>>{{0, 5460}, {5461, 10922}, {10923, 16383}},

        // Four ranges (quarters)
        std::vector<std::pair<uint16_t, uint16_t>>{{0, 4095}, {4096, 8191}, {8192, 12287}, {12288, 16383}},

        // Single slot ranges
        std::vector<std::pair<uint16_t, uint16_t>>{{0, 0}, {100, 100}, {16383, 16383}},

        // Irregular ranges
        std::vector<std::pair<uint16_t, uint16_t>>{{0, 1000}, {5000, 6000}, {10000, 16383}}
    )
);

// Parameterized test for adding slot range info
TEST_P(MRCommandSlotRangeTest, testAddSlotRangeInfo) {
    // Create a command
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");

    // Add slot range info
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info should succeed";

    // Verify the command structure
    EXPECT_EQ(cmd.num, 6) << "Command should have 6 arguments after adding slot range info";
    EXPECT_STREQ(cmd.strs[3], "RANGE_SLOTS_BINARY") << "Fourth argument should be RANGE_SLOTS_BINARY";

    // Verify the size argument
    std::string expected_size = std::to_string(sizeof(int32_t) + testSlotArray->num_ranges * sizeof(RedisModuleSlotRange));
    EXPECT_STREQ(cmd.strs[4], expected_size.c_str()) << "Fifth argument should be the binary data size";

    // Verify binary data length
    size_t expected_binary_len = sizeof(int32_t) + testSlotArray->num_ranges * sizeof(RedisModuleSlotRange);
    EXPECT_EQ(cmd.lens[5], expected_binary_len) << "Binary data length should match expected size";

    MRCommand_Free(&cmd);
}

// Parameterized test for round-trip slot range serialization
TEST_P(MRCommandSlotRangeTest, testSlotRangeRoundTrip) {
    // Create a command with slot range info
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info should succeed";

    // Format the command using redisFormatSdsCommandArgv
    sds formatted_cmd = NULL;
    long long cmd_len = redisFormatSdsCommandArgv(&formatted_cmd, cmd.num, (const char **)cmd.strs, cmd.lens);
    EXPECT_GT(cmd_len, 0) << "Command formatting should succeed";
    EXPECT_NE(formatted_cmd, (sds)NULL) << "Formatted command should not be NULL";

    // Parse the formatted command back using redisReader
    redisReader *reader = redisReaderCreate();
    EXPECT_NE(reader, (redisReader*)NULL) << "Reader creation should succeed";

    int feed_result = redisReaderFeed(reader, formatted_cmd, cmd_len);
    EXPECT_EQ(feed_result, REDIS_OK) << "Feeding data to reader should succeed";

    void *reply_ptr;
    int get_result = redisReaderGetReply(reader, &reply_ptr);
    EXPECT_EQ(get_result, REDIS_OK) << "Getting reply should succeed";
    EXPECT_NE(reply_ptr, (void*)NULL) << "Reply should not be NULL";

    redisReply *reply = (redisReply*)reply_ptr;
    EXPECT_EQ(reply->type, REDIS_REPLY_ARRAY) << "Reply should be an array";
    EXPECT_EQ(reply->elements, (size_t)cmd.num) << "Reply should have same number of elements as original command";

    std::vector<RedisModuleString*> argv_vec;
    for (size_t i = 0; i < reply->elements; i++) {
        RedisModuleString *rms = RedisModule_CreateString(NULL, reply->element[i]->str, reply->element[i]->len);
        argv_vec.push_back(rms);
    }

    RedisModuleString **argv = argv_vec.data();
    int argc = argv_vec.size();

    // Use the helper function to extract slot range data
    RedisModuleSlotRangeArray *reconstructed = extractSlotRangeFromArgs(argv, argc);

    EXPECT_NE(reconstructed, (RedisModuleSlotRangeArray*)NULL) << "Should successfully extract slot range data";
    EXPECT_EQ(reconstructed->num_ranges, testSlotArray->num_ranges) << "Should have same number of ranges";

    for (int i = 0; i < reconstructed->num_ranges && i < testSlotArray->num_ranges; i++) {
        EXPECT_EQ(reconstructed->ranges[i].start, testSlotArray->ranges[i].start) << "Start slot should match";
        EXPECT_EQ(reconstructed->ranges[i].end, testSlotArray->ranges[i].end) << "End slot should match";
    }

    rm_free(reconstructed);

    // Free the RedisModuleString objects we created
    for (RedisModuleString *rms : argv_vec) {
        RedisModule_FreeString(NULL, rms);
    }

    // Cleanup
    freeReplyObject(reply);
    redisReaderFree(reader);
    sdsfree(formatted_cmd);
    MRCommand_Free(&cmd);
}

// Parameterized test for adding human-readable slot range info
TEST_P(MRCommandSlotRangeTest, testAddSlotRangeInfoHumanReadable) {
    // Create a command
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");

    // Add slot range info in human-readable format
    bool result = MRCommand_AddSlotRangeInfo_HumanReadable(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding human-readable slot range info should succeed";

    // Verify the command structure
    // Expected: original 3 args + RANGE_SLOTS_HR + num_ranges + (start,end) pairs
    int expected_args = 3 + 1 + 1 + (testSlotArray->num_ranges * 2);
    EXPECT_EQ(cmd.num, expected_args) << "Command should have correct number of arguments";
    EXPECT_STREQ(cmd.strs[3], "RANGE_SLOTS_HR") << "Fourth argument should be RANGE_SLOTS_HR";

    // Verify the num_ranges argument
    std::string expected_num_ranges = std::to_string(testSlotArray->num_ranges);
    EXPECT_STREQ(cmd.strs[4], expected_num_ranges.c_str()) << "Fifth argument should be the number of ranges";

    // Verify the slot range pairs
    for (int i = 0; i < testSlotArray->num_ranges; i++) {
        int start_arg_idx = 5 + (i * 2);
        int end_arg_idx = 5 + (i * 2) + 1;

        std::string expected_start = std::to_string(testSlotArray->ranges[i].start);
        std::string expected_end = std::to_string(testSlotArray->ranges[i].end);

        EXPECT_STREQ(cmd.strs[start_arg_idx], expected_start.c_str())
            << "Start slot for range " << i << " should match";
        EXPECT_STREQ(cmd.strs[end_arg_idx], expected_end.c_str())
            << "End slot for range " << i << " should match";
    }

    MRCommand_Free(&cmd);
}

// Parameterized test for human-readable slot range round-trip serialization
TEST_P(MRCommandSlotRangeTest, testSlotRangeRoundTripHumanReadable) {
    // Create a command with human-readable slot range info
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");
    bool result = MRCommand_AddSlotRangeInfo_HumanReadable(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding human-readable slot range info should succeed";

    // Format the command using redisFormatSdsCommandArgv
    sds formatted_cmd = NULL;
    long long len = redisFormatSdsCommandArgv(&formatted_cmd, cmd.num, (const char**)cmd.strs, cmd.lens);
    EXPECT_GT(len, 0) << "Command formatting should succeed";
    EXPECT_NE(formatted_cmd, (sds)NULL) << "Formatted command should not be NULL";

    // Parse the formatted command back using redisReader
    redisReader *reader = redisReaderCreate();
    EXPECT_NE(reader, (redisReader*)NULL) << "Reader creation should succeed";

    // Feed the formatted command to the reader
    int feed_result = redisReaderFeed(reader, formatted_cmd, len);
    EXPECT_EQ(feed_result, REDIS_OK) << "Feeding command to reader should succeed";

    // Get the parsed reply
    redisReply *reply = NULL;
    int parse_result = redisReaderGetReply(reader, (void**)&reply);
    EXPECT_EQ(parse_result, REDIS_OK) << "Parsing command should succeed";

    EXPECT_EQ(reply->type, REDIS_REPLY_ARRAY) << "Reply should be an array";
    EXPECT_EQ(reply->elements, cmd.num) << "Reply should have same number of elements as original command";

    for (size_t i = 0; i < reply->elements; i++) {
        EXPECT_EQ(reply->element[i]->type, REDIS_REPLY_STRING) << "Each element should be a string";
    }

    std::vector<RedisModuleString*> argv_vec;
    for (size_t i = 0; i < reply->elements; i++) {
        // Create RedisModuleString from the reply data
        RedisModuleString *rms = RedisModule_CreateString(NULL, reply->element[i]->str, reply->element[i]->len);
        argv_vec.push_back(rms);
    }

    RedisModuleString **argv = argv_vec.data();
    int argc = argv_vec.size();
    // Use the human-readable helper function to extract slot range data
    RedisModuleSlotRangeArray *reconstructed = extractSlotRangeFromArgs_HumanReadable(argv, argc);

    EXPECT_NE(reconstructed, (RedisModuleSlotRangeArray*)NULL) << "Should successfully extract human-readable slot range data";
    EXPECT_EQ(reconstructed->num_ranges, testSlotArray->num_ranges) << "Should have same number of ranges";

    for (int i = 0; i < reconstructed->num_ranges && i < testSlotArray->num_ranges; i++) {
        EXPECT_EQ(reconstructed->ranges[i].start, testSlotArray->ranges[i].start) << "Start slot should match";
        EXPECT_EQ(reconstructed->ranges[i].end, testSlotArray->ranges[i].end) << "End slot should match";
    }

    rm_free(reconstructed);

    // Free the RedisModuleString objects we created
    for (RedisModuleString *rms : argv_vec) {
        RedisModule_FreeString(NULL, rms);
    }

    // Cleanup
    freeReplyObject(reply);
    redisReaderFree(reader);
    sdsfree(formatted_cmd);
    MRCommand_Free(&cmd);
}
