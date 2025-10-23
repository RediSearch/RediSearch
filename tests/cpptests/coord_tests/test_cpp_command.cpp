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

    // Helper function to verify command arguments
    bool verifyCommandArgs(const MRCommand *cmd, const std::vector<std::string>& expected) {
        if (cmd->num != expected.size()) {
            return false;
        }
        for (size_t i = 0; i < expected.size(); i++) {
            if (cmd->lens[i] != expected[i].length() ||
                memcmp(cmd->strs[i], expected[i].c_str(), expected[i].length()) != 0) {
                return false;
            }
        }
        return true;
    }

    // Helper function to verify the first N arguments of a command
    bool verifyCommandArgsPrefix(const MRCommand *cmd, const std::vector<std::string>& expected) {
        if (cmd->num < expected.size()) {
            return false;
        }
        for (size_t i = 0; i < expected.size(); i++) {
            if (cmd->lens[i] != expected[i].length() ||
                memcmp(cmd->strs[i], expected[i].c_str(), expected[i].length()) != 0) {
                return false;
            }
        }
        return true;
    }

    void printMRCommand(const MRCommand *cmd) {
        printf("MRCommand (%d args): ", cmd->num);
        for (int i = 0; i < cmd->num; i++) {
            // Check if this looks like binary data (contains non-printable chars or null bytes)
            bool isBinary = false;
            for (size_t j = 0; j < cmd->lens[i]; j++) {
                unsigned char c = (unsigned char)cmd->strs[i][j];
                if (c == 0 || c < 32 || c > 126) {
                    isBinary = true;
                    break;
                }
            }

            if (isBinary) {
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

    // Helper function to check if a command contains slot range information
    bool hasSlotRangeInfo(const MRCommand *cmd) {
        int rangeSlotsBinaryPos = -1;

        // Find RANGE_SLOTS_BINARY position
        for (int i = 0; i < cmd->num; i++) {
            if (strncmp(cmd->strs[i], "RANGE_SLOTS_BINARY", cmd->lens[i]) == 0) {
                rangeSlotsBinaryPos = i;
                break;
            }
        }

        // If RANGE_SLOTS_BINARY is found, check that it's followed by size and binary data
        if (rangeSlotsBinaryPos >= 0 && rangeSlotsBinaryPos + 2 < cmd->num) {
            // The next argument should be the size (as a string)
            // The argument after that should be the binary data
            return true;
        }

        return false;
    }

    // Helper function to find the position of an argument in the command
    int findArgPosition(const MRCommand *cmd, const char *arg) {
        for (int i = 0; i < cmd->num; i++) {
            if (strncmp(cmd->strs[i], arg, cmd->lens[i]) == 0 && strlen(arg) == cmd->lens[i]) {
                return i;
            }
        }
        return -1;
    }


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

// Test that MRCommand_AddSlotRangeInfo adds the correct arguments
TEST_F(MRCommandTest, testAddSlotRangeInfo) {
    // Create a basic command
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");

    // Print initial command
    printf("Initial command: ");
    printMRCommand(&cmd);

    // Add slot range information
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info should succeed";

    // Print command after adding slot range info
    printf("Command after adding slot range info: ");
    printMRCommand(&cmd);

    // In a real cluster environment, this should succeed
    // In a non-cluster environment, it might fail (return false)
    // For this test, we'll check both cases

    // If successful, verify the slot range arguments were added
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "Command should contain slot range information";

    // Verify the arguments were added correctly
    int rangePos = findArgPosition(&cmd, "RANGE_SLOTS_BINARY");
    EXPECT_GT(rangePos, 0) << "RANGE_SLOTS_BINARY should be added at the end of the command";

    // Verify that RANGE_SLOTS_BINARY is followed by size and binary data
    EXPECT_LT(rangePos + 2, cmd.num) << "RANGE_SLOTS_BINARY should be followed by size and binary data";

    // The next argument should be the size (as a string)
    std::string sizeArg(cmd.strs[rangePos + 1], cmd.lens[rangePos + 1]);
    EXPECT_GT(std::stoi(sizeArg), 0) << "Size argument should be a positive number";

    // The argument after that should be the binary data
    EXPECT_GT(cmd.lens[rangePos + 2], 0) << "Binary data should be present";

    MRCommand_Free(&cmd);
}

// Test that slot range info is added to different types of commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToHybridCommand) {
    // Create a hybrid command
    MRCommand cmd = MR_NewCommand(7, "_FT.HYBRID", "test_index", "SEARCH", "hello", "VSIM", "@vector", "data");

    printf("Initial hybrid command: ");
    printMRCommand(&cmd);

    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);

    EXPECT_TRUE(result) << "Adding slot range info to cursor command should succeed";

    printf("Cursor command after adding slot range info: ");
    printMRCommand(&cmd);
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "Cursor command should contain slot range information";
    MRCommand_Free(&cmd);
}

// Test that slot range info is added to cursor commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToCursorCommand) {
    // Create a cursor command
    MRCommand cmd = MR_NewCommand(4, "_FT.CURSOR", "READ", "test_index", "12345");

    printf("Initial cursor command: ");
    printMRCommand(&cmd);

    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to cursor command should succeed";

    printf("Cursor command after adding slot range info: ");
    printMRCommand(&cmd);
    EXPECT_TRUE(hasSlotRangeInfo(&cmd)) << "Cursor command should contain slot range information";

    MRCommand_Free(&cmd);
}

// Test that slot range info is added to FT.SEARCH commands
TEST_F(MRCommandTest, testAddSlotRangeInfoToSearchCommand) {
    // Create a FT.SEARCH command
    MRCommand cmd = MR_NewCommand(5, "FT.SEARCH", "myindex", "hello", "LIMIT", "10");

    printf("Initial FT.SEARCH command: ");
    printMRCommand(&cmd);

    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to FT.SEARCH command should succeed";

    printf("FT.SEARCH command after adding slot range info: ");
    printMRCommand(&cmd);
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

    printf("Initial FT.AGGREGATE command: ");
    printMRCommand(&cmd);

    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info to FT.AGGREGATE command should succeed";

    printf("FT.AGGREGATE command after adding slot range info: ");
    printMRCommand(&cmd);
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

// Test round-trip: Command → Format → Parse → Reconstruct SlotRangeArray
TEST_F(MRCommandTest, testSlotRangeRoundTrip) {
    // Create a command with slot range info
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info should succeed";

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

// Test the helper function for extracting slot range data from argc/argv
TEST_F(MRCommandTest, testExtractSlotRangeFromArgs) {
    // Create a command with slot range info
    MRCommand cmd = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");
    bool result = MRCommand_AddSlotRangeInfo(&cmd, testSlotArray);
    EXPECT_TRUE(result) << "Adding slot range info should succeed";
    // Convert MRCommand to RedisModuleString array (simulating real usage)
    std::vector<RedisModuleString*> argv_vec;
    for (int i = 0; i < cmd.num; i++) {
        RedisModuleString *rms = RedisModule_CreateString(NULL, cmd.strs[i], cmd.lens[i]);
        argv_vec.push_back(rms);
    }

    // Use the helper function to extract slot range data
    RedisModuleSlotRangeArray *extracted = extractSlotRangeFromArgs(argv_vec.data(), argv_vec.size());

    EXPECT_NE(extracted, (RedisModuleSlotRangeArray*)NULL) << "Should successfully extract slot range data";

    EXPECT_EQ(extracted->num_ranges, testSlotArray->num_ranges) << "Should have same number of ranges";

    for (int i = 0; i < extracted->num_ranges && i < testSlotArray->num_ranges; i++) {
        EXPECT_EQ(extracted->ranges[i].start, testSlotArray->ranges[i].start) << "Start slot should match";
        EXPECT_EQ(extracted->ranges[i].end, testSlotArray->ranges[i].end) << "End slot should match";
    }

    rm_free(extracted);

    // Test with command that doesn't have slot range data
    MRCommand cmd_no_slots = MR_NewCommand(3, "FT.SEARCH", "test_index", "hello");
    std::vector<RedisModuleString*> argv_no_slots;
    for (int i = 0; i < cmd_no_slots.num; i++) {
        RedisModuleString *rms = RedisModule_CreateString(NULL, cmd_no_slots.strs[i], cmd_no_slots.lens[i]);
        argv_no_slots.push_back(rms);
    }

    RedisModuleSlotRangeArray *no_slots_result = extractSlotRangeFromArgs(argv_no_slots.data(), argv_no_slots.size());
    EXPECT_EQ(no_slots_result, (RedisModuleSlotRangeArray*)NULL) << "Should return NULL when no slot range data present";

    // Cleanup
    for (RedisModuleString *rms : argv_vec) {
        RedisModule_FreeString(NULL, rms);
    }
    for (RedisModuleString *rms : argv_no_slots) {
        RedisModule_FreeString(NULL, rms);
    }

    MRCommand_Free(&cmd);
    MRCommand_Free(&cmd_no_slots);
}
