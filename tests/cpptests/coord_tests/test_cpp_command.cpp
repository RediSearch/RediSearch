#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "rmr/command.h"
#include "slot_ranges.h"
#include "rmalloc.h"

#include <vector>
#include <string>

extern "C" {
// Function under test - declared in command.h
bool MRCommand_AddSlotRangeInfo(MRCommand *cmd, const RedisModuleSlotRangeArray *slotArray);
}

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
