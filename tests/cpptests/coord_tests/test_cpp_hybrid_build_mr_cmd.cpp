#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "hybrid/hybrid_request.h"
#include "rmr/command.h"
#include "dist_plan.h"
#include <vector>

#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="

extern "C" {
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc, MRCommand *xcmd, arrayof(char *) serialized,
                            IndexSpec *sp, HybridPipelineParams *hybridParams);
}

class HybridBuildMRCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);
        memset(&hybridParams, 0, sizeof(hybridParams));
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
        }
    }

    RedisModuleCtx *ctx = nullptr;
    HybridPipelineParams hybridParams;

    void printMRCommand(const MRCommand *cmd) {
        printf("MRCommand: ");
        for (int i = 0; i < cmd->num; i++) {
            printf("%s ", cmd->strs[i]);
        }
        printf("\n");
    }

    void printArgvList(RedisModuleString **argv, int argc) {
        printf("ArgvList: ");
        for (int i = 0; i < argc; i++) {
            size_t len;
            const char *str = RedisModule_StringPtrLen(argv[i], &len);
            printf("%.*s ", (int)len, str);
        }
        printf("\n");
    }

    // Helper function to test command transformation
    void testCommandTransformation(const std::vector<const char*>& inputArgs) {
        // Convert vector to array for ArgvList constructor
        std::vector<const char*> argsWithNull = inputArgs;
        argsWithNull.push_back(nullptr);  // ArgvList expects null-terminated

        // Create ArgvList from input
        RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

        // Build MR command
        MRCommand xcmd;
        HybridRequest_buildMRCommand(args, args.size(), &xcmd, NULL, nullptr, &hybridParams);

        // Verify transformation: FT.HYBRID -> _FT.HYBRID
        EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");

        // Verify all other original args are preserved (except first)
        for (size_t i = 1; i < inputArgs.size(); i++) {
            EXPECT_STREQ(xcmd.strs[i], inputArgs[i]) << "Argument at index " << i << " should be preserved";
        }

        // Verify WITHCURSOR and _NUM_SSTRING are added at the end
        EXPECT_STREQ(xcmd.strs[xcmd.num - 3], "WITHCURSOR") << "WITHCURSOR should be third to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 2], "WITHSCORES") << "WITHSCORES should be second to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 1], "_NUM_SSTRING") << "_NUM_SSTRING should be last";

        printArgvList(args, args.size());
        printMRCommand(&xcmd);

        MRCommand_Free(&xcmd);
    }
};

// Test basic command transformation
TEST_F(HybridBuildMRCommandTest, testBasicCommandTransformation) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", TEST_BLOB_DATA
    });
}

// Test command with PARAMS
TEST_F(HybridBuildMRCommandTest, testCommandWithParams) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA
    });
}

// Test command with TIMEOUT
TEST_F(HybridBuildMRCommandTest, testCommandWithTimeout) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "TIMEOUT", "5000"
    });
}

// Test command with DIALECT
TEST_F(HybridBuildMRCommandTest, testCommandWithDialect) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "DIALECT", "2"
    });
}

// Test command with DIALECT
TEST_F(HybridBuildMRCommandTest, testCommandWithCombine) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA, "FILTER", "@tag:{invalid_tag}",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "DIALECT", "2"
    });
}

// Test complex command with all optional parameters
TEST_F(HybridBuildMRCommandTest, testComplexCommandWithAllParams) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000", "DIALECT", "2"
    });
}

// Test complex command with all optional parameters
TEST_F(HybridBuildMRCommandTest, testComplexCommandParamsAfterTimeout) {
    testCommandTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000",
        "DIALECT", "2"
    });
}

// Test minimal command
TEST_F(HybridBuildMRCommandTest, testMinimalCommand) {
    testCommandTransformation({
        "FT.HYBRID", "idx", "SEARCH", "test", "VSIM", "@vec", "data"
    });
}
