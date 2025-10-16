#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "hybrid/hybrid_request.h"
#include "rmr/command.h"
#include "dist_plan.h"
#include "index_utils.h"
#include "common.h"

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
    void testCommandTransformationWithoutIndexSpec(const std::vector<const char*>& inputArgs) {
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

        // Verify all other original args are preserved (except first). Attention: This is not true if TIMEOUT is not at the end before DIALECT
        for (size_t i = 1; i < inputArgs.size(); i++) {
            EXPECT_STREQ(xcmd.strs[i], inputArgs[i]) << "Argument at index " << i << " should be preserved";
        }

        // Verify WITHCURSOR, WITHSCORES, _NUM_SSTRING, _INDEX_PREFIXES, and prefix count are added at the end
        EXPECT_STREQ(xcmd.strs[xcmd.num - 3], "WITHCURSOR") << "WITHCURSOR should be fifth to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 2], "WITHSCORES") << "WITHSCORES should be fourth to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 1], "_NUM_SSTRING") << "_NUM_SSTRING should be third to last";

        MRCommand_Free(&xcmd);
    }

    // Helper function to test command transformation
    void testCommandTransformationWithIndexSpec(const std::vector<const char*>& inputArgs) {
      // Convert vector to array for ArgvList constructor
      std::vector<const char*> argsWithNull = inputArgs;
      argsWithNull.push_back(nullptr);  // ArgvList expects null-terminated

      // Create ArgvList from input
      RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());
      RefManager *ism = createSpec(ctx, {"prefix1", "prefix2"});

      // Get the IndexSpec from the RefManager
      IndexSpec *sp = get_spec(ism);
      ASSERT_NE(sp, nullptr) << "IndexSpec should be accessible from RefManager";
      ASSERT_NE(sp->rule, nullptr) << "IndexSpec should have a rule";
      ASSERT_NE(sp->rule->prefixes, nullptr) << "IndexSpec rule should have prefixes";
      ASSERT_EQ(array_len(sp->rule->prefixes), 2) << "IndexSpec rule should have 2 prefixes";

      // Build MR command
      MRCommand xcmd;
      HybridRequest_buildMRCommand(args, args.size(), &xcmd, NULL, sp, &hybridParams);
      // Verify transformation: FT.HYBRID -> _FT.HYBRID
      EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");
        // Verify all other original args are preserved (except first). Attention: This is not true if TIMEOUT is not at the end before DIALECT
      for (size_t i = 1; i < inputArgs.size(); i++) {
          EXPECT_STREQ(xcmd.strs[i], inputArgs[i]) << "Argument at index " << i << " should be preserved";
      }
      // Verify WITHCURSOR, WITHSCORES, _NUM_SSTRING, _INDEX_PREFIXES, and prefix count are added at the end
      EXPECT_STREQ(xcmd.strs[xcmd.num - 7], "WITHCURSOR") << "WITHCURSOR should be seventh to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 6], "WITHSCORES") << "WITHSCORES should be sixth to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 5], "_NUM_SSTRING") << "_NUM_SSTRING should be fifth to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 4], "_INDEX_PREFIXES") << "_INDEX_PREFIXES should be fourth to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 3], "2") << "Prefix count should be third to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 2], "prefix1") << "First prefix should be second to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 1], "prefix2") << "Second prefix should be last";

      // Clean up
      MRCommand_Free(&xcmd);
      freeSpec(ism);
  }
};

// Test basic command transformation
TEST_F(HybridBuildMRCommandTest, testBasicCommandTransformation) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", TEST_BLOB_DATA
    });
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", TEST_BLOB_DATA
    });
}

// Test command with PARAMS
TEST_F(HybridBuildMRCommandTest, testCommandWithParams) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA
    });
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA
    });
}

// Test command with TIMEOUT
TEST_F(HybridBuildMRCommandTest, testCommandWithTimeout) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "TIMEOUT", "5000"
    });
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "TIMEOUT", "5000"
    });
}

// Test command with DIALECT
TEST_F(HybridBuildMRCommandTest, testCommandWithDialect) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "DIALECT", "2"
    });
}

// Test command with DIALECT
TEST_F(HybridBuildMRCommandTest, testCommandWithCombine) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA, "FILTER", "@tag:{invalid_tag}",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "DIALECT", "2"
    });
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA, "FILTER", "@tag:{invalid_tag}",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "DIALECT", "2"
    });
}

// Test complex command with all optional parameters
TEST_F(HybridBuildMRCommandTest, testComplexCommandWithAllParams) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000", "DIALECT", "2"
    });
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000", "DIALECT", "2"
    });
}

// Test complex command with all optional parameters
TEST_F(HybridBuildMRCommandTest, testComplexCommandParamsAfterTimeout) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000",
        "DIALECT", "2"
    });
    testCommandTransformationWithIndexSpec({
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
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "idx", "SEARCH", "test", "VSIM", "@vec", "data"
    });
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "idx", "SEARCH", "test", "VSIM", "@vec", "data"
    });
}
