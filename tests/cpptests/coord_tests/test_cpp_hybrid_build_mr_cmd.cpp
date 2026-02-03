#include "gtest/gtest.h"
#include "redismock/redismock.h"
#include "redismock/util.h"
#include "hybrid/hybrid_request.h"
#include "hybrid/parse_hybrid.h"
#include "rmr/command.h"
#include "dist_plan.h"
#include "index_utils.h"
#include "common.h"
#include "profile/options.h"
#include "vector_index.h"
#include "shard_window_ratio.h"
#include "redisearch_rs/headers/query_error.h"

#include <vector>

#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="

extern "C" {
void HybridRequest_buildMRCommand(RedisModuleString **argv, int argc,
                                  ProfileOptions profileOptions,
                                  MRCommand *xcmd, arrayof(char *) serialized,
                                  IndexSpec *sp,
                                  HybridPipelineParams *hybridParams,
                                  VectorQuery *vq);

// Access the global NumShards variable for testing
extern size_t NumShards;
}

class HybridBuildMRCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);
        memset(&hybridParams, 0, sizeof(hybridParams));

        // Create index used by SHARD_K_RATIO tests
        QueryError qerr = QueryError_Default();
        RMCK::ArgvList createArgs(ctx, "FT.CREATE", "test_idx", "ON", "HASH",
                                  "SCHEMA", "title", "TEXT", "vector_field", "VECTOR", "FLAT", "6",
                                  "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE");
        testIndexSpec = IndexSpec_CreateNew(ctx, createArgs, createArgs.size(), &qerr);
        ASSERT_NE(testIndexSpec, nullptr) << "Failed to create index: " << QueryError_GetDisplayableError(&qerr, false);
    }

    void TearDown() override {
        if (ctx) {
            RedisModule_FreeThreadSafeContext(ctx);
        }
    }

    RedisModuleCtx *ctx = nullptr;
    HybridPipelineParams hybridParams;
    IndexSpec *testIndexSpec = nullptr;

    // Helper structure to hold parsed hybrid command context and resources
    struct ParsedHybridCommand {
        HybridRequest *hreq = nullptr;
        HybridPipelineParams parsedHybridParams = {0};
        ParseHybridCommandCtx cmd = {0};

        ~ParsedHybridCommand() {
            if (hreq) {
                HybridRequest_Free(hreq);
            }
            if (parsedHybridParams.scoringCtx) {
                HybridScoringContext_Free(parsedHybridParams.scoringCtx);
            }
        }
    };

    // Helper function to parse a hybrid command using the full parser
    ParsedHybridCommand* parseHybridCommandHelper(RMCK::ArgvList& args, const char* indexName) {
        RedisSearchCtx *sctx = NewSearchCtxC(ctx, indexName, true);
        if (!sctx) return nullptr;

        HybridRequest *hreq = MakeDefaultHybridRequest(sctx);
        if (!hreq) return nullptr;

        ArgsCursor ac = {0};
        HybridRequest_InitArgsCursor(hreq, &ac, args, args.size());

        ParsedHybridCommand *parsed = new ParsedHybridCommand();
        parsed->hreq = hreq;
        parsed->cmd.search = hreq->requests[0];
        parsed->cmd.vector = hreq->requests[1];
        parsed->cmd.tailPlan = &hreq->tailPipeline->ap;
        parsed->cmd.hybridParams = &parsed->parsedHybridParams;
        parsed->cmd.reqConfig = &hreq->reqConfig;
        parsed->cmd.cursorConfig = &hreq->cursorConfig;
        parsed->cmd.coordDispatchTime = &hreq->coordDispatchTime;

        QueryError status = QueryError_Default();
        int rc = parseHybridCommand(ctx, &ac, sctx, &parsed->cmd, &status, false);
        if (rc != REDISMODULE_OK) {
            delete parsed;
            return nullptr;
        }

        return parsed;
    }

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

    // Helper function to find K value in MRCommand
    // Returns the index of K keyword, or -1 if not found
    // If found, kValue will contain the K value as long long
    int findKValue(const MRCommand *cmd, long long *kValue) {
        for (int i = 0; i < cmd->num; i++) {
            if (strcasecmp(cmd->strs[i], "K") == 0 && i + 1 < cmd->num) {
                if (kValue) {
                    *kValue = atoll(cmd->strs[i + 1]);
                }
                return i;
            }
        }
        return -1;
    }

    // Helper function to test command transformation
    void testCommandTransformationWithoutIndexSpec(const std::vector<const char*>& inputArgs) {
        // Convert vector to array for ArgvList constructor
        std::vector<const char*> argsWithNull = inputArgs;
        argsWithNull.push_back(nullptr);  // ArgvList expects null-terminated

        // Create ArgvList from input
        RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

        // Build MR command (pass NULL for VectorQuery - not testing
        // SHARD_K_RATIO here)
        MRCommand xcmd;
        HybridRequest_buildMRCommand(args, args.size(), EXEC_NO_FLAGS, &xcmd,
                                     NULL, nullptr, &hybridParams, NULL);

        // Verify transformation: FT.HYBRID -> _FT.HYBRID
        EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");

        // Verify all other original args are preserved (except first). Attention: This is not true if TIMEOUT is not at the end before DIALECT
        for (size_t i = 1; i < inputArgs.size(); i++) {
            EXPECT_STREQ(xcmd.strs[i], inputArgs[i]) << "Argument at index " << i << " should be preserved";
        }

        // Verify WITHCURSOR, WITHSCORES, _NUM_SSTRING, _COORD_DISPATCH_TIME are added at the end
        // Note: _COORD_DISPATCH_TIME and its placeholder value (2 args) are added after _NUM_SSTRING
        EXPECT_STREQ(xcmd.strs[xcmd.num - 7], "WITHCURSOR") << "WITHCURSOR should be seventh to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 6], "WITHSCORES") << "WITHSCORES should be sixth to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 5], "_NUM_SSTRING") << "_NUM_SSTRING should be fifth to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 2], "_COORD_DISPATCH_TIME") << "_COORD_DISPATCH_TIME should be second to last";
        EXPECT_STREQ(xcmd.strs[xcmd.num - 1], "") << "Dispatch time placeholder should be last (empty)";

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

      // Build MR command (pass NULL for VectorQuery - not testing
      // SHARD_K_RATIO here)
      MRCommand xcmd;
      HybridRequest_buildMRCommand(args, args.size(), EXEC_NO_FLAGS, &xcmd,
                                   NULL, sp, &hybridParams, NULL);
      // Verify transformation: FT.HYBRID -> _FT.HYBRID
      EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");
        // Verify all other original args are preserved (except first). Attention: This is not true if TIMEOUT is not at the end before DIALECT
      for (size_t i = 1; i < inputArgs.size(); i++) {
          EXPECT_STREQ(xcmd.strs[i], inputArgs[i]) << "Argument at index " << i << " should be preserved";
      }
      // Verify WITHCURSOR, WITHSCORES, _NUM_SSTRING, SLOTS, _COORD_DISPATCH_TIME, _INDEX_PREFIXES, and prefixes are added at the end
      // Order: ... WITHCURSOR WITHSCORES _NUM_SSTRING _SLOTS <slots_blob> _COORD_DISPATCH_TIME <placeholder> _INDEX_PREFIXES 2 prefix1 prefix2
      EXPECT_STREQ(xcmd.strs[xcmd.num - 11], "WITHCURSOR") << "WITHCURSOR should be 11th to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 10], "WITHSCORES") << "WITHSCORES should be 10th to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 9], "_NUM_SSTRING") << "_NUM_SSTRING should be 9th to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 8], SLOTS_STR) << SLOTS_STR << " should be 8th to last";
      // slots blob is 7th to last (xcmd.num - 7)
      EXPECT_STREQ(xcmd.strs[xcmd.num - 6], "_COORD_DISPATCH_TIME") << "_COORD_DISPATCH_TIME should be 6th to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 5], "") << "Dispatch time placeholder should be 5th to last (empty)";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 4], "_INDEX_PREFIXES") << "_INDEX_PREFIXES should be 4th to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 3], "2") << "Prefix count should be 3rd to last";
      EXPECT_STREQ(xcmd.strs[xcmd.num - 2], "prefix1") << "First prefix should be 2nd to last";
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


// Test FILTER with POLICY BATCHES
TEST_F(HybridBuildMRCommandTest, testFilterWithPolicyBatches) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "FILTER", "3", "@tag:{test}", "POLICY", "BATCHES"
    });
}

// Test FILTER with BATCH_SIZE only
TEST_F(HybridBuildMRCommandTest, testFilterWithBatchSize) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "FILTER", "3","@tag:{test}", "BATCH_SIZE", "100"
    });
}

// Test FILTER with POLICY and BATCH_SIZE together
TEST_F(HybridBuildMRCommandTest, testFilterWithPolicyAndBatchSize) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "FILTER", "5", "@tag:{test}", "POLICY", "BATCHES", "BATCH_SIZE", "50"
    });
}

// Test FILTER with BATCH_SIZE and POLICY (reversed order - order independent)
TEST_F(HybridBuildMRCommandTest, testFilterWithBatchSizeAndPolicyReversed) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "FILTER", "5", "@tag:{test}", "BATCH_SIZE", "75", "POLICY", "ADHOC"
    });
}

// Test FILTER with POLICY, BATCH_SIZE and COMBINE
TEST_F(HybridBuildMRCommandTest, testFilterWithPolicyBatchSizeAndCombine) {
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "FILTER", "5", "@tag:{test}", "POLICY", "BATCHES", "BATCH_SIZE", "100",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3"
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

// Test SHARD_K_RATIO modifies K value in distributed command with multiple shards
TEST_F(HybridBuildMRCommandTest, testShardKRatioModifiesK) {
    // Save original NumShards and set to 4 shards for this test
    size_t originalNumShards = NumShards;
    NumShards = 4;

    // Input command with K=100
    // Need to set WINDOW >= K to prevent K from being capped
    std::vector<const char*> inputArgs = {
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "4", "K", "100", "SHARD_K_RATIO", "0.5",
        "COMBINE", "RRF", "2", "WINDOW", "100",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    };

    std::vector<const char*> argsWithNull = inputArgs;
    argsWithNull.push_back(nullptr);

    RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

    // Use the helper to parse the hybrid command
    ParsedHybridCommand *parsed = parseHybridCommandHelper(args, "test_idx");
    ASSERT_NE(parsed, nullptr) << "Failed to parse hybrid command";

    // Extract VectorQuery from parsed result
    ASSERT_NE(parsed->cmd.vector->ast.root, nullptr) << "Vector AST root should not be NULL";
    ASSERT_EQ(parsed->cmd.vector->ast.root->type, QN_VECTOR) << "Vector AST root should be QN_VECTOR";
    VectorQuery *vq = parsed->cmd.vector->ast.root->vn.vq;
    ASSERT_NE(vq, nullptr) << "VectorQuery should not be NULL";

    // Verify the parsed VectorQuery has the expected values
    // effectiveK = max(K/#shards, ceil(K * ratio))
    //            = max(100/4, ceil(100 * 0.5)) = max(25, 50) = 50
    EXPECT_EQ(vq->type, VECSIM_QT_KNN);
    EXPECT_EQ(vq->knn.k, 100);
    EXPECT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.5);

    MRCommand xcmd;
    HybridRequest_buildMRCommand(args, args.size(), &xcmd, NULL, testIndexSpec,
                                 &hybridParams, vq);

    // Verify the command was built correctly
    EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");

    // With 4 shards, K=100, ratio=0.5:
    // effectiveK = max(100/4, ceil(100*0.5)) = max(25, 50) = 50
    long long kValue;
    int kIndex = findKValue(&xcmd, &kValue);
    EXPECT_NE(kIndex, -1) << "K keyword should be present in output command";
    EXPECT_EQ(kValue, 50) << "K value should be modified to 50 (effectiveK)";

    MRCommand_Free(&xcmd);
    delete parsed;  // Cleanup handled by destructor
    NumShards = originalNumShards;  // Restore
}

// Test SHARD_K_RATIO with small ratio where min guarantee kicks in
TEST_F(HybridBuildMRCommandTest, testShardKRatioMinGuarantee) {
    // Save original NumShards and set to 4 shards for this test
    size_t originalNumShards = NumShards;
    NumShards = 4;

    // Need to set WINDOW >= K to prevent K from being capped
    std::vector<const char*> inputArgs = {
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "4", "K", "100", "SHARD_K_RATIO", "0.1",
        "COMBINE", "RRF", "2", "WINDOW", "100",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    };

    std::vector<const char*> argsWithNull = inputArgs;
    argsWithNull.push_back(nullptr);

    RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

    // Use the helper to parse the hybrid command
    ParsedHybridCommand *parsed = parseHybridCommandHelper(args, "test_idx");
    ASSERT_NE(parsed, nullptr) << "Failed to parse hybrid command";

    // Extract VectorQuery from parsed result
    ASSERT_NE(parsed->cmd.vector->ast.root, nullptr) << "Vector AST root should not be NULL";
    ASSERT_EQ(parsed->cmd.vector->ast.root->type, QN_VECTOR) << "Vector AST root should be QN_VECTOR";
    VectorQuery *vq = parsed->cmd.vector->ast.root->vn.vq;
    ASSERT_NE(vq, nullptr) << "VectorQuery should not be NULL";

    // Verify the parsed VectorQuery has the expected values
    // effectiveK = max(K/#shards, ceil(K * ratio))
    //            = max(100/4, ceil(100 * 0.1)) = max(25, 10) = 25
    EXPECT_EQ(vq->type, VECSIM_QT_KNN);
    EXPECT_EQ(vq->knn.k, 100);
    EXPECT_DOUBLE_EQ(vq->knn.shardWindowRatio, 0.1);

    MRCommand xcmd;
    HybridRequest_buildMRCommand(args, args.size(), &xcmd, NULL, testIndexSpec,
                                 &hybridParams, vq);

    // With 4 shards, K=100, ratio=0.1:
    // effectiveK = max(100/4, ceil(100*0.1)) = max(25, 10) = 25
    long long kValue;
    int kIndex = findKValue(&xcmd, &kValue);
    EXPECT_NE(kIndex, -1) << "K keyword should be present in output command";
    EXPECT_EQ(kValue, 25) << "K value should be 25 (min guarantee K/numShards)";

    MRCommand_Free(&xcmd);
    delete parsed;  // Cleanup handled by destructor
    NumShards = originalNumShards;  // Restore
}

// Test SHARD_K_RATIO with ratio = 1.0 (no modification)
TEST_F(HybridBuildMRCommandTest, testShardKRatioNoModificationWhenRatioIsOne) {
    // Save original NumShards and set to 4 shards for this test
    size_t originalNumShards = NumShards;
    NumShards = 4;

    // Need to set WINDOW >= K to prevent K from being capped
    std::vector<const char*> inputArgs = {
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "4", "K", "50", "SHARD_K_RATIO", "1.0",
        "COMBINE", "RRF", "2", "WINDOW", "50",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    };

    std::vector<const char*> argsWithNull = inputArgs;
    argsWithNull.push_back(nullptr);

    RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

    // Use the helper to parse the hybrid command
    ParsedHybridCommand *parsed = parseHybridCommandHelper(args, "test_idx");
    ASSERT_NE(parsed, nullptr) << "Failed to parse hybrid command";

    // Extract VectorQuery from parsed result
    ASSERT_NE(parsed->cmd.vector->ast.root, nullptr) << "Vector AST root should not be NULL";
    ASSERT_EQ(parsed->cmd.vector->ast.root->type, QN_VECTOR) << "Vector AST root should be QN_VECTOR";
    VectorQuery *vq = parsed->cmd.vector->ast.root->vn.vq;
    ASSERT_NE(vq, nullptr) << "VectorQuery should not be NULL";

    // Verify the parsed VectorQuery has the expected values
    EXPECT_EQ(vq->type, VECSIM_QT_KNN);
    EXPECT_EQ(vq->knn.k, 50);
    EXPECT_DOUBLE_EQ(vq->knn.shardWindowRatio, 1.0);

    MRCommand xcmd;
    HybridRequest_buildMRCommand(args, args.size(), &xcmd, NULL, testIndexSpec,
                                 &hybridParams, vq);

    // K value should remain 50 since ratio = 1.0 means no modification
    long long kValue;
    int kIndex = findKValue(&xcmd, &kValue);
    EXPECT_NE(kIndex, -1) << "K keyword should be present in output command";
    EXPECT_EQ(kValue, 50) << "K value should remain 50 when ratio = 1.0";

    MRCommand_Free(&xcmd);
    delete parsed;  // Cleanup handled by destructor
    NumShards = originalNumShards;  // Restore
}

// Test SHARD_K_RATIO with NULL VectorQuery (backward compatibility)
// This tests that when VectorQuery is NULL, K is not modified by SHARD_K_RATIO logic
TEST_F(HybridBuildMRCommandTest, testShardKRatioNullVectorQuery) {
    // Save original NumShards and set to 4 shards for this test
    size_t originalNumShards = NumShards;
    NumShards = 4;

    // Need to set WINDOW >= K to prevent K from being capped
    std::vector<const char*> inputArgs = {
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "2", "K", "25",
        "COMBINE", "RRF", "2", "WINDOW", "25",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    };

    std::vector<const char*> argsWithNull = inputArgs;
    argsWithNull.push_back(nullptr);

    RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

    // Use the helper to parse the hybrid command
    ParsedHybridCommand *parsed = parseHybridCommandHelper(args, "test_idx");
    ASSERT_NE(parsed, nullptr) << "Failed to parse hybrid command";

    // Extract VectorQuery from parsed result (but we'll pass NULL to buildMRCommand)
    ASSERT_NE(parsed->cmd.vector->ast.root, nullptr) << "Vector AST root should not be NULL";
    ASSERT_EQ(parsed->cmd.vector->ast.root->type, QN_VECTOR) << "Vector AST root should be QN_VECTOR";
    VectorQuery *vq = parsed->cmd.vector->ast.root->vn.vq;
    ASSERT_NE(vq, nullptr) << "VectorQuery should not be NULL";
    EXPECT_EQ(vq->knn.k, 25);

    // Pass NULL for VectorQuery to test backward compatibility - should not modify K
    MRCommand xcmd;
    HybridRequest_buildMRCommand(args, args.size(), &xcmd, NULL, testIndexSpec,
                                 &hybridParams, NULL);  // NULL VectorQuery

    // K value should remain 25 since no VectorQuery provided
    long long kValue;
    int kIndex = findKValue(&xcmd, &kValue);
    EXPECT_NE(kIndex, -1) << "K keyword should be present in output command";
    EXPECT_EQ(kValue, 25) << "K value should remain 25 when VectorQuery is NULL";

    MRCommand_Free(&xcmd);
    delete parsed;  // Cleanup handled by destructor
    NumShards = originalNumShards;  // Restore
}
