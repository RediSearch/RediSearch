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

#include "dist_hybrid.h"
#include "hybrid/hybrid_scoring.h"

#include <string>
#include <vector>

#define TEST_BLOB_DATA "AQIDBAUGBwgJCg=="

// Format a double exactly as MRCommand_appendCombine does for the wire
// (round-trip-safe "%.17g"). Tests use this so expectations can be written
// against readable source values - fmtWireDouble(0.7) - instead of pinning the
// expanded form (0.69999999999999996) by hand.
static std::string fmtWireDouble(double v) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%.17g", v);
  return std::string(buf);
}

// Rebuild the exact tokens the coordinator is expected to emit for a
// reconstructed COMBINE clause. Mirrors MRCommand_appendCombine so the wire
// format is pinned byte-for-byte (including the "%.17g" round-trip double
// formatting via fmtWireDouble).
static std::vector<std::string> expectedCombineTokens(const HybridCombineWireParams &cp) {
  std::vector<std::string> t;
  if (!cp.scoringCtx) {
    return t;
  }
  const HybridScoringContext *sc = cp.scoringCtx;
  const bool hasAlias = cp.scoreAlias != nullptr;
  t.emplace_back("COMBINE");
  if (sc->scoringType == HYBRID_SCORING_RRF) {
    t.emplace_back("RRF");
    t.push_back(std::to_string(hasAlias ? 6 : 4));
    t.emplace_back("CONSTANT");
    t.push_back(fmtWireDouble(sc->rrfCtx.constant));
    t.emplace_back("WINDOW");
    t.push_back(std::to_string(sc->rrfCtx.window));
  } else {
    t.emplace_back("LINEAR");
    t.push_back(std::to_string(hasAlias ? 8 : 6));
    t.emplace_back("ALPHA");
    t.push_back(fmtWireDouble(sc->linearCtx.linearWeights[0]));
    t.emplace_back("BETA");
    t.push_back(fmtWireDouble(sc->linearCtx.linearWeights[1]));
    t.emplace_back("WINDOW");
    t.push_back(std::to_string(sc->linearCtx.window));
  }
  if (hasAlias) {
    t.emplace_back("YIELD_SCORE_AS");
    t.emplace_back(cp.scoreAlias);
  }
  return t;
}

// Fill a caller-owned HybridScoringContext for a LINEAR wire clause. Both `sc`
// and the 2-element `weights` array must outlive the returned params.
static HybridCombineWireParams linearWireParams(HybridScoringContext *sc, double *weights,
                                                double alpha, double beta, size_t window,
                                                const char *alias = nullptr) {
  weights[0] = alpha;
  weights[1] = beta;
  sc->scoringType = HYBRID_SCORING_LINEAR;
  sc->linearCtx.linearWeights = weights;
  sc->linearCtx.numWeights = 2;
  sc->linearCtx.window = window;
  return HybridCombineWireParams{sc, alias};
}

// Assert every input arg is preserved by position in the output. The
// coordinator always inserts one reconstructed COMBINE clause after the VSIM
// block; the oracle spots it in the output (COMBINE followed by a scoring
// method - a PARAMS-value COMBINE never is) and, if the client supplied its own
// COMBINE clause, drops that clause from the input side since the reconstructed
// form replaces it. Returns the output index just past the matched prefix.
static int verifyArgsPreservedWithReconstructedCombine(
    const MRCommand *xcmd, const std::vector<const char *> &inputArgs,
    const HybridCombineWireParams *cp) {
  const bool expectReconstructed = cp && cp->scoringCtx;
  bool reconstructedMatched = false;
  int oi = 1;  // strs[0] is _FT.HYBRID, checked separately
  for (size_t ii = 1; ii < inputArgs.size();) {
    const bool outStartsReconstructed =
        expectReconstructed && !reconstructedMatched && oi + 1 < xcmd->num &&
        xcmd->lens[oi] == strlen("COMBINE") &&
        strncasecmp(xcmd->strs[oi], "COMBINE", xcmd->lens[oi]) == 0 &&
        (!strcasecmp(xcmd->strs[oi + 1], "RRF") || !strcasecmp(xcmd->strs[oi + 1], "LINEAR"));
    if (outStartsReconstructed) {
      for (const auto &tok : expectedCombineTokens(*cp)) {
        EXPECT_STREQ(xcmd->strs[oi], tok.c_str())
            << "Reconstructed COMBINE token mismatch at output index " << oi;
        oi++;
      }
      reconstructedMatched = true;
      // If the client supplied its own COMBINE clause at this position, drop it
      // from the input stream: COMBINE + method + count + N args.
      if (strcasecmp(inputArgs[ii], "COMBINE") == 0) {
        ii += 2;                          // COMBINE, method
        long n = atol(inputArgs[ii]);     // count
        ii += 1 + (size_t)n;              // count token + its args
        if (ii < inputArgs.size() && strcasecmp(inputArgs[ii], "YIELD_SCORE_AS") == 0) {
          ii += 2;                        // positional YIELD_SCORE_AS <alias>
        }
      }
      continue;
    }
    EXPECT_STREQ(xcmd->strs[oi], inputArgs[ii])
        << "Argument at input index " << ii << " should be preserved";
    oi++;
    ii++;
  }
  return oi;
}

class HybridBuildMRCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        ctx = RedisModule_GetThreadSafeContext(NULL);

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
    IndexSpec *testIndexSpec = nullptr;

    // Helper function to validate VectorQuery from AREQ
    // Returns the VectorQuery pointer if validation passes, nullptr otherwise
    VectorQuery* validateVectorQuery(AREQ *vectorReq, size_t expectedK, double expectedShardWindowRatio) {
        EXPECT_NE(vectorReq->ast.root, nullptr) << "Vector AST root should not be NULL";
        if (!vectorReq->ast.root) return nullptr;

        EXPECT_EQ(vectorReq->ast.root->type, QN_VECTOR) << "Vector AST root should be QN_VECTOR";
        if (vectorReq->ast.root->type != QN_VECTOR) return nullptr;

        VectorQuery *vq = vectorReq->ast.root->vn.vq;
        EXPECT_NE(vq, nullptr) << "VectorQuery should not be NULL";
        if (!vq) return nullptr;

        EXPECT_EQ(vq->type, VECSIM_QT_KNN);
        EXPECT_EQ(vq->knn.k, expectedK);
        EXPECT_DOUBLE_EQ(vq->knn.shardWindowRatio, expectedShardWindowRatio);

        return vq;
    }

    // Helper function to test SHARD_K_RATIO command transformation
    // Uses stack-allocated variables following the pattern in hybrid_debug.c
    void testShardKRatioTransformation(const std::vector<const char*>& inputArgs,
                                       size_t numShards,
                                       size_t expectedK,
                                       double expectedRatio,
                                       long long expectedEffectiveK,
                                       bool passNullVectorQuery = false) {
        // Access the global NumShards variable for testing
        extern size_t NumShards;

        // Save and set NumShards
        size_t originalNumShards = NumShards;
        NumShards = numShards;

        // Set up args
        std::vector<const char*> argsWithNull = inputArgs;
        argsWithNull.push_back(nullptr);
        RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

        // Create search context and hybrid request
        RedisSearchCtx *sctx = NewSearchCtxC(ctx, "test_idx", true);
        ASSERT_NE(sctx, nullptr) << "Failed to create search context";

        HybridRequest *hreq = MakeDefaultHybridRequest(sctx);
        ASSERT_NE(hreq, nullptr) << "Failed to create hybrid request";

        // Stack-allocated variables (following hybrid_debug.c pattern)
        HybridPipelineParams hybridParams = {};
        ParseHybridCommandCtx cmd = {};
        cmd.search = hreq->requests[0];
        cmd.vector = hreq->requests[1];
        cmd.tailPlan = &hreq->tailPipeline->ap;
        cmd.hybridParams = &hybridParams;
        cmd.reqConfig = &hreq->reqConfig;
        cmd.cursorConfig = &hreq->cursorConfig;
        cmd.coordDispatchTime = &hreq->profileClocks.coordDispatchTime;

        ArgsCursor ac = {};
        HybridRequest_InitArgsCursor(hreq, &ac, args, args.size());

        QueryError status = QueryError_Default();
        if (int rc = parseHybridCommand(ctx, &ac, sctx, &cmd, &status, false, EXEC_NO_FLAGS); rc != REDISMODULE_OK) {
            if (hybridParams.scoringCtx) {
                HybridScoringContext_Free(hybridParams.scoringCtx);
            }
            HybridRequest_DecrRef(hreq);
            NumShards = originalNumShards;
            FAIL() << "Failed to parse hybrid command";
        }

        // Validate VectorQuery
        const VectorQuery *vq = validateVectorQuery(cmd.vector, expectedK, expectedRatio);
        ASSERT_NE(vq, nullptr) << "VectorQuery validation failed";

        // Borrow the resolved scoring context (as the coordinator does before
        // the merger takes ownership) so the reconstructed COMBINE clause is
        // forwarded to the shard.
        HybridCombineWireParams combineParams{hybridParams.scoringCtx,
                                              hybridParams.aggregationParams.common.scoreAlias};

        // Build MR command.
        MRCommand xcmd;
        HybridRequest_buildMRCommand(args, args.size(), EXEC_NO_FLAGS,
                                     &combineParams, &xcmd, nullptr, testIndexSpec,
                                     passNullVectorQuery ? nullptr : vq, numShards);

        // Verify the command was built correctly
        EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");

        // Verify K value in output command
        long long kValue;
        int kIndex = findKValue(&xcmd, &kValue);
        EXPECT_NE(kIndex, -1) << "K keyword should be present in output command";
        EXPECT_EQ(kValue, expectedEffectiveK) << "K value mismatch";

        // Cleanup (following hybrid_debug.c pattern)
        MRCommand_Free(&xcmd);
        if (hybridParams.scoringCtx) {
            HybridScoringContext_Free(hybridParams.scoringCtx);
        }
        HybridRequest_DecrRef(hreq);
        NumShards = originalNumShards;
    }

    // Parse a full FT.HYBRID command, build the per-shard MR command as the
    // coordinator would (capturing resolved scoring params), and return the
    // reconstructed COMBINE clause tokens found in the output (empty if none).
    // The clause is self-delimiting: COMBINE <method> <count> <count args...>.
    std::vector<std::string> buildAndExtractCombineClause(const std::vector<const char*>& inputArgs) {
        std::vector<std::string> out;
        std::vector<const char*> argsWithNull = inputArgs;
        argsWithNull.push_back(nullptr);
        RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

        RedisSearchCtx *sctx = NewSearchCtxC(ctx, "test_idx", true);
        EXPECT_NE(sctx, nullptr);
        if (!sctx) return out;
        HybridRequest *hreq = MakeDefaultHybridRequest(sctx);

        HybridPipelineParams hybridParams = {};
        ParseHybridCommandCtx cmd = {};
        cmd.search = hreq->requests[0];
        cmd.vector = hreq->requests[1];
        cmd.tailPlan = &hreq->tailPipeline->ap;
        cmd.hybridParams = &hybridParams;
        cmd.reqConfig = &hreq->reqConfig;
        cmd.cursorConfig = &hreq->cursorConfig;
        cmd.coordDispatchTime = &hreq->profileClocks.coordDispatchTime;

        ArgsCursor ac = {};
        HybridRequest_InitArgsCursor(hreq, &ac, args, args.size());
        QueryError status = QueryError_Default();
        int rc = parseHybridCommand(ctx, &ac, sctx, &cmd, &status, false, EXEC_NO_FLAGS);
        EXPECT_EQ(rc, REDISMODULE_OK) << QueryError_GetDisplayableError(&status, false);
        if (rc != REDISMODULE_OK) {
            if (hybridParams.scoringCtx) HybridScoringContext_Free(hybridParams.scoringCtx);
            HybridRequest_DecrRef(hreq);
            return out;
        }

        HybridCombineWireParams cp{hybridParams.scoringCtx,
                                   hybridParams.aggregationParams.common.scoreAlias};

        MRCommand xcmd;
        extern size_t NumShards;
        HybridRequest_buildMRCommand(args, args.size(), EXEC_NO_FLAGS,
                                     &cp, &xcmd, nullptr, testIndexSpec, nullptr,
                                     NumShards);

        // Assert the parse-driven path also preserves every non-COMBINE arg by
        // position (this is what verifies the PARAMS block - including any
        // COMBINE token used as a param value - is forwarded verbatim) and
        // reconstructs the COMBINE clause against the computed oracle. The caller
        // additionally pins the extracted clause against a literal vector, so the
        // two oracles stay independent.
        EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");
        verifyArgsPreservedWithReconstructedCombine(&xcmd, inputArgs, &cp);

        for (int i = 0; i < xcmd.num; i++) {
            if (xcmd.lens[i] == strlen("COMBINE") &&
                strncasecmp(xcmd.strs[i], "COMBINE", xcmd.lens[i]) == 0) {
                int count = (i + 2 < xcmd.num) ? atoi(xcmd.strs[i + 2]) : 0;
                int end = i + 3 + count;
                for (int j = i; j < end && j < xcmd.num; j++) {
                    out.emplace_back(xcmd.strs[j], xcmd.lens[j]);
                }
                break;
            }
        }

        MRCommand_Free(&xcmd);
        HybridScoringContext_Free(hybridParams.scoringCtx);
        HybridRequest_DecrRef(hreq);
        return out;
    }

    // Helper function to find K value in MRCommand
    // Returns the index of K keyword, or -1 if not found
    // If found, kValue will contain the K value as long long
    int findKValue(const MRCommand *cmd, long long *kValue) {
        for (int i = 0; i < cmd->num; i++) {
            bool kFound = (cmd->lens[i] == 1 && strncasecmp(cmd->strs[i], "K", 1) == 0);
            if (kFound && i + 1 < cmd->num) {
                if (kValue) {
                    *kValue = atoll(cmd->strs[i + 1]);
                }
                return i;
            }
        }
        return -1;
    }

    // Helper function to test command transformation
    void testCommandTransformationWithoutIndexSpec(const std::vector<const char*>& inputArgs,
                                                   const HybridCombineWireParams *combineParams = nullptr) {
        // Access the global NumShards variable
        extern size_t NumShards;

        // Convert vector to array for ArgvList constructor
        std::vector<const char*> argsWithNull = inputArgs;
        argsWithNull.push_back(nullptr);  // ArgvList expects null-terminated

        // Create ArgvList from input
        RMCK::ArgvList args(ctx, argsWithNull.data(), inputArgs.size());

        // Build MR command (pass NULL for VectorQuery - not testing
        // SHARD_K_RATIO here)
        MRCommand xcmd;
        HybridRequest_buildMRCommand(args, args.size(), EXEC_NO_FLAGS,
                                     combineParams, &xcmd, nullptr, nullptr, nullptr,
                                     NumShards);

        // Verify transformation: FT.HYBRID -> _FT.HYBRID
        EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");

        // Verify original args are preserved, with the COMBINE clause replaced by
        // its reconstructed (old-shard-compatible) form.
        verifyArgsPreservedWithReconstructedCombine(&xcmd, inputArgs, combineParams);

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
    void testCommandTransformationWithIndexSpec(const std::vector<const char*>& inputArgs,
                                                const HybridCombineWireParams *combineParams = nullptr) {
      // Access the global NumShards variable
      extern size_t NumShards;

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
      HybridRequest_buildMRCommand(args, args.size(), EXEC_NO_FLAGS,
                                   combineParams, &xcmd, nullptr, sp, nullptr, NumShards);
      // Verify transformation: FT.HYBRID -> _FT.HYBRID
      EXPECT_STREQ(xcmd.strs[0], "_FT.HYBRID");
      // Verify original args are preserved, with the COMBINE clause replaced by
      // its reconstructed (old-shard-compatible) form.
      verifyArgsPreservedWithReconstructedCombine(&xcmd, inputArgs, combineParams);
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
    HybridScoringContext sc = {};
    double w[2];
    HybridCombineWireParams cp = linearWireParams(&sc, w, 0.7, 0.3, HYBRID_DEFAULT_WINDOW);
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA, "FILTER", "@tag:{invalid_tag}",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "DIALECT", "2"
    }, &cp);
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA, "FILTER", "@tag:{invalid_tag}",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "DIALECT", "2"
    }, &cp);
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
    HybridScoringContext sc = {};
    double w[2];
    HybridCombineWireParams cp = linearWireParams(&sc, w, 0.7, 0.3, HYBRID_DEFAULT_WINDOW);
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", TEST_BLOB_DATA,
        "FILTER", "5", "@tag:{test}", "POLICY", "BATCHES", "BATCH_SIZE", "100",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3"
    }, &cp);
}

// Test complex command with all optional parameters
TEST_F(HybridBuildMRCommandTest, testComplexCommandWithAllParams) {
    HybridScoringContext sc = {};
    double w[2];
    HybridCombineWireParams cp = linearWireParams(&sc, w, 0.7, 0.3, HYBRID_DEFAULT_WINDOW);
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000", "DIALECT", "2"
    }, &cp);
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000", "DIALECT", "2"
    }, &cp);
}

// Test complex command with all optional parameters
TEST_F(HybridBuildMRCommandTest, testComplexCommandParamsAfterTimeout) {
    HybridScoringContext sc = {};
    double w[2];
    HybridCombineWireParams cp = linearWireParams(&sc, w, 0.7, 0.3, HYBRID_DEFAULT_WINDOW);
    testCommandTransformationWithoutIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000",
        "DIALECT", "2"
    }, &cp);
    testCommandTransformationWithIndexSpec({
        "FT.HYBRID", "test_idx", "SEARCH", "@title:($param1)",
        "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
        "PARAMS", "4", "param1", "hello", "BLOB", TEST_BLOB_DATA,
        "TIMEOUT", "3000",
        "DIALECT", "2"
    }, &cp);
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
// With 4 shards, K=100, ratio=0.5:
// effectiveK = max(100/4, ceil(100*0.5)) = max(25, 50) = 50
TEST_F(HybridBuildMRCommandTest, testShardKRatioModifiesK) {
    testShardKRatioTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "4", "K", "100", "SHARD_K_RATIO", "0.5",
        "COMBINE", "RRF", "2", "WINDOW", "100",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    }, /*numShards=*/4, /*expectedK=*/100, /*expectedRatio=*/0.5,
    /*expectedEffectiveK=*/50);
}

// Test SHARD_K_RATIO with small ratio where min guarantee kicks in
// With 4 shards, K=100, ratio=0.1:
// effectiveK = max(100/4, ceil(100*0.1)) = max(25, 10) = 25
TEST_F(HybridBuildMRCommandTest, testShardKRatioMinGuarantee) {
    testShardKRatioTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "4", "K", "100", "SHARD_K_RATIO", "0.1",
        "COMBINE", "RRF", "2", "WINDOW", "100",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    }, /*numShards=*/4, /*expectedK=*/100, /*expectedRatio=*/0.1,
    /*expectedEffectiveK=*/25);
}

// Test SHARD_K_RATIO with ratio = 1.0 (no modification)
// K value should remain 50 since ratio = 1.0 means no modification
TEST_F(HybridBuildMRCommandTest, testShardKRatioNoModificationWhenRatioIsOne) {
    testShardKRatioTransformation({
        "FT.HYBRID", "test_idx", "SEARCH", "hello",
        "VSIM", "@vector_field", "$BLOB",
        "KNN", "4", "K", "50", "SHARD_K_RATIO", "1.0",
        "COMBINE", "RRF", "2", "WINDOW", "50",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    }, /*numShards=*/4, /*expectedK=*/50, /*expectedRatio=*/1.0,
    /*expectedEffectiveK=*/50);
}

// ============================================================================
// Old-shard-compatible COMBINE reconstruction
//
// The coordinator must never forward the positional YIELD_SCORE_AS form or a
// zero argument count to shards (old shards reject both). It reconstructs the
// clause from the resolved scoring params into the legacy counted form with an
// explicit, positive, even argument count.
// ============================================================================

// COMBINE RRF 0 -> explicit defaults with a positive even count.
TEST_F(HybridBuildMRCommandTest, testCombineRRFZeroCountReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "RRF", "0",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {"COMBINE", "RRF", "4", "CONSTANT", "60", "WINDOW", "20"};
    EXPECT_EQ(clause, expected);
}

// COMBINE RRF 0 YIELD_SCORE_AS s (positional alias, zero count) ->
// counted form with the alias folded into the count (6).
TEST_F(HybridBuildMRCommandTest, testCombineRRFZeroCountPositionalAliasReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "RRF", "0", "YIELD_SCORE_AS", "s",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {"COMBINE", "RRF", "6", "CONSTANT", "60",
                                         "WINDOW", "20", "YIELD_SCORE_AS", "s"};
    EXPECT_EQ(clause, expected);
}

// COMBINE RRF 2 YIELD_SCORE_AS s (counted alias) produces the same
// wire form as the positional variant above.
TEST_F(HybridBuildMRCommandTest, testCombineRRFCountedAliasReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "s",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {"COMBINE", "RRF", "6", "CONSTANT", "60",
                                         "WINDOW", "20", "YIELD_SCORE_AS", "s"};
    EXPECT_EQ(clause, expected);
}

// COMBINE RRF 4 CONSTANT 60 YIELD_SCORE_AS s (counted alias) produces the same
// wire form as the positional variant above.
TEST_F(HybridBuildMRCommandTest, testCombineRRFCountedAliasAndConstantReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "RRF", "4", "CONSTANT", "60.5", "YIELD_SCORE_AS", "s",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "RRF", "6", "CONSTANT", "60.5", "WINDOW", "20",
        "YIELD_SCORE_AS", "s"};
    EXPECT_EQ(clause, expected);
}

// COMBINE LINEAR 0 -> explicit default weights with a positive even count.
TEST_F(HybridBuildMRCommandTest, testCombineLinearZeroCountReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "0",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "LINEAR", "6", "ALPHA", fmtWireDouble(0.3), "BETA", fmtWireDouble(0.7), "WINDOW", "20"};
    EXPECT_EQ(clause, expected);
}

// COMBINE LINEAR 2 YIELD_SCORE_AS s (counted alias) produces the same
// wire form as the positional variant above.
TEST_F(HybridBuildMRCommandTest, testCombineLinearCountedAliasReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "2", "YIELD_SCORE_AS", "s",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "LINEAR", "8", "ALPHA", fmtWireDouble(0.3), "BETA", fmtWireDouble(0.7), "WINDOW", "20",
        "YIELD_SCORE_AS", "s"};
    EXPECT_EQ(clause, expected);
}

// COMBINE LINEAR 4 YIELD_SCORE_AS s WINDOW 20 (counted alias) produces the same
// wire form as the positional variant above.
TEST_F(HybridBuildMRCommandTest, testCombineLinearCountedAliasAndWindowReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "4", "YIELD_SCORE_AS", "s", "WINDOW", "20",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "LINEAR", "8", "ALPHA", fmtWireDouble(0.3), "BETA", fmtWireDouble(0.7), "WINDOW", "20",
        "YIELD_SCORE_AS", "s"};
    EXPECT_EQ(clause, expected);
}

TEST_F(HybridBuildMRCommandTest, testCombineLinearArgsCountedAliasAndWindowReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "8", "ALPHA", "0.45", "BETA", "0.65",
            "YIELD_SCORE_AS", "s", "WINDOW", "20",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "LINEAR", "8", "ALPHA", fmtWireDouble(0.45), "BETA", fmtWireDouble(0.65),
        "WINDOW", "20", "YIELD_SCORE_AS", "s"};
    EXPECT_EQ(clause, expected);
}

// No COMBINE clause -> the coordinator still forwards its resolved defaults in
// counted form, so every shard fuses identically regardless of its own version
// defaults (rolling-upgrade consistency).
TEST_F(HybridBuildMRCommandTest, testNoCombineForwardsResolvedDefault) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {"COMBINE", "RRF", "4", "CONSTANT", "60", "WINDOW", "20"};
    EXPECT_EQ(clause, expected);
}

// COMBINE LINEAR 0 YIELD_SCORE_AS s (positional alias, zero count) ->
// counted form with the alias folded into the count (8), full defaults expanded.
TEST_F(HybridBuildMRCommandTest, testCombineLinearZeroCountPositionalAliasReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "LINEAR", "0", "YIELD_SCORE_AS", "fused_score",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "LINEAR", "8", "ALPHA", fmtWireDouble(0.3), "BETA", fmtWireDouble(0.7), "WINDOW", "20",
        "YIELD_SCORE_AS", "fused_score"};
    EXPECT_EQ(clause, expected);
}

// COMBINE RRF 4 CONSTANT 60 WINDOW 20 YIELD_SCORE_AS s (full args + positional
// alias) -> counted form with the alias folded into the count (6).
TEST_F(HybridBuildMRCommandTest, testCombineRRFFullPositionalAliasReconstructed) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "COMBINE", "RRF", "4", "CONSTANT", "60", "WINDOW", "20", "YIELD_SCORE_AS", "fused_score",
        "PARAMS", "2", "BLOB", TEST_BLOB_DATA
    });
    std::vector<std::string> expected = {
        "COMBINE", "RRF", "6", "CONSTANT", "60", "WINDOW", "20",
        "YIELD_SCORE_AS", "fused_score"};
    EXPECT_EQ(clause, expected);
}

// A COMBINE token that appears only as a PARAMS *value* (not as a top-level
// clause after VSIM) must not be mistaken for the fusion clause. The
// coordinator forwards exactly one reconstructed clause built from its resolved
// (default) scoring context; the extracted clause is that default, and the
// positional oracle inside the helper confirms the PARAMS block - including its
// literal "COMBINE" value - is preserved verbatim.
TEST_F(HybridBuildMRCommandTest, testCombineInsideParamsNotMistakenForClause) {
    auto clause = buildAndExtractCombineClause({
        "FT.HYBRID", "test_idx", "SEARCH", "hello", "VSIM", "@vector_field", "$BLOB",
        "PARAMS", "4", "BLOB", TEST_BLOB_DATA, "COMBINE", "value"
    });
    std::vector<std::string> expected = {"COMBINE", "RRF", "4", "CONSTANT", "60", "WINDOW", "20"};
    EXPECT_EQ(clause, expected);
}
