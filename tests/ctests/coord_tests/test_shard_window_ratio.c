/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "minunit.h"
#include "query.h"
#include "query_node.h"
#include "vector_index.h"
#include "rmutil/alloc.h"
#include "coord/rmr/command.h"
#include "shard_window_ratio.h"
#include <string.h>
#include <math.h>

// Test helper functions
static QueryNode* createTestVectorNode() {
    QueryNode* node = rm_calloc(1, sizeof(QueryNode));
    node->type = QN_VECTOR;
    node->vn.vq = rm_calloc(1, sizeof(VectorQuery));
    node->vn.vq->knn.shardWindowRatio = 1.0; // Initialize with DEFAULT_SHARD_WINDOW_RATIO
    node->vn.vq->params.params = NULL;
    node->vn.vq->params.needResolve = NULL;
    node->vn.vq->scoreField = NULL;
    node->opts.flags |= QueryNode_YieldsDistance; // Enable distance yielding for compatibility tests

    // Initialize params array for vector nodes (params[0] = vector, params[1] = k)
    QueryNode_InitParams(node, 2);

    return node;
}



static void freeTestVectorNode(QueryNode* node) {
    if (node) {
        QueryNode_Free(node);
    }
}

static QueryAttribute createTestAttribute(const char* name, const char* value) {
    QueryAttribute attr = {0};
    attr.name = name;
    attr.namelen = strlen(name);
    attr.value = rm_strdup(value);
    attr.vallen = strlen(value);
    return attr;
}

static void freeTestAttribute(QueryAttribute* attr) {
    if (attr->value) {
        rm_free((char*)attr->value);
        attr->value = NULL;
    }
}

// Helper function to test modifyKNNCommand with different configurations
// paramType: PARAM_NONE for literal K, PARAM_SIZE for parameter K
// paramName: parameter name for PARAM_SIZE case, NULL for PARAM_NONE
// originalK, effectiveK: K values to test with
// testContext: descriptive string for error messages
static void runModifyKNNTest(const char** args, int argCount,
                            int paramType, const char* paramName,
                            size_t originalK, size_t effectiveK,
                            const char* testContext) {
    // Create MRCommand from provided arguments
    MRCommand cmd = {0};
    cmd.num = argCount;
    cmd.strs = rm_malloc(cmd.num * sizeof(char*));
    cmd.lens = rm_malloc(cmd.num * sizeof(size_t));

    for (int i = 0; i < cmd.num; i++) {
        cmd.strs[i] = rm_strdup(args[i]);
        cmd.lens[i] = strlen(args[i]);
    }

    // Create QueryNode based on parameter type
    QueryNode *node = createTestVectorNode();
    node->params[1].type = paramType;

    if (paramType == PARAM_NONE) {
        // Literal K: find position in query string using originalK value
        const char *query = args[2];
        char originalK_str[32];
        snprintf(originalK_str, sizeof(originalK_str), "%zu", originalK);
        const char *k_pos = strstr(query, originalK_str);
        node->vn.vq->knn.k_literal_pos = k_pos - query;
        node->vn.vq->knn.k_literal_len = strlen(originalK_str);
    } else {
        // Parameter K: set parameter name (allocate memory for proper cleanup)
        node->params[1].name = rm_strdup(paramName);
    }

    knnContext knnCtx = {0};
    knnCtx.queryNode = node;

    // Test modifyKNNCommand with provided K values
    int result = modifyKNNCommand(&cmd, originalK, effectiveK, &knnCtx);

    // Verify modification was successful
    char msg[256];
    snprintf(msg, sizeof(msg), "modifyKNNCommand should succeed for %s", testContext);
    mu_assert_int_eq(0, result);

    // Verify command modifications using dynamic validation
    char expectedK_str[32];
    snprintf(expectedK_str, sizeof(expectedK_str), "%zu", effectiveK);

    for (int i = 0; i < cmd.num; i++) {
        if (paramType == PARAM_SIZE && i > 0 && !strcmp(cmd.strs[i-1], paramName)) {
            // Parameter case: value after parameter name should be modified
            snprintf(msg, sizeof(msg), "Parameter %s value should be modified for %s: expected '%zu', got '%s'",
                     paramName, testContext, effectiveK, cmd.strs[i]);
            mu_assert(!strcmp(expectedK_str, cmd.strs[i]), msg);
        } else if (paramType == PARAM_NONE && i == 2) {
            // Literal case: query string should be modified with effectiveK
            char expectedQuery[64];
            snprintf(expectedQuery, sizeof(expectedQuery), "*=>[KNN %zu @v $vec]", effectiveK);
            snprintf(msg, sizeof(msg), "Query string should be modified for %s: expected '%s', got '%s'",
                     testContext, expectedQuery, cmd.strs[i]);
            mu_assert(!strcmp(expectedQuery, cmd.strs[i]), msg);
        } else {
            // All other arguments should remain unchanged
            snprintf(msg, sizeof(msg), "Argument %d should remain unchanged for %s: expected '%s', got '%s'", i, testContext, args[i], cmd.strs[i]);
            mu_assert(!strcmp(args[i], cmd.strs[i]), msg);
        }
    }

    // Cleanup
    for (int i = 0; i < cmd.num; i++) {
        rm_free(cmd.strs[i]);
    }
    rm_free(cmd.strs);
    rm_free(cmd.lens);
    freeTestVectorNode(node);
}

// Helper function to test a single attribute value
// expectedResult: 1 for success, 0 for failure
static void testSingleAttribute(const char* name, const char* value, int expectedResult, double expectedRatio) {
    QueryNode* node = createTestVectorNode();
    QueryError status = {0};

    QueryAttribute attr = createTestAttribute(name, value);
    int result = QueryNode_ApplyAttributes(node, &attr, 1, &status);

    char msg[256];
    snprintf(msg, sizeof(msg), "Testing '%s'='%s': expected result %d, got %d",
             name, value, expectedResult, result);
    mu_assert(result == expectedResult, msg);
    if (expectedResult) {
        mu_check(!QueryError_HasError(&status));
        mu_assert_double_eq(expectedRatio, node->vn.vq->knn.shardWindowRatio);
    } else {
        mu_check(QueryError_HasError(&status));
        QueryError_ClearError(&status);
    }

    freeTestAttribute(&attr);
    freeTestVectorNode(node);
}

// Test valid and invalid shard k ratio values
void testShardKRatioValues() {
    // Test valid values
    testSingleAttribute("shard_k_ratio", "0.1", 1, 0.1);
    testSingleAttribute("shard_k_ratio", "0.5", 1, 0.5);
    testSingleAttribute("shard_k_ratio", "1.0", 1, 1.0);
    testSingleAttribute("shard_k_ratio", "0.75", 1, 0.75);
    testSingleAttribute("shard_k_ratio", "1", 1, 1.0);  // Integer format
    testSingleAttribute("shard_k_ratio", "5e-1", 1, 0.5);  // Scientific notation
    testSingleAttribute("shard_k_ratio", "0.001", 1, 0.001);  // Very small but valid

    // Test invalid values
    testSingleAttribute("shard_k_ratio", "1.5", 0, 0);   // Above maximum
    testSingleAttribute("shard_k_ratio", "-0.1", 0, 0);  // Negative
    testSingleAttribute("shard_k_ratio", "0.0", 0, 0);   // Zero (now invalid)
    testSingleAttribute("shard_k_ratio", "invalid", 0, 0);  // Non-numeric
    testSingleAttribute("shard_k_ratio", "1.00001", 0, 0);  // Just above maximum
    testSingleAttribute("shard_k_ratio", " 0.5 ", 0, 0);   // Whitespace
    testSingleAttribute("shard_k_ratio", "0.5.5", 0, 0);   // Multiple decimals
    testSingleAttribute("shard_k_ratio", "0.5abc", 0, 0);  // Mixed alphanumeric
}

// Test attribute name variations and unrecognized attributes
void testAttributeNames() {
    // Test case sensitivity
    testSingleAttribute("shard_k_ratio", "0.5", 1, 0.5);  // Lowercase
    testSingleAttribute("SHARD_K_RATIO", "0.3", 1, 0.3);  // Uppercase

    // Test unrecognized attribute names
    testSingleAttribute("unknown_attr", "0.5", 0, 0);
    testSingleAttribute("shard_ratio", "0.5", 0, 0);
}

// Test default value behavior
void testDefaultValue() {
    QueryNode* node = createTestVectorNode();

    // Verify default value is 1.0 (DEFAULT_SHARD_WINDOW_RATIO)
    mu_assert_double_eq(1.0, node->vn.vq->knn.shardWindowRatio);

    freeTestVectorNode(node);
}

// Test modifyKNNCommand with literal K in FT.SEARCH
void testModifyLiteralKInSearch() {
    const char *searchArgs[] = {
        "FT.SEARCH",                                // Command name
        "idx",                                      // Index name
        "*=>[KNN 50 @v $vec]",                      // Query with literal K=50
        "PARAMS", "2", "vec", "binary_vector_data"  // PARAMS section
    };

    // Test literal K modification: 50 -> 30
    runModifyKNNTest(searchArgs, sizeof(searchArgs) / sizeof(searchArgs[0]),
                     PARAM_NONE, NULL, 50, 30, "literal K in FT.SEARCH");
}

// Test modifyKNNCommand with literal K in FT.AGGREGATE
void testModifyLiteralKInAggregate() {
    const char *searchArgs[] = {
        "FT.AGGREGATE",                                // Command name
        "idx",                                      // Index name
        "*=>[KNN 50 @v $vec]",                      // Query with literal K=50
        "PARAMS", "2", "vec", "binary_vector_data"  // PARAMS section
    };

    // Test literal K modification: 50 -> 30
    runModifyKNNTest(searchArgs, sizeof(searchArgs) / sizeof(searchArgs[0]),
                     PARAM_NONE, NULL, 50, 30, "literal K in FT.AGGREGATE");
}

// Test modifyKNNCommand with parameter K in FT.SEARCH
void testModifyParameterKInSearch() {
    const char *searchArgs[] = {
        "FT.SEARCH",                                // Command name
        "idx",                                      // Index name
        "*=>[KNN $k @v $vec]",                      // Query with parameter K=$k
        "PARAMS", "4", "k", "50", "vec", "binary_vector_data"  // PARAMS with k=50
    };

    // Test parameter K modification: 50 -> 30
    runModifyKNNTest(searchArgs, sizeof(searchArgs) / sizeof(searchArgs[0]),
                     PARAM_SIZE, "k", 50, 30, "parameter K in FT.SEARCH");
}

// Test modifyKNNCommand with parameter K in FT.AGGREGATE
void testModifyParameterKInAggregate() {
    const char *searchArgs[] = {
        "FT.AGGREGATE",                                // Command name
        "idx",                                      // Index name
        "*=>[KNN $k @v $vec]",                      // Query with parameter K=$k
        "PARAMS", "4", "k", "50", "vec", "binary_vector_data"  // PARAMS with k=50
    };

    // Test parameter K modification: 50 -> 30
    runModifyKNNTest(searchArgs, sizeof(searchArgs) / sizeof(searchArgs[0]),
                     PARAM_SIZE, "k", 50, 30, "parameter K in FT.AGGREGATE");
}

// Test error message validation
void testErrorMessages() {
    QueryNode* node = createTestVectorNode();
    QueryError status = {0};

    // Test invalid range error message
    QueryAttribute attr1 = createTestAttribute("shard_k_ratio", "2.0");
    int result1 = QueryNode_ApplyAttributes(node, &attr1, 1, &status);
    mu_assert_int_eq(0, result1);
    mu_check(QueryError_HasError(&status));
    const char* errorMsg = QueryError_GetUserError(&status);
    mu_check(strstr(errorMsg, "greater than 0 and at most 1") != NULL);
    QueryError_ClearError(&status);
    freeTestAttribute(&attr1);

    // Test invalid format error message
    QueryAttribute attr2 = createTestAttribute("shard_k_ratio", "not_a_number");
    int result2 = QueryNode_ApplyAttributes(node, &attr2, 1, &status);
    mu_assert_int_eq(0, result2);
    mu_check(QueryError_HasError(&status));
    errorMsg = QueryError_GetUserError(&status);
    mu_check(strstr(errorMsg, "Invalid shard k ratio value") != NULL);
    QueryError_ClearError(&status);
    freeTestAttribute(&attr2);

    freeTestVectorNode(node);
}

// Test backward compatibility with existing vector queries
void testBackwardCompatibility() {
    QueryNode* node = createTestVectorNode();
    QueryError status = {0};

    // Test that existing vector queries work without shard window ratio
    mu_assert_double_eq(1.0, node->vn.vq->knn.shardWindowRatio);

    // Test that other vector attributes still work
    QueryAttribute attr1 = createTestAttribute("yield_distance_as", "dist");
    int result1 = QueryNode_ApplyAttributes(node, &attr1, 1, &status);
    mu_assert_int_eq(1, result1);
    mu_check(!QueryError_HasError(&status));
    freeTestAttribute(&attr1);

    // Test that setting other attributes doesn't affect the default ratio
    mu_assert_double_eq(1.0, node->vn.vq->knn.shardWindowRatio);

    freeTestVectorNode(node);
}

// Test multiple attributes together
void testMultipleAttributes() {
    QueryNode* node = createTestVectorNode();
    QueryError status = {0};

    // Test applying multiple attributes including shard k ratio
    QueryAttribute attrs[2];
    attrs[0] = createTestAttribute("shard_k_ratio", "0.7");
    attrs[1] = createTestAttribute("yield_distance_as", "distance");

    int result = QueryNode_ApplyAttributes(node, attrs, 2, &status);
    mu_assert_int_eq(1, result);
    mu_assert(!QueryError_HasError(&status), "Should not have error for valid attributes");
    mu_assert_double_eq(0.7, node->vn.vq->knn.shardWindowRatio);

    freeTestAttribute(&attrs[0]);
    freeTestAttribute(&attrs[1]);
    freeTestVectorNode(node);
}

// Main test runner following minunit framework pattern
int main(int argc, char **argv) {
    RMUTil_InitAlloc();
    MU_RUN_TEST(testShardKRatioValues);
    MU_RUN_TEST(testAttributeNames);
    MU_RUN_TEST(testDefaultValue);
    MU_RUN_TEST(testErrorMessages);
    MU_RUN_TEST(testBackwardCompatibility);
    MU_RUN_TEST(testMultipleAttributes);
    MU_RUN_TEST(testModifyLiteralKInSearch);
    MU_RUN_TEST(testModifyParameterKInSearch);
    MU_RUN_TEST(testModifyLiteralKInAggregate);
    MU_RUN_TEST(testModifyParameterKInAggregate);
    MU_REPORT();

    return minunit_status;
}
