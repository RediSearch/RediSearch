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
    node->vn.vq->knn.shardWindowRatio = DEFAULT_SHARD_WINDOW_RATIO;
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
// kTokenInQuery: the K token as it appears in query string ("50" for literal, "$k_costume" for parameter)
// originalK, effectiveK: K values to test with
// testContext: descriptive string for error messages
static void runModifyKNNTest(const char** args, int argCount,
                            const char* kTokenInQuery,
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

    QueryNode *node = createTestVectorNode();

    // set original K in VectorQuery
    node->vn.vq->knn.k = originalK;

    // Find k token position in query string
    const char *query = args[2];
    const char *k_pos = strstr(query, kTokenInQuery);
    node->vn.vq->knn.k_token_pos = k_pos - query;
    node->vn.vq->knn.k_token_len = strlen(kTokenInQuery);

    // Test modifyKNNCommand with provided K values
    modifyKNNCommand(&cmd, 2, effectiveK, node->vn.vq);

    // Verify command modifications using dynamic validation
    char expectedK_str[32];
    char msg[256];
    size_t expectedK_str_len = snprintf(expectedK_str, sizeof(expectedK_str), "%zu", effectiveK);

    for (int i = 0; i < cmd.num; i++) {
        if (i == 2) { // query string
            char expectedQuery[128];
            mu_check(sizeof(expectedQuery) >= strlen(query) + 1);
            if (node->vn.vq->knn.k_token_len >= expectedK_str_len) {
                // Copy query
                memcpy(expectedQuery, query, strlen(query));
                expectedQuery[strlen(query)] = '\0';  // Null terminate
                // Set new k
                memcpy(expectedQuery + node->vn.vq->knn.k_token_pos, expectedK_str, expectedK_str_len);
                // Pad remaining space with spaces (no memmove needed)
                memset(expectedQuery + node->vn.vq->knn.k_token_pos + expectedK_str_len, ' ', node->vn.vq->knn.k_token_len - expectedK_str_len);
            } else { // we need to reallocate the query
                snprintf(expectedQuery, sizeof(expectedQuery), "*=>[KNN %zu @v $vec]", effectiveK);
            }
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
    QueryError status = QueryError_Default();

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
                     "50", 50, 30, "literal K in FT.SEARCH");
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
                     "50", 50, 30, "literal K in FT.AGGREGATE");
}

// Test modifyKNNCommand with parameter K in FT.SEARCH
void testModifyParameterKInSearch() {
    const char *searchArgs[] = {
        "FT.SEARCH",                                // Command name
        "idx",                                      // Index name
        "*=>[KNN $k_costume @v $vec]",                      // Query with parameter K=$k
        "PARAMS", "4", "k_costume", "50", "vec", "binary_vector_data"  // PARAMS with k=50
    };

    // Test parameter K modification: 50 -> 30
    runModifyKNNTest(searchArgs, sizeof(searchArgs) / sizeof(searchArgs[0]),
                     "$k_costume", 50, 30, "parameter K in FT.SEARCH");
}

// Test modifyKNNCommand with parameter K in FT.AGGREGATE
// This test also covers re-allocation of the query because strlen("$k") < strlen("300")
void testModifyParameterKInAggregate() {
    const char *searchArgs[] = {
        "FT.AGGREGATE",                                // Command name
        "idx",                                      // Index name
        "*=>[KNN $k @v $vec]",                      // Query with parameter K=$k
        "PARAMS", "4", "k", "500", "vec", "binary_vector_data"  // PARAMS with k=500
    };

    // Test parameter K modification: 50 -> 30
    runModifyKNNTest(searchArgs, sizeof(searchArgs) / sizeof(searchArgs[0]),
                     "$k", 500, 300, "parameter K in FT.AGGREGATE");
}

// Test error message validation
void testErrorMessages() {
    QueryNode* node = createTestVectorNode();
    QueryError status = QueryError_Default();

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
    QueryError status = QueryError_Default();

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
    QueryError status = QueryError_Default();

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

// Test calculateEffectiveK function with various scenarios
MU_TEST(test_calculateEffectiveK) {
    // Test case 1: k = 0 - should return 0 regardless of ratio and numShards
    size_t k = 0;
    double ratio = 0.5;
    size_t numShards = 4;
    size_t result = calculateEffectiveK(k, ratio, numShards);
    mu_assert_int_eq(0, result);

    // Test case 2: k * ratio < k / numShards - should use k / numShards
    k = 100;
    ratio = 0.1;
    numShards = 4;
    size_t expected = k / numShards;  // 100/4 = 25
    // k * ratio = 100 * 0.1 = 10, k / numShards = 25, so 10 < 25
    result = calculateEffectiveK(k, ratio, numShards);
    mu_assert_int_eq(expected, result);

    // Test case 3: k * ratio > k / numShards - should use ceil(k * ratio)
    k = 100;
    ratio = 0.8;
    numShards = 10;
    expected = (size_t)ceil(k * ratio);  // ceil(100 * 0.8) = ceil(80) = 80
    // k * ratio = 80, k / numShards = 10, so 80 > 10
    result = calculateEffectiveK(k, ratio, numShards);
    mu_assert_int_eq(expected, result);

    // Test case 4: Test rounding behavior - ceil should be used, not floor
    k = 7;
    ratio = 0.2;
    numShards = 10;  // k/numShards = 0.7, k*ratio = 1.4, so 1.4 > 0.7
    expected = (size_t)ceil(k * ratio);  // ceil(7 * 0.2) = ceil(1.4) = 2
    result = calculateEffectiveK(k, ratio, numShards);
    mu_assert_int_eq(expected, result);

    // Test case 5: ratio = 1 - should return original k (no optimization)
    k = 50;
    ratio = 1.0;
    numShards = 4;
    expected = k;  // When ratio = 1, effective K should equal original K
    result = calculateEffectiveK(k, ratio, numShards);
    mu_assert_int_eq(expected, result);
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
    MU_RUN_TEST(test_calculateEffectiveK);
    MU_REPORT();

    return minunit_status;
}
