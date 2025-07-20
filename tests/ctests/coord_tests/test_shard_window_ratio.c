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

// Helper function to test a single attribute value
static void testSingleAttribute(const char* name, const char* value, int expectedResult, double expectedRatio) {
    QueryNode* node = createTestVectorNode();
    QueryError status = {0};

    QueryAttribute attr = createTestAttribute(name, value);
    int result = QueryNode_ApplyAttributes(node, &attr, 1, &status);

    mu_assert_int_eq(expectedResult, result);
    if (expectedResult) {
        mu_check(!QueryError_HasError(&status));
        mu_check(fabs(node->vn.vq->knn.shardWindowRatio - expectedRatio) < 0.0001);
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
    mu_check(fabs(node->vn.vq->knn.shardWindowRatio - 1.0) < 0.0001);

    freeTestVectorNode(node);
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
    mu_check(fabs(node->vn.vq->knn.shardWindowRatio - 1.0) < 0.0001);

    // Test that other vector attributes still work
    QueryAttribute attr1 = createTestAttribute("yield_distance_as", "dist");
    int result1 = QueryNode_ApplyAttributes(node, &attr1, 1, &status);
    mu_assert_int_eq(1, result1);
    mu_check(!QueryError_HasError(&status));
    freeTestAttribute(&attr1);

    // Test that setting other attributes doesn't affect the default ratio
    mu_check(fabs(node->vn.vq->knn.shardWindowRatio - 1.0) < 0.0001);

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
    mu_check(!QueryError_HasError(&status));
    mu_check(fabs(node->vn.vq->knn.shardWindowRatio - 0.7) < 0.0001);

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
    MU_REPORT();

    return minunit_status;
}
