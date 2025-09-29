/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "src/redisearch.h"
#include "src/spec.h"
#include "query_test_utils.h"

#include "gtest/gtest.h"

#include <algorithm>

class QueryValidationTest : public ::testing::Test {};

QAST_ValidationFlags hybridVectorFilterValidationFlags = (QAST_ValidationFlags)(QAST_NO_VECTOR | QAST_NO_WEIGHT);
QAST_ValidationFlags hybridSearchValidationFlags = QAST_NO_VECTOR;

bool isValidAsHybridVectorFilter(const char *qt, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.isValidQuery(qt, hybridVectorFilterValidationFlags);
}

#define assertValidHybridVectorFilter(qt, ctx) ASSERT_TRUE(isValidAsHybridVectorFilter(qt, ctx))

bool isValidAsHybridSearch(const char *qt, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.isValidQuery(qt, hybridSearchValidationFlags);
}

bool isInvalidHybridSearch(const char *qt, RedisSearchCtx &ctx,
  QAST_ValidationFlags validationFlags, QueryErrorCode error) {
  QASTCXX ast;
  ast.setContext(&ctx);
  ast.isValidQuery(qt, validationFlags);
  // Then check if the error message contains the expected error
  QueryErrorCode actual_err_code = ast.getErrorCode();
  // If the query is valid or the error code doesn't match, the test should fail
  if (actual_err_code != error) {
    ADD_FAILURE() << "Error code mismatch for query '" << qt
                  << "': expected " << error
                  << " but got " << actual_err_code
                  << " Error message: " << ast.getError();
    return false;
  }
  return true;
}

#define assertValidHybridSearch(qt, ctx) ASSERT_TRUE(isValidAsHybridSearch(qt, ctx))
#define assertInvalidHybridVectorFilterQuery(qt, ctx) \
  ASSERT_TRUE(isInvalidHybridSearch(qt, ctx, hybridVectorFilterValidationFlags, QUERY_EVECTOR_NOT_ALLOWED))
#define assertInvalidHybridVectorFilterWeight(qt, ctx) \
  ASSERT_TRUE(isInvalidHybridSearch(qt, ctx, hybridVectorFilterValidationFlags, QUERY_EWEIGHT_NOT_ALLOWED))
#define assertInvalidHybridSearchQuery(qt, ctx) \
  ASSERT_TRUE(isInvalidHybridSearch(qt, ctx, hybridSearchValidationFlags, QUERY_EVECTOR_NOT_ALLOWED))


TEST_F(QueryValidationTest, testInvalidVectorFilter) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING", "INDEXEMPTY",
    "v", "vector", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"};

  QueryError err = QUERY_ERROR_DEFAULT;
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Invalid queries with KNN
  assertInvalidHybridVectorFilterQuery("*=>[KNN 10 @v $BLOB]", ctx);
  assertInvalidHybridVectorFilterQuery("@title:hello =>[KNN 10 @v $BLOB]", ctx);

  // Invalid queries with range
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidHybridVectorFilterQuery("hello | @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);

  // Invalid queries with weight attribute
  assertInvalidHybridVectorFilterWeight("@title:hello => {$weight: 2.0}", ctx);
  assertInvalidHybridVectorFilterWeight("hello | @title:hello => {$weight: 2.0}", ctx);
  assertInvalidHybridVectorFilterWeight("@title:'hello' => {$weight: 2.0}", ctx);
  assertInvalidHybridVectorFilterWeight("( @title:(foo bar) @body:lol => {$weight: 2.0;} )=> {$slop:2; $inorder:true}", ctx);
  assertInvalidHybridVectorFilterWeight("( @title:(foo bar) @body:lol )=> {$weight:2.0; $inorder:true}", ctx);
  assertInvalidHybridVectorFilterWeight("(ismissing(@body))=> {$weight: 2.0}", ctx);
  assertInvalidHybridVectorFilterWeight("(@body:'')=> {$weight: 2.0}", ctx);
  assertInvalidHybridVectorFilterWeight("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => { $weight: 2.0 }", ctx);

  // // Complex queries with range
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar", ctx);
  assertInvalidHybridVectorFilterQuery("bar OR @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar @v:[VECTOR_RANGE 0.04 $BLOB2]", ctx);
  assertInvalidHybridVectorFilterQuery("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => [KNN 5 @v $BLOB2]", ctx);
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB] => [KNN 5 @v $BLOB2 AS second_score]", ctx);
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v $BLOB2 AS second_score]", ctx);
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v $BLOB2] => {$yield_distance_as:second_score;}", ctx);
  assertInvalidHybridVectorFilterQuery("@v:[VECTOR_RANGE 0.01 $BLOB] VECTOR_RANGE", ctx); // Fallback VECTOR_RANGE into a term.

  IndexSpec_RemoveFromGlobals(ref, false);
}

TEST_F(QueryValidationTest, testValidVectorFilter) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING", "INDEXEMPTY"
  };

  QueryError err = QUERY_ERROR_DEFAULT;
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Valid queries
  assertValidHybridVectorFilter("hello", ctx);
  assertValidHybridVectorFilter("@body:''", ctx);
  assertValidHybridVectorFilter("@title:hello", ctx);
  assertValidHybridVectorFilter("@title:hello world", ctx);
  assertValidHybridVectorFilter("@title:hello world -@title:world", ctx);
  assertValidHybridVectorFilter("@title:hello world -@title:world @title:hello", ctx);
  assertValidHybridVectorFilter("( @title:(foo bar) @body:lol )=> {$slop:2; $inorder:true}", ctx);
  assertValidHybridVectorFilter("", ctx);
  assertValidHybridVectorFilter("such that their", ctx);
  assertValidHybridVectorFilter("ismissing(@body)", ctx);
  assertValidHybridVectorFilter("@body:''", ctx);

  IndexSpec_RemoveFromGlobals(ref, false);
}

// Hybrid text filters accept weight attribute, but not vector queries
TEST_F(QueryValidationTest, testInvalidHybridSearch) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text",
    "v", "vector", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"};

  QueryError err = QUERY_ERROR_DEFAULT;
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetUserError(&err);

  // Invalid queries with KNN
  assertInvalidHybridSearchQuery("*=>[KNN 10 @v $BLOB]", ctx);
  assertInvalidHybridSearchQuery("(@title:hello)=>[KNN 10 @v $BLOB]", ctx);

  // Invalid queries with range
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidHybridSearchQuery("hello | @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);

  // Complex queries with range
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar", ctx);
  assertInvalidHybridSearchQuery("bar OR @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidHybridSearchQuery("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => { $weight: 2.0 }", ctx);
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar @v:[VECTOR_RANGE 0.04 $BLOB2]", ctx);
  assertInvalidHybridSearchQuery("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => [KNN 5 @v $BLOB2]", ctx);
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB] => [KNN 5 @v $BLOB2 AS second_score]", ctx);
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v $BLOB2 AS second_score]", ctx);
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v $BLOB2] => {$yield_distance_as:second_score;}", ctx);
  assertInvalidHybridSearchQuery("@v:[VECTOR_RANGE 0.01 $BLOB] VECTOR_RANGE", ctx); // Fallback VECTOR_RANGE into a term.

  IndexSpec_RemoveFromGlobals(ref, false);
}

TEST_F(QueryValidationTest, testValidHybridSearch) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING", "INDEXEMPTY"
  };

  QueryError err = QUERY_ERROR_DEFAULT;
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Valid queries
  assertValidHybridSearch("hello", ctx);
  assertValidHybridSearch("@body:''", ctx);
  assertValidHybridSearch("@title:hello", ctx);
  assertValidHybridSearch("@title:hello world", ctx);
  assertValidHybridSearch("@title:hello world -@title:world", ctx);
  assertValidHybridSearch("@title:hello world -@title:world @title:hello", ctx);
  assertValidHybridSearch("( @title:(foo bar) @body:lol )=> {$slop:2; $inorder:true}", ctx);
  assertValidHybridSearch("", ctx);
  assertValidHybridSearch("such that their", ctx);
  assertValidHybridSearch("ismissing(@body)", ctx);

  // Valid queries with weight attribute
  assertValidHybridSearch("@title:hello => {$weight: 2.0}", ctx);
  assertValidHybridSearch("hello | @title:hello => {$weight: 2.0}", ctx);
  assertValidHybridSearch("@title:'hello' => {$weight: 2.0}", ctx);
  assertValidHybridSearch("( @title:(foo bar) @body:lol => {$weight: 2.0;} )=> {$slop:2; $inorder:true}", ctx);
  assertValidHybridSearch("( @title:(foo bar) @body:lol )=> {$weight:2.0; $inorder:true}", ctx);

  IndexSpec_RemoveFromGlobals(ref, false);
}
