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

bool isValidAsVectorFilter(const char *qt, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.isValidAsVectorFilter(qt);
}

#define assertValidVectorFilter(qt, ctx) ASSERT_TRUE(isValidAsVectorFilter(qt, ctx))
#define assertInvalidVectorFilter(qt, ctx) ASSERT_FALSE(isValidAsVectorFilter(qt, ctx))

bool isValidAsHybridSearch(const char *qt, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.isValidAsHybridSearch(qt);
}
#define assertValidHybridSearch(qt, ctx) ASSERT_TRUE(isValidAsHybridSearch(qt, ctx))
#define assertInvalidHybridSearch(qt, ctx) ASSERT_FALSE(isValidAsHybridSearch(qt, ctx))


TEST_F(QueryValidationTest, testInvalidVectorFilter) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING", "INDEXEMPTY"
    "v", "vector", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
    "v2", "vector", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"};

  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Invalid queries with KNN
  assertInvalidVectorFilter("*=>[KNN 10 @vec_field $BLOB]", ctx);
  assertInvalidVectorFilter("@title:hello =>[KNN 10 @vec_field $BLOB]", ctx);

  // Invalid queries with range
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidVectorFilter("hello | @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);

  // Invalid queries with weight attribute
  assertInvalidVectorFilter("@title:hello => {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("hello | @title:hello => {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("@title:'hello' => {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("( @title:(foo bar) @body:lol => {$weight: 2.0;} )=> {$slop:2; $inorder:true}", ctx);
  assertInvalidVectorFilter("( @title:(foo bar) @body:lol )=> {$weight:2.0; $inorder:true}", ctx);
  assertInvalidVectorFilter("(ismissing(@body))=> {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("(@body:'')=> {$weight: 2.0}", ctx);

  // Complex queries with range
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar", ctx);
  assertInvalidVectorFilter("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => { $weight: 2.0 }", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar @v:[VECTOR_RANGE 0.04 $BLOB2]", ctx);
  assertInvalidVectorFilter("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => [KNN 5 @v $BLOB2]", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2] => {$yield_distance_as:second_score;}", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] VECTOR_RANGE", ctx); // Fallback VECTOR_RANGE into a term.

  // Invalid queries with empty string - field does not index empty strings
  assertInvalidVectorFilter("@title:''", ctx);

  IndexSpec_RemoveFromGlobals(ref, false);
}

TEST_F(QueryValidationTest, testValidVectorFilter) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING", "INDEXEMPTY"
  };

  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Valid queries
  assertValidVectorFilter("hello", ctx);
  assertValidVectorFilter("@body:''", ctx);
  assertValidVectorFilter("@title:hello", ctx);
  assertValidVectorFilter("@title:hello world", ctx);
  assertValidVectorFilter("@title:hello world -@title:world", ctx);
  assertValidVectorFilter("@title:hello world -@title:world @title:hello", ctx);
  assertValidVectorFilter("( @title:(foo bar) @body:lol )=> {$slop:2; $inorder:true}", ctx);
  assertValidVectorFilter("", ctx);
  assertValidVectorFilter("such that their", ctx);
  assertValidVectorFilter("ismissing(@body)", ctx);
  assertValidVectorFilter("@body:''", ctx);

  IndexSpec_RemoveFromGlobals(ref, false);
}

// Hybrid text filters accept weight attribute, but not vector queries
TEST_F(QueryValidationTest, testInvalidHybridSearch) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text",
    "v", "vector", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
    "v2", "vector", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"};

  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Invalid queries with KNN
  assertInvalidHybridSearch("*=>[KNN 10 @vec_field $BLOB]", ctx);
  assertInvalidHybridSearch("@title:hello =>[KNN 10 @vec_field $BLOB]", ctx);

  // Invalid queries with range
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidHybridSearch("hello | @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);

  // Complex queries with range
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar", ctx);
  assertInvalidHybridSearch("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => { $weight: 2.0 }", ctx);
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar @v:[VECTOR_RANGE 0.04 $BLOB2]", ctx);
  assertInvalidHybridSearch("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => [KNN 5 @v $BLOB2]", ctx);
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB] => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2] => {$yield_distance_as:second_score;}", ctx);
  assertInvalidHybridSearch("@v:[VECTOR_RANGE 0.01 $BLOB] VECTOR_RANGE", ctx); // Fallback VECTOR_RANGE into a term.

  // Invalid queries with empty string - field does not index empty strings
  assertInvalidHybridSearch("@title:''", ctx);

  IndexSpec_RemoveFromGlobals(ref, false);
}

TEST_F(QueryValidationTest, testValidHybridSearch) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING", "INDEXEMPTY"
  };

  QueryError err = {QUERY_OK};
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
