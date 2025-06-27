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

class VectorFilterTest : public ::testing::Test {};

bool isValidAsVectorFilter(const char *qt, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.isValidAsVectorFilter(qt);
}

#define assertValidVectorFilter(qt, ctx) ASSERT_TRUE(isValidAsVectorFilter(qt, ctx))
#define assertInvalidVectorFilter(qt, ctx) ASSERT_FALSE(isValidAsVectorFilter(qt, ctx))

TEST_F(VectorFilterTest, testInvalidVectorFilter) {
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
  assertInvalidVectorFilter("*=>[KNN 10 @vec_field $BLOB]", ctx);
  assertInvalidVectorFilter("@title:hello =>[KNN 10 @vec_field $BLOB]", ctx);

  // Invalid queries with range
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertInvalidVectorFilter("hello | @v:[VECTOR_RANGE 0.01 $BLOB]", ctx);

  // Invalid queries with weight
  assertInvalidVectorFilter("@title:hello => {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("hello | @title:hello => {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("@title:'' => {$weight: 2.0}", ctx);
  assertInvalidVectorFilter("( @title:(foo bar) @body:lol => {$weight: 2.0;} )=> {$slop:2; $inorder:true}", ctx);
  assertInvalidVectorFilter("( @title:(foo bar) @body:lol )=> {$weight:2.0; $inorder:true}", ctx);

  // Complex queries with range
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar", ctx);
  assertInvalidVectorFilter("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => { $weight: 2.0 }", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo OR bar @v:[VECTOR_RANGE 0.04 $BLOB2]", ctx);
  assertInvalidVectorFilter("(@v:[VECTOR_RANGE 0.01 $BLOB] @title:foo) => [KNN 5 @v $BLOB2]", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2] => {$yield_distance_as:second_score;}", ctx);
  assertInvalidVectorFilter("@v:[VECTOR_RANGE 0.01 $BLOB] VECTOR_RANGE", ctx); // Fallback VECTOR_RANGE into a term.

  IndexSpec_RemoveFromGlobals(ref, false);
}

TEST_F(VectorFilterTest, testValidVectorFilter) {
  // Create an index spec with a title field and a vector field
  static const char *args[] = {
    "SCHEMA",
    "title", "text", "weight", "1.2",
    "body", "text", "INDEXMISSING"};

  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_ParseC("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_EQ(err.code, QUERY_OK) << QueryError_GetUserError(&err);

  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  // Valid queries
  assertValidVectorFilter("hello", ctx);
  assertValidVectorFilter("@title:''", ctx);
  assertValidVectorFilter("@title:hello", ctx);
  assertValidVectorFilter("@title:hello world", ctx);
  assertValidVectorFilter("@title:hello world -@title:world", ctx);
  assertValidVectorFilter("@title:hello world -@title:world @title:hello", ctx);
  assertValidVectorFilter("( @title:(foo bar) @body:lol )=> {$slop:2; $inorder:true}", ctx);
  assertValidVectorFilter("", ctx);
  assertValidVectorFilter("such that their", ctx);
  assertValidVectorFilter("ismissing(@body)", ctx);

  IndexSpec_RemoveFromGlobals(ref, false);
}
