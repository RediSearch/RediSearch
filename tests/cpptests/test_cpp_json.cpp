/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "json.h"
#include "document.h"
#include "field_spec.h"
#include "VecSim/vec_sim.h"
#include <climits>
#include <cstring>

class JSONTest : public ::testing::Test {};

TEST_F(JSONTest, testStoreTextOverflow) {
  // Test that JSON_StoreTextInDocField returns an error when `len` would cause
  // an overflow in the allocation size calculation (len * sizeof(char*)).
  DocumentField df = {0};
  QueryError status = {QueryErrorCode(0)};

  // Use a length that would cause overflow when multiplied by sizeof(char*)
  // SIZE_MAX / sizeof(char*) + 1 will overflow when multiplied by sizeof(char*)
  size_t overflow_len = (SIZE_MAX / sizeof(char*)) + 1;

  int rv = JSON_StoreTextInDocField(overflow_len, nullptr, &df, &status);
  ASSERT_EQ(REDISMODULE_ERR, rv);
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_EGENERIC);
  const char *err_msg = QueryError_GetUserError(&status);
  ASSERT_STREQ(err_msg, "Failed to allocate memory for text field");

  QueryError_ClearError(&status);
}

TEST_F(JSONTest, testStoreMultiVectorOverflow) {
  // Test that JSON_StoreMultiVectorInDocField returns an error when len would
  // cause an overflow in the allocation size calculation (expBlobSize * len).
  DocumentField df = {0};
  QueryError status = {QueryErrorCode(0)};

  // Set up a minimal FieldSpec with vector options for BF algorithm with
  // multi=true
  FieldSpec fs;
  memset(&fs, 0, sizeof(fs));
  fs.types = INDEXFLD_T_VECTOR;

  // Configure BF params with multi=true
  fs.vectorOpts.vecSimParams.algo = VecSimAlgo_BF;
  fs.vectorOpts.vecSimParams.algoParams.bfParams.type = VecSimType_FLOAT32;
  fs.vectorOpts.vecSimParams.algoParams.bfParams.dim = 4;
  fs.vectorOpts.vecSimParams.algoParams.bfParams.multi = true;

  // Set expBlobSize to a value that will overflow when multiplied by a large len
  // expBlobSize = dim * sizeof(float) = 4 * 4 = 16 bytes
  fs.vectorOpts.expBlobSize = 16;

  // Use a length that would cause overflow when multiplied by expBlobSize (16)
  // SIZE_MAX / 16 + 1 will overflow when multiplied by 16
  size_t overflow_len = (SIZE_MAX / fs.vectorOpts.expBlobSize) + 1;

  int rv = JSON_StoreMultiVectorInDocField(&fs, nullptr, overflow_len, &df, &status);
  ASSERT_EQ(REDISMODULE_ERR, rv);
  ASSERT_EQ(QueryError_GetCode(&status), QUERY_EGENERIC);
  const char *err_msg = QueryError_GetUserError(&status);
  ASSERT_STREQ(err_msg, "Failed to allocate memory for multi-vector field");

  QueryError_ClearError(&status);
}

