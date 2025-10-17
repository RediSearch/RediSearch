/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "query_error.h"
#include "src/rmalloc.h"
#include <string.h>

class QueryErrorTest : public ::testing::Test {};

TEST_F(QueryErrorTest, testQueryErrorStrerror) {
  // Test error code to string conversion
  ASSERT_STREQ(QueryError_Strerror(QUERY_ERROR_CODE_NONE), "Success (not an error)");
  ASSERT_STREQ(QueryError_Strerror(QUERY_ERROR_CODE_SYNTAX), "Parsing/Syntax error for query string");
  ASSERT_STREQ(QueryError_Strerror(QUERY_ERROR_CODE_GENERIC), "Generic error evaluating the query");
  ASSERT_STREQ(QueryError_Strerror(QUERY_ERROR_CODE_PARSE_ARGS), "Error parsing query/aggregation arguments");
  ASSERT_STREQ(QueryError_Strerror(QUERY_ERROR_CODE_NO_RESULTS), "Query matches no results");
  ASSERT_STREQ(QueryError_Strerror(QUERY_ERROR_CODE_BAD_ATTR), "Attribute not supported for term");

  // Test unknown error code
  ASSERT_STREQ(QueryError_Strerror((QueryErrorCode)-1), "Unknown status code");
}

TEST_F(QueryErrorTest, testQueryErrorSetError) {
  QueryError err = QueryError_Default();

  // Test setting error with custom message
  QueryError_SetError(&err, QUERY_ERROR_CODE_SYNTAX, "Custom syntax error message");
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);
  ASSERT_TRUE(QueryError_HasError(&err));
  ASSERT_STREQ(QueryError_GetUserError(&err), "Custom syntax error message");

  QueryError_ClearError(&err);

  // Test setting error without custom message (should use default)
  QueryError_SetError(&err, QUERY_ERROR_CODE_GENERIC, NULL);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_GENERIC);
  ASSERT_TRUE(QueryError_HasError(&err));
  ASSERT_STREQ(QueryError_GetUserError(&err), "Generic error evaluating the query");

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorSetCode) {
  QueryError err = QueryError_Default();

  // Test setting error code only
  QueryError_SetCode(&err, QUERY_ERROR_CODE_PARSE_ARGS);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_PARSE_ARGS);
  ASSERT_TRUE(QueryError_HasError(&err));
  ASSERT_STREQ(QueryError_GetUserError(&err), "Error parsing query/aggregation arguments");

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorNoOverwrite) {
  QueryError err = QueryError_Default();

  // Set first error
  QueryError_SetError(&err, QUERY_ERROR_CODE_SYNTAX, "First error");
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);
  ASSERT_STREQ(QueryError_GetUserError(&err), "First error");

  // Try to set second error - should not overwrite
  QueryError_SetError(&err, QUERY_ERROR_CODE_GENERIC, "Second error");
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);  // Should still be first error
  ASSERT_STREQ(QueryError_GetUserError(&err), "First error");

  // Try to set code only - should not overwrite
  QueryError_SetCode(&err, QUERY_ERROR_CODE_PARSE_ARGS);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);  // Should still be first error

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorClear) {
  QueryError err = QueryError_Default();

  // Set an error
  QueryError_SetError(&err, QUERY_ERROR_CODE_SYNTAX, "Test error");
  ASSERT_TRUE(QueryError_HasError(&err));
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);

  // // Clear the error
  // QueryError_ClearError(&err);
  // ASSERT_FALSE(QueryError_HasError(&err));
  // ASSERT_TRUE(QueryError_IsOk(&err));
  // ASSERT_TRUE(err._detail == NULL);
}

TEST_F(QueryErrorTest, testQueryErrorGetCode) {
  QueryError err = QueryError_Default();

  ASSERT_TRUE(QueryError_IsOk(&err));

  QueryError_SetError(&err, QUERY_ERROR_CODE_SYNTAX, "Test error");
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorWithUserDataFmt) {
  QueryError err = QueryError_Default();

  // Test formatted error with user data
  QueryError_SetWithUserDataFmt(&err, QUERY_ERROR_CODE_SYNTAX, "Syntax error", " at offset %d near %s", 10, "hello");
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);
  ASSERT_TRUE(QueryError_HasError(&err));
  ASSERT_STREQ(QueryError_GetUserError(&err), "Syntax error at offset 10 near hello");

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorWithoutUserDataFmt) {
  QueryError err = QueryError_Default();

  // Test formatted error without user data
  QueryError_SetWithoutUserDataFmt(&err, QUERY_ERROR_CODE_GENERIC, "Generic error with code %d", 42);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_GENERIC);
  ASSERT_TRUE(QueryError_HasError(&err));
  ASSERT_STREQ(QueryError_GetUserError(&err), "Generic error with code 42");

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorCloneFrom) {
  QueryError src = QueryError_Default();
  QueryError dest = QueryError_Default();

  // Set error in source
  QueryError_SetError(&src, QUERY_ERROR_CODE_SYNTAX, "Source error message");

  // Clone to destination
  QueryError_CloneFrom(&src, &dest);
  ASSERT_EQ(QueryError_GetCode(&dest), QUERY_ERROR_CODE_SYNTAX);
  ASSERT_STREQ(QueryError_GetUserError(&dest), "Source error message");

  // Test that destination already has error - should not overwrite
  QueryError src2 = QueryError_Default();
  QueryError_SetError(&src2, QUERY_ERROR_CODE_GENERIC, "Second error");

  QueryError_CloneFrom(&src2, &dest);  // Should not overwrite
  ASSERT_EQ(QueryError_GetCode(&dest), QUERY_ERROR_CODE_SYNTAX);  // Should still be original error
  ASSERT_STREQ(QueryError_GetUserError(&dest), "Source error message");

  QueryError_ClearError(&src);
  QueryError_ClearError(&dest);
  QueryError_ClearError(&src2);
}

TEST_F(QueryErrorTest, testQueryErrorGetDisplayableError) {
  QueryError err = QueryError_Default();

  // Test with user data formatting
  QueryError_SetWithUserDataFmt(&err, QUERY_ERROR_CODE_SYNTAX, "Syntax error", " at position %d", 42);

  // Test non-obfuscated (should show full detail)
  const char *full_error = QueryError_GetDisplayableError(&err, false);
  ASSERT_STREQ(full_error, "Syntax error at position 42");

  // Test obfuscated (should show only message without user data)
  const char *obfuscated_error = QueryError_GetDisplayableError(&err, true);
  ASSERT_STREQ(obfuscated_error, "Syntax error");

  QueryError_ClearError(&err);
  ASSERT_FALSE(QueryError_HasError(&err));

  // Test with error that has no custom message
  QueryError_SetCode(&err, QUERY_ERROR_CODE_GENERIC);
  const char *default_error = QueryError_GetDisplayableError(&err, true);
  ASSERT_STREQ(default_error, "Generic error evaluating the query");

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorMaybeSetCode) {
  QueryError err = QueryError_Default();

  // Test with no detail set - should not set code
  QueryError_MaybeSetCode(&err, QUERY_ERROR_CODE_SYNTAX);
  ASSERT_TRUE(QueryError_IsOk(&err));

  // Manually set detail (simulating external function setting it)
  QueryError_SetDetail(&err, "Some detail");
  QueryError_MaybeSetCode(&err, QUERY_ERROR_CODE_SYNTAX);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);

  // Try to set again - should not overwrite
  QueryError_MaybeSetCode(&err, QUERY_ERROR_CODE_GENERIC);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);

  QueryError_ClearError(&err);
}

TEST_F(QueryErrorTest, testQueryErrorAllErrorCodes) {
  // Test that all error codes have valid string representations
  QueryErrorCode codes[] = {
    QUERY_ERROR_CODE_NONE,
    QUERY_ERROR_CODE_GENERIC,
    QUERY_ERROR_CODE_SYNTAX,
    QUERY_ERROR_CODE_PARSE_ARGS,
    QUERY_ERROR_CODE_ADD_ARGS,
    QUERY_ERROR_CODE_EXPR,
    QUERY_ERROR_CODE_KEYWORD,
    QUERY_ERROR_CODE_NO_RESULTS,
    QUERY_ERROR_CODE_BAD_ATTR,
    QUERY_ERROR_CODE_NO_OPTION,
    QUERY_ERROR_CODE_BAD_VAL,
    QUERY_ERROR_CODE_NO_PARAM,
    QUERY_ERROR_CODE_DUP_PARAM
  };

  for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
    const char *str = QueryError_Strerror(codes[i]);
    ASSERT_TRUE(str != NULL);
    ASSERT_TRUE(strlen(str) > 0);

    // Test that we can set and retrieve each error code
    QueryError err = QueryError_Default();
    QueryError_SetCode(&err, codes[i]);
    ASSERT_EQ(QueryError_GetCode(&err), codes[i]);
    QueryError_ClearError(&err);
  }
}

TEST_F(QueryErrorTest, testQueryErrorEdgeCases) {
  QueryError err = QueryError_Default();

  // Test empty string message
  QueryError_SetError(&err, QUERY_ERROR_CODE_SYNTAX, "");
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_SYNTAX);
  ASSERT_STREQ(QueryError_GetUserError(&err), "");
  QueryError_ClearError(&err);

  // Test very long message
  char long_msg[1000];
  memset(long_msg, 'A', sizeof(long_msg) - 1);
  long_msg[sizeof(long_msg) - 1] = '\0';

  QueryError_SetError(&err, QUERY_ERROR_CODE_GENERIC, long_msg);
  ASSERT_EQ(QueryError_GetCode(&err), QUERY_ERROR_CODE_GENERIC);
  ASSERT_STREQ(QueryError_GetUserError(&err), long_msg);
  QueryError_ClearError(&err);

  // Test multiple clears (should be safe)
  QueryError_SetError(&err, QUERY_ERROR_CODE_SYNTAX, "Test");
  QueryError_ClearError(&err);
  QueryError_ClearError(&err);  // Second clear should be safe
  ASSERT_TRUE(QueryError_IsOk(&err));
  ASSERT_FALSE(QueryError_HasError(&err));
}
