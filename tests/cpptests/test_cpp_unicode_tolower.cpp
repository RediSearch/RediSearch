/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "rmutil/alloc.h"
#include "gtest/gtest.h"
#include "src/util/strconv.h"
#include <string.h>

class UnicodeToLowerTest : public ::testing::Test {};

TEST_F(UnicodeToLowerTest, testBasicLowercase) {
  // Test with ASCII characters
  char str1[] = "HELLO WORLD";
  size_t len = unicode_tolower(str1, strlen(str1));
  ASSERT_EQ(len, strlen(str1));
  ASSERT_STREQ(str1, "hello world");

  // Test with already lowercase
  char str2[] = "already lowercase";
  len = unicode_tolower(str2, strlen(str2));
  ASSERT_EQ(len, strlen(str2));
  ASSERT_STREQ(str2, "already lowercase");
}

TEST_F(UnicodeToLowerTest, testUnicodeCharacters) {
  // Test with mixed case unicode characters
  char str[] = "ÄÖÜäöüÇçÑñ";
  size_t len = unicode_tolower(str, strlen(str));
  ASSERT_EQ(len, strlen(str));
  ASSERT_STREQ(str, "äöüäöüççññ");

  // Test with Hebrew and Russian characters
  char hebrew[] = "שָׁלוֹם";
  len = unicode_tolower(hebrew, strlen(hebrew));
  ASSERT_EQ(len, strlen(hebrew));
  // Hebrew doesn't have case distinctions like Latin scripts,
  // but we can verify the string remains intact after processing
  ASSERT_STREQ(hebrew, "שָׁלוֹם");

  char russian[] = "ПРИВЕТ мир";
  len = unicode_tolower(russian, strlen(russian));
  ASSERT_EQ(len, strlen(russian));
  ASSERT_STREQ(russian, "привет мир");
}

TEST_F(UnicodeToLowerTest, testEmptyAndSpecialCases) {
  // Test with empty string
  char empty[] = "";
  size_t len = unicode_tolower(empty, 0);
  ASSERT_EQ(len, 0);

  // Test with mixed symbols and numbers (should remain unchanged)
  char symbols[] = "123!@#$%^&*()";
  len = unicode_tolower(symbols, strlen(symbols));
  ASSERT_EQ(len, strlen(symbols));
  ASSERT_STREQ(symbols, "123!@#$%^&*()");
}

TEST_F(UnicodeToLowerTest, testLongString) {
  // Test with a string longer than SSO_MAX_LENGTH
  char* longStr = (char*)malloc(SSO_MAX_LENGTH * 2);
  for (int i = 0; i < SSO_MAX_LENGTH * 2 - 1; i++) {
    longStr[i] = 'A' + (i % 26);
  }
  longStr[SSO_MAX_LENGTH * 2 - 1] = '\0';

  size_t len = unicode_tolower(longStr, strlen(longStr));
  ASSERT_EQ(len, strlen(longStr));

  // Verify first few characters are lowercase
  ASSERT_EQ(longStr[0], 'a');
  ASSERT_EQ(longStr[1], 'b');
  ASSERT_EQ(longStr[2], 'c');

  free(longStr);
}

TEST_F(UnicodeToLowerTest, testSpecialUnicodeCase) {
  // Test with german (ẞ) and its lowercase form (ß)
  // Its lowercase form occupies fewer bytes in UTF-8 than its uppercase form
  char str[] = "STRAẞE";
  size_t uppercaseLen = strlen(str);
  size_t lowercaseLen = unicode_tolower(str, strlen(str));
  ASSERT_EQ(lowercaseLen, strlen("straße"));
  ASSERT_EQ(uppercaseLen, 8);
  ASSERT_EQ(lowercaseLen, 7);
  // Unicode to lower does not add the NULL terminator when the returned length
  // is less than the original length, so we need to ensure the string is
  // properly null-terminated after the conversion.
  str[lowercaseLen] = '\0';
  ASSERT_STREQ(str, "straße");
}

TEST_F(UnicodeToLowerTest, testTurkishDottedI) {
  // Test with Turkish İ (capital I with dot above, U+0130)
  // Its lowercase form occupies more bytes in UTF-8 than its uppercase form
  char str[] = "İSTANBUL";
  size_t len = unicode_tolower(str, strlen(str));

  // The lowercase version should have more bytes than the original
  // because 'İ' (2 bytes in UTF-8) becomes 'i' + combining dot above
  // (3 bytes in UTF-8)
  // That case is still unsupported in the current implementation, so the
  // resulting string will be equal to the original string, and the length
  // returned will be 0.
  ASSERT_EQ(len, 0);
  ASSERT_STREQ(str, "İSTANBUL");
}
