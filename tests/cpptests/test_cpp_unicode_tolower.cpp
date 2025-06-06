/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "src/util/strconv.h"
#include <string.h>

class UnicodeToLowerTest : public ::testing::Test {};

TEST_F(UnicodeToLowerTest, testBasicLowercase) {
  // Test with ASCII characters
  char str1[] = "HELLO WORLD";
  char *dst = NULL;
  size_t newLen = strlen(str1);

  dst = unicode_tolower(str1, &newLen);
  // Function should return nullptr because no memory allocation is needed
  ASSERT_EQ(dst, nullptr);
  ASSERT_EQ(newLen, strlen("hello world"));
  ASSERT_STREQ(str1, "hello world");

  // Test with already lowercase
  char str2[] = "already lowercase";
  newLen = strlen(str2);
  dst = unicode_tolower(str2, &newLen);
  ASSERT_EQ(dst, nullptr); // No memory allocation needed
  ASSERT_EQ(newLen, strlen("already lowercase"));
  ASSERT_STREQ(str2, "already lowercase");
}

TEST_F(UnicodeToLowerTest, testUnicodeCharacters) {
  // Test with mixed case unicode characters
  char str[] = "ÄÖÜäöüÇçÑñ";
  char *dst = NULL;
  size_t newLen = strlen(str);

  dst = unicode_tolower(str, &newLen);
  ASSERT_EQ(dst, nullptr); // No memory allocation needed
  ASSERT_EQ(newLen, strlen("äöüäöüççññ"));
  ASSERT_STREQ(str, "äöüäöüççññ");

  // Test with Hebrew and Russian characters
  char hebrew[] = "שָׁלוֹם";
  newLen = strlen(hebrew);
  dst = unicode_tolower(hebrew, &newLen);
  ASSERT_EQ(dst, nullptr);  // No memory allocation needed
  ASSERT_EQ(newLen, strlen("שָׁלוֹם"));
  // Hebrew doesn't have case distinctions like Latin scripts,
  // but we can verify the string remains intact after processing
  ASSERT_STREQ(hebrew, "שָׁלוֹם");

  char russian[] = "ПРИВЕТ мир";
  newLen = strlen(russian);
  dst = unicode_tolower(russian, &newLen);
  ASSERT_EQ(dst, nullptr);  // No memory allocation needed
  ASSERT_EQ(newLen, strlen("привет мир"));
  ASSERT_STREQ(russian, "привет мир");
}

TEST_F(UnicodeToLowerTest, testEmptyAndSpecialCases) {
  // Test with empty string
  char empty[] = "";
  char *dst = NULL;
  size_t newLen = 0;

  dst = unicode_tolower(empty, &newLen);
  ASSERT_EQ(dst, nullptr); // No memory allocation needed
  ASSERT_EQ(newLen, 0);

  // Test with mixed symbols and numbers (should remain unchanged)
  char symbols[] = "123!@#$%^&*()";
  newLen = strlen(symbols);
  dst = unicode_tolower(symbols, &newLen);
  ASSERT_EQ(dst, nullptr); // No memory allocation needed
  ASSERT_EQ(newLen, strlen(symbols));
  ASSERT_STREQ(symbols, "123!@#$%^&*()");
}

TEST_F(UnicodeToLowerTest, testLongString) {
  // Test with a string longer than SSO_MAX_LENGTH
  char* longStr = (char*)malloc(SSO_MAX_LENGTH * 2);
  char *dst = NULL;
  size_t newLen = 0;

  for (int i = 0; i < SSO_MAX_LENGTH * 2 - 1; i++) {
    longStr[i] = 'A' + (i % 26);
  }
  longStr[SSO_MAX_LENGTH * 2 - 1] = '\0';

  newLen = strlen(longStr);
  size_t originalLen = newLen;
  dst = unicode_tolower(longStr, &newLen);
  ASSERT_EQ(dst, nullptr); // No memory allocation needed
  ASSERT_EQ(newLen, originalLen);

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
  char *dst = NULL;

  size_t uppercaseLen = strlen(str);
  size_t lowercaseLen = strlen(str);
  dst = unicode_tolower(str, &lowercaseLen);
  ASSERT_EQ(dst, nullptr); // No memory allocation needed
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
  char *dst = NULL;
  size_t newLen = strlen(str);

  // The lowercase version should have more bytes than the original
  // because 'İ' (2 bytes in UTF-8) becomes 'i' + combining dot above
  // (3 bytes in UTF-8)
  dst = unicode_tolower(str, &newLen);
  ASSERT_NE(dst, nullptr);
  ASSERT_EQ(newLen, strlen("i̇stanbul"));
  ASSERT_STREQ(str, "İSTANBUL"); // Original string should remain unchanged
  ASSERT_STREQ(dst, "i̇stanbul");
  rm_free(dst); // Free the allocated memory for dst
}
