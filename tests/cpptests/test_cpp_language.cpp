/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "language.h"

class LanguageTest : public ::testing::Test {};

TEST_F(LanguageTest, testFindZeroLengthIsUnsupported) {
  // Zero length is the empty string, even when a valid name follows the pointer.
  EXPECT_EQ(RSLanguage_Find("english", 0), RS_LANG_UNSUPPORTED);
}

TEST_F(LanguageTest, testFindReadsOnlyLenBytes) {
  EXPECT_EQ(RSLanguage_Find("englishXXX", 7), RS_LANG_ENGLISH);
}

TEST_F(LanguageTest, testFindNullReturnsDefault) {
  EXPECT_EQ(RSLanguage_Find(NULL, 0), DEFAULT_LANGUAGE);
}

TEST_F(LanguageTest, testDefaultLanguageIsEnglish) {
  EXPECT_EQ(DEFAULT_LANGUAGE, RS_LANG_ENGLISH);
}

TEST_F(LanguageTest, testStoredValuesKeepTheirLanguage) {
  // RDB files and index fingerprints store these raw numbers, so they are written out here
  // rather than taken from the RS_LANG_* constants: renumbering the enum must fail this test.
  static const struct {
    uint32_t stored;
    const char *name;
  } cases[] = {
      {0, "english"},     {1, "arabic"},   {2, "basque"},      {3, "catalan"},
      {4, "chinese"},     {5, "danish"},   {6, "dutch"},       {7, "finnish"},
      {8, "french"},      {9, "german"},   {10, "greek"},      {11, "hindi"},
      {12, "hungarian"},  {13, "italian"}, {14, "indonesian"}, {15, "irish"},
      {16, "lithuanian"}, {17, "nepali"},  {18, "norwegian"},  {19, "portuguese"},
      {20, "romanian"},   {21, "russian"}, {22, "spanish"},    {23, "swedish"},
      {24, "tamil"},      {25, "turkish"}, {26, "armenian"},   {27, "serbian"},
      {28, "yiddish"},    {29, "tagalog"}, {30, "malay"},
  };
  for (const auto &c : cases) {
    EXPECT_STREQ(RSLanguage_ToString(c.stored), c.name) << c.stored;
  }
}

TEST_F(LanguageTest, testNamesOfValuesWithoutANameAreNull) {
  // C can store any integer in an RSLanguage, so callers can pass values past the enum.
  for (uint32_t lang : {(uint32_t)RS_LANG_UNSUPPORTED, (uint32_t)RS_LANG_UNSET, 1000u}) {
    EXPECT_EQ(RSLanguage_ToString(lang), nullptr) << lang;
    EXPECT_EQ(RSLanguage_ToSnowballStemmer(lang), nullptr) << lang;
  }
}
