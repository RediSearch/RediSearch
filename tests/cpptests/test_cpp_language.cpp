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
