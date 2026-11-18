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

// C writes these values to the RDB, so the generated header must keep them.
static_assert(RS_LANG_MALAY == 30, "last persisted language value");
// C picks the Chinese tokenizer by this name, so it must stay bound to Chinese.
static_assert(RS_LANG_CHINESE == 4, "RS_LANG_CHINESE names Chinese");
static_assert(RS_LANG_UNSUPPORTED == 31, "RDB loader rejects values from here up");
static_assert(DEFAULT_LANGUAGE == RS_LANG_ENGLISH, "the default language is English");

class LanguageTest : public ::testing::Test {};

TEST_F(LanguageTest, testFindNullReturnsDefault) {
  EXPECT_EQ(RSLanguage_Find(NULL, 0), DEFAULT_LANGUAGE);
}
