/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "util/hash/hash.h"

#include <stdlib.h>
#include <string.h>

class HashTest : public ::testing::Test {};

TEST_F(HashTest, testSha1Hash) {
  Sha1 sha1;
  Sha1_Compute("hello", 5, &sha1);
  char buffer[SHA1_TEXT_MAX_LENGTH + 1];
  Sha1_FormatIntoBuffer(&sha1, buffer);
  ASSERT_STREQ(buffer, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
}
