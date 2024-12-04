#include "gtest/gtest.h"
#include "hash/hash.h"

#include <stdlib.h>
#include <string.h>

class HashTest : public ::testing::Test {};

TEST_F(HashTest, testSha1Hash) {
  Sha1 sha1;
  Sha1_Compute(&sha1, "hello", 5);
  char buffer[SHA1_TEXT_MAX_LENGTH + 1];
  Sha1_FormatIntoBuffer(&sha1, buffer);
  ASSERT_STREQ(buffer, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
}