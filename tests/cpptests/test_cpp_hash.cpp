#include "gtest/gtest.h"
#include "hash/hash.h"

#include <stdlib.h>
#include <string.h>

class HashTest : public ::testing::Test {};

TEST_F(HashTest, testSha1Hash) {
  Sha1 sha1;
  Sha1_Compute(&sha1, "hello", 5);
  char *formatted = Sha1_Format(&sha1);
  ASSERT_STREQ(formatted, "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
}