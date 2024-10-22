#include "gtest/gtest.h"
#include "obfuscation/hidden.h"

#include <stdlib.h>
#include <string.h>

class HiddenTest : public ::testing::Test {};

TEST_F(HiddenTest, testHiddenOwnership) {
  const char *expected = "Text";
  HiddenName *view = NewHiddenName(expected, strlen(expected), false);
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  ASSERT_EQ(HiddenName_GetUnsafe(view, NULL), expected);
  ASSERT_NE(HiddenName_GetUnsafe(name, NULL), expected);
  HiddenName_TakeOwnership(view);
  ASSERT_NE(HiddenName_GetUnsafe(view, NULL), expected);
  HiddenName_Free(view, true);
  HiddenName_Free(name, true);
}

TEST_F(HiddenTest, testHiddenCompare) {
  const char *expected = "Text";
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  ASSERT_EQ(HiddenName_CompareC(name, expected, strlen(expected)), 0);
  ASSERT_NE(HiddenName_CompareC(name, expected, strlen(expected) + 1), 0);
  const char *lowerCase = "text";
  ASSERT_EQ(HiddenName_CaseInsensitiveCompareC(name, lowerCase, strlen(lowerCase)), 0);
  ASSERT_NE(HiddenName_CaseInsensitiveCompareC(name, lowerCase, strlen(lowerCase) + 1), 0);
  HiddenName_Free(name, true);
}

TEST_F(HiddenTest, testHiddenDuplicate) {
  const char *expected = "Text";
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  HiddenName *clone = HiddenName_Duplicate(name);
  ASSERT_EQ(HiddenName_Compare(name, clone), 0);
  HiddenName_Free(name, true);
  HiddenName_Free(clone, true);
}
