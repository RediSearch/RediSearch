#include "gtest/gtest.h"
#include "obfuscation/hidden.h"

#include <stdlib.h>
#include <string.h>

class HiddenTest : public ::testing::Test {};

TEST_F(HiddenTest, testHiddenOwnership) {
  const char *expected = "Text";
  size_t length = 0;
  HiddenName *view = NewHiddenName(expected, strlen(expected), false);
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  ASSERT_EQ(HiddenName_GetUnsafe(view, &length), expected);
  ASSERT_EQ(length, strlen(expected));
  ASSERT_NE(HiddenName_GetUnsafe(name, NULL), expected);
  HiddenName_TakeOwnership(view);
  ASSERT_NE(HiddenName_GetUnsafe(view, NULL), expected);
  HiddenName_Free(view, true);
  HiddenName_Free(name, true);
}

TEST_F(HiddenTest, testHiddenCompare) {
  const char *expected = "Text";
  HiddenName *first = NewHiddenName(expected, strlen(expected), true);
  HiddenName *second = NewHiddenName(expected, strlen(expected), true);
  ASSERT_EQ(HiddenName_Compare(first, second), 0);
  ASSERT_EQ(HiddenName_CompareC(first, expected, strlen(expected)), 0);
  ASSERT_NE(HiddenName_CompareC(first, expected, strlen(expected) + 1), 0);
  const char *lowerCase = "text";
  HiddenName *lower = NewHiddenName(lowerCase, strlen(lowerCase), true);
  ASSERT_EQ(HiddenName_CaseInsensitiveCompareC(first, lowerCase, strlen(lowerCase)), 0);
  ASSERT_EQ(HiddenName_CaseInsensitiveCompare(first, lower), 0);
  ASSERT_NE(HiddenName_CaseInsensitiveCompareC(first, lowerCase, strlen(lowerCase) + 1), 0);
  HiddenName_Free(first, true);
  HiddenName_Free(second, true);
  HiddenName_Free(lower, true);
}

TEST_F(HiddenTest, testHiddenDuplicate) {
  const char *expected = "Text";
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  HiddenName *clone = HiddenName_Duplicate(name);
  ASSERT_EQ(HiddenName_Compare(name, clone), 0);
  HiddenName_Free(name, true);
  HiddenName_Free(clone, true);
}

TEST_F(HiddenTest, testHiddenClone) {
  const char *longText = "LongerText";
  const char *shortText = "ShortText";

  HiddenName *l = NewHiddenName(longText, strlen(longText), true);
  HiddenName *s = NewHiddenName(shortText, strlen(shortText), true);
  HiddenName *clone = NULL;
  HiddenName_Clone(l, &clone);
  size_t length = 0;
  HiddenName_GetUnsafe(clone, &length);
  ASSERT_EQ(length, strlen(longText));
  HiddenName_Clone(s, &clone);
  HiddenName_GetUnsafe(clone, &length);
  ASSERT_EQ(length, strlen(shortText));
  HiddenName_Free(l, true);
  HiddenName_Free(s, true);
  HiddenName_Free(clone, true);
}