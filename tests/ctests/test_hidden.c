#include "test_util.h"
#include "src/query_node.h"
#include "src/obfuscation/hidden.h"

#include <stdlib.h>
#include <string.h>

int testHiddenOwnership() {
  const char *expected = "Text";
  HiddenName *view = NewHiddenName(expected, strlen(expected), false);
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  ASSERT(HiddenName_GetUnsafe(view, NULL) == expected);
  ASSERT(HiddenName_GetUnsafe(name, NULL) != expected);
  HiddenName_TakeOwnership(view);
  ASSERT(HiddenName_GetUnsafe(view, NULL) != expected);
  HiddenName_Free(view, true);
  HiddenName_Free(name, true);
  return 0;
}

int testHiddenCompare() {
  const char *expected = "Text";
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  ASSERT(HiddenName_CompareC(name, expected, strlen(expected)) == 0);
  ASSERT(HiddenName_CompareC(name, expected, strlen(expected) + 1) != 0);
  const char *lowerCase = "text";
  ASSERT(HiddenName_CaseInsensitiveCompareC(name, lowerCase, strlen(lowerCase)) == 0);
  ASSERT(HiddenName_CaseInsensitiveCompareC(name, lowerCase, strlen(lowerCase) + 1) != 0);
  HiddenName_Free(name, true);
  return 0;
}

int testHiddenDuplicate() {
  const char *expected = "Text";
  HiddenName *name = NewHiddenName(expected, strlen(expected), true);
  HiddenName *clone = HiddenName_Duplicate(name);
  ASSERT(HiddenName_Compare(name, clone) == 0);
  HiddenName_Free(name, true);
  HiddenName_Free(clone, true);
  return 0;
}

TEST_MAIN({
  TESTFUNC(testHiddenOwnership);
  TESTFUNC(testHiddenCompare);
  TESTFUNC(testHiddenDuplicate);
})
