#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "test_util.h"
#include "src/util/misc.h"

// Test function for isAlphabetic
int test_isAlphabetic() {
  // Test case 1: All alphabetic characters
  ASSERT(isAlphabetic("HelloWorld", strlen("HelloWorld")) == 1);

  // Test case 2: Contains non-alphabetic characters
  ASSERT(isAlphabetic("Hello123", strlen("Hello123")) == 0);

  // Test case 3: Empty string
  ASSERT(isAlphabetic("", 0) == 1);

  // Test case 4: All alphabetic characters with mixed case
  ASSERT(isAlphabetic("HelloWorld", strlen("HelloWorld")) == 1);

  // Test case 5: String with spaces
  ASSERT(isAlphabetic("Hello World", strlen("Hello World")) == 0);

  // Test case 6: String with special characters
  ASSERT(isAlphabetic("Hello@World", strlen("Hello@World")) == 0);

  printf("All tests passed!\n");
  return 0;
}


TEST_MAIN({
  TESTFUNC(test_isAlphabetic);
});