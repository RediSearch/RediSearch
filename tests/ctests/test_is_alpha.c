#include <string.h>
#include <assert.h>
#include "util/misc.h"
#include "test_util.h"

int test_isAlphabetic() {
    ASSERT(isAlphabetic("HelloWorld", strlen("HelloWorld")) == true);
    ASSERT(isAlphabetic("Hello123", strlen("Hello123")) == false);
    ASSERT(isAlphabetic("", 0) == true);
    ASSERT(isAlphabetic("HelloWorld", strlen("HelloWorld")) == true);
    ASSERT(isAlphabetic("Hello World", strlen("Hello World")) == false);
    ASSERT(isAlphabetic("Hello@World", strlen("Hello@World")) == false);
    ASSERT(isAlphabetic("HolaMundo", strlen("HolaMundo")) == true);
    ASSERT(isAlphabetic("Canción", strlen("Canción")) == true);
    ASSERT(isAlphabetic("你好世界", strlen("你好世界")) == true);
    ASSERT(isAlphabetic("你好World", strlen("你好World")) == true);
    ASSERT(isAlphabetic("Hola123", strlen("Hola123")) == false);
    ASSERT(isAlphabetic("你好123", strlen("你好123")) == false);

    printf("All tests passed!\n");
    return 0;
}

TEST_MAIN({
  TESTFUNC(test_isAlphabetic);
})