#include "test_util.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "src/util/misc.h"

int test_get_redis_error_code_length() {
    // Test empty
    ASSERT_EQUAL(GetRedisErrorCodeLength(""), 0);
    
    // Test no space
    ASSERT_EQUAL(GetRedisErrorCodeLength("ERROR"), 0);
    
    // Test space at start
    ASSERT_EQUAL(GetRedisErrorCodeLength(" ERROR"), 0);
    
    // Test normal cases
    ASSERT_EQUAL(GetRedisErrorCodeLength("ERR invalid"), 3);
    ASSERT_EQUAL(GetRedisErrorCodeLength("WRONGTYPE Operation"), 9);
    
    // Test multiple spaces
    ASSERT_EQUAL(GetRedisErrorCodeLength("ERR multiple spaces here"), 3);
    
    return 0;
}

TEST_MAIN({
  TESTFUNC(test_get_redis_error_code_length);
});