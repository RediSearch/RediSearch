#include "test_util.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "src/util/misc.h"

int test_strtolower() {
    // Test empty string
    char str1[] = "";
    ASSERT_STRING_EQ(strtolower(str1), "");
    
    // Test all uppercase
    char str2[] = "HELLO";
    ASSERT_STRING_EQ(strtolower(str2), "hello");
    
    // Test mixed case
    char str3[] = "Hello World";
    ASSERT_STRING_EQ(strtolower(str3), "hello world");
    
    // Test already lowercase
    char str4[] = "hello";
    ASSERT_STRING_EQ(strtolower(str4), "hello");
    
    // Test with numbers and special chars
    char str5[] = "123ABC!@#";
    ASSERT_STRING_EQ(strtolower(str5), "123abc!@#");
    
    return 0;
}

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
  TESTFUNC(test_strtolower);
  TESTFUNC(test_get_redis_error_code_length);
});