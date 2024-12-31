#include "test_util.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include "src/util/misc.h"

int test_contains_non_alphabetic_char() {
    // Test NULL string
    ASSERT_EQUAL(contains_non_alphabetic_char(NULL, 0),false);
    
    // Test empty string
    ASSERT_EQUAL(contains_non_alphabetic_char("", 0), false);
    
    // Test only alphabetic chars
    ASSERT_EQUAL(contains_non_alphabetic_char("abcXYZ", 6), false);
    ASSERT_EQUAL(contains_non_alphabetic_char("ABCdef", 6), false);
    
    // Test with numbers
    ASSERT_EQUAL(contains_non_alphabetic_char("abc123", 6), true);
    ASSERT_EQUAL(contains_non_alphabetic_char("1abc", 4), true);
    
    // Test with special chars
    ASSERT_EQUAL(contains_non_alphabetic_char("abc!", 4), true);
    ASSERT_EQUAL(contains_non_alphabetic_char("@abc", 4) ,true);
    
    // Test with spaces
    ASSERT_EQUAL(contains_non_alphabetic_char("ab c", 4) , true);
    ASSERT_EQUAL(contains_non_alphabetic_char(" abc", 4) , true);
    
    // Test mixed content
    ASSERT_EQUAL(contains_non_alphabetic_char("a1@b c", 6) , true);
    
    // Test boundary cases
    ASSERT_EQUAL(contains_non_alphabetic_char("a", 1) , false);
    ASSERT_EQUAL(contains_non_alphabetic_char("1", 1) , true);
    
    // Test length parameter
    ASSERT_EQUAL(contains_non_alphabetic_char("abc123", 3) , false); // Only checks "abc"

    return 0;
}
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
    // Test NULL and empty
    ASSERT_EQUAL(GetRedisErrorCodeLength(NULL), 0);
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
  TESTFUNC(test_contains_non_alphabetic_char);
  TESTFUNC(test_strtolower);
  TESTFUNC(test_get_redis_error_code_length);

});