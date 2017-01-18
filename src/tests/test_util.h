#ifndef __TESTUTIL_H__
#define __TESTUTIL_H__

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#define TESTFUNC(f)                                            \
  printf("Testing %s ...\n------------------\n", __STRING(f)); \
  fflush(stdout);                                              \
  if (f()) {                                                   \
    printf("Test %s FAILED!\n", __STRING(f));                  \
    exit(1);                                                   \
  } else                                                       \
    printf("Test %s PASSED\n", __STRING(f));

#define ASSERTM(expr, ...)                                                                 \
  if (!(expr)) {                                                                           \
    fprintf(stderr, "%s:%d: Assertion '%s' Failed: " __VA_ARGS__ "\n", __FILE__, __LINE__, \
            __STRING(expr));                                                               \
    return -1;                                                                             \
  }
#define ASSERT(expr)                                                                       \
  if (!(expr)) {                                                                           \
    fprintf(stderr, "%s:%d Assertion '%s' Failed\n", __FILE__, __LINE__, __STRING(expr));  \
    return -1;                                                                             \
  }

#define ASSERT_STRING_EQ(s1, s2) ASSERT(!strcmp(s1, s2));

#define ASSERT_EQUAL(x, y, ...)                                 \
  if (x != y) {                                                     \
    fprintf(stderr, "%s:%d: Assertion Failed " __VA_ARGS__ ": ", __FILE__, __LINE__);                 \
    return -1;                                                      \
  }

#define FAIL(fmt, ...)                                                            \
  {                                                                               \
    fprintf(stderr, "%s:%d: FAIL: " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
    return -1;                                                                    \
  }

#define RETURN_TEST_SUCCESS return 0;
#define TEST_CASE(x, block) \
  int x {                   \
    block;                  \
    return 0                \
  }

// #define TEST_START()                                                           \
//   printf("Testing %s... ", __FUNCTION__);                                      \
//   fflush(stdout);
// #define TEST_END() printf("PASS!");

#endif