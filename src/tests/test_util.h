#ifndef __TESTUTIL_H__
#define __TESTUTIL_H__

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#define TESTFUNC(f)                                                            \
  printf("Testing %s ...\n------------------\n", __STRING(f));                 \
  fflush(stdout);                                                              \
  if (f()) {                                                                   \
    printf("Test %s FAILED!\n", __STRING(f));                                  \
    exit(1);                                                                   \
  } else                                                                       \
    printf("Test %s PASSED\n", __STRING(f));

#define ASSERTM(expr, ...)                                                     \
  if (!(expr)) {                                                               \
    fprintf(stderr, "Assertion '%s' Failed: " __VA_ARGS__ "\n",                \
            __STRING(expr));                                                   \
    return -1;                                                                 \
  }
#define ASSERT(expr)                                                           \
  if (!(expr)) {                                                               \
    fprintf(stderr, "Assertion '%s' Failed\n", __STRING(expr));                \
    return -1;                                                                 \
  }
#define ASSERT_EQUAL_INT(x, y, ...)                                            \
  if (x != y) {                                                                \
    fprintf(stderr, "%d != %d: " __VA_ARGS__ "\n", x, y);                      \
    return -1;                                                                 \
  }

#define TEST_CASE(x, block) int x { block; return 0 }

// #define TEST_START()                                                           \
//   printf("Testing %s... ", __FUNCTION__);                                      \
//   fflush(stdout);
// #define TEST_END() printf("PASS!");

#endif