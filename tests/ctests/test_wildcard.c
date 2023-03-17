/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "wildcard/wildcard.h"
#include "suffix.h"
#include "rmutil/alloc.h"
#include "test_util.h"

#include <stdio.h>
#include <assert.h>
#include <stddef.h>


int _testStarBreak(char *str, int slen, char **resArray, int reslen) {
  size_t tokenIdx[8];
  size_t tokenLen[8];

  int len = Suffix_ChooseToken(str, slen, tokenIdx, tokenLen);
  ASSERT_EQUAL(len, reslen);
  for (int i = 0; i < reslen; ++i) {
    // printf("%s %ld\n", &str[tokenIdx[i]], tokenLen[i]);
    ASSERT(!strncmp(resArray[i], &str[tokenIdx[i]], tokenLen[i]));
  }
  return 0;
}

int test_StarBreak() {
  char *str = "foo*bar";
  char *results1[8] = {"foo", "bar"};
  _testStarBreak(str, strlen(str), results1, 1);

  str = "*foo*bar";
  _testStarBreak(str, strlen(str), results1, 1);

  str = "foo*bar*";
  _testStarBreak(str, strlen(str), results1, 1);

  str = "foo*bar*red??*l*bs?";
  char *results2[] = {"foo", "bar", "red??", "l", "bs?"};
  _testStarBreak(str, strlen(str), results2, 4);

  str = "******";
  _testStarBreak(str, strlen(str), NULL, -1);

  str = "foobar";
  _testStarBreak(str, strlen(str), &str, 0);

  return 0;
}

//int i = 0;
int _testRemoveEscape(char *str, char *strAfter, int lenAfter) {
  //printf("%d %s ", i++, str);
  int len = Wildcard_RemoveEscape(str, strlen(str));
  //printf("%s %d\n", str, len);

  ASSERT_EQUAL(len, lenAfter);
  ASSERT_STRING_EQ(str, strAfter);
  return 0;
}

int test_removeEscape() {
  char buf[16];

  memcpy(buf, "foo", 4);
  _testRemoveEscape(buf, "foo", 3);

  // beginning of string
  memcpy(buf, "\\foo", 5);
  _testRemoveEscape(buf, "foo", 3);
  memcpy(buf, "\\\\foo", 6);
  _testRemoveEscape(buf, "\\foo", 4);
  memcpy(buf, "\'foo", 5);
  _testRemoveEscape(buf, "'foo", 4);
  memcpy(buf, "\\'foo", 5);
  _testRemoveEscape(buf, "'foo", 4);
  memcpy(buf, "\\\'foo", 6);
  _testRemoveEscape(buf, "'foo", 4);
  memcpy(buf, "\\\\'foo", 7);
  _testRemoveEscape(buf, "\\'foo", 5);

  // mid string
  memcpy(buf, "f\\oo", 5);
  _testRemoveEscape(buf, "foo", 3);
  memcpy(buf, "f\\\\oo", 6);
  _testRemoveEscape(buf, "f\\oo", 4);
  memcpy(buf, "f\'oo", 5);
  _testRemoveEscape(buf, "f'oo", 4);
  memcpy(buf, "f\\'oo", 6);
  _testRemoveEscape(buf, "f'oo", 4);
  memcpy(buf, "f\\\'oo", 6);
  _testRemoveEscape(buf, "f'oo", 4);
  memcpy(buf, "f\\\\'oo", 7);
  _testRemoveEscape(buf, "f\\'oo", 5);

  // end of string
  memcpy(buf, "foo\\", 5);
  _testRemoveEscape(buf, "foo", 3);
  memcpy(buf, "foo\\\\", 6);
  _testRemoveEscape(buf, "foo\\", 4);
  memcpy(buf, "foo\'", 5);
  _testRemoveEscape(buf, "foo'", 4);
  memcpy(buf, "foo\\'", 5);
  _testRemoveEscape(buf, "foo'", 4);
  memcpy(buf, "foo\\\'", 6);
  _testRemoveEscape(buf, "foo'", 4);
  memcpy(buf, "foo\\\\'", 7);
  _testRemoveEscape(buf, "foo\\'", 5);
  return 0;
}

int _testTrimPattern(char *str, char *strAfter, int lenAfter) {
  //printf("%d %s ", i++, str);
  size_t len = Wildcard_TrimPattern(str, strlen(str));
  //printf("%s %d\n", str, len);

  ASSERT_EQUAL(len, lenAfter);
  ASSERT_STRING_EQ(str, strAfter);
  return 0;
}

int test_trimPattern() {
  char buf[16];

  // no change
  memcpy(buf, "foobar", 7);
  _testTrimPattern(buf, "foobar", 6);
  memcpy(buf, "*foobar", 8);
  _testTrimPattern(buf, "*foobar", 7);
  memcpy(buf, "foo*bar", 8);
  _testTrimPattern(buf, "foo*bar", 7);
  memcpy(buf, "foobar*", 8);
  _testTrimPattern(buf, "foobar*", 7);

  // remove single *
  memcpy(buf, "**foobar", 9);
  _testTrimPattern(buf, "*foobar", 7);
  memcpy(buf, "foo**bar", 9);
  _testTrimPattern(buf, "foo*bar", 7);
  memcpy(buf, "foobar**", 9);
  _testTrimPattern(buf, "foobar*", 7);

  // change order
  memcpy(buf, "foo?*", 6);
  _testTrimPattern(buf, "foo?*", 5);
  memcpy(buf, "foo*?", 6);
  _testTrimPattern(buf, "foo?*", 5);
  memcpy(buf, "foo?**", 6);
  _testTrimPattern(buf, "foo?*", 5);
  memcpy(buf, "foo*?*", 6);
  _testTrimPattern(buf, "foo?*", 5);
  memcpy(buf, "foo**?", 6);
  _testTrimPattern(buf, "foo?*", 5);

  // go crazy
  memcpy(buf, "***?***?***", 12);
  _testTrimPattern(buf, "??*", 3);

  return 0;
}

//int i = 0;
int _testMatch(char *pattern, char *str, match_t expected) {
  match_t actual = Wildcard_MatchChar(pattern, strlen(pattern), str, strlen(str));
  //printf("%d %s\n", i++, str);
  ASSERT_EQUAL(expected, actual);
  return 0;
}

int test_match() {
  // no wildcard
  _testMatch("foo", "foo", FULL_MATCH);
  _testMatch("foo", "fo", PARTIAL_MATCH);
  _testMatch("foo", "fooo", NO_MATCH);
  _testMatch("foo", "bar", NO_MATCH);

  // ? at end
  _testMatch("fo?", "foo", FULL_MATCH);
  _testMatch("fo?", "fo", PARTIAL_MATCH);
  _testMatch("fo?", "fooo", NO_MATCH);
  _testMatch("fo?", "bar", NO_MATCH);

  // ? at beginning
  _testMatch("?oo", "foo", FULL_MATCH);
  _testMatch("?oo", "fo", PARTIAL_MATCH);
  _testMatch("?oo", "fooo", NO_MATCH);
  _testMatch("?oo", "bar", NO_MATCH);

  // * at end
  _testMatch("fo*", "foo", FULL_MATCH);
  _testMatch("fo*", "fo", FULL_MATCH);
  _testMatch("fo*", "fooo", FULL_MATCH);
  _testMatch("fo*", "bar", NO_MATCH);

  // * at beginning - at least partial match
  _testMatch("*oo", "foo", FULL_MATCH);
  _testMatch("*oo", "fo", PARTIAL_MATCH);
  _testMatch("*oo", "fooo", FULL_MATCH);
  _testMatch("*oo", "bar", PARTIAL_MATCH);
  _testMatch("*", "bar", FULL_MATCH);
  _testMatch("*", "", FULL_MATCH);

  // mix
  _testMatch("f?o*bar", "foobar", FULL_MATCH);
  _testMatch("f?o*bar", "fobar", NO_MATCH);
  _testMatch("f?o*bar", "fooooobar", FULL_MATCH);
  _testMatch("f?o*bar", "barfoo", NO_MATCH);
  _testMatch("f?o*bar", "bar", NO_MATCH);
  _testMatch("*f?o*bar", "bar", PARTIAL_MATCH);

  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(test_StarBreak);
  TESTFUNC(test_removeEscape);
  TESTFUNC(test_trimPattern);
  TESTFUNC(test_match); 
});