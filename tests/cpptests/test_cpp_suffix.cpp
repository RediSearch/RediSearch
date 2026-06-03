/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "suffix.h"
#include "trie/rune_util.h"
#include "redisearch.h"

#include <string>
#include <vector>

// Suffix_ChooseToken (char) and Suffix_ChooseToken_rune (rune) pick the literal
// token of a wildcard "contains" pattern used to narrow the suffix-trie scan.
// The two must agree on which token they choose; the rune variant once
// decremented the shared `score` accumulator on every '?', underflowing it from
// INT32_MIN to INT32_MAX so that no token was ever selected.

class SuffixChooseTokenTest : public ::testing::Test {};

// Run the rune scorer on an ASCII pattern (one byte == one rune here).
static int chooseTokenRune(const char *pattern) {
  size_t n = strlen(pattern);
  runeBuf buf;
  size_t rlen;
  rune *runes = runeBufFill(pattern, n, &buf, &rlen);
  std::vector<size_t> idx(rlen + 1), len(rlen + 1);
  int res = Suffix_ChooseToken_rune(runes, rlen, idx.data(), len.data());
  runeBufFree(&buf);
  return res;
}

static int chooseTokenChar(const char *pattern) {
  size_t n = strlen(pattern);
  std::vector<size_t> idx(n + 1), len(n + 1);
  return Suffix_ChooseToken(pattern, n, idx.data(), len.data());
}

// A '?' in the first qualifying (len >= MIN_SUFFIX) token must not prevent a
// token from being chosen. Before the fix the rune scorer returned
// REDISEARCH_UNINITIALIZED here, forcing a full-trie brute-force fallback.
TEST_F(SuffixChooseTokenTest, questionMarkInFirstTokenStillSelects) {
  // Single token, '?' as its leading character.
  EXPECT_EQ(chooseTokenRune("?abcd"), 0);
  // Single token, '?' in the middle.
  EXPECT_EQ(chooseTokenRune("ab?cd"), 0);
  // First token carries the '?', a clean selective token follows: token 1 wins.
  EXPECT_EQ(chooseTokenRune("a?c*defg"), 1);
}

// The rune scorer must agree with the char scorer on the chosen token index.
TEST_F(SuffixChooseTokenTest, runeAgreesWithChar) {
  const char *patterns[] = {
      "?abcd", "ab?cd", "a?c*defg", "abc*defg", "ab*cd*efgh", "*magicneedl?*",
  };
  for (const char *p : patterns) {
    EXPECT_EQ(chooseTokenRune(p), chooseTokenChar(p)) << "pattern: " << p;
  }
}

// Tokens shorter than MIN_SUFFIX are not usable; with no qualifying token the
// scorer reports UNINITIALIZED so the caller falls back to a brute-force scan.
TEST_F(SuffixChooseTokenTest, noQualifyingTokenIsUninitialized) {
  EXPECT_EQ(chooseTokenRune("a*b*c"), REDISEARCH_UNINITIALIZED);
}
