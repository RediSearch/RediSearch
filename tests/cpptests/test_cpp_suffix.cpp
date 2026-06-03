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

// The chosen token: its ordinal (REDISEARCH_UNINITIALIZED if none) and length,
// so tests can assert exactly which token the scorer picked.
struct RuneChoice {
  int idx;
  size_t len;
};

// Run the rune scorer on an ASCII pattern (one byte == one rune here).
static RuneChoice chooseRune(const char *pattern) {
  size_t n = strlen(pattern);
  runeBuf buf;
  size_t rlen;
  rune *runes = runeBufFill(pattern, n, &buf, &rlen);
  std::vector<size_t> idx(rlen + 1), len(rlen + 1);
  int res = Suffix_ChooseToken_rune(runes, rlen, idx.data(), len.data());
  size_t chosenLen = res == REDISEARCH_UNINITIALIZED ? 0 : len[res];
  runeBufFree(&buf);
  return {res, chosenLen};
}

static int chooseTokenRune(const char *pattern) { return chooseRune(pattern).idx; }

static int chooseTokenChar(const char *pattern) {
  size_t n = strlen(pattern);
  std::vector<size_t> idx(n + 1), len(n + 1);
  return Suffix_ChooseToken(pattern, n, idx.data(), len.data());
}

// A '?' in the first qualifying (len >= MIN_SUFFIX) token must not prevent a
// token from being chosen. Before the fix the rune scorer returned
// REDISEARCH_UNINITIALIZED here, forcing a full-trie brute-force fallback.
TEST_F(SuffixChooseTokenTest, questionMarkInFirstTokenStillSelects) {
  // Each case asserts the chosen token's ordinal and length, making it explicit
  // which '?'-bearing token survives (token shapes shown in comments).
  // Single token "?abcd" (len 5), '?' leading.
  EXPECT_EQ(chooseRune("?abcd").idx, 0);
  EXPECT_EQ(chooseRune("?abcd").len, 5u);
  // Single token "ab?cd" (len 5), '?' in the middle.
  EXPECT_EQ(chooseRune("ab?cd").idx, 0);
  EXPECT_EQ(chooseRune("ab?cd").len, 5u);
  // Single token "abc?" (len 4), '?' trailing.
  EXPECT_EQ(chooseRune("abc?").idx, 0);
  EXPECT_EQ(chooseRune("abc?").len, 4u);
  // Single token "a??cd" (len 5): each '?' must penalize only this candidate,
  // not the running best, so the token is still selectable.
  EXPECT_EQ(chooseRune("a??cd").idx, 0);
  EXPECT_EQ(chooseRune("a??cd").len, 5u);
  // Degenerate token "??" (len 2, == MIN_SUFFIX) is a weak but valid filter and
  // must be chosen over a full-trie fallback.
  EXPECT_EQ(chooseRune("??").idx, 0);
  EXPECT_EQ(chooseRune("??").len, 2u);
  // "a?c"*"defg": clean token "defg" (idx 1, len 4) beats the '?'-bearing first.
  EXPECT_EQ(chooseRune("a?c*defg").idx, 1);
  EXPECT_EQ(chooseRune("a?c*defg").len, 4u);
  // "a?b"*"c?d": both tokens carry a '?'; the later "c?d" (idx 1, len 3) wins by
  // avoiding the trailing-'*' penalty the first token pays.
  EXPECT_EQ(chooseRune("a?b*c?d").idx, 1);
  EXPECT_EQ(chooseRune("a?b*c?d").len, 3u);
  // "abcde?fghij"*"xy": the long first token (idx 0, len 11) keeps winning
  // despite its '?' and trailing '*', over the short clean later token.
  EXPECT_EQ(chooseRune("abcde?fghij*xy").idx, 0);
  EXPECT_EQ(chooseRune("abcde?fghij*xy").len, 11u);
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
