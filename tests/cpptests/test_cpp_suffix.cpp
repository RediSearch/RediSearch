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
// The two must agree on which token they choose.

class SuffixChooseTokenTest : public ::testing::Test {};

// Result of the rune scorer for one pattern, so tests can assert exactly which
// token it picked.
struct RuneChoice {
  // 0-based position of the chosen token among the '*'-split tokens (1st, 2nd,
  // ...), NOT a character offset into the pattern. REDISEARCH_UNINITIALIZED when
  // no token qualifies.
  int tokenOrdinal;
  size_t len;  // length of the chosen token, in runes
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

static int chooseTokenRune(const char *pattern) { return chooseRune(pattern).tokenOrdinal; }

static int chooseTokenChar(const char *pattern) {
  size_t n = strlen(pattern);
  std::vector<size_t> idx(n + 1), len(n + 1);
  return Suffix_ChooseToken(pattern, n, idx.data(), len.data());
}

// A '?' in the first qualifying (len >= MIN_SUFFIX) token must not prevent a
// token from being chosen. Before the fix (MOD-16054) the rune scorer returned
// REDISEARCH_UNINITIALIZED here, forcing a full-trie brute-force fallback.
TEST_F(SuffixChooseTokenTest, questionMarkInFirstTokenStillSelects) {
  // Each case asserts the chosen token's ordinal (which token in the split) and
  // length, making it explicit which '?'-bearing token survives.
  // Single token "?abcd" (len 5), '?' leading.
  EXPECT_EQ(chooseRune("?abcd").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("?abcd").len, 5u);
  // Single token "ab?cd" (len 5), '?' in the middle.
  EXPECT_EQ(chooseRune("ab?cd").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("ab?cd").len, 5u);
  // Single token "abc?" (len 4), '?' trailing.
  EXPECT_EQ(chooseRune("abc?").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("abc?").len, 4u);
  // Single token "a??cd" (len 5): each '?' must penalize only this candidate,
  // not the running best, so the token is still selectable.
  EXPECT_EQ(chooseRune("a??cd").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("a??cd").len, 5u);
  // Degenerate token "??" (len 2, == MIN_SUFFIX) is a weak but valid filter and
  // must be chosen over a full-trie fallback.
  EXPECT_EQ(chooseRune("??").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("??").len, 2u);
  // "a?c"*"defg": the clean 2nd token "defg" (ordinal 1, len 4) beats the
  // '?'-bearing 1st.
  EXPECT_EQ(chooseRune("a?c*defg").tokenOrdinal, 1);
  EXPECT_EQ(chooseRune("a?c*defg").len, 4u);
  // "a?b"*"c?d": both tokens carry a '?'; the 2nd token "c?d" (ordinal 1, len 3)
  // wins by avoiding the trailing-'*' penalty the 1st token pays.
  EXPECT_EQ(chooseRune("a?b*c?d").tokenOrdinal, 1);
  EXPECT_EQ(chooseRune("a?b*c?d").len, 3u);
  // "abcde?fghij"*"xy": the long 1st token (ordinal 0, len 11) keeps winning
  // despite its '?' and trailing '*', over the short clean 2nd token.
  EXPECT_EQ(chooseRune("abcde?fghij*xy").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("abcde?fghij*xy").len, 11u);
}

// Tokenization splits the pattern on runs of '*': consecutive '*' collapse, and
// leading/trailing '*' are dropped. The scorer returns the chosen token's
// ordinal in that split, which is unrelated to its character offset.
TEST_F(SuffixChooseTokenTest, multipleStarsSelectByTokenOrdinal) {
  // Consecutive and leading/trailing '*' collapse to a single token "abcd".
  EXPECT_EQ(chooseRune("**abcd**").tokenOrdinal, 0);
  EXPECT_EQ(chooseRune("**abcd**").len, 4u);
  // "ab"**"cd": a double '*' still yields just two tokens; the clean trailing
  // "cd" (2nd token, ordinal 1) wins.
  EXPECT_EQ(chooseRune("ab**cd").tokenOrdinal, 1);
  EXPECT_EQ(chooseRune("ab**cd").len, 2u);
  // "ab"*"cd"*"ef": three tokens; the last (3rd token, ordinal 2) avoids the
  // trailing-'*' penalty and wins.
  EXPECT_EQ(chooseRune("ab*cd*ef").tokenOrdinal, 2);
  EXPECT_EQ(chooseRune("ab*cd*ef").len, 2u);
  // Sub-MIN_SUFFIX tokens are skipped for scoring but still counted in the
  // ordinal: "a"/"b"/"cd"/"ef"/"ghij" -> the chosen "ghij" is the 5th token
  // (ordinal 4), even though its character offset is 10. Confirms the result is
  // the split ordinal, not an offset.
  EXPECT_EQ(chooseRune("a*b*cd*ef*ghij").tokenOrdinal, 4);
  EXPECT_EQ(chooseRune("a*b*cd*ef*ghij").len, 4u);
}

// The rune scorer must agree with the char scorer on the chosen token ordinal.
TEST_F(SuffixChooseTokenTest, runeAgreesWithChar) {
  const char *patterns[] = {
      "?abcd", "ab?cd", "a?c*defg", "abc*defg", "ab*cd*efgh", "*magicneedl?*",
      "**abcd**", "ab**cd", "ab*cd*ef", "a*b*cd*ef*ghij",
  };
  for (const char *p : patterns) {
    EXPECT_EQ(chooseTokenRune(p), chooseTokenChar(p)) << "pattern: " << p;
  }
}

// Tokens shorter than MIN_SUFFIX are not usable; with no qualifying token the
// scorer reports UNINITIALIZED so the caller falls back to a brute-force scan.
TEST_F(SuffixChooseTokenTest, noQualifyingTokenIsUninitialized) {
  EXPECT_EQ(chooseTokenRune(""), REDISEARCH_UNINITIALIZED);       // no tokens
  EXPECT_EQ(chooseTokenRune("*"), REDISEARCH_UNINITIALIZED);      // only '*'
  EXPECT_EQ(chooseTokenRune("**"), REDISEARCH_UNINITIALIZED);     // only '*'
  EXPECT_EQ(chooseTokenRune("a"), REDISEARCH_UNINITIALIZED);      // single short token
  EXPECT_EQ(chooseTokenRune("a*b*c"), REDISEARCH_UNINITIALIZED);  // all tokens len 1
  // A '?' below MIN_SUFFIX is still too short to be a usable filter: it must be
  // skipped, not force a selection.
  EXPECT_EQ(chooseTokenRune("?"), REDISEARCH_UNINITIALIZED);      // single '?'
  EXPECT_EQ(chooseTokenRune("*?*"), REDISEARCH_UNINITIALIZED);    // token "?" len 1
  EXPECT_EQ(chooseTokenRune("a*?*b"), REDISEARCH_UNINITIALIZED);  // "a","?","b" all len 1
}
