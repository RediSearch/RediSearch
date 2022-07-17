
// Influenced by
/*
 ***********************************************************************
 *                 C++ Wildcard Pattern Matching Library               *
 *                                                                     *
 * Author: Arash Partow (2001)                                         *
 * URL: https://www.partow.net/programming/WildcardMatching/index.html *
 *                                                                     *
 * Copyright notice:                                                   *
 * Free use of the C++ Wildcard Pattern Matching Library is permitted  *
 * under the guidelines and in accordance with the most current        *
 * version of the MIT License.                                         *
 * https://www.opensource.org/licenses/MIT                             *
 *                                                                     *
 ***********************************************************************
*/

#pragma once

#include <string.h>

#include "trie/rune_util.h"

typedef enum {
  FULL_MATCH = 0,
  PARTIAL_MATCH = 1,
  NO_MATCH = 2,
} match_t;

/* Check string vs pattern for a match.
 * Return FULL_MATCH for a match.
 * Return PARTIAL_MATCH if there is no match so far but a match is possible with additional characters
 * Return NO_MATCH if match is no possible. */
match_t Wildcard_MatchChar(const char *pattern, size_t p_len, const char *str, size_t str_len);
match_t Wildcard_MatchRune(const rune *pattern, size_t p_len, const rune *str, size_t str_len);

/* Moves '?' before '*' and removes multiple '*' */
size_t Wildcard_TrimPattern(char *pattern, size_t p_len);

/* Removes '\\' */
size_t Wildcard_RemoveEscape(char *str, size_t len);

/* Breaks wildcard at '*'s */
int Wildcard_StarBreak(const char *str, size_t len, size_t *tokenIdx, size_t *tokenLen);
