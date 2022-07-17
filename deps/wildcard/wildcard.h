
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

match_t Wildcard_MatchChar(const char *pattern, size_t p_len, const char *str, size_t str_len);
match_t Wildcard_MatchRune(const rune *pattern, size_t p_len, const rune *str, size_t str_len);
size_t Wildcard_TrimPattern(char *pattern, size_t p_len);
size_t Wildcard_RemoveEscape(char *str, size_t len);
int Wildcard_StarBreak(const char *str, size_t len, size_t *tokenIdx, size_t *tokenLen);