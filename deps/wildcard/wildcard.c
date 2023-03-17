#include <stdio.h>
#include <assert.h>

#include "wildcard.h"

match_t Wildcard_MatchChar(const char *pattern, size_t p_len, const char *str, size_t str_len) {
  const char *pattern_end = pattern + p_len;
  const char *str_end = str + str_len;
  const char *pattern_itr = pattern;
  const char *str_itr = str;

  const char *np_itr = NULL;
  const char *ns_itr = NULL;
  int i = 0;
  while (++i) {
    //printf("%d ", i);
    if (pattern_end != pattern_itr) {
      const char c = *pattern_itr;
      if ((str_end != str_itr) && (c == *str_itr || c == '?')) {
        ++str_itr;
        ++pattern_itr;
        continue;
      } else if (c == '*') {
        while ((pattern_end != pattern_itr) && (*pattern_itr == '*')) {
          ++pattern_itr;
        }
        const char d = *pattern_itr;
        while ((str_end != str_itr) && !(d == *str_itr || d == '?')) {
          ++str_itr;
        }
        np_itr = pattern_itr - 1;
        ns_itr = str_itr + 1;
        continue;
      } 
    } else if (str_end == str_itr) {
      return FULL_MATCH;
    }

    if (str_end == str_itr) {
      return PARTIAL_MATCH;
    } else if (ns_itr == NULL) {
      return NO_MATCH;
    }
    pattern_itr = np_itr;
    str_itr = ns_itr;
  }
  assert(0);
  return FULL_MATCH;
}

match_t Wildcard_MatchRune(const rune *pattern, size_t p_len, const rune *str, size_t str_len) {
  const rune *pattern_end = pattern + p_len;
  const rune *str_end = str + str_len;
  const rune *pattern_itr = pattern;
  const rune *str_itr = str;

  const rune *np_itr = NULL;
  const rune *ns_itr = NULL;
  while (1) {
    if (pattern_end != pattern_itr) {
      const rune c = *pattern_itr;
      if ((str_end != str_itr) && (c == *str_itr || c == '?')) {
        ++str_itr;
        ++pattern_itr;
        continue;
      } else if (c == '*') {
        while ((pattern_end != pattern_itr) && (*pattern_itr == '*')) {
          ++pattern_itr;
        }
        const rune d = *pattern_itr;
        while ((str_end != str_itr) && !(d == *str_itr || d == '?')) {
          ++str_itr;
        }
        np_itr = pattern_itr - 1;
        ns_itr = str_itr + 1;
        continue;
      } 
    } else if (str_end == str_itr) {
      return FULL_MATCH;
    }

    if (str_end == str_itr) {
      return PARTIAL_MATCH;
    } else if (ns_itr == NULL) {
      return NO_MATCH;
    }
    pattern_itr = np_itr;
    str_itr = ns_itr;
  }
  assert(0);
  return FULL_MATCH;
}

size_t Wildcard_TrimPattern(char *pattern, size_t p_len) {
  size_t i = 0;
  size_t runner = 0;

  while (i < p_len) {
    if (pattern[i] == '*') {
      // skip following starts
      while (pattern[i + 1] == '*') {
        ++i;
        //continue;
      }
      // swap ? and *
      if (pattern[i + 1] == '?') {
        pattern[i] = '?';
        pattern[i + 1] = '*';
      }
    }
    pattern[runner++] = pattern[i++];
  }

  pattern[runner] = '\0';
  return runner;
}

size_t Wildcard_RemoveEscape(char *str, size_t len) {
  int i = 0;
  do {
    if (str[i] == '\\') break;
  } while (++i < len && str[i] != '\0');

  // check if we haven't remove any backslash
  if (i == len) {
    return len;
  }

  // skip '\'
  int runner = i;
  for (; i < len; ++i, ++runner) {
    if (str[i] == '\\') {
      ++i;
    }
    // printf("%c %c\n", str[runner], str[i]);
    str[runner] = str[i];
    if (str[runner] == '\0') {
      break;
    }
  }

  str[runner] = '\0';
  return runner;
}
