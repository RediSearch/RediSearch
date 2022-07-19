#include <ctype.h>
#include "wildcard.h"

/* Glob-style pattern matching. */
int Glob_MatchChar(const char *pattern, int patternLen,
  const char *string, int stringLen, int nocase) {
  if(patternLen == 1 && pattern[0] == '*'){
      return FULL_MATCH;
  }

  while(patternLen && stringLen) {
    switch(pattern[0]) {
    case '*':
      while (patternLen && pattern[1] == '*') {
        pattern++;
        patternLen--;
      }
      if (patternLen == 1)
        return FULL_MATCH; /* match */
      while(stringLen) {
        match_t match = Glob_MatchChar(pattern+1, patternLen-1, string, stringLen, nocase);
        switch (match) {
          case FULL_MATCH: return FULL_MATCH;
          case PARTIAL_MATCH: return PARTIAL_MATCH;
          case NO_MATCH: ;                    
        string++;
        stringLen--;
        }
      }
      return stringLen ? NO_MATCH : PARTIAL_MATCH;

    case '?':
      string++;
      stringLen--;
      break;

    case '[':
    {
      int not, match;

      pattern++;
      patternLen--;
      not = pattern[0] == '^';
      if (not) {
          pattern++;
          patternLen--;
      }
      match = 0;
      while(1) {
        if (pattern[0] == '\\' && patternLen >= 2) {
          pattern++;
          patternLen--;
          if (pattern[0] == string[0])
            match = 1;
        } else if (pattern[0] == ']') {
          break;
        } else if (patternLen == 0) {
          pattern--;
          patternLen++;
          break;
        } else if (patternLen >= 3 && pattern[1] == '-') {
          int start = pattern[0];
          int end = pattern[2];
          int c = string[0];
          if (start > end) {
            int t = start;
            start = end;
            end = t;
          }
          if (nocase) {
            start = tolower(start);
            end = tolower(end);
            c = tolower(c);
          }
          pattern += 2;
          patternLen -= 2;
          if (c >= start && c <= end)
            match = 1;
        } else {
          if (!nocase) {
            if (pattern[0] == string[0])
                match = 1;
          } else {
            if (tolower((int)pattern[0]) == tolower((int)string[0]))
              match = 1;
          }
        }
        pattern++;
        patternLen--;
      }
      if (not)
        match = !match;
      if (!match)
        return NO_MATCH; /* no match */
      string++;
      stringLen--;
      break;
    }

    case '\\':
      if (patternLen >= 2) {
        pattern++;
        patternLen--;
      }
      /* fall through */
    default:
      if (!nocase) {
        if (pattern[0] != string[0])
          return NO_MATCH; /* no match */
      } else {
        if (tolower((int)pattern[0]) != tolower((int)string[0]))
          return NO_MATCH; /* no match */
      }
      string++;
      stringLen--;
      break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while(*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }

  if (stringLen == 0) {
    if (patternLen == 0) {
      return FULL_MATCH;
    } else {
      return PARTIAL_MATCH;
    }
  }
  return NO_MATCH;
}
