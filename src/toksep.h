/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef TOKSEP_H
#define TOKSEP_H

#include <stdint.h>
#include <stdlib.h>
#include <delimiters.h>

//! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ ` { | } ~
static const char ToksepMap_g[256] = {
    [' '] = 1, ['\t'] = 1, [','] = 1,  ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1, ['{'] = 1,
    ['}'] = 1, ['['] = 1,  [']'] = 1,  [':'] = 1, [';'] = 1, ['~'] = 1, ['!'] = 1, ['@'] = 1,
    ['#'] = 1, ['$'] = 1,  ['%'] = 1,  ['^'] = 1, ['&'] = 1, ['*'] = 1, ['-'] = 1, ['='] = 1,
    ['+'] = 1, ['|'] = 1,  ['\''] = 1, ['`'] = 1, ['"'] = 1, ['<'] = 1, ['>'] = 1, ['?'] = 1,
};

/**
 * Function reads string pointed to by `s` and indicates the length of the next
 * token in `tokLen`. `s` is set to NULL if this is the last token.
 */
static inline char *toksep(char **s, size_t *tokLen, DelimiterList *dl) {
  char delimiter;
  const char *map;

  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if(dl != NULL) {
      delimiter = dl->delimiterMap[*pos];
    } else {
      delimiter = 0;
    }
    if ((ToksepMap_g[*pos] && delimiter == 0) && ((char *)pos == orig || *(pos - 1) != '\\')) {
      *s = (char *)++pos;
      *tokLen = ((char *)pos - orig) - 1;
      if (!*pos) {
        *s = NULL;
      }
      return orig;
    }
  }

  // Didn't find a terminating token. Use a simpler length calculation
  *s = NULL;
  *tokLen = (char *)pos - orig;
  return orig;
}

static inline int istoksep(int c, DelimiterList *dl) {
  int delimiter = 0;
  if(dl != NULL) {
    delimiter = dl->delimiterMap[(uint8_t)c];
  }
  return (ToksepMap_g[(uint8_t)c] != 0 && delimiter == 0);
}

#endif
