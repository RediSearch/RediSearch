/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef TOKSEP_H
#define TOKSEP_H

#include <stdint.h>
#include <stdlib.h>
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
static inline char *toksep(char **s, size_t *tokLen) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  int escaped = 0;
  for (; *pos; ++pos) {
    if (ToksepMap_g[*pos] && !escaped) {
      *s = (char *)++pos;
      *tokLen = ((char *)pos - orig) - 1;
      if (!*pos) {
        *s = NULL;
      }
      return orig;
    }
    escaped = !escaped && *pos == '\\';
  }

  // Didn't find a terminating token. Use a simpler length calculation
  *s = NULL;
  *tokLen = (char *)pos - orig;
  return orig;
}

static inline int istoksep(int c) {
  return ToksepMap_g[(uint8_t)c] != 0;
}

#endif