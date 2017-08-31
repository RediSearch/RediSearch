#ifndef TOKSEP_H
#define TOKSEP_H

#include <stdint.h>
#include <stdlib.h>
//! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ _ ` { | } ~
static const char ToksepMap_g[256] = {
        [' '] = 1, ['\t'] = 1, [','] = 1, ['.'] = 1, ['/'] = 1, ['('] = 1, [')'] = 1,
        ['{'] = 1, ['}'] = 1,  ['['] = 1, [']'] = 1, [':'] = 1, [';'] = 1, ['\\'] = 1,
        ['~'] = 1, ['!'] = 1,  ['@'] = 1, ['#'] = 1, ['$'] = 1, ['%'] = 1, ['^'] = 1,
        ['&'] = 1, ['*'] = 1,  ['-'] = 1, ['='] = 1, ['+'] = 1, ['|'] = 1, ['\''] = 1,
        ['`'] = 1, ['"'] = 1,  ['<'] = 1, ['>'] = 1, ['?'] = 1,
};

/**
 * This function acts exactly like strsep, but is optimized specifically to
 * separate tokens. This means ignoring things like newlines, and so on.
 */
static inline char *toksep(char **s) {
  uint8_t *pos = (uint8_t *)*s;
  char *orig = *s;
  for (; *pos; ++pos) {
    if (ToksepMap_g[*pos]) {
      *s = (char *)++pos;
      break;
    }
  }
  if (!*pos) {
    *s = NULL;
  }
  return orig;
}

#endif