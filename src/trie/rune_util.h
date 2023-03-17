/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "libnu/libnu.h"
#include "rmalloc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Internally, the trie works with 16/32 bit "Runes", i.e. fixed width unicode
 * characters. 16 bit should be fine for most use cases */
#ifdef TRIE_32BIT_RUNES
typedef uint32_t rune;
#else  // default - 16 bit runes
typedef uint16_t rune;
#endif

#define RUNE_STATIC_ALLOC_SIZE 127
typedef struct {
  int isDynamic;
  union {
    rune s[RUNE_STATIC_ALLOC_SIZE + 1];
    rune *p;
  } u;
} runeBuf;

/* fold rune: assumes rune is of the correct size */
rune runeFold(rune r);

/* Convert a rune string to utf-8 characters */
char *runesToStr(const rune *in, size_t len, size_t *utflen);

rune *strToFoldedRunes(const char *str, size_t *len);

/* Convert a utf-8 string to constant width runes */
rune *strToRunes(const char *str, size_t *len);

/* Decode a string to a rune in-place */
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

/* similar to strchr */
const rune *runenchr(const rune *r, size_t len, rune c);

static inline rune *runeBufFill(const char *s, size_t n, runeBuf *buf, size_t *len) {
  /**
   * Assumption: the number of bytes in a utf8 string is always greater than the
   * number of codepoints it can produce.
   */
  *len = n;
  rune *target;
  if (*len > RUNE_STATIC_ALLOC_SIZE) {
    buf->isDynamic = 1;
    target = buf->u.p = (rune *)rm_malloc(((*len) + 1) * sizeof(rune));
  } else {
    buf->isDynamic = 0;
    target = buf->u.s;
  }
  *len = strToRunesN(s, n, target);
  target[*len] = 0;
  return target;
}

static inline void runeBufFree(runeBuf *buf) {
  if (buf->isDynamic) {
    rm_free(buf->u.p);
  }
}

/* used for debug */
static inline void printfRune(const rune *rune, size_t len) {
  size_t newlen;
  char *str = runesToStr(rune, len, &newlen);
  printf("%s", str);
  rm_free(str);
}

static inline void printfRuneNL(const rune *rune, size_t len) {
  printfRune(rune, len);
  puts("");
}

#ifdef __cplusplus
}
#endif
