#ifndef __RUNE_UTIL_H__
#define __RUNE_UTIL_H__

#include "../dep/libnu/libnu.h"
#include <stdlib.h>

/* Internally, the trie works with 16/32 bit "Runes", i.e. fixed width unicode
 * characters. 16 bit shuold be fine for most use cases */
#ifdef TRIE_32BIT_RUNES
typedef uint32_t rune;
#else  // default - 16 bit runes
typedef uint16_t rune;
#endif

/* fold rune: assumes rune is of the correct size */
rune runeFold(rune r);

/* Convert a rune string to utf-8 characters */
char *runesToStr(rune *in, size_t len, size_t *utflen);

rune *strToFoldedRunes(char *str, size_t *len);

/* Convert a utf-8 string to constant width runes */
rune *strToRunes(const char *str, size_t *len);

/* Decode a string to a rune in-place */
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

/**
 * RuneBuf can be used for smaller strings which may not require heap allocation
 * for rune conversion.
 */
#define RUNE_STATIC_ALLOC_SIZE 127
typedef struct {
  int isDynamic;
  union {
    rune s[RUNE_STATIC_ALLOC_SIZE + 1];
    rune *p;
  } u;
} RuneBuf;

static inline rune *RuneBuf_Fill(const char *s, size_t n, RuneBuf *buf, size_t *len) {
  // Assume x2 growth.
  *len = n * 2;
  if (*len <= RUNE_STATIC_ALLOC_SIZE) {
    buf->isDynamic = 0;
    *len = strToRunesN(s, n, buf->u.s);
    buf->u.s[*len] = 0;
    return buf->u.s;
  } else {
    buf->isDynamic = 1;
    buf->u.p = strToRunes(s, len);
    return buf->u.p;
  }
}

static inline void RuneBuf_Release(RuneBuf *buf) {
  if (buf->isDynamic) {
    free(buf->u.p);
  }
}

#endif
