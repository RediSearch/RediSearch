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

// The maximum size we allow converting to at once
#define MAX_RUNESTR_LEN 1024

// Threshold for Small String Optimization (SSO)
#define SSO_MAX_LENGTH 128

/* A callback for a rune transformation function */
typedef rune (*runeTransform)(rune r);

/* fold rune: assumes rune is of the correct size */
rune runeFold(rune r);

/* Convert rune to lowercase */
rune runeLower(rune r);

/* Convert a rune string to utf-8 characters */
char *runesToStr(const rune *in, size_t len, size_t *utflen);

/* Convert a string to runes, lowercase them and return the transformed runes.
 * This function supports lowercasing of multi-codepoint runes. */

/* This function reads a UTF-8 encoded string, transforms it to lowercase,
 * and returns a dynamically allocated array of runes (32-bit integers).
 * Parameters:
 * - str: The input UTF-8 encoded string.
 * - utf8_len: The length of the input string in bytes.
 * - unicode_len: A pointer to a size_t variable where the length of the
 *   resulting array of runes will be stored. Must be non-NULL.
 */
rune *strToLowerRunes(const char *str, size_t utf8_len, size_t *unicode_len);

/* Convert a string to runes, fold them and return the folded runes.
 * If a folded runes contains more than one codepoint, only the first
 * codepoint is taken, the rest are ignored. */
rune *strToSingleCodepointFoldedRunes(const char *str, size_t *len);

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
  rune *target;
  if (n > RUNE_STATIC_ALLOC_SIZE) {
    buf->isDynamic = 1;
    target = buf->u.p = (rune *)rm_malloc((n + 1) * sizeof(rune));
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

#ifdef __cplusplus
}
#endif
