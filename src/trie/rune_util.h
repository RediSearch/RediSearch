/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "libnu/libnu.h"
#include "rmalloc.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Internally, the trie works with 16-bit "Runes", i.e. fixed-width Unicode
 * characters limited to the Basic Multilingual Plane (U+0000..U+FFFF).
 * Codepoints above U+FFFF are truncated on conversion. */
typedef uint16_t rune;

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

/* Convert a UTF-8 byte buffer to runes, fold them and return the folded
 * runes. If a folded rune contains more than one codepoint, only the
 * first codepoint is taken, the rest are ignored.
 * Parameters:
 * - str: The input UTF-8 encoded buffer (need not be NUL-terminated).
 * - utf8_len: The length of the input buffer in bytes.
 * - len: A pointer to a size_t where the rune-count is written.
 *   May be NULL. */
rune *strToSingleCodepointFoldedRunes(const char *str, size_t utf8_len, size_t *len);

/* Convert a utf-8 string to constant width runes */
rune *strToRunes(const char *str, size_t *len);

/* Decode a string to a rune in-place */
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

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
