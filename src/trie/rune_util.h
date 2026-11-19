/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "rmalloc.h"
// The conversions between strings and runes, implemented in Rust.
#include "string_utils_ffi.h"

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
#define MAX_RUNE_STR_LEN 1024

/* A callback for a rune transformation function */
typedef rune (*runeTransform)(rune r);

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
  *len = strToRunes(s, n, target, n);
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
