/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "libnu/libnu.h"
#include "rune_util.h"
#include "rmalloc.h"

#include <stdlib.h>
#include <string.h>

static uint32_t __fold(uint32_t runelike) {
  uint32_t lowered = 0;
  const char *map = 0;
  map = nu_tofold(runelike);
  if (!map) {
    return runelike;
  }
  nu_casemap_read(map, &lowered);
  return lowered;
}

rune runeFold(rune r) {
  return __fold((uint32_t)r);
}

/* Widen runes to a NUL-terminated uint32 codepoint array, using
 * `stackbuf` (of SSO_MAX_LENGTH entries) when len fits and heap storage
 * otherwise. Returns the array to use, or NULL on allocation failure;
 * the caller frees the result only when it differs from `stackbuf`. */
static uint32_t *widenRunes(const rune *in, size_t len, uint32_t *stackbuf) {
  uint32_t *unicode = stackbuf;

  if (len > SSO_MAX_LENGTH - 1) {
    unicode = rm_malloc((len + 1) * sizeof(*unicode));
    if (!unicode) {
      return NULL;
    }
  }

  for (int i = 0; i < len; i++) {
    unicode[i] = (uint32_t)in[i];
  }
  unicode[len] = 0;
  return unicode;
}

char *runesToStr(const rune *in, size_t len, size_t *utflen) {
  if (len > MAX_RUNE_STR_LEN) {
    if (utflen) *utflen = 0;
    return NULL;
  }

  uint32_t u_stack_buffer[SSO_MAX_LENGTH];
  uint32_t *unicode = widenRunes(in, len, u_stack_buffer);
  if (!unicode) {
    *utflen = 0;
    return NULL;
  }

  size_t bytelen = nu_bytelen(unicode, nu_utf8_write);
  char *ret = rm_calloc(1, bytelen + 1);

  nu_writestr(unicode, ret, nu_utf8_write);
  if (unicode != u_stack_buffer) {
    rm_free(unicode);
  }
  *utflen = bytelen;
  return ret;
}

char *runesToStrBuf(const rune *in, size_t len, utf8Buf *buf, size_t *utflen) {
  buf->isDynamic = 0;
  if (len > MAX_RUNE_STR_LEN) {
    *utflen = 0;
    return NULL;
  }

  uint32_t u_stack_buffer[SSO_MAX_LENGTH];
  uint32_t *unicode = widenRunes(in, len, u_stack_buffer);
  if (!unicode) {
    *utflen = 0;
    return NULL;
  }

  size_t bytelen = nu_bytelen(unicode, nu_utf8_write);
  char *ret;
  if (bytelen > UTF8_STATIC_ALLOC_SIZE) {
    buf->isDynamic = 1;
    ret = buf->u.p = rm_malloc(bytelen + 1);
  } else {
    ret = buf->u.s;
  }

  nu_writestr(unicode, ret, nu_utf8_write);
  ret[bytelen] = '\0';
  if (unicode != u_stack_buffer) {
    rm_free(unicode);
  }
  *utflen = bytelen;
  return ret;
}

rune *strToLowerRunes(const char *str, size_t utf8_len, size_t *unicode_len) {

  // determine the length of the folded string
  ssize_t rlen = nu_strtransformnlen(str, utf8_len, nu_utf8_read,
                                     nu_tolower, nu_casemap_read);
  if (rlen > MAX_RUNE_STR_LEN) {
    *unicode_len = 0;
    return NULL;
  }

  rune *ret = rm_calloc(rlen + 1, sizeof(rune));
  const char *encoded_char = str;
  uint32_t codepoint;
  unsigned i = 0;
  while (encoded_char < str + utf8_len) {
    // Read unicode codepoint from utf8 string
    encoded_char = nu_utf8_read(encoded_char, &codepoint);
    // Transform unicode codepoint to lower case
    const char *map = nu_tolower(codepoint);

    // Read the transformed codepoint and store it in the unicode buffer
    if (map != NULL) {
      uint32_t mu;
      while (1) {
        map = nu_casemap_read(map, &mu);
        if (mu == 0) {
          break;
        }
        ret[i++] = mu;
      }
    } else {
        ret[i++] = codepoint;
    }
  }
  *unicode_len = rlen;

  return ret;
}

char *strToLowerStr(const char *str, size_t utf8_len, utf8Buf *buf, size_t *outlen) {
  buf->isDynamic = 0;

  // Same length gate as strToLowerRunes, so a token rejected as too
  // long by one representation is rejected by the other as well.
  ssize_t rlen = nu_strtransformnlen(str, utf8_len, nu_utf8_read,
                                     nu_tolower, nu_casemap_read);
  if (rlen < 0 || rlen > MAX_RUNE_STR_LEN) {
    *outlen = 0;
    return NULL;
  }

  // A UTF-8 codepoint encodes to at most 4 bytes.
  size_t maxBytes = (size_t)rlen * 4;
  char *ret;
  if (maxBytes > UTF8_STATIC_ALLOC_SIZE) {
    buf->isDynamic = 1;
    ret = buf->u.p = rm_malloc(maxBytes + 1);
  } else {
    ret = buf->u.s;
  }

  const char *encoded_char = str;
  char *w = ret;
  uint32_t codepoint;
  while (encoded_char < str + utf8_len) {
    encoded_char = nu_utf8_read(encoded_char, &codepoint);
    const char *map = nu_tolower(codepoint);

    if (map != NULL) {
      uint32_t mu;
      while (1) {
        map = nu_casemap_read(map, &mu);
        if (mu == 0) {
          break;
        }
        w = nu_utf8_write(mu, w);
      }
    } else {
      w = nu_utf8_write(codepoint, w);
    }
  }
  *w = '\0';
  *outlen = w - ret;
  return ret;
}

/* implementation is identical to that of strToRunes except for line where
 * __fold is called.
 * If the folded rune occupies more than 1 codepoint, only the first
 * is used, the rest are ignored. */
rune *strToSingleCodepointFoldedRunes(const char *str, size_t utf8_len, size_t *len) {

  ssize_t rlen = nu_strnlen(str, utf8_len, nu_utf8_read);
  if (rlen > MAX_RUNE_STR_LEN) {
    if (len) *len = 0;
    return NULL;
  }

  uint32_t decoded[rlen + 1];
  decoded[rlen] = 0;
  nu_readnstr(str, utf8_len, decoded, nu_utf8_read);

  rune *ret = rm_calloc(rlen + 1, sizeof(rune));
  for (int i = 0; i < rlen; i++) {
    uint32_t runelike = decoded[i];
    ret[i] = (rune)__fold(runelike);
  }
  if (len) *len = rlen;

  return ret;
}

rune *strToRunes(const char *str, size_t *len) {
  // Determine the length
  ssize_t rlen = nu_strlen(str, nu_utf8_read);
  if (rlen > MAX_RUNE_STR_LEN) {
    if (len) *len = 0;
    return NULL;
  }

  rune *ret = rm_malloc((rlen + 1) * sizeof(rune));
  strToRunesN(str, strlen(str), ret);
  ret[rlen] = '\0';
  if (len) {
    *len = rlen;
  }
  return ret;
}

size_t strToRunesN(const char *src, size_t slen, rune *out) {
  const char *end = src + slen;
  size_t nout = 0;
  while (src < end) {
    uint32_t cp;
    src = nu_utf8_read(src, &cp);
    if (cp == 0) {
      break;
    }
    out[nout++] = (rune)cp;
  }
  return nout;
}
