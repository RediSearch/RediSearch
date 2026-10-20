/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "rune_util.h"
#include "rmalloc.h"
#include "libnu/casemap.h"
#include "libnu/extra.h"
#include "libnu/strings.h"
#include "libnu/utf8.h"

/* nu_utf8_read sizes a sequence from its lead byte alone and has no end bound,
 * so a lead byte among the last three bytes makes it read past the buffer.
 * Such a sequence is decoded from a zero-padded copy instead. The bytes past
 * `end` read as 0, as the first of them does in a NUL-terminated string, so
 * every sequence that fits decodes exactly as before. */
static const char *utf8ReadBounded(const char *p, const char *end, uint32_t *codepoint) {
  size_t avail = end - p;
  if (avail >= 4) {
    return nu_utf8_read(p, codepoint);
  }
  char padded[4] = {0};
  memcpy(padded, p, avail);
  size_t consumed = nu_utf8_read(padded, codepoint) - padded;
  return consumed < avail ? p + consumed : end;
}

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

char *runesToStr(const rune *in, size_t len, size_t *utflen) {
  if (len > MAX_RUNE_STR_LEN) {
    *utflen = 0;
    return NULL;
  }

  uint32_t u_stack_buffer[SSO_MAX_LENGTH];
  uint32_t *unicode = u_stack_buffer;

  if (len > SSO_MAX_LENGTH - 1) {
    unicode = rm_malloc((len + 1) * sizeof(*unicode));
    if (!unicode) {
      *utflen = 0;
      return NULL;
    }
  }

  for (int i = 0; i < len; i++) {
    unicode[i] = (uint32_t)in[i];
  }
  unicode[len] = 0;

  size_t bytelen = nu_bytelen(unicode, nu_utf8_write);
  char *ret = rm_calloc(1, bytelen + 1);

  nu_writestr(unicode, ret, nu_utf8_write);
  if (unicode != u_stack_buffer) {
    rm_free(unicode);
  }
  *utflen = bytelen;
  return ret;
}

/* Lowers `str` up to its first decoded NUL, writing at most `cap` runes to
 * `out`, and returns the lowered length. Once that length exceeds
 * MAX_RUNE_STR_LEN it stops early and returns a value above the limit, so a
 * caller that rejects such strings never walks the rest. Sizing and filling
 * both go through here so the buffer always fits what is written to it. */
static size_t lowerRunes(const char *str, const char *end, rune *out, size_t cap) {
  size_t n = 0;
  for (const char *p = str; p < end && n <= MAX_RUNE_STR_LEN;) {
    uint32_t codepoint;
    p = utf8ReadBounded(p, end, &codepoint);
    if (codepoint == 0) {
      break;
    }
    const char *map = nu_tolower(codepoint);
    if (map == NULL) {
      if (n < cap) out[n] = codepoint;
      n++;
      continue;
    }
    uint32_t mu;
    while ((map = nu_casemap_read(map, &mu)), mu != 0) {
      if (n < cap) out[n] = mu;
      n++;
    }
  }
  return n;
}

rune *strToLowerRunes(const char *str, size_t utf8_len, size_t *unicode_len) {
  const char *end = str + utf8_len;
  size_t rlen = lowerRunes(str, end, NULL, 0);
  if (rlen > MAX_RUNE_STR_LEN) {
    *unicode_len = 0;
    return NULL;
  }

  rune *ret = rm_calloc(rlen + 1, sizeof(rune));
  lowerRunes(str, end, ret, rlen);
  *unicode_len = rlen;

  return ret;
}

rune *strToSingleCodepointFoldedRunes(const char *str, size_t utf8_len, size_t *len) {

  const char *end = str + utf8_len;
  ssize_t rlen = 0;
  for (const char *p = str; p < end; rlen++) {
    uint32_t codepoint;
    p = utf8ReadBounded(p, end, &codepoint);
    if (codepoint == 0) {
      break;
    }
  }
  if (rlen > MAX_RUNE_STR_LEN) {
    if (len) *len = 0;
    return NULL;
  }

  rune *ret = rm_calloc(rlen + 1, sizeof(rune));
  const char *p = str;
  for (int i = 0; i < rlen; i++) {
    uint32_t runelike;
    p = utf8ReadBounded(p, end, &runelike);
    ret[i] = (rune)__fold(runelike);
  }
  if (len) *len = rlen;

  return ret;
}

size_t strToRunes(const char *src, size_t slen, rune *out, size_t outcap) {
  const char *end = src + slen;
  size_t nout = 0;
  while (src < end) {
    uint32_t cp;
    src = utf8ReadBounded(src, end, &cp);
    if (cp == 0) {
      break;
    }
    // Keep counting past `outcap` so the caller can detect truncation.
    if (nout < outcap) {
      out[nout] = (rune)cp;
    }
    nout++;
  }
  return nout;
}
