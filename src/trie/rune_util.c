#include "../dep/libnu/libnu.h"
#include "rune_util.h"
#include <stdlib.h>
#include <string.h>
#include "rmalloc.h"

// The maximum size we allow converting to at once
#define MAX_RUNESTR_LEN 1024

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
  if (len > MAX_RUNESTR_LEN) {
    if (utflen) *utflen = 0;
    return NULL;
  }
  uint32_t unicode[len + 1];
  for (int i = 0; i < len; i++) {
    unicode[i] = (uint32_t)in[i];
  }
  unicode[len] = 0;

  *utflen = nu_bytelen(unicode, nu_utf8_write);
  char *ret = rm_calloc(1, *utflen + 1);

  nu_writestr(unicode, ret, nu_utf8_write);
  return ret;
}

/* implementation is identical to that of
 * strToRunes except for line where __fold is called */
rune *strToFoldedRunes(const char *str, size_t *len) {

  ssize_t rlen = nu_strlen(str, nu_utf8_read);
  if (rlen > MAX_RUNESTR_LEN) {
    if (len) *len = 0;
    return NULL;
  }

  uint32_t decoded[rlen + 1];
  decoded[rlen] = 0;
  nu_readstr(str, decoded, nu_utf8_read);

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
  if (rlen > MAX_RUNESTR_LEN) {
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
