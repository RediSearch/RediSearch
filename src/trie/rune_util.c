#include "../dep/libnu/libnu.h"
#include "rune_util.h"

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
  return __fold((uint32_t) r);
}

char *runesToStr(rune *in, size_t len, size_t *utflen) {

  uint32_t unicode[len + 1];
  for (int i = 0; i < len; i++) {
    unicode[i] = (uint32_t)in[i];
  }
  unicode[len] = 0;

  *utflen = nu_bytelen(unicode, nu_utf8_write);

  char *ret = calloc(1, *utflen + 1);

  nu_writestr(unicode, ret, nu_utf8_write);
  return ret;
}

/* implementation is identical to that of
* strToRunes except for line where __fold is called */
rune *strToFoldedRunes(char *str, size_t *len) {

  ssize_t rlen = nu_strlen(str, nu_utf8_read);
  uint32_t decoded[sizeof(uint32_t) * (rlen + 1)];

  nu_readstr(str, decoded, nu_utf8_read);

  rune *ret = calloc(rlen + 1, sizeof(rune));
  for (int i = 0; i < rlen; i++) {
    uint32_t runelike = decoded[i];
    ret[i] = (rune)__fold(runelike);
  }
  if (len)
    *len = rlen;
  
  return ret;
}

rune *strToRunes(char *str, size_t *len) {

  ssize_t rlen = nu_strlen(str, nu_utf8_read);
  uint32_t decoded[sizeof(uint32_t) * (rlen + 1)];

  nu_readstr(str, decoded, nu_utf8_read);

  rune *ret = calloc(rlen + 1, sizeof(rune));
  for (int i = 0; i < rlen; i++) {
    ret[i] = (rune)decoded[i];
  }
  if (len)
    *len = rlen;

  return ret;
}
