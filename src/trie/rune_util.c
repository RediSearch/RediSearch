#include "trie/rune_util.h"

#include "libnu/libnu.h"
#include "rmalloc.h"

#include <stdlib.h>
#include <string.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// The maximum size we allow converting to at once
#define MAX_RUNESTR_LEN 1024

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

rune runeFold(rune r) {
  return __fold((uint32_t)r);
}

//---------------------------------------------------------------------------------------------

// Convert a rune string to utf-8 characters

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

String runesToStr(const rune *in, size_t len) {
  if (len > MAX_RUNESTR_LEN) {
    return "";
  }
  uint32_t unicode[len + 1];
  for (int i = 0; i < len; i++) {
    unicode[i] = (uint32_t)in[i];
  }
  unicode[len] = 0;

  size_t utflen = nu_bytelen(unicode, nu_utf8_write);
  String str;
  str.resize(utflen + 1);

  nu_writestr(unicode, str.data(), nu_utf8_write);
  return str;
}

//---------------------------------------------------------------------------------------------

// implementation is identical to that of strToRunes except for line where __fold is called

rune *strToFoldedRunes(const char *str, size_t *len, bool &dynamic, rune *buf) {
  ssize_t rlen = nu_strlen(str, nu_utf8_read);
  if (rlen > MAX_RUNESTR_LEN) {
    if (len) *len = 0;
    return NULL;
  }

  uint32_t decoded[rlen + 1];
  decoded[rlen] = 0;
  nu_readstr(str, decoded, nu_utf8_read);

  rune *ret;
  dynamic = rlen > RUNE_STATIC_ALLOC_SIZE || !buf;
  if (dynamic) {
    ret = rm_calloc(rlen + 1, sizeof(rune));
  } else {
    ret = buf;
  }
  for (int i = 0; i < rlen; i++) {
    uint32_t runelike = decoded[i];
    ret[i] = (rune)__fold(runelike);
  }
  if (len) *len = rlen;

  return ret;
}

//---------------------------------------------------------------------------------------------

// Convert a utf-8 string to constant width runes

rune *strToRunes(const char* str, size_t& len, bool& dynamic, rune* buf) {
  // Determine the length
  ssize_t rlen = nu_strlen(str, nu_utf8_read);
  if (rlen > MAX_RUNESTR_LEN) {
    len = 0;
    return NULL;
  }

  rune *ret;
  dynamic = rlen > RUNE_STATIC_ALLOC_SIZE || !buf;
  if (dynamic) {
    ret = rm_malloc((rlen + 1) * sizeof(rune));
  } else {
    ret = buf;
  }

  strToRunesN(str, strlen(str), ret);
  ret[rlen] = '\0';
  len = rlen;
  return ret;
}

//---------------------------------------------------------------------------------------------

// Decode a string to a rune in-place

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

//---------------------------------------------------------------------------------------------

int runecmp(const rune *sa, size_t na, const rune *sb, size_t nb) {
  size_t minlen = na < nb ? na : nb;
  for (size_t ii = 0; ii < minlen; ++ii) {
    int rc = sa[ii] - sb[ii];
    if (rc == 0) {
      continue;
    }
    return rc;
  }

  // Both strings match up to this point
  if (na > nb) {
    // nb is a substring of na; na is greater
    return 1;
  } else if (nb > na) {
    // na is a substring of nb; nb is greater
    return -1;
  }
  // strings are the same
  return 0;
}

///////////////////////////////////////////////////////////////////////////////////////////////

Runes::~Runes() {
  if (_dynamic) rm_free(_runes);
}

///////////////////////////////////////////////////////////////////////////////////////////////
