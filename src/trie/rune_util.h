#pragma once

#include "libnu/libnu.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Internally, the trie works with 16/32 bit "Runes", i.e. fixed width unicode characters.
// 16 bit shuold be fine for most use cases.

#ifdef TRIE_32BIT_RUNES
typedef uint32_t rune;
#else  // default - 16 bit runes
typedef uint16_t rune;
#endif

// fold rune: assumes rune is of the correct size
rune runeFold(rune r);

// Convert a rune string to utf-8 characters
char *runesToStr(const rune *in, size_t len, size_t *utflen);

rune *strToFoldedRunes(const char *str, size_t *len, bool &dynamic, rune *buf = NULL);

// Convert a utf-8 string to constant width runes
rune *strToRunes(const char *str, size_t *len, bool &dynamic, rune *buf = NULL);

// Decode a string to a rune in-place
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

//---------------------------------------------------------------------------------------------

#define RUNE_STATIC_ALLOC_SIZE 127

struct Runes {
  enum class Folded { No, Yes };

  Runes(const char *str, Folded folded = Folded::No) {
    rune *p;
    if (folded == Folded::No) {
      _runes = strToRunes(str, &_len, dynamic, _runes_s);
    } else {
      _runes = strToFoldedRunes(str, &_len, dynamic, _runes_s);
    }
  }
  ~Runes();

  bool dynamic;
  rune _runes_s[RUNE_STATIC_ALLOC_SIZE + 1];
  rune *_runes;
  size_t _len;

  size_t len() const { return _len; }
  rune *operator*() { return _runes; }
  const rune *operator*() const { return _runes; }
  bool operator!() const { return !_runes; }
  rune &operator[](int i) { return _runes[i]; }
  rune operator[](int i) const { return _runes[i]; }

  char *toUTF8(size_t *utflen) const { return runesToStr(_runes, _len, utflen); }
};

///////////////////////////////////////////////////////////////////////////////////////////////
