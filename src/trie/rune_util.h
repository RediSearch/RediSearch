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

rune *strToFoldedRunes(const char *str, size_t *len, bool dynamic, rune *buf = NULL);

// Convert a utf-8 string to constant width runes
rune *strToRunes(const char *str, size_t *len, bool dynamic, rune *buf = NULL);

// Decode a string to a rune in-place
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

int runecmp(const rune *sa, size_t na, const rune *sb, size_t nb);

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

  Runes(const Runes &runes) { copy(runes); }
  Runes(const rune *runes, size_t len) { copy(runes, len); }

  ~Runes();

  bool dynamic;
  rune _runes_s[RUNE_STATIC_ALLOC_SIZE + 1];
  rune *_runes;
  size_t _len;

  void copy(const rune *runes, size_t len) {
    dynamic = len > RUNE_STATIC_ALLOC_SIZE;
    if (dynamic) {
      _runes = (rune *) rm_malloc((len + 1) * sizeof(rune));
    } else {
      _runes = _runes_s;
    }
    memcpy(_runes, runes, len);
    _runes[len] = '\0';
    _len = len;
  }

  void append(Runes str, size_t len) {
    size_t nlen = _len + len;
    rune nstr[nlen + 1];
    memcpy(nstr, &_runes[0], sizeof(rune) * _len);
    memcpy(&nstr[_len], str[0], sizeof(rune) * len);
    nstr[nlen] = 0;
    copy(*nstr, nlen);
  }

  void copy(const Runes &runes) { copy(_runes, _len); }

  size_t len() const { return _len; }
  bool empty() const { return !_runes || !_len;}

  rune *operator*() { return _runes; }
  const rune *operator*() const { return _runes; }

  bool operator!() const { return !_runes; }

  rune &operator[](int i) { return _runes[i]; }
  const rune &operator[](int i) const { return _runes[i]; }

  char *toUTF8(size_t *utflen) const { return runesToStr(_runes, _len, utflen); }

  bool operator<(const Runes &r) const {
    return runecmp(_runes, _len, r._runes, r._len) < 0;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////
