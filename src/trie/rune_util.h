#pragma once

#include "libnu/libnu.h"
#include "rmalloc.h"
#include "object.h"

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
String runesToStr(const rune *in, size_t len);

rune *strToFoldedRunes(const char *str, size_t *len, bool &dynamic, rune *buf = NULL);

// Convert a utf-8 string to constant width runes
rune *strToRunes(const char *str, size_t *len, bool &dynamic, rune *buf = NULL);

// Decode a string to a rune in-place
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

int runecmp(const rune *sa, size_t na, const rune *sb, size_t nb);

//---------------------------------------------------------------------------------------------

#define RUNE_STATIC_ALLOC_SIZE 255

struct Runes {
  enum class Folded { No, Yes };

  Runes(const char *str = "", Folded folded = Folded::No) {
    if (folded == Folded::No) {
      _runes = strToRunes(str, _len, _dynamic, _runes_s);
    } else {
      _runes = strToFoldedRunes(str, _len, _dynamic, _runes_s);
    }
	_nbytes = _len + 1;
  }

  Runes(const char *str, size_t len) { copy(str, len); }
  Runes(const Runes &runes) { copy(runes); }
  Runes(const Runes &runes, size_t offset) { copy(runes, offset); }

  ~Runes();

  bool _dynamic;
  size_t _len; // logical size of rune (not allocated bytes)
  size_t _nbytes; // size of storage
  rune _runes_s[RUNE_STATIC_ALLOC_SIZE + 1];
  rune *_runes;

  void setup_storage(size_t nbytes) {
    _dynamic = nbytes > RUNE_STATIC_ALLOC_SIZE;
    _nbytes = nbytes + 1;
    if (_dynamic) {
      _runes = (rune *) rm_malloc(_nbytes * sizeof(rune));
    } else {
      _runes = _runes_s;
    }
  }

  void fix_storage(size_t new_nbytes) {
    if (!_dynamic && new_nbytes > RUNE_STATIC_ALLOC_SIZE) {
      _runes = (rune *) rm_malloc(new_nbytes + 1);
      memcpy(_runes, _runes_s, _nbytes);
      _dynamic = true;
    }
    _nbytes = new_nbytes;
  }

  void copy(const char *str, size_t str_len) {
    _runes = strToRunes(str, &_nbytes, _dynamic, _runes_s);
    _len = str_len;
  }

  void append(rune r) {
    size_t end = _nbytes;
    fix_storage(_nbytes + 1); // sets _nbytes
    _runes[end] = r;
    _runes[_nbytes] = 0;
    ++_len;
  }

  void append(const Runes &runes) {
    size_t end = _nbytes;
    fix_storage(_nbytes + runes._nbytes); // sets _nbytes
    memcpy(&_runes[end], runes._runes, runes._nbytes);
    _runes[_nbytes] = 0;
    _len += runes._len;
  }

  void pop(size_t len) {
    if (_len > len) {
      _len -= len;
    } else {
      _len = 0;
    }
  }

  void copy(const Runes &runes) {
    setup_storage(runes._nbytes); // sets _nbytes
    memcpy(_runes, runes._runes, runes._nbytes);
    _runes[_nbytes] = 0;
    _len = runes._len;
  }

  void copy(const Runes &runes, size_t offset) {
    if (offset > runes._len) {
	  _runes[0] = 0;
	  return;
	}
    setup_storage(runes._nbytes - offset); // sets _nbytes
    memcpy(_runes, runes._runes + offset, runes._nbytes);
    _runes[_nbytes] = 0;
    _len = runes._len;
  }

  size_t len() const { return _len; }
  bool empty() const { return !_runes || !_len;}
  size_t nbytes() const { return _nbytes; }

  rune *operator*() { return _runes; }
  const rune *operator*() const { return _runes; }

  bool operator!() const { return !_runes; }

  rune &operator[](int i) { return _runes[i]; }
  const rune &operator[](int i) const { return _runes[i]; }

  char *toUTF8(size_t *utflen) const { return runesToStr(_runes, _len, utflen); }
  String toUTF8() const { return runesToStr(_runes, _len); }

  bool operator==(const Runes &r) const {
    if (_nbytes != r._nbytes) return false;
    return !memcpy(_runes, r._runes, _nbytes);
  }

  bool operator<(const Runes &r) const {
    return runecmp(_runes, _len, r._runes, r._len) < 0;
  }

  bool operator>(const Runes &r) const {
    return runecmp(_runes, _len, r._runes, r._len) > 0;
  }

  void operator=(const rune runes) { _runes = runes; } //@@ check this out
};

///////////////////////////////////////////////////////////////////////////////////////////////
