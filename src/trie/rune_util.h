#ifndef __RUNE_UTIL_H__
#define __RUNE_UTIL_H__

#include "../dep/libnu/libnu.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Internally, the trie works with 16/32 bit "Runes", i.e. fixed width unicode
 * characters. 16 bit shuold be fine for most use cases */
#ifdef TRIE_32BIT_RUNES
typedef uint32_t rune;
#else  // default - 16 bit runes
typedef uint16_t rune;
#endif

/* fold rune: assumes rune is of the correct size */
rune runeFold(rune r);

/* Convert a rune string to utf-8 characters */
char *runesToStr(const rune *in, size_t len, size_t *utflen);

rune *strToFoldedRunes(const char *str, size_t *len);

/* Convert a utf-8 string to constant width runes */
rune *strToRunes(const char *str, size_t *len);

/* Decode a string to a rune in-place */
size_t strToRunesN(const char *s, size_t slen, rune *outbuf);

#ifdef __cplusplus
}
#endif
#endif