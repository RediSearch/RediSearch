#ifndef __RUNE_UTIL_H__
#define __RUNE_UTIL_H__

#include "../dep/libnu/libnu.h"

/* Internally, the trie works with 16/32 bit "Runes", i.e. fixed with unicode
 * characters. 16 bit shuold be fine for most use cases */
#ifdef TRIE_32BIT_RUNES
    typedef u_int32_t rune;
    #define TRIE_RUNE_MASK 0xffffffff
#else // default - 16 bit runes
    typedef u_int16_t rune;
    #define TRIE_RUNE_MASK 0x0000ffff
#endif

/* Convert a utf-8 string to constant width runes */
rune *__strToRunes(char *str, size_t *len);

/* Convert a rune string to utf-8 characters */
char *__runesToStr(rune *in, size_t len, size_t *utflen);

rune __runeToFold(rune r);

rune *__strToFoldedRunes(char *str, size_t *len);

#endif
