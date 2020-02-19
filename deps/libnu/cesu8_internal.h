#ifndef NU_CESU8_INTERNAL_H
#define NU_CESU8_INTERNAL_H

#include "utf8_internal.h"

static inline
unsigned cesu8_char_length(const char c) {
	if ((unsigned char)(c) == 0xED) {
		return 6;
	}

	return utf8_char_length(c);
}

static inline
void cesu8_6b(const char *p, uint32_t *codepoint) {
	const unsigned char *up = (const unsigned char *)(p);

	/* CESU-8: 11101101 1010xxxx 10xxxxxx 11101101 1011xxxx 10xxxxxx
	 *
	 *                                             |__ 1st unicode octet
	 * 1010xxxx      -> 0000xxxx 00000000 00000000 |
	 *                  --------
	 * 10xxxxxx << 2 -> 0000xxxx xxxxxx00 00000000 |__ 2nd unicode octet
	 * 1011xxxx >> 2 -> 0000xxxx xxxxxxxx 00000000 |
	 *                           --------
	 * 1011xxxx << 6 -> 0000xxxx xxxxxxxx xx000000 |__ 3rd unicode octet
	 * 10xxxxxx      -> 0000xxxx xxxxxxxx xxxxxxxx |
	 *                                    --------  */
	*codepoint =
	(((*(up + 1) & 0x0F) + 1) << 16)
	| (((*(up + 2) & 0x3F) << 2 | (*(up + 4) & 0x0C) >> 2) << 8)
	| ((*(up + 4) & 0x03) << 6 | (*(up + 5) & 0x3F));
}

static inline
unsigned cesu8_codepoint_length(uint32_t codepoint) {
	if (codepoint > 0xFFFF) {
		return 6;
	}

	return utf8_codepoint_length(codepoint);
}

static inline
void b6_cesu8(uint32_t codepoint, char *p) {
	unsigned char *up = (unsigned char *)(p);

	/* UNICODE: 0000xxxx xxxxxxxx xxxxxxxx
	 *
	 *                -> 11101101 10100000 10000000 11101101 10110000 10000000
	 *                                                                         |__ 2nd CESU-8 octet
	 * 0000xxxx >> 16 -> 11101101 1010xxxx 10000000 11101101 10110000 10000000 |
	 *                            --------
	 *                                                                         |__ 3rd CESU-8 octet
	 * xxxxxxxx >> 10  -> 11101101 1010xxxx 10xxxxxx 11101101 10110000 10000000 |
	 *                                     --------
	 * xxxxxxxx >> 6  -> 11101101 1010xxxx 10xxxxxx 11101101 1011xx00 10000000 |__ 5th CESU-8 octet
	 * xxxxxxxx >> 6  -> 11101101 1011xxxx 10xxxxxx 11101101 1011xxxx 10000000 |
	 *                                                       --------
	 *                                                                         |__ 6th CESU-8 octet
	 * xxxxxxxx       -> 11101101 1011xxxx 10xxxxxx 11101101 1011xxxx 10xxxxxx |
	 *                                                                --------  */
	*(up) = 0xED;
	*(up + 1) = 0xA0 | (((codepoint & 0x1F0000) >> 16) - 1);
	*(up + 2) = 0x80 | (codepoint & 0xFC00) >> 10;
	*(up + 3) = 0xED;
	*(up + 4) = 0xB0 | (codepoint & 0x0C00) >> 6 | (codepoint & 0xC0) >> 6;
	*(up + 5) = 0x80 | (codepoint & 0x3F);
}

#endif /* NU_CESU8_INTERNAL_H */
