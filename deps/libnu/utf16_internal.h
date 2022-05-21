#ifndef NU_UTF16_INTERNAL_H
#define NU_UTF16_INTERNAL_H

#include <sys/types.h>

static inline
uint16_t nu_letohs(const char *p) {
	const unsigned char *up = (const unsigned char *)(p);
	return (*(up + 1) << 8 | *(up));
}

static inline
void nu_htoles(uint16_t s, char *p) {
	unsigned char *up = (unsigned char *)(p);
	*(up) = (s & 0xFF);
	*(up + 1) = ((s & 0xFF00) >> 8);
}

static inline
uint16_t nu_betohs(const char *p) {
	const unsigned char *up = (const unsigned char *)(p);
	return (*(up) << 8 | *(up + 1));
}

static inline
void nu_htobes(uint16_t s, char *p) {
	unsigned char *up = (unsigned char *)(p);
	*(up + 1) = (s & 0xFF);
	*(up) = ((s & 0xFF00) >> 8);
}

static inline
unsigned utf16_char_length(uint16_t c) {
	if (c >= 0xD800 && c <= 0xDBFF) {
		return 4;
	}

	return 2;
}

static inline
unsigned utf16_codepoint_length(uint32_t codepoint) {
	if (codepoint >= 0x10000) {
		return 4;
	}
	return 2;
}

static inline
void b4_utf16(uint32_t codepoint, uint16_t *lead, uint16_t *trail) {
	/** UNICODE: 00000000 0000xxxx xxxxxxyy yyyyyyyy
	 *
	 * 0000xxxx xxxxxxyy >> 10 -> 110110xx xxxxxxxx |__ lead
	 *                                              |
	 * xxxxxxyy yyyyyyyy       -> 110111yy yyyyyyyy |__ trail
	 *                                              |
	 *                                                */
	 *lead = 0xD800 | ((codepoint - 0x10000) & 0x000FFC00) >> 10;
	 *trail = 0xDC00 | (codepoint & 0x03FF);
}

static inline
int utf16_valid_lead(char lead_high_byte) {
	unsigned char up = (unsigned char)(lead_high_byte);
	return (up >= 0xD8 && up <= 0xDB) ? 1 : 0;
}

static inline
int utf16_valid_trail(char trail_high_byte) {
	unsigned char up = (unsigned char)(trail_high_byte);
	return (up >= 0xDC && up <= 0xDF) ? 1 : 0;
}

static inline
int utf16_validread(const char *lead_high_byte, size_t max_len) {

	/* this implementation use the fact that
	 * lead surrogate high byte and trail surrogate high byte
	 * are always 2 bytes away from each other independently
	 * from endianess. therefore pointer passed to this function
	 * is called lead_high_byte and pointing to endianess-dependent
	 * lead's high byte
	 *
	 * e.g.
	 * UTF-16LE: 0x41 0xD8 0x00 0xDC
	 *                ^-------------- lead_high_byte
	 * UTF-16BE: 0xD8 0x41 0xDC 0x00
	 *           ^------------------- lead_high_byte
	 *
	 * note though that max_len is real max_len of original pointer */

	if (utf16_valid_lead(*lead_high_byte) != 0) { /* lead surrogate */
		if (max_len < 4) {
			return 0;
		}

		if (utf16_valid_trail(*(lead_high_byte + 2)) == 0) { /* trail surrogate */
			return 0;
		}

		return 4;
	}

	/* detect misplaced surrogates */
	if (utf16_valid_trail(*lead_high_byte) != 0) {
		return 0;
	}

	return 2;
}

#endif /* NU_UTF16_INTERNAL_H */
