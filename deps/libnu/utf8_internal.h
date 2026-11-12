#ifndef NU_UTF8_INTERNAL_H
#define NU_UTF8_INTERNAL_H

#include <sys/types.h>

static inline
unsigned utf8_char_length(const char c) {
	const unsigned char uc = c;

	if ((uc & 0x80) == 0) {
		return 1;
	}
	if ((uc & 0xE0) == 0xC0) {
		return 2;
	}
	if ((uc & 0xF0) == 0xE0) {
		return 3;
	}
	if ((uc & 0xF8) == 0xF0) {
		return 4;
	}

	return 0; /* undefined */
}

static inline
void utf8_2b(const char *p, uint32_t *codepoint) {
	const unsigned char *up = (const unsigned char *)(p);

	/* UTF-8: 110yyyyy 10xxxxxx
	 *                                    |
	 *                                    |__ 1st unicode octet
	 * 110yyy00 << 6 -> 00000yyy 00000000 |
	 *                       ---
	 * 110000yy << 6 -> 00000yyy yy000000 |
	 *                           --       |__ 2nd unicode octet
	 * 10xxxxxx      -> 00000yyy yyxxxxxx |
	 *                             ------  */
	*codepoint = (*(up) & 0x1C) << 6
	| ((*(up) & 0x03) << 6 | (*(up + 1) & 0x3F));
}

static inline
void utf8_3b(const char *p, uint32_t *codepoint) {
	const unsigned char *up = (const unsigned char *)(p);

	/* UTF-8: 1110zzzz 10yyyyyy 10xxxxxx
	 *
	 * 1110zzzz << 12 -> zzzz0000 0000000 |
	 *                   ----             |__ 1st unicode octet
	 * 10yyyy00 << 6  -> zzzzyyyy 0000000 |
	 *                       ----
	 * 100000yy << 6  -> zzzzyyyy yy00000 |
	 *                            --      |__ 2nd unicode octet
	 * 10xxxxxx       -> zzzzyyyy yyxxxxx |
	 *                              -----  */
	*codepoint =
	((*(up) & 0x0F) << 12 | (*(up + 1) & 0x3C) << 6)
	| ((*(up + 1) & 0x03) << 6 | (*(up + 2) & 0x3F));
}

static inline
void utf8_4b(const char *p, uint32_t *codepoint) {
	const unsigned char *up = (const unsigned char *)(p);

	/* UTF-8: 11110uuu 10uuzzzz 10yyyyyy 10xxxxxx
	 *
	 * 11110uuu << 18 -> 00uuu00 00000000 00000000 |
	 *                     ---                     |__ 1st unicode octet
	 * 10uu0000 << 12 -> 00uuuuu 00000000 00000000 |
	 *                        --
	 * 1000zzzz << 12 -> 00uuuuu zzzz0000 00000000 |
	 *                           ----              |__ 2nd unicode octet
	 * 10yyyy00 << 6  -> 00uuuuu zzzzyyyy 00000000 |
	 *                               ----
	 * 100000yy << 6  -> 00uuuuu zzzzyyyy yy000000 |
	 *                                    --       |__ 3rd unicode octet
	 * 10xxxxxx       -> 00uuuuu zzzzyyyy yyxxxxxx |
	 *                                      ------  */
	 *codepoint =
	((*(up) & 0x07) << 18 | (*(up + 1) & 0x30) << 12)
	| ((*(up + 1) & 0x0F) << 12 | (*(up + 2) & 0x3C) << 6)
	| ((*(up + 2) & 0x03) << 6 | (*(up + 3) & 0x3F));
}

static inline
unsigned utf8_codepoint_length(uint32_t codepoint) {
	if (codepoint < 128) {
		return 1;
	}
	if (codepoint < 0x0800) {
		return 2;
	}
	if (codepoint < 0x10000) {
		return 3;
	}

	return 4; /* de facto max length in UTF-8 */
}

static inline
void b2_utf8(uint32_t codepoint, char *p) {
	unsigned char *up = (unsigned char *)(p);

	/* UNICODE: 00000yyy yyxxxxxx
	 *
	 *                                |__ 1st UTF-8 octet
	 * bits 6-10 -> 110yyyyy 10000000 |
	 *                 -----
	 *                                |__ 2nd UTF-8 octet
	 * bits  0-5 -> 110yyyyy 10xxxxxx |
	 *                         ------  */
	*(up) = (0xC0 | (codepoint & 0xFF00) >> 6 | (codepoint & 0xFF) >> 6);
	*(up + 1) = (0x80 | (codepoint & 0x3F));
}

static inline
void b3_utf8(uint32_t codepoint, char *p) {
	unsigned char *up = (unsigned char *)(p);

	/* UNICODE: zzzzyyyy yyxxxxxx
	 *                                          |__ 1st UTF-8 octet
	 * bits 12-15 -> 1110zzzz 10000000 10000000 |
	 *                   ----
	 *                                          |__ 2nd UTF-8 octet
	 * bits  6-11 -> 1110zzzz 10yyyyyy 10000000 |
	 *                          ------
	 *                                          |__ 3rd UTF-8 octet
	 * bits   0-5 -> 1110zzzz 10yyyyyy 10xxxxxx |
	 *                                   ------  */
	*(up) = (0xE0 | (codepoint & 0xF000) >> 12);
	*(up + 1) = (0x80 | (codepoint & 0x0F00) >> 6 | (codepoint & 0xC0) >> 6);
	*(up + 2) = (0x80 | (codepoint & 0x3F));
}

static inline
void b4_utf8(uint32_t codepoint, char *p) {
	unsigned char *up = (unsigned char *)(p);

	/* UNICODE: 000uuuuu zzzzyyyy yyxxxxxx
	 *                                                       |__ 1st UTF-8 octet
	 * bits 18-20 -> 11110uuu 10000000 10000000 10000000     |
	 *                    ---
	 *                                                       |__ 2nd UTF-8 octet
	 * bits 12-17 -> 11110uuu 10uuzzzz 10000000 10000000     |
	 *                          ------
	 *                                                       |__ 3rd UTF-8 octet
	 * bits  6-11 -> 11110uuu 10uuzzzz 10yyyyyy 10000000     |
	 *                                   ------
	 *                                                       |__ 4th UTF-8 octet
	 * bits   0-5 -> 11110uuu 10uuzzzz 10yyyyyy 10xxxxxx     |
	 *                                            ------      */
	*(up) = (0xF0 | ((codepoint & 0x1C0000) >> 18));
	*(up + 1) = (0x80 | ((codepoint & 0x03F000) >> 12));
	*(up + 2) = (0x80 | ((codepoint & 0x000FC0) >> 6));
	*(up + 3) = (0x80 | (codepoint & 0x3F));
}

static inline
int utf8_validread_basic(const char *p, size_t max_len) {
	const unsigned char *up = (const unsigned char *)(p);

	/* it should be 0xxxxxxx or 110xxxxx or 1110xxxx or 11110xxx
	 * latter should be followed by number of 10xxxxxx */

	unsigned len = utf8_char_length(*p);

	/* codepoints longer than 6 bytes does not currently exist
	 * and not currently supported
	 * TODO: longer UTF-8 sequences support
	 */
	if (max_len < len) {
		return 0;
	}

	switch (len) {
		case 1: return 1; /* one byte codepoint */
		case 2: return ((*(up + 1) & 0xC0) == 0x80 ? 2 : 0);
		case 3: return ((*(up + 1) & 0xC0) == 0x80
		&& (*(up + 2) & 0xC0) == 0x80 ? 3 : 0);

		case 4: return ((*(up + 1) & 0xC0) == 0x80
		&& (*(up + 2) & 0xC0) == 0x80
		&& (*(up + 3) & 0xC0) == 0x80 ? 4 : 0);
	}

	return 0;
}

#endif /* NU_UTF8_INTERNAL_H */
