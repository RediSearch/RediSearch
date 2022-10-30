#include "cesu8.h"

#ifdef NU_WITH_CESU8_READER
#ifdef NU_WITH_VALIDATION

int nu_cesu8_validread(const char *encoded, size_t max_len) {
	const unsigned char *up = (const unsigned char *)(encoded);

	/* i guess there is no way to detect misplaceed CESU-8
	 * trail surrogate alone, it will produce valid UTF-8 sequence
	 * greater than U+10000 */

	/* 6-bytes sequence
	 *
	 * 11101101 followed by 1010xxxx should be
	 * then followed by xxxxxxxx 11101101 1011xxxx xxxxxxxx */
	if (*(up) == 0xED && (*(up + 1) & 0xF0) == 0xA0) {
		if (max_len < 6) {
			return 0;
		}

		if (*(up + 3) != 0xED || (*(up + 4) & 0xF0) != 0xB0) {
			return 0;
		}

		return 6;
	}

	return utf8_validread_basic(encoded, max_len);
}

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_CESU8_READER */

#ifdef NU_WITH_CESU8_WRITER

char* nu_cesu8_write(uint32_t unicode, char *cesu8) {
	unsigned codepoint_len = cesu8_codepoint_length(unicode);

	if (cesu8 != 0) {
		switch (codepoint_len) {
		case 1: *cesu8 = (char)(unicode); break;
		case 2: b2_utf8(unicode, cesu8); break;
		case 3: b3_utf8(unicode, cesu8); break;
		default: b6_cesu8(unicode, cesu8); break; /* len == 6 */
		}
	}

	return cesu8 + codepoint_len;
}

#endif /* NU_WITH_CESU8_WRITER */
