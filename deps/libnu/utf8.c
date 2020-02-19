#include "utf8.h"

#ifdef NU_WITH_UTF8_READER
#ifdef NU_WITH_VALIDATION

int nu_utf8_validread(const char *encoded, size_t max_len) {
	int len = utf8_validread_basic(encoded, max_len);

	if (len <= 0) {
		return 0;
	}

	/* Unicode core spec, D92, Table 3-7
	 */

	switch (len) {
	/* case 1: single byte sequence can't be > 0x7F and produce len == 1
	 */

	case 2: {
		uint8_t p1 = *(const unsigned char *)(encoded);

		if (p1 < 0xC2) { /* 2-byte sequences with p1 > 0xDF are 3-byte sequences */
			return 0;
		}

		/* the rest will be handled by utf8_validread_basic() */

		break;
	}

	case 3: {
		uint8_t p1 = *(const unsigned char *)(encoded);

		/* 3-byte sequences with p1 < 0xE0 are 2-byte sequences,
		 * 3-byte sequences with p1 > 0xEF are 4-byte sequences */

		uint8_t p2 = *(const unsigned char *)(encoded + 1);

		if (p1 == 0xE0 && p2 < 0xA0) {
			return 0;
		}
		else if (p1 == 0xED && p2 > 0x9F) {
			return 0;
		}

		/* (p2 < 0x80 || p2 > 0xBF) and p3 will be covered
		 * by utf8_validread_basic() */

		break;
	}

	case 4: {
		uint8_t p1 = *(const unsigned char *)(encoded);

		if (p1 > 0xF4) { /* 4-byte sequence with p1 < 0xF0 are 3-byte sequences */
			return 0;
		}

		uint8_t p2 = *(const unsigned char *)(encoded + 1);

		if (p1 == 0xF0 && p2 < 0x90) {
			return 0;
		}

		/* (p2 < 0x80 || p2 > 0xBF) and the rest (p3, p4)
		 * will be covered by utf8_validread_basic() */

		break;
	}

	} /* switch */

	return len;
}

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF8_READER */

#ifdef NU_WITH_UTF8_WRITER

char* nu_utf8_write(uint32_t unicode, char *utf8) {
	unsigned codepoint_len = utf8_codepoint_length(unicode);

	if (utf8 != 0) {
		switch (codepoint_len) {
			case 1: *utf8 = (char)(unicode); break;
			case 2: b2_utf8(unicode, utf8); break;
			case 3: b3_utf8(unicode, utf8); break;
			default: b4_utf8(unicode, utf8); break; /* len == 4 */
		}
	}

	return utf8 + codepoint_len;
}

#endif /* NU_WITH_UTF8_WRITER */
