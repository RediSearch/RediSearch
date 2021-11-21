#include "utf16be.h"

#ifdef NU_WITH_UTF16BE_READER
#ifdef NU_WITH_VALIDATION

int nu_utf16be_validread(const char *encoded, size_t max_len) {
	if (max_len < 2) {
		return 0;
	}

	return utf16_validread(encoded, max_len);
}

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF16BE_READER */

#ifdef NU_WITH_UTF16BE_WRITER

char* nu_utf16be_write(uint32_t unicode, char *utf16) {
	unsigned codepoint_len = utf16_codepoint_length(unicode);

	if (utf16 != 0) {
		switch (codepoint_len) {
			case 2: nu_htobes((uint16_t)(unicode), utf16); break;
			default: { /* len == 4 */
				uint16_t c0 = 0, c1 = 0;
				b4_utf16(unicode, &c0, &c1);
				nu_htobes(c0, utf16);
				nu_htobes(c1, utf16 + 2);
				break;
			}
		}
	}

	return utf16 + codepoint_len;
}

#endif /* NU_WITH_UTF16BE_WRITER */
