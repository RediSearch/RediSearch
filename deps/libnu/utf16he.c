#include "utf16he.h"
#include "utf16_internal.h"

#ifdef NU_WITH_UTF16HE_READER
#ifdef NU_WITH_VALIDATION

int nu_utf16he_validread(const char *encoded, size_t max_len) {
	if (max_len < 2) {
		return 0;
	}

	char lead = (*(uint16_t *)(encoded) & 0xFF00) >> 8;

	if (utf16_valid_lead(lead) != 0) {
		if (max_len < 4) {
			return 0;
		}

		char trail = (*(uint16_t *)(encoded + 2) & 0xFF00) >> 8;

		if (utf16_valid_trail(trail) == 0) {
			return 0;
		}

		return 4;
	}

	if (utf16_valid_trail(lead) != 0) {
		return 0;
	}

	return 2;
}

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF16HE_READER */

#ifdef NU_WITH_UTF16HE_WRITER

char* nu_utf16he_write(uint32_t unicode, char *utf16) {
	unsigned codepoint_len = utf16_codepoint_length(unicode);

	if (utf16 != 0) {
		switch (codepoint_len) {
			case 2: *(uint16_t *)(utf16) = (uint16_t)(unicode); break;
			default: { /* len == 4 */
				uint16_t c0 = 0, c1 = 0;
				b4_utf16(unicode, &c0, &c1);
				*(uint16_t *)(utf16) = c0;
				*(uint16_t *)(utf16 + 2) = c1;
				break;
			}
		}
	}

	return utf16 + codepoint_len;
}

#endif /* NU_WITH_UTF16HE_WRITER */
