#include "utf32he.h"

#ifdef NU_WITH_UTF32HE_READER
#ifdef NU_WITH_REVERSE_READ

#endif /* NU_WITH_REVERSE_READ */

#ifdef NU_WITH_VALIDATION

int nu_utf32he_validread(const char *p, size_t max_len) {
	if (utf32_validread_basic(p, max_len) == 0) {
		return 0;
	}

	uint32_t u = 0;
	nu_utf32he_read(p, &u);

	if (u > NU_UTF32_MAX_CODEPOINT) {
		return 0;
	}

	return (u >= 0xD800 && u <= 0xDFFF ? 0 : 4);
}

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF32HE_READER */

#ifdef NU_WITH_UTF32HE_WRITER

char* nu_utf32he_write(uint32_t unicode, char *utf32) {
	*(uint32_t *)(utf32) = unicode;

	return utf32 + 4;
}

#endif /* NU_WITH_UTF32HE_WRITER */
