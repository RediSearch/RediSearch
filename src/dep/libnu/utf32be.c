#include "utf32be.h"

#ifdef NU_WITH_UTF32BE_READER
#ifdef NU_WITH_VALIDATION

int nu_utf32be_validread(const char *p, size_t max_len) {
	if (utf32_validread_basic(p, max_len) == 0) {
		return 0;
	}

	uint32_t u = 0;
	nu_utf32be_read(p, &u);

	if (u > NU_UTF32_MAX_CODEPOINT) {
		return 0;
	}

	return (u >= 0xD800 && u <= 0xDFFF ? 0 : 4);
}

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF32BE_READER */

#ifdef NU_WITH_UTF32BE_WRITER

char* nu_utf32be_write(uint32_t unicode, char *utf32) {
	if (utf32 != 0) {
		nu_htobel(unicode, utf32);
	}

	return utf32 + 4;
}

#endif /* NU_WITH_UTF32BE_WRITER */
