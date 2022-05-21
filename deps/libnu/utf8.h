#ifndef NU_UTF8_H
#define NU_UTF8_H

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "defines.h"
#include "utf8_internal.h"

/** @defgroup utf8 UTF-8 support
 *
 * Note: There is no utf8_string[i] equivalent - it will be slow,
 * use nu_utf8_read() and nu_utf8_revread() instead
 *
 * @example utf8.c
 * @example revread.c
 */

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_UTF8_READER

/** Read codepoint from UTF-8 string
 *
 * @ingroup utf8
 * @param utf8 pointer to UTF-8 encoded string
 * @param unicode output unicode codepoint or 0
 * @return pointer to next codepoint in UTF-8 string
 */
static inline
const char* nu_utf8_read(const char *utf8, uint32_t *unicode) {
	uint32_t c = *(unsigned char *)(utf8);

	if (c >= 0x80) {
		if (c < 0xE0) {
			if (unicode != 0) {
				utf8_2b(utf8, unicode);
			}
			return utf8 + 2;
		}
		else if (c < 0xF0) {
			if (unicode != 0) {
				utf8_3b(utf8, unicode);
			}
			return utf8 + 3;
		}
		else {
			if (unicode != 0) {
				utf8_4b(utf8, unicode);
			}
			return utf8 + 4;
		}
	}
	else if (unicode != 0) {
		*unicode = c;
	}

	return utf8 + 1;
}

#ifdef NU_WITH_REVERSE_READ

/** Read codepoint from UTF-8 string in backward direction
 *
 * Note that it is your responsibility to check that this call
 * is not going under beginning of encoded string. Normally you
 * shouldn't call it like this: nu_utf8_revread(&u, "hello"); which
 * will result in undefined behavior
 *
 * @ingroup utf8
 * @param unicode output unicode codepoint or 0
 * @param utf8 pointer to UTF-8 encoded string
 * @return pointer to previous codepoint in UTF-8 string
 */
static inline
const char* nu_utf8_revread(uint32_t *unicode, const char *utf8) {
	/* valid UTF-8 has either 10xxxxxx (continuation byte)
	 * or beginning of byte sequence */
	const char *p = utf8 - 1;
	while (((unsigned char)(*p) & 0xC0) == 0x80) { /* skip every 0b10000000 */
		--p;
	}

	if (unicode != 0) {
		nu_utf8_read(p, unicode);
	}

	return p;
}

#endif /* NU_WITH_REVERSE_READ */

#ifdef NU_WITH_VALIDATION

/** Validate codepoint in string
 *
 * @ingroup utf8
 * @param encoded buffer with encoded string
 * @param max_len buffer length
 * @return codepoint length or 0 on error
 */
NU_EXPORT
int nu_utf8_validread(const char *encoded, size_t max_len);

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF8_READER */

#ifdef NU_WITH_UTF8_WRITER

/** Write unicode codepoints into UTF-8 encoded string
 *
 * @ingroup utf8
 * @param unicode unicode codepoint
 * @param utf8 pointer to buffer to write UTF-8 encoded text to,
 * should be large enough to hold encoded value
 * @return pointer to byte after last written
 */
NU_EXPORT
char* nu_utf8_write(uint32_t unicode, char *utf8);

#endif /* NU_WITH_UTF8_WRITER */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UTF8_H */
