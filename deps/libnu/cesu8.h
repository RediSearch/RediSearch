#ifndef NU_CESU8_H
#define NU_CESU8_H

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "cesu8_internal.h"
#include "defines.h"
#include "utf8_internal.h"

/** @defgroup cesu8 CESU-8 support
 *
 * http://www.unicode.org/reports/tr26/
 */

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_CESU8_READER

/** Read codepoint from UTF-8 string
 *
 * @ingroup cesu8
 * @param cesu8 pointer to CESU-8 encoded string
 * @param unicode output unicode codepoint or 0
 * @return pointer to next codepoint in CESU-8 string
 */
static inline
const char* nu_cesu8_read(const char *cesu8, uint32_t *unicode) {
	uint32_t c = *(unsigned char *)(cesu8);

	if (c == 0xED) { /* 6-bytes sequence */
		if (unicode != 0) {
			cesu8_6b(cesu8, unicode);
		}
		return cesu8 + 6;
	}
	else if (c >= 0x80) {
		if (c < 0xE0) {
			if (unicode != 0) {
				utf8_2b(cesu8, unicode);
			}
			return cesu8 + 2;
		}
		else {
			if (unicode != 0) {
				utf8_3b(cesu8, unicode);
			}
			return cesu8 + 3;
		}
	}
	else if (unicode != 0) {
		*unicode = c;
	}

	return cesu8 + 1;
}

#ifdef NU_WITH_REVERSE_READ

/** Read codepoint from CESU-8 string in backward direction
 *
 * Note that it is your responsibility to check that this call
 * is not going under beginning of encoded string. Normally you
 * shouldn't call it like this: nu_cesu8_revread(&u, "hello"); which
 * will result in undefined behavior
 *
 * @ingroup cesu8
 * @param unicode output unicode codepoint or 0
 * @param cesu8 pointer to CESU-8 encoded string
 * @return pointer to previous codepoint in CESU-8 string
 */
static inline
const char* nu_cesu8_revread(uint32_t *unicode, const char *cesu8) {
	/* valid CESU-8 has either 10xxxxxx (continuation byte)
	 * or beginning of byte sequence
	 *
	 * one exception is 11101101 followed by 1011xxxx which is
	 * trail surrogate of 6-byte sequence.
	 */
	const char *p = cesu8 - 1;
	while (((unsigned char)(*p) & 0xC0) == 0x80) { /* skip every 0b10000000 */
		--p;
	}

	if ((unsigned char)(*p) == 0xED
	&& ((unsigned char)*(p + 1) & 0xF0) == 0xB0) { /* trail surrogate */
		p -= 3;
	}

	if (unicode != 0) {
		nu_cesu8_read(p, unicode);
	}

	return p;
}

#endif /* NU_WITH_REVERSE_READ */

#ifdef NU_WITH_VALIDATION

/** Validate codepoint in string
 *
 * @ingroup cesu8
 * @param encoded buffer with encoded string
 * @param max_len buffer length
 * @return codepoint length or 0 on error
 */
NU_EXPORT
int nu_cesu8_validread(const char *encoded, size_t max_len);

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_CESU8_READER */

#ifdef NU_WITH_CESU8_WRITER

/** Write unicode codepoints into CESU-8 encoded string
 *
 * @ingroup cesu8
 * @param unicode unicode codepoint
 * @param cesu8 pointer to buffer to write CESU-8 encoded text to,
 * shoud be large enough to hold encoded value
 * @return pointer to byte after last written
 */
NU_EXPORT
char* nu_cesu8_write(uint32_t unicode, char *cesu8);

#endif /* NU_WITH_CESU8_WRITER */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_CESU8_H */
