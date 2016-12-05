#ifndef NU_UTF16LE_H
#define NU_UTF16LE_H

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "defines.h"
#include "utf16_internal.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_UTF16LE_READER

/** Read codepoint from UTF-16 string
 *
 * @ingroup utf16
 * @param utf16 pointer to UTF-16 encoded string
 * @param unicode output unicode codepoint or 0
 * @return pointer to next codepoint in UTF-16 string
 */
static inline
const char* nu_utf16le_read(const char *utf16, uint32_t *unicode) {
	uint32_t c = nu_letohs(utf16);

	if (c >= 0xD800 && c <= 0xDBFF) {
		if (unicode != 0) {
			*unicode = ((c & 0x03FF) << 10 | (nu_letohs(utf16 + 2) & 0x03FF)) + 0x10000;
		}
		return utf16 + 4;
	}
	else if (unicode != 0) {
		*unicode = c;
	}

	return utf16 + 2;
}

#ifdef NU_WITH_REVERSE_READ

/** Read codepoint from UTF-16 string in backward direction
 *
 * Note that it is your responsibility to check that this call
 * is not going under beginning of encoded string. Normally you
 * shouldn't call it like this: nu_utf16_revread(&u, "\x67\x00"); which
 * will result in undefined behavior.
 *
 * Also don't mess with pointers, nu_utf16_revread(&u, "\x67\x00\x67\x00" + 3);
 * won't work correctly. You are supposed to pass pointer received from
 * nu_utf16le_read().
 *
 * @ingroup utf16
 * @param unicode output unicode codepoint or 0
 * @param utf16 pointer to UTF-8 encoded string
 * @return pointer to previous codepoint in UTF-16 string
 */
static inline
const char* nu_utf16le_revread(uint32_t *unicode, const char *utf16) {
	/* valid UTF-16 sequences are either 2 or 4 bytes long
	 * trail sequences are between 0xDC00 .. 0xDFFF */
	const char *p = utf16 - 2;
	uint16_t ec = nu_letohs(p);

	if (ec >= 0xDC00 && ec <= 0xDFFF) { /* trail surrogate */
		p -= 2;
	}

	if (unicode != 0) {
		nu_utf16le_read(p, unicode);
	}

	return p;
}

#endif /* NU_WITH_REVERSE_READ */

#ifdef NU_WITH_VALIDATION

/** Validate codepoint in string
 *
 * @ingroup utf16
 * @param encoded buffer with encoded string
 * @param max_len buffer length
 * @return codepoint length or 0 on error
 */
NU_EXPORT
int nu_utf16le_validread(const char *encoded, size_t max_len);

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF16LE_READER */

#ifdef NU_WITH_UTF16LE_WRITER

/** Write unicode codepoints into UTF-16 encoded string
 *
 * Note that length of decoded UTF-16 string is not entirely strlen(encoded) / 2.
 * You need to use nu_strlen(encoded, nu_utf16_read) to find out exact value.
 *
 * @ingroup utf16
 * @param unicode unicode codepoint
 * @param utf16 pointer to buffer to write UTF-16 encoded text to,
 * should be large enough to hold encoded value
 * @return pointer to byte after last written
 */
NU_EXPORT
char* nu_utf16le_write(uint32_t unicode, char *utf16);

#endif /* NU_WITH_UTF16LE_WRITER */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UTF16LE_H */
