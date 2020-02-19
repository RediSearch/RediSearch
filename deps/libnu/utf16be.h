#ifndef NU_UTF16BE_H
#define NU_UTF16BE_H

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "defines.h"
#include "utf16_internal.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_UTF16BE_READER

/**
 * @ingroup utf16
 * @see nu_utf16le_read
 */
static inline
const char* nu_utf16be_read(const char *utf16, uint32_t *unicode) {
	uint32_t c = nu_betohs(utf16);

	if (c >= 0xD800 && c <= 0xDBFF) {
		if (unicode != 0) {
			*unicode = ((c & 0x03FF) << 10 | (nu_betohs(utf16 + 2) & 0x03FF)) + 0x10000;
		}
		return utf16 + 4;
	}
	else if (unicode != 0) {
		*unicode = c;
	}

	return utf16 + 2;
}

#ifdef NU_WITH_REVERSE_READ

/**
 * @ingroup utf16
 * @see nu_utf16le_revread
 */
static inline
const char* nu_utf16be_revread(uint32_t *unicode, const char *utf16) {
	/* valid UTF-16 sequences are either 2 or 4 bytes long
	 * trail sequences are between 0xDC00 .. 0xDFFF */
	const char *p = utf16 - 2;
	uint16_t ec = nu_betohs(p);

	if (ec >= 0xDC00 && ec <= 0xDFFF) { /* trail surrogate */
		p -= 2;
	}

	if (unicode != 0) {
		nu_utf16be_read(p, unicode);
	}

	return p;
}

#endif /* NU_WITH_REVERSE_READ */

#ifdef NU_WITH_VALIDATION

/**
 * @ingroup utf16
 * @see nu_utf16le_validread
 */
NU_EXPORT
int nu_utf16be_validread(const char *encoded, size_t max_len);

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF16BE_READER */

#ifdef NU_WITH_UTF16BE_WRITER

/**
 * @ingroup utf16
 * @see nu_utf16le_write
 */
NU_EXPORT
char* nu_utf16be_write(uint32_t unicode, char *utf16);

#endif /* NU_WITH_UTF16BE_WRITER */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UTF16BE_H */
