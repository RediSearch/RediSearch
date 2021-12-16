#ifndef NU_UTF32HE_H
#define NU_UTF32HE_H

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "defines.h"
#include "utf32_internal.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_UTF32HE_READER

/**
 * @ingroup utf32
 * @see nu_utf16le_read
 */
static inline
const char* nu_utf32he_read(const char *utf32, uint32_t *unicode) {
	if (unicode != 0) {
		*unicode = *(uint32_t *)(utf32);
	}

	return utf32 + 4;
}

#ifdef NU_WITH_REVERSE_READ

/*
 * @ingroup utf32
 * @see nu_utf16le_revread
 */
static inline
const char* nu_utf32he_revread(uint32_t *unicode, const char *utf32) {
	const char *p = utf32 - 4;

	if (unicode != 0) {
		nu_utf32he_read(p, unicode);
	}

	return p;
}

#endif /* NU_WITH_REVERSE_READ */

#ifdef NU_WITH_VALIDATION

/**
 * @ingroup utf32
 * @see nu_utf16le_validread
 */
NU_EXPORT
int nu_utf32he_validread(const char *p, size_t max_len);

#endif /* NU_WITH_VALIDATION */
#endif /* NU_WITH_UTF32HE_READER */

#ifdef NU_WITH_UTF32HE_WRITER

/**
 * @ingroup utf32
 * @see nu_utf16le_write
 */
NU_EXPORT
char* nu_utf32he_write(uint32_t unicode, char *utf32);

#endif /* NU_WITH_UTF32LE_WRITER */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UTF32HE_H */
