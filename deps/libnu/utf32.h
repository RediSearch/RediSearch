#ifndef NU_UTF32_H
#define NU_UTF32_H

#include <stdint.h>

#include "config.h"
#include "defines.h"
#include "strings.h"
#include "validate.h"

/** @defgroup utf32 UTF-32 support
 */

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#if (defined NU_WITH_UTF32_READER) || (defined NU_WITH_UTF32_WRITER)
/** For sizeof() only
 *
 * @ingroup utf32
 */
static const uint32_t NU_UTF32_BOM = 0;
#endif

/** Endianess-specific UTF-32 write BOM function */
typedef char* (*nu_utf32_write_bom_t)(char *);

#ifdef NU_WITH_UTF32_READER

/** Holder for endianess-specific UTF-32 functions
 *
 * @ingroup utf32
 * @see nu_utf32_write_bom
 */
typedef struct {
	/** Read (decode) function
	 */
	nu_read_iterator_t read;
	/** Write (encode) function
	 */
	nu_write_iterator_t write;
	/** Reverse-read (decode) function
	 */
	nu_revread_iterator_t revread;
	/** Validation function
	 */
	nu_validread_iterator_t validread;
	/** BOM writing function
	 */
	nu_utf32_write_bom_t write_bom;
} nu_utf32_bom_t;

/**
 * @ingroup utf32
 * @see nu_utf16_read_bom
 */
NU_EXPORT
const char* nu_utf32_read_bom(const char *encoded, nu_utf32_bom_t *bom);

#endif /* NU_WITH_UTF32_READER */

#ifdef NU_WITH_UTF32_WRITER

/**
 * @ingroup utf32
 * @see nu_utf16le_write_bom
 */
NU_EXPORT
char* nu_utf32le_write_bom(char *encoded);

/**
 * @ingroup utf32
 * @see nu_utf16be_write_bom
 */
NU_EXPORT
char* nu_utf32be_write_bom(char *encoded);

#endif /* NU_WITH_UTF32_WRITER */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UTF32_H */
