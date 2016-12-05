#ifndef NU_STRINGS_H
#define NU_STRINGS_H

/** @defgroup strings String functions
 *
 * Note on "n" functions variant: "n" is in bytes in all functions,
 * note though that those are not for memory overrun control.
 * They are just for strings not having terminating 0 byte and those
 * functions won't go further than m-th *codepoint* in string, but might go
 * further than n-th byte in case of multibyte sequence.
 *
 * E.g.: ``nu_strnlen("абв", 3, nu_utf8_read);``.
 * Since codepoints are 2-byte sequences, nu_strnlen() won't go further than 2nd
 * codepoint, but will go further than 3rd byte while reading "б".
 */

/** @defgroup transformations Codepoint transformations
 *
 * @example folding.c
 */

/** @defgroup transformations_internal Codepoint transformations (internal)
 *
 * @example special_casing.c
 */

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "defines.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

/** @defgroup iterators Iterators
 */

/** Read (decode) iterator
 *
 * @ingroup iterators
 * @see nu_utf8_read
 */
typedef const char* (*nu_read_iterator_t)(const char *encoded, uint32_t *unicode);

/** Read (decode) backwards iterator
 *
 * Arguments intentionally reversed to not mix this with nu_read_iterator_t.
 * Reverse read is not compatible with any of string functions.
 *
 * @ingroup iterators
 * @see nu_utf8_revread
 */
typedef const char* (*nu_revread_iterator_t)(uint32_t *unicode, const char *encoded);

/** Write (encode) iterator
 *
 * @ingroup iterators
 * @see nu_utf8_write
 */
typedef char* (*nu_write_iterator_t)(uint32_t unicode, char *encoded);

/** Transform codepoint
 *
 * @ingroup transformations
 * @see nu_toupper
 * @see nu_tolower
 */
typedef const char* (*nu_transformation_t)(uint32_t codepoint);

/** Transform codepoint (used internally). This kind of transformation
 * delegates iteration on string to transformation implementation.
 *
 * @ingroup transformations_internal
 * @see _nu_toupper
 * @see _nu_tolower
 */
typedef const char* (*nu_transform_read_t)(
	const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transformed,
	void *context);

#if (defined NU_WITH_Z_STRINGS) || (defined NU_WITH_N_STRINGS)

#endif /* NU_WITH_Z_STRINGS NU_WITH_N_STRINGS */

#ifdef NU_WITH_Z_STRINGS

/** Get decoded string codepoints length
 *
 * @ingroup strings
 * @param encoded encoded string
 * @param it decoding function
 * @return string length or negative error
 *
 * @see nu_strnlen
 */
NU_EXPORT
ssize_t nu_strlen(const char *encoded, nu_read_iterator_t it);

/** Get encoded string bytes length (encoding variant)
 *
 * @ingroup strings
 * @param unicode unicode codepoints
 * @param it encoding function
 * @return byte length or negative error
 *
 * @see nu_bytenlen
 */
NU_EXPORT
ssize_t nu_bytelen(const uint32_t *unicode, nu_write_iterator_t it);

/** Get encoded string bytes length
 *
 * @ingroup strings
 * @param encoded encoded string
 * @param it decoding function
 * @return string length or negative error
 */
NU_EXPORT
ssize_t nu_strbytelen(const char *encoded, nu_read_iterator_t it);

#endif /* NU_WITH_Z_STRINGS */

#ifdef NU_WITH_N_STRINGS

/**
 * @ingroup strings
 * @see nu_strlen
 */
NU_EXPORT
ssize_t nu_strnlen(const char *encoded, size_t max_len, nu_read_iterator_t it);

/**
 * @ingroup strings
 * @see nu_bytelen
 */
NU_EXPORT
ssize_t nu_bytenlen(const uint32_t *unicode, size_t max_len,
	nu_write_iterator_t it);

#endif /* NU_WITH_N_STRINGS */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_STRINGS_H */
