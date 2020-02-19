#ifndef NU_EXTRA_H
#define NU_EXTRA_H

/** @defgroup extra Extra string functions
 *
 * Note on "n" functions variant: those are not for memory overrun control.
 * They are just for strings not having terminating 0 byte and those
 * functions won't go further than n-th *codepoint* in string, not byte.
 */

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "strings.h"
#include "udb.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#define NU_TRANSFORM_DECODING_FUNCTION NU_UDB_DECODING_FUNCTION

#ifdef NU_WITH_Z_EXTRA

/** Read 0-terminated string
 *
 * @ingroup extra
 * @param encoded source buffer
 * @param unicode destination buffer, should be large enough to hold
 * decoded string
 * @param it read (decode) function
 * @return 0
 *
 * @see nu_utf8_read
 * @see nu_readnstr
 */
NU_EXPORT
int nu_readstr(const char *encoded, uint32_t *unicode,
	nu_read_iterator_t it);

/** Write 0-terminated string
 *
 * @ingroup extra
 * @param unicode 0x0000-terminated codepoints
 * @param encoded destination buffer, should be large enough to hold
 * encoded string
 * @param it write (encode) function
 * @return 0
 *
 * @see nu_bytenlen
 * @see nu_utf8_write
 * @see nu_writenstr
 */
NU_EXPORT
int nu_writestr(const uint32_t *unicode, char *encoded,
	nu_write_iterator_t it);

/** Recode string
 *
 * @ingroup extra
 * @param source source encoded string
 * @param dest dest encoded string, should be large enough
 * @param read_it decoding function
 * @param write_it encoding function
 * @return 0
 *
 * @see nu_bytenlen
 * @see nu_utf8_read
 * @see nu_utf8_write
 * @see nu_transformnstr
 */
NU_EXPORT
int nu_transformstr(const char *source, char *dest,
	nu_read_iterator_t read_it, nu_write_iterator_t write_it);

/** Get decoded string codepoint length taking into account transformation
 *
 * @ingroup extra
 * @param encoded encoded string
 * @param read read (decode) function
 * @param transform transformation to take into account
 * @param transform_read transformation result decoding function
 * @return number of codepoints in transformed string
 *
 * @see nu_toupper
 * @see nu_tolower
 */
NU_EXPORT
ssize_t nu_strtransformlen(const char *encoded, nu_read_iterator_t read,
	nu_transformation_t transform, nu_read_iterator_t transform_read);

/** Get decoded string codepoint length taking into account transformation
 * (internal version)
 *
 * @ingroup extra
 * @param encoded encoded string
 * @param read read (decode) function
 * @param transform transformation to take into account
 * @param transform_read transformation result decoding function
 * @param context pointer passed to each call of transformation
 * @return number of codepoints in transformed string
 *
 * @see _nu_tolower
 * @see _nu_toupper
 */
NU_EXPORT
ssize_t _nu_strtransformlen(const char *encoded, nu_read_iterator_t read,
	nu_transform_read_t transform, nu_read_iterator_t transform_read,
	void *context);

#endif /* NU_WITH_Z_EXTRA */

#ifdef NU_WITH_N_EXTRA

/**
 * @ingroup extra
 * @see nu_readstr
 */
NU_EXPORT
int nu_readnstr(const char *encoded, size_t max_len, uint32_t *unicode,
	nu_read_iterator_t it);

/**
 * @ingroup extra
 * @see nu_writestr
 */
NU_EXPORT
int nu_writenstr(const uint32_t *unicode, size_t max_len, char *encoded,
	nu_write_iterator_t it);

/**
 * @ingroup extra
 * @see nu_transformstr
 */
NU_EXPORT
int nu_transformnstr(const char *source, size_t max_len, char *dest,
	nu_read_iterator_t read_it, nu_write_iterator_t write_it);

/**
 * @ingroup extra
 * @see nu_strtransformlen
 */
NU_EXPORT
ssize_t nu_strtransformnlen(const char *encoded, size_t max_len, nu_read_iterator_t read,
	nu_transformation_t transform, nu_read_iterator_t transform_read);

/**
 * @ingroup extra
 * @see _nu_strtransformlen
 */
NU_EXPORT
ssize_t _nu_strtransformnlen(const char *encoded, size_t max_len, nu_read_iterator_t read,
	nu_transform_read_t transform, nu_read_iterator_t transform_read,
	void *context);

#endif /* NU_WITH_N_EXTRA */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_EXTRA_H */
