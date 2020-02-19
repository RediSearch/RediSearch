#ifndef NU_VALIDATE_H
#define NU_VALIDATE_H

/** @defgroup validation Encoding validation
 */

#include <sys/types.h>
#include <stddef.h>

#include "config.h"
#include "defines.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

/** Validation function
 *
 * @ingroup iterators
 * @see nu_utf8_validread
 */
typedef int (*nu_validread_iterator_t)(const char *p, size_t max_len);

#ifdef NU_WITH_VALIDATION

/** Validate string encoding
 *
 * If this check fails then none of the nunicode functions is applicable to
 * 'encoded'. Calling any function on such string will lead to undefined
 * behavior.
 *
 * @ingroup validation
 * @param encoded encoded string
 * @param max_len length of the buffer, nu_validate() won't go further
 * than this
 * @param it validating iterator (e.g. nu_utf8_validread)
 * @return 0 on valid string, pointer to invalid segment in string on
 * validation error
 *
 * @see nu_utf8_validread
 */
NU_EXPORT
const char* nu_validate(const char *encoded, size_t max_len,
	nu_validread_iterator_t it);

#endif /* NU_WITH_VALIDATION */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_VALIDATE_H */
