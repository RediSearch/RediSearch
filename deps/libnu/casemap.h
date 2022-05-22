#ifndef NU_TOUPPER_H
#define NU_TOUPPER_H

#include <stdint.h>

#include "config.h"
#include "defines.h"
#include "strings.h"
#include "udb.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

/** Synonim to nu_casemap_read. It is recommended to use
 * nu_casemap_read instead.
 */
#define NU_CASEMAP_DECODING_FUNCTION NU_UDB_DECODING_FUNCTION
/** Read (decoding) function for use with transformation results of
 * casemapping functions. E.g. nu_casemap_read(nu_tolower(0x0041));
 * will read first codepoint of 'A' transformed to lower case.
 */
#define nu_casemap_read (nu_udb_read)

/** Casemap codepoint
 *
 * @ingroup transformations
 */
typedef nu_transformation_t nu_casemapping_t;

#ifdef NU_WITH_TOUPPER

/** Return uppercase value of codepoint. Uncoditional casemapping.
 *
 * @ingroup transformations
 * @param codepoint unicode codepoint
 * @return uppercase codepoint or 0 if mapping doesn't exist
 */
NU_EXPORT
const char* nu_toupper(uint32_t codepoint);

/** Return uppercase value of codepoint. Context-sensitivity is not
 * implemented internally, returned result is equal to calling nu_toupper()
 * on corresponding codepoint.
 *
 * @ingroup transformations_internal
 * @param encoded pointer to encoded string
 * @param limit memory limit of encoded string or NU_UNLIMITED
 * @param read read (decoding) function
 * @param u (optional) codepoint which was (or wasn't) transformed
 * @param transform output value of codepoint transformed into uppercase or 0
 * if mapping doesn't exist. Can't be NULL, supposed to be decoded with
 * nu_casemap_read
 * @param context not used
 * @return pointer to the next codepoint in string
 */
NU_EXPORT
const char* _nu_toupper(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context);

#endif /* NU_WITH_TOUPPER */

#ifdef NU_WITH_TOLOWER

/** Return lowercase value of codepoint. Unconditional casemapping.
 *
 * @ingroup transformations
 * @param codepoint unicode codepoint
 * @return lowercase codepoint or 0 if mapping doesn't exist
 */
NU_EXPORT
const char* nu_tolower(uint32_t codepoint);

/** Return lowercase value of codepoint. Will transform uppercase
 * Sigma ('Σ') into final sigma ('ς') if it occurs at string boundary or
 * followed by U+0000. Might require single read-ahead when
 * encountering Sigma.
 *
 * @ingroup transformations_internal
 * @param encoded pointer to encoded string
 * @param limit memory limit of encoded string or NU_UNLIMITED
 * @param read read (decoding) function
 * @param u (optional) codepoint which was (or wasn't) transformed
 * @param transform output value of codepoint transformed into lowercase or 0
 * if mapping doesn't exist. Can't be NULL, supposed to be decoded with
 * nu_casemap_read
 * @param context not used
 * @return pointer to the next codepoint in string
 */
NU_EXPORT
const char* _nu_tolower(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context);

#endif /* NU_WITH_TOLOWER */

#ifdef NU_WITH_TOFOLD

/** Return value of codepoint with case differences eliminated
 *
 * @ingroup transformations
 * @param codepoint unicode codepoint
 * @return casefolded codepoint or 0 if mapping doesn't exist
 */
NU_EXPORT
const char* nu_tofold(uint32_t codepoint);

/** Return value of codepoint with case differences eliminated.
 * Context-sensitivity is not implemented internally, returned result is equal
 * to calling nu_tofold() on corresponding codepoint.
 *
 * @ingroup transformations_internal
 * @param encoded pointer to encoded string
 * @param limit memory limit of encoded string or NU_UNLIMITED
 * @param read read (decoding) function
 * @param u (optional) codepoint which was (or wasn't) transformed
 * @param transform output value of casefolded codepoint or 0
 * if mapping doesn't exist. Can't be NULL, supposed to be decoded with
 * nu_casemap_read
 * @param context not used
 * @return pointer to the next codepoint in string
 */
NU_EXPORT
const char* _nu_tofold(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context);

#endif /* NU_WITH_TOFOLD */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_TOUPPER_H */
