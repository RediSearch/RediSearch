#ifndef NU_UNACCENT_H
#define NU_UNACCENT_H

#include "casemap.h"
#include "strings.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

/**
 * @example unaccent.c
 */

#ifdef NU_WITH_UNACCENT

/** Return unaccented value of codepoint. If codepoint is
 * accent (disacritic) itself, returns empty string.
 *
 * @note This is nunicode extension.
 *
 * @ingroup transformations
 * @param codepoint unicode codepoint
 * @return unaccented codepoint, 0 if mapping doesn't exist
 * and empty string if codepoint is accent
 */
NU_EXPORT
const char* nu_tounaccent(uint32_t codepoint);

/** Return unaccented value of codepoint. If codepoint is
 * accent (disacritic) itself, returns empty string.
 *
 * @note This is nunicode extenstion.
 *
 * @ingroup transformations_internal
 * @param encoded pointer to encoded string
 * @param limit memory limit of encoded string or NU_UNLIMITED
 * @param read read (decoding) function
 * @param u (optional) codepoint which was (or wasn't) transformed
 * @param transform output value of codepoint unaccented or 0 if
 * mapping doesn't exist, or empty string if codepoint is accent.
 * Can't be NULL, supposed to be decoded with nu_casemap_read
 * @param context not used
 * @return pointer to the next codepoint in string
 */
NU_EXPORT
const char* _nu_tounaccent(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context);

#endif /* NU_WITH_UNACCENT */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UNACCENT_H */
