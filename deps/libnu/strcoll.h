#ifndef NU_STRCOLL_H
#define NU_STRCOLL_H

/** @defgroup collation Collation functions
 *
 * All functions in this group are following full Unicode collation rules,
 * i.e. nu_strstr(haystack, "Æ") will find "AE" in haystack and
 * nu_strstr(haystack, "ß") will find "ss".
 *
 * Same applies for *every* function, nu_strchr(str, 0x00DF), as you would
 * guess, will also find "ss" in str.
 *
 * Please expect this.
 *
 * Note on "n" functions variant: please see comment on this topic
 * in strings.h
 */

#include <sys/types.h>

#include "config.h"
#include "casemap.h"
#include "defines.h"
#include "strings.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_TOFOLD
# define NU_FOLDING_FUNCTION nu_tofold
#else
# define NU_FOLDING_FUNCTION nu_toupper
#endif /* NU_WITH_TOFOLD */

#ifdef NU_WITH_Z_COLLATION

/** Locate codepoint in string
 *
 * @ingroup collation
 * @param encoded encoded string
 * @param c charater  to locate
 * @param read read (decode) function for encoded string
 * @return pointer to codepoint in string or 0
 */
NU_EXPORT
const char* nu_strchr(const char *encoded, uint32_t c, nu_read_iterator_t read);

/** Locate codepoint in string ignoring case
 *
 * @ingroup collation
 * @see nu_strchr
 */
NU_EXPORT
const char* nu_strcasechr(const char *encoded, uint32_t c, nu_read_iterator_t read);

/** Locate codepoint in string in reverse direction
 *
 * @ingroup collation
 * @param encoded encoded string
 * @param c charater  to locate
 * @param read read (decode) function for encoded string
 * @return pointer to codepoint in string or 0
 */
NU_EXPORT
const char* nu_strrchr(const char *encoded, uint32_t c, nu_read_iterator_t read);

/** Locate codepoint in string in reverse direction, case-insensitive
 *
 * @ingroup collation
 * @see nu_strrchr
 */
NU_EXPORT
const char* nu_strrcasechr(const char *encoded, uint32_t c, nu_read_iterator_t read);

/** Compare strings in case-sensitive manner.
 *
 * @ingroup collation
 * @param s1 first encoded strings
 * @param s2 second encoded strings
 * @param s1_read read (decode) function for first string
 * @param s2_read read (decode) function for second string
 * @return -1, 0, 1
 */
NU_EXPORT
int nu_strcoll(const char *s1, const char *s2,
	nu_read_iterator_t s1_read, nu_read_iterator_t s2_read);

/** Compare strings in case-insensitive manner.
 *
 * @ingroup collation
 * @see nu_strcoll
 */
NU_EXPORT
int nu_strcasecoll(const char *s1, const char *s2,
	nu_read_iterator_t s1_read, nu_read_iterator_t s2_read);

/** Find needle in haystack
 *
 * @ingroup collation
 * @param haystack encoded haystack
 * @param needle encoded needle
 * @param haystack_read haystack read (decode) function
 * @param needle_read needle read (decode) function
 * @return pointer to found string or 0, will return
 * haystack if needle is empty string
 */
NU_EXPORT
const char* nu_strstr(const char *haystack, const char *needle,
	nu_read_iterator_t haystack_read, nu_read_iterator_t needle_read);

/** Find needle in haystack (case-insensitive)
 *
 * @ingroup collation
 * @see nu_strstr
 */
NU_EXPORT
const char* nu_strcasestr(const char *haystack, const char *needle,
	nu_read_iterator_t haystack_read, nu_read_iterator_t needle_read);

#endif /* NU_WITH_Z_COLLATION */

#ifdef NU_WITH_N_COLLATION

/**
 * @ingroup collation
 * @see nu_strchr
 */
NU_EXPORT
const char* nu_strnchr(const char *encoded, size_t max_len, uint32_t c,
	nu_read_iterator_t read);

/**
 * @ingroup collation
 * @see nu_strcasechr
 */
NU_EXPORT
const char* nu_strcasenchr(const char *encoded, size_t max_len, uint32_t c,
	nu_read_iterator_t read);

/**
 * @ingroup collation
 * @see nu_strrchr
 */
NU_EXPORT
const char* nu_strrnchr(const char *encoded, size_t max_len, uint32_t c,
	nu_read_iterator_t read);

/**
 * @ingroup collation
 * @see nu_strrcasechr
 */
NU_EXPORT
const char* nu_strrcasenchr(const char *encoded, size_t max_len, uint32_t c,
	nu_read_iterator_t read);

/**
 * @ingroup collation
 * @see nu_strcoll
 */
NU_EXPORT
int nu_strncoll(const char *s1, size_t s1_max_len,
	const char *s2, size_t s2_max_len,
	nu_read_iterator_t s1_read, nu_read_iterator_t s2_read);

/**
 * @ingroup collation
 * @see nu_strncoll
 */
NU_EXPORT
int nu_strcasencoll(const char *s1, size_t s1_max_len,
	const char *s2, size_t s2_max_len,
	nu_read_iterator_t s1_read, nu_read_iterator_t s2_read);

/**
 * @ingroup collation
 * @see nu_strstr
 */
NU_EXPORT
const char* nu_strnstr(const char *haystack, size_t haystack_max_len,
	const char *needle, size_t needle_max_len,
	nu_read_iterator_t haystack_read, nu_read_iterator_t needle_read);

/**
 * @ingroup collation
 * @see nu_strcasestr
 */
NU_EXPORT
const char* nu_strcasenstr(const char *haystack, size_t haystack_max_len,
	const char *needle, size_t needle_max_len,
	nu_read_iterator_t haystack_read, nu_read_iterator_t needle_read);

#endif /* NU_WITH_N_COLLATION */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_STRCOLL_H */
