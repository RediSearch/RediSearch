#ifndef NU_BUILD_CONFIG_H
#define NU_BUILD_CONFIG_H
#define NU_WITH_EVERYTHING
/** @file config.h
 *
 * This file list available build options and provide some shortcuts,
 * like NU_WITH_UTF16 will enable NU_WITH_UTF16LE + NU_WITH_UTF16BE.
 *
 * At build time you might set either particular option or shortcut. Either
 * way you don't have to and shouldn't modify this file, just set build flags
 * at the environment.
 *
 * This file will also enable several dependencies for you: case-mapping
 * depends on NU_WITH_UDB, NU_UTF8_READER and so.
 */

/* Definitions not covered in this file which should be defined
 * externally.
 *
 * NU_BUILD_STATIC: will change functions visibility to "hidden" (GCC).
 * @see defines.h
 *
 * NU_DISABLE_CONTRACTIONS: disables forward-reading during collation,
 * only weights of a single codepoints will be compared (enabled in release build)
 */

/* Enable everything, see below for details on a specific option */

#ifdef NU_WITH_EVERYTHING
# define NU_WITH_UTF8
# define NU_WITH_CESU8
# define NU_WITH_UTF16
# define NU_WITH_UTF16HE
# define NU_WITH_UTF32
# define NU_WITH_UTF32HE
# define NU_WITH_STRINGS
# define NU_WITH_EXTRA
# define NU_WITH_REVERSE_READ
# define NU_WITH_VALIDATION
# define NU_WITH_COLLATION
# define NU_WITH_CASEMAP
#endif /* NU_WITH_EVERYTHING */

/* Enable UTF-8 decoding and encoding */
#ifdef NU_WITH_UTF8
# define NU_WITH_UTF8_READER /* UTF-8 decoding functions */
# define NU_WITH_UTF8_WRITER /* UTF-8 encoding functions */
#endif /* NU_WITH_UTF8 */

/* Enable CESU-8 decoding and encoding */
#ifdef NU_WITH_CESU8
# define NU_WITH_CESU8_READER
# define NU_WITH_CESU8_WRITER
#endif /* NU_WITH_CESU8 */

/* Enable UTF-16LE decoding and encoding */
#ifdef NU_WITH_UTF16LE
# define NU_WITH_UTF16LE_READER
# define NU_WITH_UTF16LE_WRITER
#endif /* NU_WITH_UTF16LE */

/* Enable UTF-16BE decoding and encoding */
#ifdef NU_WITH_UTF16BE
# define NU_WITH_UTF16BE_READER
# define NU_WITH_UTF16BE_WRITER
#endif /* NU_WITH_UTF16BE */

/* Enable UTF-16HE decoding and encoding */
#ifdef NU_WITH_UTF16HE
# define NU_WITH_UTF16HE_READER
# define NU_WITH_UTF16HE_WRITER
#endif /* NU_WITH_UTF16HE */

/* Enable all UTF-16 options */
#ifdef NU_WITH_UTF16
# define NU_WITH_UTF16_READER
# define NU_WITH_UTF16_WRITER
#endif /* NU_WITH_UTF16 */

/* Enable UTF-16LE and BE decoders of UTF-16 decoder is requested */
#ifdef NU_WITH_UTF16_READER
# define NU_WITH_UTF16LE_READER
# define NU_WITH_UTF16BE_READER
#endif /* NU_WITH_UTF16_READER */

/* Enable UTF-16LE and BE encoders of UTF-16 encoder is requested */
#ifdef NU_WITH_UTF16_WRITER
# define NU_WITH_UTF16LE_WRITER
# define NU_WITH_UTF16BE_WRITER
#endif /* NU_WITH_UTF16_WRITER */

/* Enable UTF-32LE decoding and encoding */
#ifdef NU_WITH_UTF32LE
# define NU_WITH_UTF32LE_READER
# define NU_WITH_UTF32LE_WRITER
#endif /* NU_WITH_UTF32LE */

/* Enable UTF-32BE decoding and encoding */
#ifdef NU_WITH_UTF32BE
# define NU_WITH_UTF32BE_READER
# define NU_WITH_UTF32BE_WRITER
#endif /* NU_WITH_UTF32BE */

/* Enable UTF-32HE decoding and encoding */
#ifdef NU_WITH_UTF32HE
# define NU_WITH_UTF32HE_READER
# define NU_WITH_UTF32HE_WRITER
#endif /* NU_WITH_UTF32HE */

/* Enable all UTF-32 options */
#ifdef NU_WITH_UTF32
# define NU_WITH_UTF32_READER
# define NU_WITH_UTF32_WRITER
#endif /* NU_WITH_UTF32 */

/* Enable UTF-32LE and BE decoders of UTF-32 decoder is requested */
#ifdef NU_WITH_UTF32_READER
# define NU_WITH_UTF32LE_READER
# define NU_WITH_UTF32BE_READER
#endif /* NU_WITH_UTF32_READER */

/* Enable UTF-32LE and BE encoders of UTF-32 encoder is requested */
#ifdef NU_WITH_UTF32_WRITER
# define NU_WITH_UTF32LE_WRITER
# define NU_WITH_UTF32BE_WRITER
#endif /* NU_WITH_UTF32_WRITER */

/* Shortcut for all string functions */
#ifdef NU_WITH_STRINGS
# define NU_WITH_Z_STRINGS /* 0-terminated string functions */
# define NU_WITH_N_STRINGS /* unterminated string functions */
#endif /* NU_WITH_STRINGS */

/* Shortcut for extra string functions */
#ifdef NU_WITH_EXTRA
# define NU_WITH_Z_EXTRA /* extra functions for 0-terminated strings */
# define NU_WITH_N_EXTRA /* extra functions for unterminated strings */
#endif /* NU_WITH_STRINGS */

/* Enable collation functions */
#ifdef NU_WITH_COLLATION
# define NU_WITH_Z_COLLATION /* collation functions for 0-terminated strings */
# define NU_WITH_N_COLLATION /* collation functions for unterminated strings */
#endif /* NU_WITH_COLLATION */

/* Requirements for collation functions on 0-terminated strings */
#ifdef NU_WITH_Z_COLLATION
# define NU_WITH_Z_STRINGS
# define NU_WITH_TOUPPER /* nu_toupper() */
#endif

/* Requirements for collation functions
 * on unterminated strings */
#ifdef NU_WITH_N_COLLATION
# define NU_WITH_N_STRINGS
# define NU_WITH_TOUPPER
#endif

/* Requirements for casemap functions */
#ifdef NU_WITH_CASEMAP
# define NU_WITH_TOLOWER /* nu_tolower() */
# define NU_WITH_TOUPPER
# define NU_WITH_TOFOLD
#endif /* NU_WITH_CASEMAP */

/* More requirements for collation functions all collation functions depends
 * on NU_WITH_DUCET */
#if (defined NU_WITH_Z_COLLATION) || (defined NU_WITH_N_COLLATION)
# ifndef NU_WITH_DUCET
#  define NU_WITH_DUCET
# endif
#endif

/* All collation and casemapping functions depends on NU_WITH_UDB */
#if (defined NU_WITH_Z_COLLATION) || (defined NU_WITH_N_COLLATION) \
|| (defined NU_WITH_TOLOWER) || (defined NU_WITH_TOUPPER) || (defined NU_WITH_TOFOLD)
# ifndef NU_WITH_UDB
#  define NU_WITH_UDB /* nu_udb_* functions, pretty much internal stuff */
# endif /* NU_WITH_UDB */
#endif

/* DUCET implementation depends on NU_WITH_UDB */
#ifdef NU_WITH_DUCET
#  define NU_WITH_UDB
#endif /* NU_WITH_DUCET */

/* NU_WITH_UDB depends on NU_WITH_UTF8_READER because internal encoding
 * of UDB is UTF-8 */
#ifdef NU_WITH_UDB
#  define NU_WITH_UTF8_READER
#endif /* NU_WITH_UDB */

#endif /* NU_BUILD_CONFIG_H */
