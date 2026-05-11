#ifndef NU_DEFINES_H
#define NU_DEFINES_H

/** @file
 */

/** @defgroup defines Defines
 */

#ifndef NU_EXPORT

# ifdef _WIN32
#  define NU_EXPORT __declspec(dllexport)

# elif __GNUC__ >= 4
#  ifdef NU_BUILD_STATIC
#   define NU_EXPORT __attribute__ ((visibility ("hidden")))
#  else
#   define NU_EXPORT __attribute__ ((visibility ("default")))
#  endif

# else
#  define NU_EXPORT
# endif

#endif /* NU_EXPORT */

/** Integer version of Unicode specification implemented. 900 == 9.0.0
 *
 * @ingroup defines
 */
#define NU_UNICODE_VERSION 1000
/** Special limit value to unset limit on string. Used internally by nunicode.
 *
 * @ingroup defines
 */
#define NU_UNLIMITED ((const void *)(-1))

#ifdef _MSC_VER
#define ssize_t ptrdiff_t
#endif

#endif /* NU_DEFINES_H */
