#include "fast_float.h"
#include <iostream>
#include <string>
#include <system_error>
#include <cerrno>

/* Like fast_float_strtod, but converts only the bytes in [NPTR, END): the string need not be
 * NUL-terminated, and nothing at or past END is read. */
extern "C" double fast_float_strtod_n(const char *nptr, const char *end, char **endptr) {
  double result = 0.0;
  auto answer = fast_float::from_chars(nptr, end, result);
  if (answer.ec != std::errc()) {
    errno = (answer.ec == std::errc::result_out_of_range) ? ERANGE : EINVAL;
  }
  if (endptr != NULL) {
    *endptr = (char *)answer.ptr;
  }
  return result;
}

/* Convert NPTR to a double using the fast_float library.
 *
 * This function behaves similarly to the standard strtod function, converting
 * the initial portion of the string pointed to by `nptr` to a `double` value,
 * using the fast_float library for high performance.
 * If the conversion fails, the function sets `errno` to:
 * - ERANGE if the result overflows or underflows
 * - EINVAL for other errors
 *
 * @param nptr   A pointer to the null-terminated byte string to be interpreted.
 * @param endptr A pointer to a pointer to character. If `endptr` is not NULL,
 *               it will point to the character after the last character used
 *               in the conversion.
 * @return       The converted value as a double. If no valid conversion could
 *               be performed, returns 0.0.
 * If ENDPTR is not NULL, a pointer to the character after the last one used
 * in the number is put in *ENDPTR.  */
extern "C" double fast_float_strtod(const char *nptr, char **endptr) {
  return fast_float_strtod_n(nptr, nptr + strlen(nptr), endptr);
}
