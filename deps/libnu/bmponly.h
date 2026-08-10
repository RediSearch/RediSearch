#ifndef NU_BMPONLY_H
#define NU_BMPONLY_H

#include <stdbool.h>

#include "defines.h"

/** @defgroup other Other
 */

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

/** Feature-test if nunicode was built with `NU_WITH_BMP_ONLY` option
 *
 * @ingroup other
 * @return true if nunicode was built in BMP-only variant
 */
NU_EXPORT
bool nu_bmp_only(void);

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* NU_BMPONLY_H */
