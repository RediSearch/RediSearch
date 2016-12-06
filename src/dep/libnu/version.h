#ifndef NU_VERSION_H
#define NU_VERSION_H

#include "defines.h"

/** @defgroup other Other
 */

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

/** This define holds human-readable version of nunicode
 *
 * @ingroup defines
 */
#define NU_VERSION "custom"

/** Human-readable version of nunicode
 *
 * @ingroup other
 * @return version string
 */
NU_EXPORT
const char* nu_version(void);

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_VERSION_H */
