#ifndef NU_DUCET_H
#define NU_DUCET_H

#include <stdint.h>

#include "config.h"
#include "defines.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_DUCET

/** Get DUCET value of codepoint
 *
 * Normally, for unlisted codepoints, this function will return number greater
 * than max weight of listed codepoints, hence putting all unlisted codepoints
 * (not letters and not numbers) to the end of the sorted list (in codepoint
 * order).
 *
 * @ingroup udb
 * @param codepoint codepoint
 * @param weight previous weight for compound weight (not used here)
 * @param context pointer passed to nu_strcoll()
 * @return comparable weight of the codepoint
 */
NU_EXPORT
int32_t nu_ducet_weight(uint32_t codepoint, int32_t *weight, void *context);

#endif /* NU_WITH_DUCET */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_DUCET_H */
