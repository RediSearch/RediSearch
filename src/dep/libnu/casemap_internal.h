#ifndef NU_CASEMAP_INTERNAL_H
#define NU_CASEMAP_INTERNAL_H

#include <stdint.h>
#include <sys/types.h>

#include "udb.h"

/** Casemap codepoint
 *
 * @ingroup transformations
 */
static inline
const char* _nu_to_something(uint32_t codepoint,
	const int16_t *G, size_t G_SIZE,
	const uint32_t *VALUES_C, const uint16_t *VALUES_I, const uint8_t *COMBINED) {

	return nu_udb_lookup(codepoint, G, G_SIZE, VALUES_C, VALUES_I, COMBINED);
}

#endif /* NU_CASEMAP_INTERNAL_H */
