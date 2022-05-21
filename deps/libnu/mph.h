#ifndef NU_MPH_H
#define NU_MPH_H

/* Intentionally undocumented
 *
 * http://iswsa.acm.org/mphf/index.html
 */

#include <stdint.h>
#include <sys/types.h>

#include "config.h"

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_UDB

/* those need to be the same values as used in MPH generation */
#define PRIME        0x01000193

/** Calculate G offset from codepoint
 */
static inline
uint32_t _nu_hash(uint32_t hash, uint32_t codepoint) {
	if (hash == 0) {
		hash = PRIME;
	}

	return hash ^ codepoint;
}

/** Get hash value of Unicode codepoint
 */
static inline
uint32_t nu_mph_hash(const int16_t *G, size_t G_SIZE,
	uint32_t codepoint) {

	uint32_t h = _nu_hash(0, codepoint);
	int16_t offset = G[h % G_SIZE];
	if (offset < 0) {
		return (uint32_t)(-offset - 1);
	}
	return (_nu_hash(offset, codepoint) % G_SIZE);
}

/** Lookup value in MPH
 */
static inline
uint32_t nu_mph_lookup(const uint32_t *V_C, const uint16_t *V_I,
	uint32_t codepoint, uint32_t hash) {

	const uint32_t *c = (V_C + hash);
	const uint16_t *i = (V_I + hash);

	/* due to nature of minimal perfect hash, it will always
	 * produce collision for codepoints outside of MPH original set.
	 * thus VALUES_C contain original codepoint to check if
	 * collision occurred */

	return (*c != codepoint ? 0 : *i);
}

#endif /* NU_WITH_UDB */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_MPH_H */
