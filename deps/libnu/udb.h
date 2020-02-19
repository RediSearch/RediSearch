#ifndef NU_UDB_H
#define NU_UDB_H

#include <stdint.h>
#include <sys/types.h>

#include "config.h"
#include "defines.h"
#include "mph.h"
#include "strings.h"
#include "utf8.h"

/** @defgroup udb Unicode database
 *
 * Note: never use it directly, it is subject to change in next releases
 */

#if defined (__cplusplus) || defined (c_plusplus)
extern "C" {
#endif

#ifdef NU_WITH_UDB

#define NU_UDB_DECODING_FUNCTION (nu_utf8_read)
#define nu_udb_read (nu_utf8_read)

/** Lookup value in UDB
 *
 * Similar to nu_udb_lookup(), but doesn't look into COMBINED
 *
 * @ingroup udb
 * @see nu_udb_lookup
 * @return raw value from VALUES_I or 0 if value wasn't found
 */
static inline
uint32_t nu_udb_lookup_value(uint32_t codepoint,
	const int16_t *G, size_t G_SIZE,
	const uint32_t *VALUES_C, const uint16_t *VALUES_I) {

	uint32_t hash = nu_mph_hash(G, G_SIZE, codepoint);
	uint32_t value = nu_mph_lookup(VALUES_C, VALUES_I, codepoint, hash);

	return value;
}

/** Lookup data in UDB
 *
 * Returned data is encoded, therefore you need to use p = it(p, &u) to
 * fetch it. Returned string might contain more than 1 codepoint.
 *
 * @ingroup udb
 * @param codepoint unicode codepoint
 * @param G first MPH table
 * @param G_SIZE first table number of elements (original MPH set size)
 * @param VALUES_C codepoints array
 * @param VALUES_I offsets array
 * @param COMBINED joined values addressed by index stored in VALUES
 * @return looked up data or 0
 */
static inline
const char* nu_udb_lookup(uint32_t codepoint,
	const int16_t *G, size_t G_SIZE,
	const uint32_t *VALUES_C, const uint16_t *VALUES_I, const uint8_t *COMBINED) {

	uint32_t combined_offset = nu_udb_lookup_value(codepoint,
		G, G_SIZE, VALUES_C, VALUES_I);

	if (combined_offset == 0) {
		return 0;
	}

	return (const char *)(COMBINED + combined_offset);
}

#endif /* NU_WITH_UDB */

#if defined (__cplusplus) || defined (c_plusplus)
}
#endif

#endif /* NU_UDB_H */
