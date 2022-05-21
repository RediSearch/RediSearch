#include <assert.h>

#include "ducet.h"
#include "udb.h"

#ifdef NU_WITH_DUCET

#include "gen/_ducet.c"

#ifndef NU_DISABLE_CONTRACTIONS
#	include "gen/_ducet_switch.c"
#else
	const size_t _NU_DUCET_CONTRACTIONS = 0;
#endif

static size_t _nu_ducet_weights_count() {
	return NU_DUCET_G_SIZE + _NU_DUCET_CONTRACTIONS;
}

int32_t nu_ducet_weight(uint32_t codepoint, int32_t *weight, void *context) {
	(void)(weight);
	(void)(context);

	assert(_nu_ducet_weights_count() < 0x7FFFFFFF - 0x10FFFF);

#ifndef NU_DISABLE_CONTRACTIONS
	int32_t switch_value = _nu_ducet_weight_switch(codepoint, weight, context);
	/* weight switch should return weight (if any) and fill value of *weight
	 * with fallback (if needed). returned value of 0 is impossible result - this
	 * special case is already handled above, this return value indicates that switch
	 * couldn't find weight for a codepoint */
	if (switch_value != 0) {
		return switch_value;
	}
#endif

	/* special case switch after contractions switch
	 * to let state-machine figure out its state on abort */
	if (codepoint == 0) {
		return 0;
	}

	uint32_t mph_value = nu_udb_lookup_value(codepoint, NU_DUCET_G, NU_DUCET_G_SIZE,
		NU_DUCET_VALUES_C, NU_DUCET_VALUES_I);

	return (mph_value != 0
		? (int32_t)(mph_value)
		: (int32_t)(codepoint + _nu_ducet_weights_count()));

	/* ISO/IEC 14651 requests that codepoints with undefined weight should be
	 * sorted before max weight in collation table. This way all codepoints
	 * defined in ducet would have weight under a value of _nu_ducet_weights_count(),
	 * all undefined codepoints would have weight under
	 * 0x10FFFF + _nu_ducet_weights_count() - 1, max weight will be
	 * 0x10FFFF + _nu_ducet_weights_count() */

	/* Regarding integer overflow:
	 *
	 * int32_t can hold 0xFFFFFFFF / 2 = 0x7FFFFFFF positive numbers, this
	 * function can safely offset codepoint value up to +2146369536 without
	 * risk of overflow. Thus max collation table size supported is
	 * 2146369536 (0x7FFFFFFF - 0x10FFFF) */
}

#endif /* NU_WITH_DUCET */
