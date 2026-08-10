#include "casemap.h"

#ifdef NU_WITH_TOFOLD

#include "casemap_internal.h"
#ifndef NU_WITH_BMP_ONLY
# include "gen/_tofold.c"
#else
# include "gen/_tofold_compact.c"
#endif /* NU_WITH_BMP_ONLY */

const char* nu_tofold(uint32_t codepoint) {
	return _nu_to_something(codepoint, NU_TOFOLD_G, NU_TOFOLD_G_SIZE,
		NU_TOFOLD_VALUES_C, NU_TOFOLD_VALUES_I, NU_TOFOLD_COMBINED);
}

const char* _nu_tofold(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context) {

	(void)(limit);
	(void)(context);

	uint32_t _u = 0;
	const char *np = read(encoded, &_u);

	*transform = nu_tofold(_u);

	if (u != 0) {
		*u = _u;
	}

	return np;
}

#endif /* NU_WITH_TOFOLD */
