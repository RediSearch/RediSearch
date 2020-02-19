#include <assert.h>

#include "casemap.h"

#ifdef NU_WITH_TOLOWER

#include "casemap_internal.h"
#include "gen/_tolower.c"

/* in nu_casemap_read (UTF-8), zero-terminated */
static const char *__nu_final_sigma = "ς";

const char* nu_tolower(uint32_t codepoint) {
	return _nu_to_something(codepoint, NU_TOLOWER_G, NU_TOLOWER_G_SIZE,
		NU_TOLOWER_VALUES_C, NU_TOLOWER_VALUES_I, NU_TOLOWER_COMBINED);
}

const char* _nu_tolower(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context) {

	(void)(context);

	uint32_t _u = 0;
	const char *np = read(encoded, &_u);

	if (u != 0) {
		*u = _u;
	}

	/* handling of 0x03A3 ('Σ')
	 *
	 * this is the only language-independent exception described in
	 * SpecialCasing.txt (Unicode 7.0) */

	assert(nu_casemap_read == nu_utf8_read);

	if (_u == 0x03A3) {
		if (np >= limit) {
			*transform = __nu_final_sigma;
			return np;
		}

		uint32_t nu = 0;
		read(np, &nu);

		if (nu == 0) {
			*transform = __nu_final_sigma;
			return np;
		}
	}

	*transform = nu_tolower(_u);

	return np;
}

#endif /* NU_WITH_TOLOWER */
