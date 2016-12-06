#include "casemap.h"

#ifdef NU_WITH_TOUPPER

#include "casemap_internal.h"
#include "gen/_toupper.c"

const char* nu_toupper(uint32_t codepoint) {
	return _nu_to_something(codepoint, NU_TOUPPER_G, NU_TOUPPER_G_SIZE,
		NU_TOUPPER_VALUES_C, NU_TOUPPER_VALUES_I, NU_TOUPPER_COMBINED);
}

const char* _nu_toupper(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context) {

	(void)(limit);
	(void)(context);

	uint32_t _u = 0;
	const char *np = read(encoded, &_u);

	*transform = nu_toupper(_u);

	if (u != 0) {
		*u = _u;
	}

	return np;
}

#endif /* NU_WITH_TOUPPER */
