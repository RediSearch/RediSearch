#include <assert.h>

#include "casemap.h"

#ifdef NU_WITH_UNACCENT

#include "casemap_internal.h"
#ifndef NU_WITH_BMP_ONLY
# include "gen/_tounaccent.c"
#else
# include "gen/_tounaccent_compact.c"
#endif /* NU_WITH_BMP_ONLY */

/* in nu_casemap_read (UTF-8), zero-terminated */
static const char *__nu_empty_string = "";

const char* nu_tounaccent(uint32_t codepoint) {
	typedef struct {
		uint32_t block_start;
		uint32_t block_end;
	} block_t;

	static const block_t blocks[] = {
		{ 0x0300, 0x036F },  /* Combining Diacritical Marks */
		{ 0x1AB0, 0x1AFF },  /* Combining Diacritical Marks Extended */
		{ 0x20D0, 0x20FF },  /* Combining Diacritical Marks for Symbols */
		{ 0x1DC0, 0x1DFF },  /* Combining Diacritical Marks Supplement */
	};
	static const size_t blocks_count = sizeof(blocks) / sizeof(*blocks);

	/* check if codepoint itself is a diacritic,
	 * return empty string in that case
	 * (transform into empty string) */
	for (size_t i = 0; i < blocks_count; ++i) {
		if (codepoint >= blocks[i].block_start && codepoint <= blocks[i].block_end) {
			return __nu_empty_string;
		}
	}

	return _nu_to_something(codepoint, NU_TOUNACCENT_G, NU_TOUNACCENT_G_SIZE,
		NU_TOUNACCENT_VALUES_C, NU_TOUNACCENT_VALUES_I, NU_TOUNACCENT_COMBINED);
}

const char* _nu_tounaccent(const char *encoded, const char *limit, nu_read_iterator_t read,
	uint32_t *u, const char **transform,
	void *context) {

	(void)(limit);
	(void)(context);

	uint32_t _u = 0;
	const char *np = read(encoded, &_u);

	*transform = nu_tounaccent(_u);

	if (u != 0) {
		*u = _u;
	}

	return np;
}

#endif /* NU_WITH_UNACCENT */
