#include "defines.h"
#include "strings.h"

#if defined (NU_WITH_Z_STRINGS) || defined(NU_WITH_N_STRINGS)

static ssize_t _nu_strlen(const char *encoded, const char *limit, nu_read_iterator_t it) {
	ssize_t len = 0;

	const char *p = encoded;
	while (p < limit) {
		uint32_t u = 0;
		p = it(p, &u);

		if (u == 0) {
			break;
		}

		++len;
	}

	return len;
}

static ssize_t _nu_bytelen(const uint32_t *unicode, const uint32_t *limit, nu_write_iterator_t it) {
	ssize_t len = 0;

	const uint32_t *p = unicode;
	while (p < limit) {
		if (*p == 0) {
			break;
		}

		/* nu_write_iterator_t will return offset relative to 0
		 * which is effectively bytes length of codepoint */
		size_t byte_len = (size_t)it(*p, 0);
		len += byte_len;

		++p;
	}

	return len;
}

static ssize_t _nu_strbytelen(const char *encoded, const char *limit, nu_read_iterator_t it) {
	uint32_t u = 0;
	const char *p = encoded;

	while (p < limit) {
		const char *np = it(p, &u);

		if (u == 0) {
			return (p - encoded);
		}

		p = np;
	}

	return 0;
}

#endif /* NU_WITH_N_STRINGS || NU_WITH_Z_STRINGS */

#ifdef NU_WITH_Z_STRINGS

ssize_t nu_strlen(const char *encoded, nu_read_iterator_t it) {
	return _nu_strlen(encoded, NU_UNLIMITED, it);
}

ssize_t nu_bytelen(const uint32_t *unicode, nu_write_iterator_t it) {
	return _nu_bytelen(unicode, NU_UNLIMITED, it);
}

ssize_t nu_strbytelen(const char *encoded, nu_read_iterator_t it) {
	return _nu_strbytelen(encoded, NU_UNLIMITED, it);
}

#endif /* NU_WITH_Z_STRINGS */

#ifdef NU_WITH_N_STRINGS

ssize_t nu_strnlen(const char *encoded, size_t max_len, nu_read_iterator_t it) {
	return _nu_strlen(encoded, encoded + max_len, it);
}

ssize_t nu_bytenlen(const uint32_t *unicode, size_t max_len, nu_write_iterator_t it) {
	return _nu_bytelen(unicode, unicode + max_len, it);
}

#endif /* NU_WITH_N_STRINGS */
