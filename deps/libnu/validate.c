#include "validate.h"

#ifdef NU_WITH_VALIDATION

const char* nu_validate(const char *encoded, size_t max_len, nu_validread_iterator_t it) {
	const char *p = encoded;

	while (p < encoded + max_len) {
		/* max_len should be tested inside of it() call */
		int byte_len = it(p, max_len - (p - encoded));

		if (byte_len <= 0) {
			return p;
		}

		p += byte_len;
	}

	return 0;
}

#endif /* NU_WITH_VALIDATION */
