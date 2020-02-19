#include "utf16.h"
#include "utf16be.h"
#include "utf16le.h"

#ifdef NU_WITH_UTF16_READER

const char* nu_utf16_read_bom(const char *encoded, nu_utf16_bom_t *bom) {
	unsigned char bom0 = *(unsigned char *)(encoded);
	unsigned char bom1 = *(unsigned char *)(encoded + 1);

	if (bom0 == 0xFF && bom1 == 0xFE) {
		if (bom != 0) {
#ifdef NU_WITH_UTF16_WRITER
			bom->write_bom = nu_utf16le_write_bom;
#endif
			bom->read = nu_utf16le_read;
			bom->write = nu_utf16le_write;
#ifdef NU_WITH_REVERSE_READ
			bom->revread = nu_utf16le_revread;
#endif
#ifdef NU_WITH_VALIDATION
			bom->validread = nu_utf16le_validread;
#endif
		}
	}
	else {
		if (bom != 0) {
#ifdef NU_WITH_UTF16_WRITER
			bom->write_bom = nu_utf16be_write_bom;
#endif
			bom->read = nu_utf16be_read;
			bom->write = nu_utf16be_write;
#ifdef NU_WITH_REVERSE_READ
			bom->revread = nu_utf16be_revread;
#endif
#ifdef NU_WITH_VALIDATION
			bom->validread = nu_utf16be_validread;
#endif
		}

		if (bom0 == 0xFE && bom1 == 0xFF) {
			return encoded + 2;
		}
		else {
			return encoded;
		}
	}

	return encoded + 2;
}

#endif /* NU_WITH_UTF16_READER */

#ifdef NU_WITH_UTF16_WRITER

char* nu_utf16le_write_bom(char *encoded) {
	unsigned char *p = (unsigned char *)(encoded);

	*(p)     = 0xFF;
	*(p + 1) = 0xFE;

	return encoded + 2;
}

char* nu_utf16be_write_bom(char *encoded) {
	unsigned char *p = (unsigned char *)(encoded);

	*(p)     = 0xFE;
	*(p + 1) = 0xFF;

	return encoded + 2;
}

#endif /* NU_WITH_UTF16_WRITER */
