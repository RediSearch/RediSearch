#include "bmponly.h"

bool nu_bmp_only(void) {
#ifdef NU_WITH_BMP_ONLY
	return true;
#else
	return false;
#endif /* NU_WITH_BMP_ONLY */
}
