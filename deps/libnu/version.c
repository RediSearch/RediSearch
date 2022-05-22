#include "version.h"

static const char *__nu_version_string = NU_VERSION;

const char* nu_version(void) {
	return __nu_version_string;
}
