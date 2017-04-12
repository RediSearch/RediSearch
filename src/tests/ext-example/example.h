#ifndef __EXT_EXAMPLE_H__
#define __EXT_EXAMPLE_H__
#include "redisearch.h"

#define EXPANDER_NAME "EXAMPLE_EXPANDER"
#define SCORER_NAME "EXAMPLE_SCORER"

const char *extentionName = "EXAMPLE_EXTENSION";

int RS_ExtensionInit(RSExtensionCtx *ctx);

#endif