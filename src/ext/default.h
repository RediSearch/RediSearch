#ifndef __EXT_DEFAULT_H__
#define __EXT_DEFAULT_H__
#include "redisearch.h"

#define DEFAULT_EXPANDER_NAME "SBSTEM"
#define DEFAULT_SCORER_NAME "TFIDF"

int DefaultExtensionInit(RSExtensionCtx *ctx);

#endif