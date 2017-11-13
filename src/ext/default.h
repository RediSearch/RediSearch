#ifndef __EXT_DEFAULT_H__
#define __EXT_DEFAULT_H__
#include "redisearch.h"

#define DEFAULT_EXPANDER_NAME "SBSTEM"
#define DEFAULT_SCORER_NAME "TFIDF"
#define TFIDF_DOCNORM_SCORER_NAME "TFIDF.DOCNORM"
#define DISMAX_SCORER_NAME "DISMAX"
#define BM25_SCORER_NAME "BM25"
#define DOCSCORE_SCORER "DOCSCORE"

int DefaultExtensionInit(RSExtensionCtx *ctx);

#endif