/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef __EXT_DEFAULT_H__
#define __EXT_DEFAULT_H__
#include "redisearch.h"

#define PHONETIC_EXPENDER_NAME "PHONETIC"
#define SYNONYMS_EXPENDER_NAME "SYNONYM"
#define STEMMER_EXPENDER_NAME "SBSTEM"
#define DEFAULT_EXPANDER_NAME "DEFAULT"
#define DEFAULT_SCORER_NAME "TFIDF"
#define TFIDF_DOCNORM_SCORER_NAME "TFIDF.DOCNORM"
#define DISMAX_SCORER_NAME "DISMAX"
#define BM25_SCORER_NAME "BM25"
#define BM25_STD_SCORER_NAME "BM25STD"
#define DOCSCORE_SCORER "DOCSCORE"
#define HAMMINGDISTANCE_SCORER "HAMMING"

int DefaultExtensionInit(RSExtensionCtx *ctx);

#endif