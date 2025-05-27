/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __EXT_DEFAULT_H__
#define __EXT_DEFAULT_H__
#include "redisearch.h"

#define DEFAULT_EXPANDER_NAME "DEFAULT"
#define DEFAULT_SCORER_NAME BM25_STD_SCORER_NAME

#define PHONETIC_EXPENDER_NAME "PHONETIC"
#define SYNONYMS_EXPENDER_NAME "SYNONYM"
#define STEMMER_EXPENDER_NAME "SBSTEM"
#define TFIDF_SCORER_NAME "TFIDF"
#define TFIDF_DOCNORM_SCORER_NAME "TFIDF.DOCNORM"
#define DISMAX_SCORER_NAME "DISMAX"
#define BM25_SCORER_NAME "BM25"
#define BM25_STD_SCORER_NAME "BM25STD"
#define BM25_STD_NORMALIZED_TANH_SCORER_NAME "BM25STD.TANH"
#define DOCSCORE_SCORER "DOCSCORE"
#define HAMMINGDISTANCE_SCORER "HAMMING"

int DefaultExtensionInit(RSExtensionCtx *ctx);

#endif
