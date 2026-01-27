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
#define BM25_STD_NORMALIZED_MAX_SCORER_NAME "BM25STD.NORM"
#define DOCSCORE_SCORER "DOCSCORE"
#define HAMMINGDISTANCE_SCORER "HAMMING"

/* Test scorer names - for debug command use */
#define TEST_NUM_DOCS_SCORER_NAME "TEST_NUM_DOCS"
#define TEST_NUM_TERMS_SCORER_NAME "TEST_NUM_TERMS"
#define TEST_AVG_DOC_LEN_SCORER_NAME "TEST_AVG_DOC_LEN"
#define TEST_SUM_IDF_SCORER_NAME "TEST_SUM_IDF"
#define TEST_SUM_BM25_IDF_SCORER_NAME "TEST_SUM_BM25_IDF"

int DefaultExtensionInit(RSExtensionCtx *ctx);

/* Register the test scorers - for debug command use */
int Ext_RegisterTestScorers(void);

#endif
