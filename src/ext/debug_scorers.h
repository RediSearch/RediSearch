/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __EXT_DEBUG_SCORERS_H__
#define __EXT_DEBUG_SCORERS_H__

#include "redisearch.h"

/* Test scorer names - for debug command use */
#define TEST_NUM_DOCS_SCORER_NAME "TEST_NUM_DOCS"
#define TEST_NUM_TERMS_SCORER_NAME "TEST_NUM_TERMS"
#define TEST_AVG_DOC_LEN_SCORER_NAME "TEST_AVG_DOC_LEN"
#define TEST_SUM_IDF_SCORER_NAME "TEST_SUM_IDF"
#define TEST_SUM_BM25_IDF_SCORER_NAME "TEST_SUM_BM25_IDF"

/* Register the test scorers - for debug command use */
int Ext_RegisterTestScorers(void);

#endif

