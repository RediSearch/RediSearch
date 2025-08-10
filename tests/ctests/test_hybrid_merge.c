/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "hybrid/hybrid_search_result.h"
#include "rmutil/alloc.h"
#include "test_util.h"
#include "redisearch.h"
#include "rlookup.h"
#include "value.h"
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

/**
 * Helper function to create a test SearchResult with specified flags
 */
static SearchResult* createTestSearchResult(uint8_t flags) {
  SearchResult* result = rm_calloc(1, sizeof(SearchResult));
  if (!result) return NULL;

  result->docId = 1;  // Use a dummy docId
  result->score = 1.0;  // Use a dummy score
  result->flags = flags;
  result->scoreExplain = NULL;
  result->dmd = NULL;
  result->indexResult = NULL;
  memset(&result->rowdata, 0, sizeof(RLookupRow));

  return result;
}

/**
 * Test MergeFlags function with no flags set
 */
int testMergeFlags_NoFlags() {
  uint8_t target_flags = 0;
  uint8_t source_flags = 0;

  // Test merging no flags
  MergeFlags(&target_flags, &source_flags);
  ASSERT_EQUAL(target_flags, 0);

  return 0;
}

/**
 * Test MergeFlags function with Result_ExpiredDoc flag
 */
int testMergeFlags_ExpiredDoc() {
  uint8_t target_flags = 0;  // No flags initially
  uint8_t source_flags = Result_ExpiredDoc;  // Source has expired flag

  // Test merging expired flag
  MergeFlags(&target_flags, &source_flags);
  ASSERT(target_flags & Result_ExpiredDoc);

  return 0;
}


TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testMergeFlags_NoFlags);
  TESTFUNC(testMergeFlags_ExpiredDoc);
})
