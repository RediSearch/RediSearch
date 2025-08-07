/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "score_explain.h"
#include "rmutil/alloc.h"
#include "test_util.h"
#include <string.h>
#include <stdio.h>
#include <assert.h>

/**
 * Test SECopy with basic string copying
 */
int testSECopy_BasicString() {
  // Create source with just a string
  RSScoreExplain *source = rm_calloc(1, sizeof(RSScoreExplain));
  source->str = rm_strdup("Test explanation string");
  source->numChildren = 0;
  source->children = NULL;

  // Copy to destination
  RSScoreExplain *destination = rm_calloc(1, sizeof(RSScoreExplain));
  SECopy(destination, source);

  // Verify string was copied correctly
  ASSERT(destination->str != NULL);
  ASSERT(strcmp(destination->str, "Test explanation string") == 0);
  ASSERT(destination->str != source->str);  // Different pointers (independent copy)
  ASSERT(destination->numChildren == 0);
  ASSERT(destination->children == NULL);

  // Cleanup
  SEDestroy(source);
  SEDestroy(destination);

  return 0;
}

/**
 * Test SECopy with children array
 */
int testSECopy_WithChildren() {
  // Create source with string and children
  RSScoreExplain *source = rm_calloc(1, sizeof(RSScoreExplain));
  source->str = rm_strdup("Parent explanation");
  source->numChildren = 2;
  source->children = rm_calloc(2, sizeof(RSScoreExplain));
  source->children[0].str = rm_strdup("Child 1 explanation");
  source->children[0].numChildren = 0;
  source->children[0].children = NULL;
  source->children[1].str = rm_strdup("Child 2 explanation");
  source->children[1].numChildren = 0;
  source->children[1].children = NULL;

  // Copy to destination
  RSScoreExplain *destination = rm_calloc(1, sizeof(RSScoreExplain));
  SECopy(destination, source);

  // Verify parent
  ASSERT(destination->str != NULL);
  ASSERT(strcmp(destination->str, "Parent explanation") == 0);
  ASSERT(destination->str != source->str);  // Independent copy
  ASSERT(destination->numChildren == 2);
  ASSERT(destination->children != NULL);
  ASSERT(destination->children != source->children);  // Independent array

  // Verify children
  ASSERT(destination->children[0].str != NULL);
  ASSERT(strcmp(destination->children[0].str, "Child 1 explanation") == 0);
  ASSERT(destination->children[0].str != source->children[0].str);  // Independent copy
  ASSERT(destination->children[0].numChildren == 0);
  ASSERT(destination->children[0].children == NULL);

  ASSERT(destination->children[1].str != NULL);
  ASSERT(strcmp(destination->children[1].str, "Child 2 explanation") == 0);
  ASSERT(destination->children[1].str != source->children[1].str);  // Independent copy
  ASSERT(destination->children[1].numChildren == 0);
  ASSERT(destination->children[1].children == NULL);

  // Cleanup
  SEDestroy(source);
  SEDestroy(destination);

  return 0;
}

/**
 * Test SECopy with nested children (recursive copying)
 */
int testSECopy_NestedChildren() {
  // Create source with nested structure
  RSScoreExplain *source = rm_calloc(1, sizeof(RSScoreExplain));
  source->str = rm_strdup("Root");
  source->numChildren = 1;
  source->children = rm_calloc(1, sizeof(RSScoreExplain));

  // First level child
  source->children[0].str = rm_strdup("Level 1");
  source->children[0].numChildren = 2;
  source->children[0].children = rm_calloc(2, sizeof(RSScoreExplain));

  // Second level children
  source->children[0].children[0].str = rm_strdup("Level 2A");
  source->children[0].children[0].numChildren = 0;
  source->children[0].children[0].children = NULL;

  source->children[0].children[1].str = rm_strdup("Level 2B");
  source->children[0].children[1].numChildren = 0;
  source->children[0].children[1].children = NULL;

  // Copy to destination
  RSScoreExplain *destination = rm_calloc(1, sizeof(RSScoreExplain));
  SECopy(destination, source);

  // Verify root
  ASSERT(strcmp(destination->str, "Root") == 0);
  ASSERT(destination->str != source->str);
  ASSERT(destination->numChildren == 1);

  // Verify level 1
  ASSERT(strcmp(destination->children[0].str, "Level 1") == 0);
  ASSERT(destination->children[0].str != source->children[0].str);
  ASSERT(destination->children[0].numChildren == 2);

  // Verify level 2
  ASSERT(strcmp(destination->children[0].children[0].str, "Level 2A") == 0);
  ASSERT(destination->children[0].children[0].str != source->children[0].children[0].str);
  ASSERT(strcmp(destination->children[0].children[1].str, "Level 2B") == 0);
  ASSERT(destination->children[0].children[1].str != source->children[0].children[1].str);

  // Cleanup
  SEDestroy(source);
  SEDestroy(destination);

  return 0;
}

/**
 * Test SECopy with NULL source
 */
int testSECopy_NullSource() {
  RSScoreExplain *destination = rm_calloc(1, sizeof(RSScoreExplain));

  // Should handle NULL source gracefully
  SECopy(destination, NULL);

  // Destination should remain unchanged (zero-initialized)
  ASSERT(destination->str == NULL);
  ASSERT(destination->numChildren == 0);
  ASSERT(destination->children == NULL);

  // Cleanup
  SEDestroy(destination);

  return 0;
}

/**
 * Test SECopy with NULL destination
 */
int testSECopy_NullDestination() {
  // Create source
  RSScoreExplain *source = rm_calloc(1, sizeof(RSScoreExplain));
  source->str = rm_strdup("Test");
  source->numChildren = 0;
  source->children = NULL;

  // Should handle NULL destination gracefully (no crash)
  SECopy(NULL, source);

  // Source should remain unchanged
  ASSERT(strcmp(source->str, "Test") == 0);
  ASSERT(source->numChildren == 0);

  // Cleanup
  SEDestroy(source);

  return 0;
}

/**
 * Test SECopy with empty source (no string, no children)
 */
int testSECopy_EmptySource() {
  // Create empty source
  RSScoreExplain *source = rm_calloc(1, sizeof(RSScoreExplain));
  source->str = NULL;
  source->numChildren = 0;
  source->children = NULL;

  // Copy to destination
  RSScoreExplain *destination = rm_calloc(1, sizeof(RSScoreExplain));
  SECopy(destination, source);

  // Verify destination is also empty
  ASSERT(destination->str == NULL);
  ASSERT(destination->numChildren == 0);
  ASSERT(destination->children == NULL);

  // Cleanup
  SEDestroy(source);
  SEDestroy(destination);

  return 0;
}

/**
 * Test memory independence after SECopy
 */
int testSECopy_MemoryIndependence() {
  // Create source
  RSScoreExplain *source = rm_calloc(1, sizeof(RSScoreExplain));
  source->str = rm_strdup("Original");
  source->numChildren = 1;
  source->children = rm_calloc(1, sizeof(RSScoreExplain));
  source->children[0].str = rm_strdup("Child Original");
  source->children[0].numChildren = 0;
  source->children[0].children = NULL;

  // Copy to destination
  RSScoreExplain *destination = rm_calloc(1, sizeof(RSScoreExplain));
  SECopy(destination, source);

  // Modify source after copying
  rm_free(source->str);
  source->str = rm_strdup("Modified");
  rm_free(source->children[0].str);
  source->children[0].str = rm_strdup("Child Modified");

  // Verify destination is unchanged (independent)
  ASSERT(strcmp(destination->str, "Original") == 0);
  ASSERT(strcmp(destination->children[0].str, "Child Original") == 0);

  // Cleanup
  SEDestroy(source);
  SEDestroy(destination);

  return 0;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testSECopy_BasicString);
  TESTFUNC(testSECopy_WithChildren);
  TESTFUNC(testSECopy_NestedChildren);
  TESTFUNC(testSECopy_NullSource);
  TESTFUNC(testSECopy_NullDestination);
  TESTFUNC(testSECopy_EmptySource);
  TESTFUNC(testSECopy_MemoryIndependence);
})