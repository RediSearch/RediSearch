/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "util/heap.h"
#include "query_node.h"

typedef enum {
  SPECIAL_CASE_NONE,
  SPECIAL_CASE_KNN,
  SPECIAL_CASE_SORTBY
} searchRequestSpecialCase;

typedef struct {
  size_t k;               // K value TODO: consider remove from here, its in querynode
  const char* fieldName;  // Field name
  bool shouldSort;        // Should run presort before the coordinator sort
  size_t offset;          // Reply offset
  heap_t *pq;             // Priority queue
  QueryNode* queryNode;   // Query node
} knnContext;

typedef struct {
  const char* sortKey;  // SortKey name;
  bool asc;             // Sort order ASC/DESC
  size_t offset;        // SortKey reply offset
} sortbyContext;

typedef struct {
  union {
    knnContext knn;
    sortbyContext sortby;
  };
  searchRequestSpecialCase specialCaseType;
} specialCaseCtx;
