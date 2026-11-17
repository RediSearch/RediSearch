/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef KNN_CONTEXT_H__
#define KNN_CONTEXT_H__

#include "query_node.h"

typedef struct {
  size_t k;               // K value TODO: consider remove from here, its in querynode
  const char* fieldName;  // Field name
  bool shouldSort;        // Should run presort before the coordinator sort
  size_t offset;          // Reply offset
  QueryNode* queryNode;   // Query node
} knnContext;

#endif
