/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "rmr/common.h"
#include "rmr/endpoint.h"

typedef enum { MRNode_Master = 0x1, MRNode_Self = 0x2} MRNodeFlags;

typedef struct {
  MREndpoint endpoint;
  const char *id;
  MRNodeFlags flags;
} MRClusterNode;

/* Return 1 both nodes have the same host */
int MRNode_IsSameHost(MRClusterNode *n, MRClusterNode *other);
