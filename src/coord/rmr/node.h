/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#pragma once

#include "common.h"
#include "endpoint.h"

typedef enum { MRNode_Master = 0x1, MRNode_Self = 0x2} MRNodeFlags;

typedef struct {
  MREndpoint endpoint;
  const char *id;
  MRNodeFlags flags;
} MRClusterNode;

/* Return 1 both nodes have the same host */
int MRNode_IsSameHost(MRClusterNode *n, MRClusterNode *other);
