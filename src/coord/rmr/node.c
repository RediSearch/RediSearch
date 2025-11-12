/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <string.h>

#include "node.h"

/* Return 1 both nodes have the same host */
int MRNode_IsSameHost(MRClusterNode *n, MRClusterNode *other) {
  if (!n || !other) return 0;
  return strcasecmp(n->endpoint.host, other->endpoint.host) == 0;
}
