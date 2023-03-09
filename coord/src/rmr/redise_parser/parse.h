/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef _RMR_PARSER_H_
#define _RMR_PARSER_H_

#include "../cluster.h"
#include "../node.h"
#include "../endpoint.h"

typedef struct {
  int startSlot;
  int endSlot;
  MRClusterNode node;
} RLShard;

void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh);
MRClusterTopology *MR_ParseTopologyRequest(const char *c, size_t len, char **err);
#endif