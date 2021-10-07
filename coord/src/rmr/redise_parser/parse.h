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