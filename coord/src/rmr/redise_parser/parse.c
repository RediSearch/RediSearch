/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "parser_ctx.h"
#include "../cluster.h"
#include "../node.h"
#include "../endpoint.h"

void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh) {

  int found = -1;
  for (int i = 0; i < t->numShards; i++) {
    if (sh->startSlot == t->shards[i].startSlot && sh->endSlot == t->shards[i].endSlot) {
      found = i;
      break;
    }
  }

  if (found >= 0) {
    MRClusterShard_AddNode(&t->shards[found], &sh->node);
  } else {
    MRClusterShard csh = MR_NewClusterShard(sh->startSlot, sh->endSlot, 2);
    MRClusterShard_AddNode(&csh, &sh->node);
    MRClusterTopology_AddShard(t, &csh);
  }
}
