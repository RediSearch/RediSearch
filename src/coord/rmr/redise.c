/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "redise.h"
#include "rmalloc.h"
#include "rmutil/args.h"
#include <strings.h>

typedef struct {
  int startSlot;
  int endSlot;
  MRClusterNode node;
} RLShard;

static void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh) {

  for (int i = 0; i < t->numShards; i++) {
    for (int r = 0; r < t->shards[i].numRanges; r++) {
      if (sh->startSlot == t->shards[i].ranges[r].start && sh->endSlot == t->shards[i].ranges[r].end) {
        // New node in the same shard
        MRClusterShard_AddNode(&t->shards[i], &sh->node);
        return;
      }
    }
    if (!(sh->node.flags & MRNode_Master)) {
      continue; // Only master nodes can cause shard merging
    }
    // Check if the master node already exists in the shard, in which case we can just
    for (int n = 0; n < t->shards[i].numNodes; n++) {
      if (!RedisModule_StringCompare(sh->node.id, t->shards[i].nodes[n].id)) {
        // Same node, extend the slot ranges (for all the nodes in the shard)
        MRClusterShard_AddRange(&t->shards[i], sh->startSlot, sh->endSlot);
        // Consume the node, we don't need to add it again
        MRClusterNode_Free(&sh->node);
        return;
      }
    }
  }

  // New shard
  MRClusterShard csh = MR_NewClusterShard(2);
  MRClusterShard_AddNode(&csh, &sh->node);
  MRClusterShard_AddRange(&csh, sh->startSlot, sh->endSlot);
  MRClusterTopology_AddShard(t, &csh);
}

/* Error replying macros, in attempt to make the code itself readable */
#define ERROR_FMT(fmt, ...)                          \
  ({                                                 \
    char *err;                                       \
    __ignore__(rm_asprintf(&err, fmt, __VA_ARGS__)); \
    RedisModule_ReplyWithError(ctx, err);            \
    rm_free(err);                                    \
  })

#define ERROR_BADVAL(arg, val) ERROR_FMT("Bad value for %s: %s", arg, val)
#define ERROR_EXPECTED(exp, arg) ERROR_FMT("Expected " exp " but got `%s`", arg)
#define ERROR_MISSING(arg) RedisModule_ReplyWithError(ctx, "Missing value for " arg)

#define ERROR_BAD_OR_MISSING(arg, ac_code)          \
  ({                                                \
    if (ac_code == AC_ERR_NOARG) {                  \
      ERROR_MISSING(arg);                           \
    } else {                                        \
      ERROR_BADVAL(arg, AC_GetStringNC(&ac, NULL)); \
    }                                               \
  })

#define VERIFY_ARG(arg)                                   \
  ({                                                      \
    if (!AC_AdvanceIfMatch(&ac, arg)) {                   \
      const char *val = AC_GetStringNC(&ac, NULL);        \
      ERROR_EXPECTED("`" arg "`", (val ? val : "(nil)")); \
      goto error;                                         \
    }                                                     \
  })

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                 int argc) {
  ArgsCursor ac; // Name is important for error macros, same goes for `ctx`
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  RedisModuleString *myID = NULL;          // Mandatory. No default.
  size_t numShards = 0;                    // Mandatory. No default.
  size_t numSlots = 16384;                 // Default.
  MRHashFunc hashFunc = MRHashFunc_CRC16;  // Default.

  // Parse general arguments. No allocation is done here, so we can just return on error
  while (!AC_IsAtEnd(&ac)) {
    if (AC_AdvanceIfMatch(&ac, "MYID")) {
      AC_GetRString(&ac, &myID, 0);  // Verified after breaking out of loop
    } else if (AC_AdvanceIfMatch(&ac, "HASHFUNC")) {
      const char *hashFuncStr = AC_GetStringNC(&ac, NULL);
      if (!hashFuncStr) {
        ERROR_MISSING("HASHFUNC");
        return NULL;
      }
      if (!strcasecmp(hashFuncStr, MRHASHFUNC_CRC12_STR)) {
        hashFunc = MRHashFunc_CRC12;
      } else if (!strcasecmp(hashFuncStr, MRHASHFUNC_CRC16_STR)) {
        hashFunc = MRHashFunc_CRC16;
      } else {
        ERROR_BADVAL("HASHFUNC", hashFuncStr);
        return NULL;
      }
    } else if (AC_AdvanceIfMatch(&ac, "NUMSLOTS")) {
      int rc = AC_GetSize(&ac, &numSlots, AC_F_GE1);
      if (rc != AC_OK) {
        ERROR_BAD_OR_MISSING("NUMSLOTS", rc);
        return NULL;
      } else if (numSlots > 16384) {
        ERROR_FMT("Bad value for NUMSLOTS: %zu", numSlots);
        return NULL;
      }
    } else if (AC_AdvanceIfMatch(&ac, "RANGES")) {  // End of general arguments
      int rc = AC_GetSize(&ac, &numShards, AC_F_GE1);
      if (rc != AC_OK) {
        ERROR_BAD_OR_MISSING("RANGES", rc);
        return NULL;
      }
      break;
    } else if (AC_AdvanceIfMatch(&ac, "HASREPLICATION")) { // TODO: remove
    } else {
      ERROR_FMT("Unexpected argument: `%s`", AC_GetStringNC(&ac, NULL));
      return NULL;
    }
  }

  if (!myID) {
    ERROR_MISSING("MYID");
    return NULL;
  }

  MRClusterTopology *topo = MR_NewTopology(numShards, numSlots, hashFunc);

  // Parse shards. We have to free the topology and previous shards if we encounter an error
  for (size_t i = 0; i < numShards; i++) {
    RLShard sh;
    int rc;
    /* Mandatory: SHARD <shard_id> SLOTRANGE <start_slot> <end_slot> ADDR <tcp> */
    VERIFY_ARG("SHARD");
    if (AC_GetRString(&ac, &sh.node.id, 0) != AC_OK) {
      ERROR_MISSING("SHARD");
      goto error;
    }

    VERIFY_ARG("SLOTRANGE");
    rc = AC_GetInt(&ac, &sh.startSlot, AC_F_GE0);
    if (rc != AC_OK) {
      ERROR_BAD_OR_MISSING("SLOTRANGE", rc);
      goto error;
    }
    rc = AC_GetInt(&ac, &sh.endSlot, AC_F_GE0);
    if (rc != AC_OK) {
      ERROR_BAD_OR_MISSING("SLOTRANGE", rc);
      goto error;
    }
    if (sh.startSlot > sh.endSlot || sh.endSlot >= numSlots) {
      ERROR_FMT("Bad values for SLOTRANGE: %d, %d", sh.startSlot, sh.endSlot);
      goto error;
    }

    VERIFY_ARG("ADDR");
    const char *addr;
    if (!(addr = AC_GetStringNC(&ac, NULL))) {
      ERROR_MISSING("ADDR");
      goto error;
    }

    /* Optional UNIXADDR <unix_addr> */
    const char *unixSock = NULL;
    if (AC_AdvanceIfMatch(&ac, "UNIXADDR")) {
      if (!(unixSock = AC_GetStringNC(&ac, NULL))) {
        ERROR_MISSING("UNIXADDR");
        goto error;
      }
    }
    if (MREndpoint_Parse(addr, &sh.node.endpoint) != REDIS_OK) {
      ERROR_BADVAL("ADDR", addr);
      goto error;
    }
    // All good. Finish up the node
    sh.node.id = RedisModule_CreateStringFromString(NULL, sh.node.id);  // Take ownership of the string
    RedisModule_TrimStringAllocation(sh.node.id);
    if (unixSock) {
      sh.node.endpoint.unixSock = rm_strdup(unixSock);
    }
    sh.node.flags = 0;
    if (!RedisModule_StringCompare(sh.node.id, myID)) {
      sh.node.flags |= MRNode_Self;
    }
    /* Optional MASTER */
    if (AC_AdvanceIfMatch(&ac, "MASTER")) {
      sh.node.flags |= MRNode_Master;
    }
    // Add the shard. This function will take ownership of the node's allocated strings
    MRTopology_AddRLShard(topo, &sh);
  }

  if (!AC_IsAtEnd(&ac)) {
    ERROR_EXPECTED("end of command", AC_GetStringNC(&ac, NULL));
    goto error;
  }

  return topo;

error:
  MRClusterTopology_Free(topo);
  return NULL;
}
