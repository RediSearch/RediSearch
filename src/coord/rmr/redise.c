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
#include "slot_ranges.h"
#include <strings.h>

typedef struct {
  arrayof(RedisModuleSlotRange) slotRanges;
  MRClusterNode node;
} RLShard;

static void RLShard_Free(RLShard *sh) {
  if (!sh) return;
  MRClusterNode_Free(&sh->node);
  array_free(sh->slotRanges);
  rm_free(sh);
}

static void RLShard_Free_(void *priv, void *val) {
  RLShard *sh = val;
  RLShard_Free(sh);
}

dictType staticRIDtoShard = {
    .hashFunction = redisStringsHashFunction,
    .keyDup = NULL,
    .valDup = NULL,
    .keyCompare = redisStringsKeyCompare,
    .keyDestructor = NULL,
    .valDestructor = RLShard_Free_,
};

static void MRTopology_AddRLShard(MRClusterTopology *t, RLShard *sh) {
  // New shard
  size_t total_size = SlotRangeArray_SizeOf(array_len(sh->slotRanges));
  RedisModuleSlotRangeArray* array = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
  array->num_ranges = array_len(sh->slotRanges);
  for (size_t i = 0; i < array_len(sh->slotRanges); i++) {
    array->ranges[i] = sh->slotRanges[i];
  }
  MRClusterShard csh = MR_NewClusterShard(&sh->node, array);
  sh->node = (MRClusterNode){0}; // ownership transferred
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
                                                 int argc, uint32_t *my_shard_idx) {
  ArgsCursor ac; // Name is important for error macros, same goes for `ctx`
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  const char *myID = NULL;                 // Mandatory. No default.
  uint32_t numRanges = 0;                  // Mandatory. No default.
  uint32_t numSlots = 16384;               // Default.

  // Parse general arguments. No allocation is done here, so we can just return on error
  while (!AC_IsAtEnd(&ac)) {
    if (AC_AdvanceIfMatch(&ac, "MYID")) {
      myID = AC_GetStringNC(&ac, NULL);  // Verified after breaking out of loop
    } else if (AC_AdvanceIfMatch(&ac, "HASHFUNC")) {
      const char *hashFuncStr = AC_GetStringNC(&ac, NULL);
      if (!hashFuncStr) {
        ERROR_MISSING("HASHFUNC");
        return NULL;
      }
      if (strcasecmp(hashFuncStr, "CRC12") && strcasecmp(hashFuncStr, "CRC16")) {
        ERROR_BADVAL("HASHFUNC", hashFuncStr);
        return NULL;
      }
    } else if (AC_AdvanceIfMatch(&ac, "NUMSLOTS")) {
      int rc = AC_GetU32(&ac, &numSlots, AC_F_GE1);
      if (rc != AC_OK) {
        ERROR_BAD_OR_MISSING("NUMSLOTS", rc);
        return NULL;
      } else if (numSlots > 16384) {
        ERROR_FMT("Bad value for NUMSLOTS: %u", numSlots);
        return NULL;
      }
    } else if (AC_AdvanceIfMatch(&ac, "RANGES")) {  // End of general arguments
      int rc = AC_GetU32(&ac, &numRanges, AC_F_GE1);
      if (rc != AC_OK) {
        ERROR_BAD_OR_MISSING("RANGES", rc);
        return NULL;
      }
      break;
    } else if (AC_AdvanceIfMatch(&ac, "HASREPLICATION")) { // ignored
    } else {
      ERROR_FMT("Unexpected argument: `%s`", AC_GetStringNC(&ac, NULL));
      return NULL;
    }
  }

  if (!myID) {
    ERROR_MISSING("MYID");
    return NULL;
  }

  dict *shards = dictCreate(&staticRIDtoShard, NULL);
  MRClusterTopology *topo = NULL;
  RLShard *sh = NULL;

  // Parse shards. We have to free the collected shards if we encounter an error
  for (uint32_t i = 0; i < numRanges; i++) {

    sh = rm_calloc(1, sizeof(RLShard));
    sh->slotRanges = array_new(RedisModuleSlotRange, 1);

    /* Mandatory: SHARD <shard_id> */
    VERIFY_ARG("SHARD");
    RedisModuleString *shardIDStr;
    if (AC_GetRString(&ac, &shardIDStr, 0) != AC_OK) {
      ERROR_MISSING("SHARD");
      goto error;
    }

    size_t len;
    const char *idstr = RedisModule_StringPtrLen(shardIDStr, &len);
    sh->node.id = rm_strndup(idstr, len);

    bool is_master = false;
    while (!AC_IsAtEnd(&ac)) {
      if (AC_AdvanceIfMatch(&ac, "SLOTRANGE")) {
        if (array_len(sh->slotRanges) > 0) {
          ERROR_FMT("Multiple SLOTRANGE specified for shard `%s` at offset %zu", sh->node.id, ac.offset);
          goto error;
        }
        RedisModuleSlotRange slotRange;
        int rc = AC_GetU16(&ac, &slotRange.start, 0);
        if (rc != AC_OK) {
          ERROR_BAD_OR_MISSING("SLOTRANGE start", rc);
          goto error;
        }
        rc = AC_GetU16(&ac, &slotRange.end, 0);
        if (rc != AC_OK || slotRange.end >= numSlots) {
          ERROR_BAD_OR_MISSING("SLOTRANGE end", rc);
          goto error;
        }
        if (slotRange.start > slotRange.end) {
          ERROR_FMT("Bad values for SLOTRANGE: %d, %d", slotRange.start, slotRange.end);
          goto error;
        }
        array_ensure_append_1(sh->slotRanges, slotRange);

      } else if (AC_AdvanceIfMatch(&ac, "ADDR")) {
        const char *addr;
        if (!(addr = AC_GetStringNC(&ac, NULL))) {
          ERROR_MISSING("ADDR");
          goto error;
        } else if (sh->node.endpoint.host) {
          ERROR_FMT("Multiple ADDR specified for shard `%s` at offset %zu", sh->node.id, ac.offset);
          goto error;
        }
        if (MREndpoint_Parse(addr, &sh->node.endpoint) != REDIS_OK) {
          ERROR_BADVAL("ADDR", addr);
          goto error;
        }

      } else if (AC_AdvanceIfMatch(&ac, "UNIXADDR")) {
        /* Optional UNIXADDR <unix_addr> */
        size_t len;
        const char *unixSock;
        if (!(unixSock = AC_GetStringNC(&ac, &len))) {
          ERROR_MISSING("UNIXADDR");
          goto error;
        }
        if (sh->node.endpoint.unixSock) {
          ERROR_FMT("Multiple UNIXADDR specified for shard `%s`", sh->node.id);
          goto error;
        }
        sh->node.endpoint.unixSock = rm_strndup(unixSock, len);

      } else if (AC_AdvanceIfMatch(&ac, "MASTER")) {
        is_master = true;
      } else {
        break;
      }
    }

    // We don't care for replicas using this command anymore
    if (!is_master) {
      RLShard_Free(sh);
      sh = NULL;
      continue;
    }

    // Ignore shards with no slot ranges (like replicas)
    if (array_len(sh->slotRanges) == 0) {
      RLShard_Free(sh);
      sh = NULL;
      continue;
    }


    dictEntry *entry = dictAddOrFind(shards, shardIDStr);
    if (!dictGetVal(entry)) {
      // New shard
      // Verify mandatory arguments on first appearance
      if (!sh->node.endpoint.host) {
        ERROR_MISSING("ADDR");
        goto error;
      }
      // Move ownership of parsed shard into dict
      dictSetVal(shards, entry, sh);
      sh = NULL;
    } else {
      // Re-appearance of shard ID
      // We verify that the endpoint is the same
      // We also verify that slot range is different from previous ones
      RLShard *existing_shard = dictGetVal(entry);

      // Verify endpoint, if currently specified
      if (sh->node.endpoint.host) {

        if (strcmp(sh->node.endpoint.host, existing_shard->node.endpoint.host)) {
          ERROR_FMT("Conflicting ADDR for shard `%s`", sh->node.id);
          goto error;
        }
        if ((sh->node.endpoint.password && !existing_shard->node.endpoint.password) ||
            (!sh->node.endpoint.password && existing_shard->node.endpoint.password) ||
            (sh->node.endpoint.password && existing_shard->node.endpoint.password && strcmp(sh->node.endpoint.password, existing_shard->node.endpoint.password))) {
          ERROR_FMT("Conflicting ADDR for shard `%s`", sh->node.id);
          goto error;
        }
        if (sh->node.endpoint.port != existing_shard->node.endpoint.port) {
          ERROR_FMT("Conflicting ADDR for shard `%s`", sh->node.id);
          goto error;
        }
      }
      if (sh->node.endpoint.unixSock) {
        if (!existing_shard->node.endpoint.unixSock || strcmp(sh->node.endpoint.unixSock, existing_shard->node.endpoint.unixSock)) {
          ERROR_FMT("Conflicting UNIXADDR for shard `%s`", sh->node.id);
          goto error;
        }
      }
      RS_ASSERT(array_len(sh->slotRanges) == 1);
      // Verify slot range starts past existing ones
      if (array_tail(existing_shard->slotRanges).end + 1 >= sh->slotRanges[0].start) {
        ERROR_FMT("SLOTRANGE out of order for shard `%s`", sh->node.id);
        goto error;
      }

      // Append new slot range
      array_ensure_append_1(existing_shard->slotRanges, sh->slotRanges[0]);

      // Discard parsed shard
      RLShard_Free(sh);
      sh = NULL;
    }
  }

  if (!AC_IsAtEnd(&ac)) {
    ERROR_EXPECTED("end of command", AC_GetStringNC(&ac, NULL));
    goto error;
  }

  // Now, build the topology.
  // 1. All shards in the dict are valid masters
  // 2. We can identify my shard by myID
  topo = MR_NewTopology(dictSize(shards));
  dictIterator *iter = dictGetIterator(shards);
  dictEntry *de;
  while ((de = dictNext(iter)) != NULL) {
    MRTopology_AddRLShard(topo, dictGetVal(de));
  }
  dictReleaseIterator(iter);

  // Sort shards to have a deterministic order
  MRClusterTopology_SortShards(topo);

  // Identify my shard index
  *my_shard_idx = UINT32_MAX;
  for (uint32_t i = 0; i < topo->numShards; i++) {
    MRClusterShard *sh = &topo->shards[i];
    if (!strcmp(sh->node.id, myID)) {
      *my_shard_idx = i;
      break;
    }
  }

  if (*my_shard_idx == UINT32_MAX) {
    ERROR_FMT("MYID `%s` does not correspond to any shard", myID);
    MRClusterTopology_Free(topo);
    topo = NULL;
    goto error;
  }

error: // Also the normal exit point

  RLShard_Free(sh);
  dictRelease(shards);
  return topo;
}
