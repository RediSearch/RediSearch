/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "redise.h"
#include "redise_parser/parse.h"

MRClusterTopology *RedisEnterprise_ParseTopology(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                 int argc) {

  size_t totalLen = 0;
  const char *cargs[argc];
  size_t lens[argc];
  for (int i = 1; i < argc; i++) {
    cargs[i - 1] = RedisModule_StringPtrLen(argv[i], &lens[i - 1]);
    totalLen += lens[i - 1] + 1;
  }

  char *str = rm_calloc(totalLen + 2, 1);
  char *p = str;
  for (int i = 0; i < argc - 1; i++) {
    strncpy(p, cargs[i], lens[i]);
    p += lens[i];
    *p++ = ' ';
  }
  p--;
  *p = 0;
  RedisModule_Log(ctx, "notice", "Got topology update: %s", str);
  char *err = NULL;
  MRClusterTopology *topo = MR_ParseTopologyRequest(str, strlen(str), &err);
  rm_free(str);
  if (err != NULL) {
    RedisModule_Log(ctx, "warning", "Could not parse cluster topology: %s", err);
    RedisModule_ReplyWithError(ctx, err);
    rm_free(err);
    return NULL;
  }

  return topo;
}

#define ERROR_FMT(fmt, ...)                          \
  {                                                  \
    char *err;                                       \
    __ignore__(rm_asprintf(&err, fmt, __VA_ARGS__)); \
    RedisModule_ReplyWithError(ctx, err);            \
    rm_free(err);                                    \
  }

#define ERROR_BAD_STRING(arg, val) ERROR_FMT("Bad value for %s: %s", arg, val)
#define ERROR_BAD_SIZE(arg, val) ERROR_FMT("Bad value for %s: %zu", arg, val)
#define ERROR_EXPECTED(exp, arg) ERROR_FMT("Expected " exp " but got `%s`", arg)
#define ERROR_MISSING(arg) RedisModule_ReplyWithError(ctx, "Missing value for " arg)

#define VERIFY_ARG(arg)                           \
  if (!AC_AdvanceIfMatch(&ac, arg)) {             \
    ERROR_EXPECTED("`" arg "`", AC_CURRENT(&ac)); \
    goto error;                                   \
  }

MRClusterTopology *RedisEnterprise_ParseTopology_(RedisModuleCtx *ctx, RedisModuleString **argv,
                                                  int argc) {
  // Minimal command: CMD MYID <myid> RANGES 1 SHARD <shard_id> SLOTRANGE <start_slot> <end_slot>
  // ADDR <tcp>
  if (argc < 12) {
    RedisModule_WrongArity(ctx);
    return NULL;
  }
  ArgsCursor ac;
  ArgsCursor_InitRString(&ac, argv + 1, argc - 1);
  const char *myID = NULL;                 // Mandatory. No default.
  size_t numShards = 0;                    // Mandatory. No default.
  size_t numSlots = 4096;                  // Default.
  MRHashFunc hashFunc = MRHashFunc_CRC12;  // Default.

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
      if (!strcasecmp(hashFuncStr, MRHASHFUNC_CRC12_STR)) {
        hashFunc = MRHashFunc_CRC12;
      } else if (!strcasecmp(hashFuncStr, MRHASHFUNC_CRC16_STR)) {
        hashFunc = MRHashFunc_CRC16;
      } else {
        ERROR_BAD_STRING("HASHFUNC", hashFuncStr);
        return NULL;
      }
    } else if (AC_AdvanceIfMatch(&ac, "NUMSLOTS")) {
      if (AC_GetSize(&ac, &numSlots, AC_F_GE1) != AC_OK || numSlots > 16384) {
        ERROR_BAD_SIZE("NUMSLOTS", numSlots);
      }
    } else if (AC_AdvanceIfMatch(&ac, "RANGES")) {  // End of general arguments
      if (AC_GetSize(&ac, &numShards, AC_F_GE1) != AC_OK) {
        ERROR_BAD_SIZE("RANGES", numShards);
      }
      break;
    } else {
      ERROR_FMT("Unexpected argument: `%s`", AC_CURRENT(&ac));
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
    /* Mandatory: SHARD <shard_id> SLOTRANGE <start_slot> <end_slot> ADDR <tcp> */
    VERIFY_ARG("SHARD");
    sh.node.id = AC_GetStringNC(&ac, NULL);
    if (!sh.node.id) {
      ERROR_MISSING("SHARD");
      goto error;
    }

    VERIFY_ARG("SLOTRANGE");
    if (AC_GetInt(&ac, &sh.startSlot, AC_F_GE0) != AC_OK ||
        AC_GetInt(&ac, &sh.endSlot, AC_F_GE0) != AC_OK || sh.startSlot > sh.endSlot ||
        sh.endSlot >= numSlots) {
      // Error bad/missing argument
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
      ERROR_BAD_STRING("ADDR", addr);
      goto error;
    }
    // All good. Finish up the node
    if (unixSock) {
      sh.node.endpoint.unixSock = rm_strdup(unixSock);
    }
    sh.node.flags = 0;
    if (!strcmp(sh.node.id, myID)) {
      sh.node.flags |= MRNode_Self;  // TODO: verify there's only one self?
    }
    /* Optional MASTER */
    if (AC_AdvanceIfMatch(&ac, "MASTER")) {
      sh.node.flags |= MRNode_Master;
    }
    // Add the shard. This function will take ownership of the node's allocated strings
    MRTopology_AddRLShard(topo, &sh);
  }

  if (!AC_IsAtEnd(&ac)) {
    ERROR_EXPECTED("end of command", AC_CURRENT(&ac));
    goto error;
  }

  return topo;

error:
  MRClusterTopology_Free(topo);
  return NULL;
}
