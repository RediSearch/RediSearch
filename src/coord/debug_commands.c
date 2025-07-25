/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "coord/rmr/rmr.h"
#include "coord/rmr/io_runtime_ctx.h"
#include "debug_commands.h"
#include "debug_command_names.h"
#include "coord/rmr/redis_cluster.h"
#include "module.h"
#include "src/config.h"
#include <assert.h>

DEBUG_COMMAND(shardConnectionStates) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) return RedisModule_WrongArity(ctx);
  MR_GetConnectionPoolState(ctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(pauseTopologyUpdater) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) return RedisModule_WrongArity(ctx);
  if (StopRedisTopologyUpdater(ctx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Topology updater is already paused");
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

DEBUG_COMMAND(resumeTopologyUpdater) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) return RedisModule_WrongArity(ctx);
  if (InitRedisTopologyUpdater(ctx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Topology updater is already running");
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

DEBUG_COMMAND(clearTopology) {
  if (!debugCommandsEnabled(ctx)) {
    return RedisModule_ReplyWithError(ctx, NODEBUG_ERR);
  }
  if (argc != 2) return RedisModule_WrongArity(ctx);
  MR_Debug_ClearPendingTopo();
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DebugCommandType coordCommands[] = {
  {"SHARD_CONNECTION_STATES", shardConnectionStates},
  {"PAUSE_TOPOLOGY_UPDATER", pauseTopologyUpdater},
  {"RESUME_TOPOLOGY_UPDATER", resumeTopologyUpdater},
  {"CLEAR_PENDING_TOPOLOGY", clearTopology},
  {NULL, NULL}
};
// Make sure the two arrays are of the same size (don't forget to update `debug_command_names.h`)
static_assert(sizeof(coordCommands)/sizeof(DebugCommandType) == sizeof(coordCommandsNames)/sizeof(const char *));

int RegisterCoordDebugCommands(RedisModuleCommand *debugCommand) {
  for (int i = 0; coordCommands[i].name != NULL; i++) {
    int rc = RedisModule_CreateSubcommand(debugCommand, coordCommands[i].name,
              coordCommands[i].callback,
              IsEnterprise() ? "readonly " CMD_PROXY_FILTERED : "readonly",
              RS_DEBUG_FLAGS);
    if (rc != REDISMODULE_OK) return rc;
  }
  return REDISMODULE_OK;
}
