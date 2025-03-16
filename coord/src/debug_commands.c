/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "coord/src/rmr/rmr.h"
#include "coord/src/rmr/rq.h"
#include "debug_commands.h"
#include "debug_command_names.h"
#include "coord/src/rmr/redis_cluster.h"
#include "coord/src/coord_module.h"
#include <assert.h>

DEBUG_COMMAND(shardConnectionStates) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  MR_GetConnectionPoolState(ctx);
  return REDISMODULE_OK;
}

DEBUG_COMMAND(pauseTopologyUpdater) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  if (StopRedisTopologyUpdater(ctx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Topology updater is already paused");
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

DEBUG_COMMAND(resumeTopologyUpdater) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  if (InitRedisTopologyUpdater(ctx) != REDISMODULE_OK) {
    return RedisModule_ReplyWithError(ctx, "Topology updater is already running");
  } else {
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
}

DEBUG_COMMAND(clearTopology) {
  if (argc != 2) return RedisModule_WrongArity(ctx);
  RQ_Debug_ClearPendingTopo();
  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

DEBUG_COMMAND(DistAggregateCommand_DebugWrapper) {
  // at least one debug_param should be provided
  // (1)_FT.DEBUG (2)FT.AGGREGATE (3)<index> (4)<query> [query_options] (5)[debug_params] (6)DEBUG_PARAMS_COUNT (7)<debug_params_count>
  if (argc < 7) {
    return RedisModule_WrongArity(ctx);
  }

  DistAggregateCommand(ctx, argv, argc);
}

DEBUG_COMMAND(DistSearchCommand_DebugWrapper) {
  // at least one debug_param should be provided
  // (1)_FT.DEBUG (2)FT.SEARCH (3)<index> (4)<query> [query_options] (5)[debug_params] (6)DEBUG_PARAMS_COUNT (7)<debug_params_count>
  if (argc < 7) {
    return RedisModule_WrongArity(ctx);
  }

  DistSearchCommand(ctx, argv, argc);
}

DebugCommandType coordCommands[] = {
  {"SHARD_CONNECTION_STATES", shardConnectionStates},
  {"PAUSE_TOPOLOGY_UPDATER", pauseTopologyUpdater},
  {"RESUME_TOPOLOGY_UPDATER", resumeTopologyUpdater},
  {"CLEAR_PENDING_TOPOLOGY", clearTopology},
  {"FT.AGGREGATE", DistAggregateCommand_DebugWrapper},
  {"FT.SEARCH", DistSearchCommand_DebugWrapper},
  {NULL, NULL}
};
// Make sure the two arrays are of the same size (don't forget to update `debug_command_names.h`)
static_assert(sizeof(coordCommands)/sizeof(DebugCommandType) == sizeof(coordCommandsNames)/sizeof(const char *));

int RegisterCoordDebugCommands(RedisModuleCommand *debugCommand) {
  for (int i = 0; coordCommands[i].name != NULL; i++) {
    int rc = RedisModule_CreateSubcommand(debugCommand, coordCommands[i].name, coordCommands[i].callback, RS_DEBUG_FLAGS);
    if (rc != REDISMODULE_OK) return rc;
  }
  return REDISMODULE_OK;
}
