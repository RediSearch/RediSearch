/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "debug_commands.h"
#include "debug_command_names.h"
#include <assert.h>

DebugCommandType coordCommands[] = {
  {NULL, NULL}
};
// Make sure the two arrays are of the same size
static_assert(sizeof(coordCommands)/sizeof(DebugCommandType) == sizeof(coordCommandsNames)/sizeof(const char *));

int RegisterCoordDebugCommands(RedisModuleCommand *debugCommand) {
  for (int i = 0; coordCommands[i].name != NULL; i++) {
    int rc = RedisModule_CreateSubcommand(debugCommand, coordCommands[i].name, coordCommands[i].callback, RS_DEBUG_FLAGS);
    if (rc != REDISMODULE_OK) return rc;
  }
  return REDISMODULE_OK;
}
