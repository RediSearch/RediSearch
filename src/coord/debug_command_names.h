/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

/**
 * List of all the debug commands in the coordinator.
 * This list is on a separate file so we can include it in the src/debug_commands.c file,
 * for the purpose of listing all the debug commands in the help command.
 */
static const char *coordCommandsNames[] = {
  "SHARD_CONNECTION_STATES",
  "PAUSE_TOPOLOGY_UPDATER",
  "RESUME_TOPOLOGY_UPDATER",
  NULL
};
