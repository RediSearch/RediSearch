/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#pragma once

#include "hiredis/sds.h"
#include "redismodule.h"

#include <assert.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum { C_READ = 0, C_DEL = 1, C_AGG = 2 } MRRootCommand;

#define INVALID_SHARD -1

/* A redis command is represented with all its arguments and its flags as MRCommand */
typedef struct {
  /* The command args starting from the command itself */
  char **strs;
  size_t *lens;

  /* Number of arguments */
  uint32_t num;

  /* if not -1, this value indicate to which shard the command should be sent */
  int16_t targetShard;

  /* 0 (undetermined), 2, or 3 */
  unsigned char protocol;

  /* Whether the user asked for a cursor */
  bool forCursor;

  /* Whether the command is for profiling */
  bool forProfiling;

  /* Whether the command chain is depleted - don't resend */
  bool depleted;

  // Root command for current response
  MRRootCommand rootCommand;

  sds cmd;
} MRCommand;

/* Free the command and all its strings. Doesn't free the actual command struct, as it is usually
 * allocated on the stack */
void MRCommand_Free(MRCommand *cmd);

/* Create a new command from an argv list of strings */
MRCommand MR_NewCommandArgv(int argc, const char **argv);
/* Variadic creation of a command from a list of strings */
MRCommand MR_NewCommand(int argc, ...);
/* Create a command from a list of redis strings */
MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv);

static inline const char *MRCommand_ArgStringPtrLen(const MRCommand *cmd, size_t idx, size_t *len) {
  // assert(idx < cmd->num);
  if (len) {
    *len = cmd->lens[idx];
  }
  return cmd->strs[idx];
}

/** Copy from an argument of an existing command */
void MRCommand_Append(MRCommand *cmd, const char *s, size_t len);
void MRCommand_AppendRstr(MRCommand *cmd, RedisModuleString *rmstr);
void MRCommand_Insert(MRCommand *cmd, int pos, const char *s, size_t n);

/** Set the prefix of the command (i.e {prefix}.{command}) to a given prefix. If the command has a
 * module style prefix it gets replaced with the new prefix. If it doesn't, we prepend the prefix to
 * the command. */
void MRCommand_SetPrefix(MRCommand *cmd, const char *newPrefix);
void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg, size_t len);
void MRCommand_ReplaceArgNoDup(MRCommand *cmd, int index, const char *newArg, size_t len);

/** Replace a substring within an argument at a specific position
 * OPTIMIZATION: Avoids reallocation when new string is same/shorter length.
 * Instead, pads with spaces.
 *
 * @param cmd - Command structure containing the arguments
 * @param index - Index of the argument to modify
 * @param pos - Starting position within the argument string
 * @param oldSubStringLen - Length of the substring to replace
 * @param newStr - New string to insert
 * @param newLen - Length of the new string
 */
void MRCommand_ReplaceArgSubstring(MRCommand *cmd, int index, size_t pos, size_t oldSubStringLen, const char *newStr, size_t newLen);

void MRCommand_WriteTaggedKey(MRCommand *cmd, int index, const char *newarg, const char *part,
                              size_t n);

void MRCommand_SetProtocol(MRCommand *cmd, RedisModuleCtx *ctx);

/* Create a copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(const MRCommand *cmd);

#ifdef __cplusplus
}
#endif
