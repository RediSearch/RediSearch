/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "hiredis/sds.h"
#include "redismodule.h"

#include <assert.h>
#include <stdbool.h>

/* A redis command is represented with all its arguments and its flags as MRCommand */
typedef struct {
  /* The command args starting from the command itself */
  char **strs;
  size_t *lens;

  /* Number of arguments */
  uint32_t num;

  /* Internal id used to get the command configuration */
  int id;

  /* if not -1, this value indicate to which slot the command should be sent */
  int targetSlot;

  /* 0 (undetermined), 2, or 3 */
  unsigned short protocol;

 /* Whether the user asked for a cursor */
  bool forCursor;

  /* Whether the command chain is depleted - don't resend */
  bool depleted;

  sds cmd;
} MRCommand;

/* Free the command and all its strings. Doesn't free the actual command struct, as it is usually
 * allocated on the stack */
void MRCommand_Free(MRCommand *cmd);

/* Create a new command from an argv list of strings */
MRCommand MR_NewCommandArgv(int argc, const char **argv);
/* Variadic creation of a command from a list of strings */
MRCommand MR_NewCommand(int argc, ...);
/* Create a command from a list of strings */
MRCommand MR_NewCommandFromStrings(int argc, char **argv);
/* Create a command from a list of redis strings */
MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv);

static inline const char *MRCommand_ArgStringPtrLen(const MRCommand *cmd, size_t idx, size_t *len) {
  // assert(idx < cmd->num);
  if (len) {
    *len = cmd->lens[idx];
  }
  return cmd->strs[idx];
}

/* A generator producing a list of commands on successive calls to Next(); */
typedef struct {
  /* Private context of what's actually going on */
  void *ctx;

  /* The number of commands in this generator. We must know it in advance */
  size_t (*Len)(void *ctx);

  /* Next callback - should yield 0 if we are at the end, 1 if not, and put the next value in cmd */
  int (*Next)(void *ctx, MRCommand *cmd);

  /* Free callback - used to free the private context */
  void (*Free)(void *ctx);
} MRCommandGenerator;

void MRCommand_AppendStringsArgs(MRCommand *cmd, int num, char **args);
void MRCommand_AppendArgs(MRCommand *cmd, int num, ...);
void MRCommand_AppendArgsAtPos(MRCommand *cmd, int pos, int num, ...);

/** Copy from an argument of an existing command */
void MRCommand_AppendFrom(MRCommand *cmd, const MRCommand *srcCmd, size_t srcidx);
void MRCommand_Append(MRCommand *cmd, const char *s, size_t len);
void MRCommand_AppendRstr(MRCommand *cmd, RedisModuleString *rmstr);

/** Set the prefix of the command (i.e {prefix}.{command}) to a given prefix. If the command has a
 * module style prefix it gets replaced with the new prefix. If it doesn't, we prepend the prefix to
 * the command. */
void MRCommand_SetPrefix(MRCommand *cmd, const char *newPrefix);
void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg, size_t len);
void MRCommand_ReplaceArgNoDup(MRCommand *cmd, int index, const char *newArg, size_t len);

void MRCommand_WriteTaggedKey(MRCommand *cmd, int index, const char *newarg, const char *part,
                              size_t n);

MRCommandGenerator *MRCommand_GetCommandGenerator(MRCommand *cmd);
int MRCommand_GetShardingKey(MRCommand *cmd);
int MRCommand_GetPartitioningKey(MRCommand *cmd);

typedef enum {
  MRCommand_SingleKey = 0x01,
  MRCommand_MultiKey = 0x02,
  MRCommand_Read = 0x04,
  MRCommand_Write = 0x08,
  MRCommand_Coordination = 0x10,
  MRCommand_NoKey = 0x20,
  // Command can be aliased. Look up the alias and rewrite if possible
  MRCommand_Aliased = 0x40
} MRCommandFlags;

MRCommandFlags MRCommand_GetFlags(MRCommand *cmd);

/* Return 1 if the command should not be sharded */
int MRCommand_IsUnsharded(MRCommand *cmd);

void MRCommand_SetProtocol(MRCommand *cmd, RedisModuleCtx *ctx);

void MRCommand_Print(MRCommand *cmd);
void MRCommand_FPrint(FILE *fd, MRCommand *cmd);

/* Create a copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(const MRCommand *cmd);
