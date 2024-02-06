/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#include "common.h"
#include "command.h"
#include "rmalloc.h"
#include "resp3.h"

#include "version.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

struct mrCommandConf {
  const char *command;
  int keyPos;
};

struct mrCommandConf __commandConfig[] = {

    // document commands
    {"_FT.SEARCH", 1},
    {"_FT.DEL", 2},
    {"_FT.GET", 2},
    {"_FT.MGET", 1},

    {"_FT.ADD", 2},
    {"_FT.AGGREGATE", 1},

    // index commands
    {"_FT.CREATE", 1},
    {"_FT.ALTER", 1},
    {"_FT.DROP", 1},
    {"_FT.INFO", 1},
    {"_FT.TAGVALS", 1},

    // Alias commands
    {"_FT.ALIASADD", 2},
    {"_FT.ALIASUPDATE", 2},
    // Del is done using fanout/broadcast

    // Suggest commands
    {"_FT.SUGADD", 1},
    {"_FT.SUGGET", 1},
    {"_FT.SUGLEN", 1},
    {"_FT.SUGDEL", 1},
    {"_FT.CURSOR", 2},

    // Synonyms commands
    {"_FT.SYNUPDATE", 1},
    {"_FT.SYNFORCEUPDATE", 1},

    // Coordination commands - they are all read commands since they can be triggered from slaves
    {"FT.ADD", -1},
    {"FT.SEARCH", -1},
    {"FT.AGGREGATE", -1},

    {"FT.EXPLAIN", -1},

    {"FT.CREATE", -1},
    {REDISEARCH_MODULE_NAME".CLUSTERINFO", -1},
    {"FT.INFO", -1},
    {"FT.DEL", -1},
    {"FT.DROP", -1},
    {"FT.CREATE", -1},
    {"FT.GET", -1},
    {"FT.MGET", -1},

    // Auto complete coordination commands
    {"FT.SUGADD", -1},
    {"FT.SUGGET", -1},
    {"FT.SUGDEL", -1},
    {"FT.SUGLEN", -1},

    {"KEYS", -1},
    {"INFO", -1},
    {"SCAN", -1},

    // dictionary commands
    {"_FT.DICTADD", 1},
    {"_FT.DICTDEL", 1},

    // spell check
    {"_FT.SPELLCHECK", 1},

    // sentinel
    {NULL},
};

static int _getCommandConfId(MRCommand *cmd) {
  cmd->id = -1;
  if (cmd->num == 0) {
    return 0;
  }

  for (int i = 0; __commandConfig[i].command != NULL; i++) {
    if (!strcasecmp(cmd->strs[0], __commandConfig[i].command)) {
      // printf("conf id for cmd %s: %d\n", cmd->strs[0], i);
      cmd->id = i;
      return 1;
    }
  }
  return 0;
}

void MRCommand_Free(MRCommand *cmd) {
  if (cmd->cmd) {
    sdsfree(cmd->cmd);
  }
  for (int i = 0; i < cmd->num; i++) {
    rm_free(cmd->strs[i]);
  }
  rm_free(cmd->strs);
  rm_free(cmd->lens);
}

static void assignStr(MRCommand *cmd, size_t idx, const char *s, size_t n) {
  char *news = rm_malloc(n + 1);
  cmd->strs[idx] = news;
  cmd->lens[idx] = n;
  news[n] = 0;
  memcpy(news, s, n);
}

static void assignCstr(MRCommand *cmd, size_t idx, const char *s) {
  assignStr(cmd, idx, s, strlen(s));
}

static void copyStr(MRCommand *dst, size_t dstidx, const MRCommand *src, size_t srcidx) {
  const char *srcs = src->strs[srcidx];
  size_t srclen = src->lens[srcidx];

  assignStr(dst, dstidx, srcs, srclen);
}

static void assignRstr(MRCommand *dst, size_t idx, RedisModuleString *src) {
  size_t n;
  const char *s = RedisModule_StringPtrLen(src, &n);
  assignStr(dst, idx, s, n);
}

static void MRCommand_Init(MRCommand *cmd, size_t len) {
  cmd->num = len;
  cmd->strs = rm_malloc(sizeof(*cmd->strs) * len);
  cmd->lens = rm_malloc(sizeof(*cmd->lens) * len);
  cmd->id = 0;
  cmd->targetSlot = -1;
  cmd->cmd = NULL;
  cmd->protocol = 0;
  cmd->depleted = false;
  cmd->forCursor = false;
}

MRCommand MR_NewCommandArgv(int argc, const char **argv) {
  MRCommand cmd;
  MRCommand_Init(&cmd, argc);

  for (int i = 0; i < argc; i++) {
    assignCstr(&cmd, i, argv[i]);
  }
  _getCommandConfId(&cmd);
  return cmd;
}

/* Create a deep copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(const MRCommand *cmd) {
  MRCommand ret;
  MRCommand_Init(&ret, cmd->num);
  ret.id = cmd->id;
  ret.protocol = cmd->protocol;
  ret.forCursor = cmd->forCursor;
  ret.rootCommand = cmd->rootCommand;
  ret.depleted = cmd->depleted;

  for (int i = 0; i < cmd->num; i++) {
    copyStr(&ret, i, cmd, i);
  }
  return ret;
}

MRCommand MR_NewCommand(int argc, ...) {
  MRCommand cmd;
  MRCommand_Init(&cmd, argc);

  va_list ap;
  va_start(ap, argc);
  for (int i = 0; i < argc; i++) {
    assignCstr(&cmd, i, va_arg(ap, const char *));
  }
  va_end(ap);
  _getCommandConfId(&cmd);
  return cmd;
}

MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv) {
  MRCommand cmd;
  MRCommand_Init(&cmd, argc);
  for (int i = 0; i < argc; i++) {
    assignRstr(&cmd, i, argv[i]);
  }
  _getCommandConfId(&cmd);
  return cmd;
}

static void extendCommandList(MRCommand *cmd, size_t toAdd) {
  cmd->num += toAdd;
  cmd->strs = rm_realloc(cmd->strs, sizeof(*cmd->strs) * cmd->num);
  cmd->lens = rm_realloc(cmd->lens, sizeof(*cmd->lens) * cmd->num);
}

void MRCommand_Insert(MRCommand *cmd, int pos, const char *s, size_t n) {
  int oldNum = cmd->num;
  extendCommandList(cmd, 1);

  // shift right all arguments that comes after pos
  memmove(cmd->strs + pos + 1, cmd->strs + pos, (oldNum - pos) * sizeof(char*));
  memmove(cmd->lens + pos + 1, cmd->lens + pos, (oldNum - pos) * sizeof(size_t));

  assignStr(cmd, pos, s, n);
}

void MRCommand_Append(MRCommand *cmd, const char *s, size_t n) {
  extendCommandList(cmd, 1);
  assignStr(cmd, cmd->num - 1, s, n);
  if (cmd->num == 1) {
    _getCommandConfId(cmd);
  }
}

void MRCommand_AppendRstr(MRCommand *cmd, RedisModuleString *rmstr) {
  size_t len;
  const char *cstr = RedisModule_StringPtrLen(rmstr, &len);
  MRCommand_Append(cmd, cstr, len);
}

/** Set the prefix of the command (i.e {prefix}.{command}) to a given prefix. If the command has a
 * module style prefx it gets replaced with the new prefix. If it doesn't, we prepend the prefix to
 * the command. */
void MRCommand_SetPrefix(MRCommand *cmd, const char *newPrefix) {

  char *suffix = strchr(cmd->strs[0], '.');
  if (!suffix) {
    suffix = cmd->strs[0];
  } else {
    suffix++;
  }

  char *buf = NULL;
  __ignore__(rm_asprintf(&buf, "%s.%s", newPrefix, suffix));
  MRCommand_ReplaceArgNoDup(cmd, 0, buf, strlen(buf));
  _getCommandConfId(cmd);
}

void MRCommand_ReplaceArgNoDup(MRCommand *cmd, int index, const char *newArg, size_t len) {
  if (index < 0 || index >= cmd->num) {
    return;
  }
  char *tmp = cmd->strs[index];
  cmd->strs[index] = (char *)newArg;
  cmd->lens[index] = len;
  rm_free(tmp);

  // if we've replaced the first argument, we need to reconfigure the command
  if (index == 0) {
    _getCommandConfId(cmd);
  }
}
void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg, size_t len) {
  char *news = rm_malloc(len + 1);
  news[len] = 0;
  memcpy(news, newArg, len);
  MRCommand_ReplaceArgNoDup(cmd, index, news, len);
}

int MRCommand_GetShardingKey(const MRCommand *cmd) {
  if (cmd->id < 0) {
    return 1;  // default
  }

  return __commandConfig[cmd->id].keyPos;
}

void MRCommand_SetProtocol(MRCommand *cmd, RedisModuleCtx *ctx) {
  cmd->protocol = is_resp3(ctx) ? 3 : 2;
}

void MRCommand_Print(MRCommand *cmd) {
  MRCommand_FPrint(stdout, cmd);
}

void MRCommand_FPrint(FILE *fd, MRCommand *cmd) {
  for (int i = 0; i < cmd->num; i++) {
    fprintf(fd, "%.*s ", (int)cmd->lens[i], cmd->strs[i]);
  }
  fprintf(fd, "\n");
}

void print_mr_cmd(MRCommand *cmd) {
  MRCommand_FPrint(stdout, cmd);
}
