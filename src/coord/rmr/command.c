/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "common.h"
#include "command.h"
#include "rmalloc.h"
#include "resp3.h"
#include "slot_ranges.h"

#include "version.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

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
  news[n] = '\0';
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
  cmd->slotsInfoArgIndex = 0;
  cmd->targetShard = INVALID_SHARD;
  cmd->cmd = NULL;
  cmd->protocol = 0;
  cmd->depleted = false;
  cmd->forCursor = false;
  cmd->forProfiling = false;
}

MRCommand MR_NewCommandArgv(int argc, const char **argv) {
  MRCommand cmd;
  MRCommand_Init(&cmd, argc);

  for (int i = 0; i < argc; i++) {
    assignCstr(&cmd, i, argv[i]);
  }
  return cmd;
}

/* Create a deep copy of a command by duplicating all strings */
MRCommand MRCommand_Copy(const MRCommand *cmd) {
  MRCommand ret;
  MRCommand_Init(&ret, cmd->num);
  ret.slotsInfoArgIndex = cmd->slotsInfoArgIndex;
  ret.targetShard = cmd->targetShard;
  ret.protocol = cmd->protocol;
  ret.forCursor = cmd->forCursor;
  ret.forProfiling = cmd->forProfiling;
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
  return cmd;
}

MRCommand MR_NewCommandFromRedisStrings(int argc, RedisModuleString **argv) {
  MRCommand cmd;
  MRCommand_Init(&cmd, argc);
  for (int i = 0; i < argc; i++) {
    assignRstr(&cmd, i, argv[i]);
  }
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

  if (pos < cmd->slotsInfoArgIndex && cmd->slotsInfoArgIndex > 0) {
    cmd->slotsInfoArgIndex++;
  }

  // shift right all arguments that comes after pos
  memmove(cmd->strs + pos + 1, cmd->strs + pos, (oldNum - pos) * sizeof(char*));
  memmove(cmd->lens + pos + 1, cmd->lens + pos, (oldNum - pos) * sizeof(size_t));

  assignStr(cmd, pos, s, n);
}

void MRCommand_Append(MRCommand *cmd, const char *s, size_t n) {
  extendCommandList(cmd, 1);
  assignStr(cmd, cmd->num - 1, s, n);
}

void MRCommand_AppendRstr(MRCommand *cmd, RedisModuleString *rmstr) {
  size_t len;
  const char *cstr = RedisModule_StringPtrLen(rmstr, &len);
  MRCommand_Append(cmd, cstr, len);
}

/** Set the prefix of the command (i.e {prefix}.{command}) to a given prefix. If the command has a
 * module style prefix it gets replaced with the new prefix. If it doesn't, we prepend the prefix to
 * the command. */
void MRCommand_SetPrefix(MRCommand *cmd, const char *newPrefix) {

  char *suffix = strchr(cmd->strs[0], '.');
  if (!suffix) {
    suffix = cmd->strs[0];
  } else {
    suffix++;
  }

  char *buf;
  int len = rm_asprintf(&buf, "%s.%s", newPrefix, suffix);
  MRCommand_ReplaceArgNoDup(cmd, 0, buf, len);
}

void MRCommand_ReplaceArgNoDup(MRCommand *cmd, int index, const char *newArg, size_t len) {
  if (index < 0 || index >= cmd->num) {
    return;
  }
  char *tmp = cmd->strs[index];
  cmd->strs[index] = (char *)newArg;
  cmd->lens[index] = len;
  rm_free(tmp);
}
void MRCommand_ReplaceArg(MRCommand *cmd, int index, const char *newArg, size_t len) {
  char *news = rm_malloc(len + 1);
  news[len] = '\0';
  memcpy(news, newArg, len);
  MRCommand_ReplaceArgNoDup(cmd, index, news, len);
}

void MRCommand_ReplaceArgSubstring(MRCommand *cmd, int index, size_t pos, size_t oldSubStringLen, const char *newStr, size_t newLen) {
  RS_LOG_ASSERT_FMT(index >= 0 && index < cmd->num, "Invalid index %d. Command has %d arguments", index, cmd->num);

  char *oldArg = cmd->strs[index];
  // Get full argument length
  size_t oldArgLen = cmd->lens[index];

  // Validate position and length
  RS_LOG_ASSERT_FMT(pos + oldSubStringLen <= oldArgLen, "Invalid position %zu. Argument length is %zu", pos, oldArgLen);

  // Calculate new total length
  size_t newArgLen = oldArgLen - oldSubStringLen + newLen;

  // OPTIMIZATION: For query string literals, pad with spaces instead of moving memory
  if (newLen <= oldSubStringLen) {
    // Copy new string
    memcpy(oldArg + pos, newStr, newLen);

    // Pad remaining space with spaces (no memmove needed)
    memset(oldArg + pos + newLen, ' ', oldSubStringLen - newLen);

    // No length change needed - argument stays same size
    return;
  }

  // Fallback: Allocate new string for longer replacements
  char *newArg = rm_malloc(newArgLen + 1);

  // Copy parts: [before] + [new] + [after]
  memcpy(newArg, oldArg, pos);                           // Copy before
  memcpy(newArg + pos, newStr, newLen);                  // Copy new substring
  memcpy(newArg + pos + newLen, oldArg + pos + oldSubStringLen,   // Copy after
         oldArgLen - pos - oldSubStringLen);

  newArg[newArgLen] = '\0';

  // Replace the argument
  MRCommand_ReplaceArgNoDup(cmd, index, newArg, newArgLen);
}

void MRCommand_SetProtocol(MRCommand *cmd, RedisModuleCtx *ctx) {
  cmd->protocol = is_resp3(ctx) ? 3 : 2;
}

void MRCommand_PrepareForSlotInfo(MRCommand *cmd, uint32_t pos) {
  // Make place for `_SLOTS <binary data>`
  extendCommandList(cmd, 2);

  // shift right all arguments that comes after pos
  memmove(cmd->strs + pos + 2, cmd->strs + pos, (cmd->num - 2 - pos) * sizeof(char*));
  memmove(cmd->lens + pos + 2, cmd->lens + pos, (cmd->num - 2 - pos) * sizeof(size_t));

  // Assign the `_SLOTS` marker at pos
  assignStr(cmd, pos, "_SLOTS", sizeof("_SLOTS") - 1);
  // Leave space for the binary data at pos + 1 (to be filled later)
  cmd->strs[pos + 1] = NULL;
  cmd->lens[pos + 1] = 0;
  cmd->slotsInfoArgIndex = pos + 1;
}

void MRCommand_SetSlotInfo(MRCommand *cmd, const RedisModuleSlotRangeArray *slots) {
  RS_ASSERT(cmd->slotsInfoArgIndex > 0 && cmd->slotsInfoArgIndex < cmd->num);
  RS_ASSERT(cmd->strs[cmd->slotsInfoArgIndex] == NULL);
  RS_ASSERT(!strcmp(cmd->strs[cmd->slotsInfoArgIndex - 1], "_SLOTS"));

  // Assign the binary data to the command
  cmd->lens[cmd->slotsInfoArgIndex] = SlotRangeArray_SizeOf(slots->num_ranges);
  cmd->strs[cmd->slotsInfoArgIndex] = SlotRangesArray_Serialize(slots);
}
