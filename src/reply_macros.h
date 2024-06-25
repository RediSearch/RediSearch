/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "redismodule.h"

#define RedisModule_ReplyWithLiteral(ctx, literal) \
  RedisModule_ReplyWithStringBuffer(ctx, literal, sizeof(literal) - 1)

#define REPLY_SIMPLE_SAFE(ctx, v)               \
  do {                                          \
    char *v_ = (char *)(v);                     \
    if (isUnsafeForSimpleString(v_)) {          \
      v_ = escapeSimpleString(v_);              \
    }                                           \
    RedisModule_ReplyWithSimpleString(ctx, v_); \
    if (v_ != (v)) rm_free(v_);                 \
  } while (0)


/*
 * This function is a workaround helper for replying with a string that may contain
 * newlines or other characters that are not safe for RESP Simple Strings.
 * Should be removed once we can replace all SimpleString replies with BulkString replies.
 */
static inline bool isUnsafeForSimpleString(const char *str) {
  return strpbrk(str, "\r\n") != NULL;
}
/*
 * This function is a workaround helper for replying with a string that may contain
 * newlines or other characters that are not safe for RESP Simple Strings.
 * Should be removed once we can replace all SimpleString replies with BulkString replies.
 */
static char *escapeSimpleString(const char *str) {
  size_t len = strlen(str);
  // This is a short lived string, so we can afford to allocate twice the size
  char *escaped = rm_malloc(len * 2 + 1);
  char *p = escaped;
  for (size_t i = 0; i < len; i++) {
    char c = str[i];
    switch (c) {
    case '\n':
      *p++ = '\\';
      *p++ = 'n';
      break;
    case '\r':
      *p++ = '\\';
      *p++ = 'r';
      break;
    default:
      *p++ = c;
    }
  }
  *p = '\0';
  return escaped;
}
