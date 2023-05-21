/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "reply.h"
#include "resp3.h"

#include <math.h>

///////////////////////////////////////////////////////////////////////////////////////////////

typedef struct RedisModule_Reply_StackEntry StackEntry;

//---------------------------------------------------------------------------------------------

bool RedisModule_HasMap(RedisModule_Reply *reply) {
  return _ReplyMap(reply->ctx);
}

int RedisModule_Reply_LocalCount(RedisModule_Reply *reply) {
  if (reply->stack) {
    if (array_len(reply->stack) > 0) {
      StackEntry *e = &array_tail(reply->stack);
      return e->count;
    }
  }
  return reply->count;
}

int RedisModule_Reply_LocalType(RedisModule_Reply *reply) {
  if (reply->stack) {
    if (array_len(reply->stack) > 0) {
      StackEntry *e = &array_tail(reply->stack);
      return e->type;
    }
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

#ifdef REDISMODULE_REPLY_DEBUG

static inline void json_add(RedisModule_Reply *reply, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p = 0;
  int n = vasprintf(&p, fmt, args);
  int count = RedisModule_Reply_LocalCount(reply);
  if (count > 0) {
     n += 2;
  }
  reply->json = array_grow(reply->json, n);
  if (count > 0) {
    strcat(reply->json, ", ");
  }
  strcat(reply->json, p);
  va_end(args);
  free(p);
}

static inline void json_add_close(RedisModule_Reply *reply, const char *s) {
  int n = strlen(s);
  reply->json = array_grow(reply->json, n);
  strcat(reply->json, s);
}

#else

static inline void json_add(RedisModule_Reply *reply, const char *fmt, ...) {}

#endif

//---------------------------------------------------------------------------------------------

RedisModule_Reply RedisModule_NewReply(RedisModuleCtx *ctx) {
  RedisModule_Reply reply = { ctx, _ReplyMap(ctx), 0, NULL, NULL };
#ifdef REDISMODULE_REPLY_DEBUG
  reply.json = array_ensure_tail(&reply.json, char);
  *reply.json = '\0';
#endif
  return reply;
}

int RedisModule_EndReply(RedisModule_Reply *reply) {
  int n = reply->stack ? array_len(reply->stack) : -1;
  if (reply->stack && array_len(reply->stack) > 0) { _BB; }
  RedisModule_Assert(!reply->stack || !array_len(reply->stack));
  if (reply->stack) {
    array_free(reply->stack);
  }
#ifdef REDISMODULE_REPLY_DEBUG
  if (reply->json) {
    array_free(reply->json);
  }
#endif
  reply->stack = 0;
  return REDISMODULE_OK;
}

static void _RedisModule_Reply_Next(RedisModule_Reply *reply) {
  StackEntry *e;
  int *count;
  if (reply->stack) {
    if (!array_len(reply->stack)) {
      e = array_ensure_tail(&reply->stack, StackEntry);
    } else {
      e = &array_tail(reply->stack);
    }
    count = &e->count;
  } else {
    count = &reply->count;
  }
  ++*count;
}

static void _RedisModule_ReplyKV_Next(RedisModule_Reply *reply) {
  StackEntry *e;
  int *count;
  if (reply->stack && array_len(reply->stack)) {
    e = &array_tail(reply->stack);
    count = &e->count;
  } else {
    count = &reply->count;
  }
  *count += reply->resp3 ? 1 : 2;
}

static void _RedisModule_Reply_Push(RedisModule_Reply *reply, int type) {
  StackEntry *e = array_ensure_tail(&reply->stack, StackEntry);
  e->count = 0;
  e->type = type;
}

static int _RedisModule_Reply_Pop(RedisModule_Reply *reply) {
  if (!reply->stack || !array_len(reply->stack)) { _BB; }
  RedisModule_Assert(reply->stack && array_len(reply->stack) > 0);
  if (reply->stack && array_len(reply->stack) > 0) {
    StackEntry *e = &array_tail(reply->stack);
    int count = e->count;
    reply->stack = array_trimm_len(reply->stack, 1);
    return count;
  } else {
    return reply->count;
  }
}

//---------------------------------------------------------------------------------------------

int RedisModule_Reply_LongLong(RedisModule_Reply *reply, long long val) {
  RedisModule_ReplyWithLongLong(reply->ctx, val);
  json_add(reply, "%ld", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Double(RedisModule_Reply *reply, double val) {
  RedisModule_ReplyWithDouble(reply->ctx, val);
  json_add(reply, "%f", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_SimpleString(RedisModule_Reply *reply, const char *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, val);
  json_add(reply, "\"%s\"", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_StringBuffer(RedisModule_Reply *reply, const char *val, size_t len) {
  RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
  json_add(reply, "\"%*s\"", len, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Stringf(RedisModule_Reply *reply, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p;
  rm_vasprintf(&p, fmt, args);
  RedisModule_ReplyWithSimpleString(reply->ctx, p);
  json_add(reply, "\"%s\"", p);
  rm_free(p);
  _RedisModule_Reply_Next(reply);
  va_end(args);
  return REDISMODULE_OK;
}

int RedisModule_Reply_String(RedisModule_Reply *reply, RedisModuleString *val) {
  RedisModule_ReplyWithString(reply->ctx, val);
#ifdef REDISMODULE_REPLY_DEBUG
  size_t n;
  const char *p = RedisModule_StringPtrLen(val, &n);
  json_add(reply, "\"%*s\"", n, val);
#endif
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Null(RedisModule_Reply *reply) {
  RedisModule_ReplyWithNull(reply->ctx);
  json_add(reply, "null");
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Error(RedisModule_Reply *reply, const char *error) {
  RedisModule_ReplyWithError(reply->ctx, error);
  json_add(reply, "\"ERR: %s\"", error);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Map(RedisModule_Reply *reply) {
  int type;
  if (reply->resp3) {
    RedisModule_ReplyWithMap(reply->ctx, REDISMODULE_POSTPONED_LEN);
    json_add(reply, "{ ");
    type = REDISMODULE_REPLY_MAP;
  } else {
    RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
    json_add(reply, "[ ");
    type = REDISMODULE_REPLY_ARRAY;
  }
  _RedisModule_Reply_Next(reply);
  _RedisModule_Reply_Push(reply, type);
  return REDISMODULE_OK;
}

int RedisModule_Reply_MapEnd(RedisModule_Reply *reply) {
  if (reply->resp3) {
    json_add_close(reply, " }");
  } else {
    json_add_close(reply, " ]");
  }
  int count = _RedisModule_Reply_Pop(reply);
  if (reply->resp3) {
    RedisModule_ReplySetMapLength(reply->ctx, count);
  } else {
    RedisModule_ReplySetArrayLength(reply->ctx, count);
  }
  return REDISMODULE_OK;
}

int RedisModule_Reply_Array(RedisModule_Reply *reply) {
  RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  json_add(reply, "[ ");
  _RedisModule_Reply_Next(reply);
  _RedisModule_Reply_Push(reply, REDISMODULE_REPLY_ARRAY);
  return REDISMODULE_OK;
}

int RedisModule_Reply_ArrayEnd(RedisModule_Reply *reply) {
  json_add_close(reply, " ]");
  int count = _RedisModule_Reply_Pop(reply);
  RedisModule_ReplySetArrayLength(reply->ctx, count);
  return REDISMODULE_OK;
}

int RedisModule_Reply_EmptyArray(RedisModule_Reply *reply) {
  json_add(reply, "[]");
  RedisModule_ReplyWithArray(reply->ctx, 0);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

int RedisModule_ReplyKV_LongLong(RedisModule_Reply *reply, const char *key, long long val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithLongLong(reply->ctx, val);
  if (reply->resp3) {
    json_add(reply, "\"%s\": %ld", key, val);
  } else {
    json_add(reply, "\"%s\", %ld", key, val);
  }
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Double(RedisModule_Reply *reply, const char *key, double val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
#if 1
  RedisModule_ReplyWithDouble(reply->ctx, val);
#else
  if (reply->resp3) {
    int c = fpclassify(val);
    if (c == FP_INFINITE) {
      RedisModule_ReplyWithSimpleString(reply->ctx, "inf");
    } else if (c == FP_NAN) {
      RedisModule_ReplyWithSimpleString(reply->ctx, "nan");
    } else {
      RedisModule_ReplyWithDouble(reply->ctx, val);
    }
  } else {
    RedisModule_ReplyWithDouble(reply->ctx, val);
  }
#endif
  if (reply->resp3) {
    json_add(reply, "\"%s\": %f", key, val);
  } else {
    json_add(reply, "\"%s\", %f", key, val);
  }
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_SimpleString(RedisModule_Reply *reply, const char *key, const char *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithSimpleString(reply->ctx, val);
  if (reply->resp3) {
    json_add(reply, "\"%s\": %s", key, val);
  } else {
    json_add(reply, "\"%s\", %s", key, val);
  }
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_StringBuffer(RedisModule_Reply *reply, const char *key, const char *val, size_t len) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
  if (reply->resp3) {
    json_add(reply, "\"%s\": %s", key, len, val);
  } else {
    json_add(reply, "\"%s\", %s", key, len, val);
  }
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_String(RedisModule_Reply *reply, const char *key, RedisModuleString *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithString(reply->ctx, val);
#ifdef REDISMODULE_REPLY_DEBUG
  size_t n;
  const char *p = RedisModule_StringPtrLen(val, &n);
  if (reply->resp3) {
    json_add(reply, "\"%s\": \"%s\"", key, n, p);
  } else {
    json_add(reply, "\"%s\", \"%s\"", key, n, p);
  }
#endif
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Null(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithNull(reply->ctx);
  if (reply->resp3) {
    json_add(reply, "\"%s\": null", key);
  } else {
    json_add(reply, "\"%s\", null", key);
  }
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Array(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  if (reply->resp3) {
    json_add(reply, "\"%s\": ", key);
  } else {
    json_add(reply, "\"%s\", ", key);
  }
  _RedisModule_ReplyKV_Next(reply);
  _RedisModule_Reply_Push(reply, REDISMODULE_REPLY_ARRAY);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Map(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  if (reply->resp3) {
    RedisModule_ReplyWithMap(reply->ctx, REDISMODULE_POSTPONED_LEN);
  } else {
    RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
  }
  if (reply->resp3) {
    json_add(reply, "\"%s\": ", key);
  } else {
    json_add(reply, "\"%s\", ", key);
  }
  _RedisModule_ReplyKV_Next(reply);
  _RedisModule_Reply_Push(reply, REDISMODULE_REPLY_MAP);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void print_reply(RedisModule_Reply *reply) {
  puts("");
  printf("count: %d\n", reply->count);
  printf("stack: ");
  if (reply->stack) {
    int n = array_len(reply->stack);
    for (int i = n - 1; i >= 0; --i) {
      printf("%d ", reply->stack[i].count);
    }
    puts("\n");
  } else {
    puts("n/a\n");
  }
  printf("%s", reply->json);
  if (reply->stack) {
    int n = array_len(reply->stack);
    for (int i = n - 1; i >= 0; --i) {
      switch (reply->stack[i].type) {
      case REDISMODULE_REPLY_ARRAY:
        printf(" ]");
        break;
      case REDISMODULE_REPLY_MAP:
      case REDISMODULE_REPLY_SET:
        printf(" }");
        break;
      }
      if (i > 0)
        printf(", ");
    }
  }
  puts("\n");
}

///////////////////////////////////////////////////////////////////////////////////////////////
