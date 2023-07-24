/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "reply.h"
#include "resp3.h"
#include "query_error.h"

#include "rmutil/rm_assert.h"

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

bool RedisModule_Reply_LocalIsKey(RedisModule_Reply *reply) {
  if (reply->stack) {
    if (array_len(reply->stack) > 0) {
      StackEntry *e = &array_tail(reply->stack);
      return e->type == REDISMODULE_REPLY_MAP && e->count % 2 == 0;
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

#ifdef REDISMODULE_REPLY_DEBUG

static inline void json_add(RedisModule_Reply *reply, bool open, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p = 0;
  int n = vasprintf(&p, fmt, args);
  int count = RedisModule_Reply_LocalCount(reply);
  StackEntry *e = reply->stack && array_len(reply->stack) > 0 ? &array_tail(reply->stack) : 0;

  bool colon = false, comma = false;
  if (e && e->type != REDISMODULE_REPLY_MAP) {
    if (count > 0) {
      n += 2; // comma
      comma = true;
    }
  } else {
    if (!open && count % 2 == 0) {
      n += 2; // colon
      colon = true;
    }
    if (count > 0 && count % 2 == 0) {
      n += 2; // comma
      comma = true;
    }
  }

  reply->json = array_grow(reply->json, n + 1);

  if (comma) {
    strcat(reply->json, ", ");
  }
  strcat(reply->json, p);
  if (colon) {
    strcat(reply->json, ": ");
  }
  va_end(args);
  free(p);
}

static inline void json_add_close(RedisModule_Reply *reply, const char *s) {
  int n = strlen(s);
  reply->json = array_grow(reply->json, n);
  strcat(reply->json, s);
}

#else

static inline void json_add(RedisModule_Reply *reply, bool open, const char *fmt, ...) {}
static inline void json_add_close(RedisModule_Reply *reply, const char *s) {}

#endif

//---------------------------------------------------------------------------------------------

RedisModule_Reply RedisModule_NewReply(RedisModuleCtx *ctx) {
#ifdef REDISMODULE_REPLY_DEBUG
  RedisModule_Reply reply = { ctx, _ReplyMap(ctx) && _ReplySet(ctx), 0, NULL, NULL };
  reply.json = array_new(char, 1);
  *reply.json = '\0';
#else
  RedisModule_Reply reply = { ctx, _ReplyMap(ctx) && _ReplySet(ctx), 0, NULL };
#endif
  return reply;
}

int RedisModule_EndReply(RedisModule_Reply *reply) {
  int n = reply->stack ? array_len(reply->stack) : -1;
  RS_LOG_ASSERT(!reply->stack || !array_len(reply->stack), "incomplete reply");
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
  StackEntry *e = 0;
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

static void _RedisModule_Reply_Push(RedisModule_Reply *reply, int type) {
  StackEntry *e = array_ensure_tail(&reply->stack, StackEntry);
  e->count = 0;
  e->type = type;
}

static int _RedisModule_Reply_Pop(RedisModule_Reply *reply) {
  RS_LOG_ASSERT(reply->stack && array_len(reply->stack) > 0, "incomplete reply");
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
  json_add(reply, false, "%ld", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Double(RedisModule_Reply *reply, double val) {
  RedisModule_ReplyWithDouble(reply->ctx, val);
  json_add(reply, false, "%f", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_SimpleString(RedisModule_Reply *reply, const char *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, val);
  json_add(reply, false, "\"%s\"", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_StringBuffer(RedisModule_Reply *reply, const char *val, size_t len) {
  RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
  json_add(reply, false, "\"%.*s\"", len, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Stringf(RedisModule_Reply *reply, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p;
  rm_vasprintf(&p, fmt, args);
  RedisModule_ReplyWithSimpleString(reply->ctx, p);
  json_add(reply, false, "\"%s\"", p);
  rm_free(p);
  _RedisModule_Reply_Next(reply);
  va_end(args);
  return REDISMODULE_OK;
}

int RedisModule_Reply_String(RedisModule_Reply *reply, const RedisModuleString *val) {
  RedisModule_ReplyWithString(reply->ctx, (RedisModuleString*)val);
#ifdef REDISMODULE_REPLY_DEBUG
  size_t n;
  const char *p = RedisModule_StringPtrLen(val, &n);
  json_add(reply, false, "\"%.*s\"", n, p);
#endif
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Null(RedisModule_Reply *reply) {
  RedisModule_ReplyWithNull(reply->ctx);
  json_add(reply, false, "null");
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Error(RedisModule_Reply *reply, const char *error) {
  RedisModule_ReplyWithError(reply->ctx, error);
  json_add(reply, false, "\"ERR: %s\"", error);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

void RedisModule_Reply_QueryError(RedisModule_Reply *reply, QueryError *error) {
  RedisModule_Reply_Error(reply, QueryError_GetError(error));
}

int RedisModule_Reply_Map(RedisModule_Reply *reply) {
  RS_LOG_ASSERT(!RedisModule_Reply_LocalIsKey(reply), "reply: should not write a map as a key");

  int type;
  if (reply->resp3) {
    RedisModule_ReplyWithMap(reply->ctx, REDISMODULE_POSTPONED_LEN);
    json_add(reply, true, "{ ");
    type = REDISMODULE_REPLY_MAP;
  } else {
    RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
    json_add(reply, true, "[ ");
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
    RedisModule_ReplySetMapLength(reply->ctx, count / 2);
  } else {
    RedisModule_ReplySetArrayLength(reply->ctx, count);
  }
  return REDISMODULE_OK;
}

int RedisModule_Reply_Array(RedisModule_Reply *reply) {
  RS_LOG_ASSERT(!RedisModule_Reply_LocalIsKey(reply), "reply: should not write an array as a key");

  RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  json_add(reply, true, "[ ");
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
  json_add(reply, false, "[]");
  RedisModule_ReplyWithArray(reply->ctx, 0);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Set(RedisModule_Reply *reply) {
  int type;
  if (reply->resp3) {
    RedisModule_ReplyWithSet(reply->ctx, REDISMODULE_POSTPONED_LEN);
    json_add(reply, true, "{ ");
    type = REDISMODULE_REPLY_SET;
  } else {
    RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
    json_add(reply, true, "[ ");
    type = REDISMODULE_REPLY_ARRAY;
  }
  _RedisModule_Reply_Next(reply);
  _RedisModule_Reply_Push(reply, type);
  return REDISMODULE_OK;
}

int RedisModule_Reply_SetEnd(RedisModule_Reply *reply) {
  if (reply->resp3) {
    json_add_close(reply, " }");
  } else {
    json_add_close(reply, " ]");
  }
  int count = _RedisModule_Reply_Pop(reply);
  if (reply->resp3) {
    RedisModule_ReplySetSetLength(reply->ctx, count);
  } else {
    RedisModule_ReplySetArrayLength(reply->ctx, count);
  }
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

int RedisModule_ReplyKV_LongLong(RedisModule_Reply *reply, const char *key, long long val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);
  RedisModule_ReplyWithLongLong(reply->ctx, val);
  json_add(reply, false, "%ld", val);
  _RedisModule_Reply_Next(reply);
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
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);
  json_add(reply, false, "%f", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_SimpleString(RedisModule_Reply *reply, const char *key, const char *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);
  RedisModule_ReplyWithSimpleString(reply->ctx, val);
  json_add(reply, false, "\"%s\"", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_StringBuffer(RedisModule_Reply *reply, const char *key, const char *val, size_t len) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);
  json_add(reply, false, "\"%.*s\"", len, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_String(RedisModule_Reply *reply, const char *key, const RedisModuleString *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  RedisModule_ReplyWithString(reply->ctx, (RedisModuleString*)val);
  _RedisModule_Reply_Next(reply);

#ifdef REDISMODULE_REPLY_DEBUG
  size_t n;
  const char *p = RedisModule_StringPtrLen(val, &n);
  json_add(reply, false, "\"%.*s\"", n, p);
#endif
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Null(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);
  RedisModule_ReplyWithNull(reply->ctx);
  json_add(reply, false, "null");
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Array(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);
  
  //RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_Reply_Array(reply);
  //_RedisModule_Reply_Push(reply, REDISMODULE_REPLY_ARRAY);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Map(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);

  //_RedisModule_Reply_Push(reply, REDISMODULE_REPLY_MAP);
  RedisModule_Reply_Map(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Set(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  json_add(reply, false, "\"%s\"", key);
  _RedisModule_Reply_Next(reply);

  RedisModule_Reply_Set(reply);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

#ifdef REDISMODULE_REPLY_DEBUG

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

#endif // REDISMODULE_REPLY_DEBUG

///////////////////////////////////////////////////////////////////////////////////////////////
