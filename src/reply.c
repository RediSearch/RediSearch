/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "reply.h"
#include "resp3.h"

#include <math.h>

///////////////////////////////////////////////////////////////////////////////////////////////

RedisModule_Reply RedisModule_NewReply(RedisModuleCtx *ctx) {
  RedisModule_Reply reply = { ctx, _ReplyMap(ctx), 0, NULL };
/*
  if (reply.resp3) {
    RedisModule_ReplyWithMap(ctx, REDISMODULE_POSTPONED_LEN);
  } else {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  }
*/
  return reply;
}

int RedisModule_EndReply(RedisModule_Reply *reply) {
  if (!(!reply->stack || !array_len(reply->stack))) { _BB; }
  RedisModule_Assert(!reply->stack || !array_len(reply->stack));
  if (reply->stack) {
    array_free(reply->stack);
  }
  reply->stack = 0;
/*
  if (reply->resp3) {
    RedisModule_ReplySetMapLength(reply->ctx, reply->count);
  } else {
    RedisModule_ReplySetArrayLength(reply->ctx, reply->count);
  }
*/
  return REDISMODULE_OK;
}

void _RedisModule_Reply_Next(RedisModule_Reply *reply) {
  int *count, n;
  if (reply->stack) {
    if (!array_len(reply->stack)) {
      count = array_ensure_tail(&reply->stack, int);
    } else {
      count = &array_tail(reply->stack);
    }
  } else {
    count = &reply->count;
  }
  ++*count;
}

void _RedisModule_ReplyKV_Next(RedisModule_Reply *reply) {
  int *count, n;
  if (reply->stack && array_len(reply->stack)) {
    count = &array_tail(reply->stack);
  } else {
    count = &reply->count;
  }
  *count += reply->resp3 ? 1 : 2;
}

void _RedisModule_Reply_Push(RedisModule_Reply *reply) {
  int *count = array_ensure_tail(&reply->stack, int);
  *count = 0;
}

int _RedisModule_Reply_Pop(RedisModule_Reply *reply) {
  RedisModule_Assert(reply->stack && array_len(reply->stack) > 0);
  int count = array_tail(reply->stack);
  reply->stack = array_trimm_len(reply->stack, 1);
  return count;
}

//---------------------------------------------------------------------------------------------

int RedisModule_Reply_LongLong(RedisModule_Reply *reply, long long val) {
  RedisModule_ReplyWithLongLong(reply->ctx, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Double(RedisModule_Reply *reply, double val) {
  RedisModule_ReplyWithDouble(reply->ctx, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_SimpleString(RedisModule_Reply *reply, const char *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_StringBuffer(RedisModule_Reply *reply, const char *val, size_t len) {
  RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_String(RedisModule_Reply *reply, RedisModuleString *val) {
  RedisModule_ReplyWithString(reply->ctx, val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Null(RedisModule_Reply *reply) {
  RedisModule_ReplyWithNull(reply->ctx);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_Map(RedisModule_Reply *reply) {
  if (reply->resp3) {
    RedisModule_ReplyWithMap(reply->ctx, REDISMODULE_POSTPONED_LEN);
  } else {
    RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
  }
  if (reply->count == 0 && !reply->stack) {
    _RedisModule_Reply_Next(reply);
  } else {
    _RedisModule_Reply_Next(reply);
    _RedisModule_Reply_Push(reply);
  }
  return REDISMODULE_OK;
}

int RedisModule_Reply_MapEnd(RedisModule_Reply *reply) {
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
  if (reply->count == 0 && !reply->stack) {
    _RedisModule_Reply_Next(reply);
  } else {
    _RedisModule_Reply_Next(reply);
    _RedisModule_Reply_Push(reply);
  }
  return REDISMODULE_OK;
}

int RedisModule_Reply_ArrayEnd(RedisModule_Reply *reply) {
  int count = _RedisModule_Reply_Pop(reply);
  RedisModule_ReplySetArrayLength(reply->ctx, count);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

int RedisModule_ReplyKV_LongLong(RedisModule_Reply *reply, const char *key, long long val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithLongLong(reply->ctx, val);
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
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_SimpleString(RedisModule_Reply *reply, const char *key, const char *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithSimpleString(reply->ctx, val);
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_StringBuffer(RedisModule_Reply *reply, const char *key, const char *val, size_t len) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_String(RedisModule_Reply *reply, const char *key, RedisModuleString *val) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithString(reply->ctx, val);
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Null(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithNull(reply->ctx);
  _RedisModule_ReplyKV_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Array(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  _RedisModule_ReplyKV_Next(reply);
  _RedisModule_Reply_Push(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_Map(RedisModule_Reply *reply, const char *key) {
  RedisModule_ReplyWithSimpleString(reply->ctx, key);
  if (reply->resp3) {
    RedisModule_ReplyWithMap(reply->ctx, REDISMODULE_POSTPONED_LEN);
  } else {
    RedisModule_ReplyWithArray(reply->ctx, REDISMODULE_POSTPONED_LEN);
  }
  _RedisModule_ReplyKV_Next(reply);
  _RedisModule_Reply_Push(reply);
  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void print_reply(RedisModule_Reply *reply) {
  puts("");
  printf("count: %d\n", reply->count);
  printf("stack: ");
  if (reply->stack) {
    int n = array_len(reply->stack);
    for (int i = 0; i < n; ++i) {
      printf("%d ", reply->stack[i]);
    }
    puts("\n");
  } else {
    puts("n/a\n");
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////
