/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "reply.h"

#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h> // for ssize_t

#include "resp3.h"
#include "query_error_ffi.h"
#include "value_ffi.h"
#include "rlookup.h"
#include "rmutil/rm_assert.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

typedef struct RedisModule_Reply_StackEntry StackEntry;

//---------------------------------------------------------------------------------------------

inline bool RedisModule_IsRESP3(RedisModule_Reply *reply) {
  return reply->resp3;
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
  RedisModule_Reply reply = { ctx, is_resp3(ctx), 0, NULL, NULL, 0, NULL };
  reply.json = array_new(char, 1);
  *reply.json = '\0';
#else
  RedisModule_Reply reply = { ctx, is_resp3(ctx), 0, NULL, NULL, 0 };
#endif
  return reply;
}

int RedisModule_EndReply(RedisModule_Reply *reply) {
  RS_LOG_ASSERT(!reply->stack || !array_len(reply->stack), "incomplete reply");
  if (reply->stack) {
    array_free(reply->stack);
  }
  if (reply->scratch) {
    rm_free(reply->scratch);
    reply->scratch = NULL;
    reply->scratch_cap = 0;
  }
#ifdef REDISMODULE_REPLY_DEBUG
  if (reply->json) {
    array_free(reply->json);
  }
#endif
  reply->stack = 0;
  return REDISMODULE_OK;
}

char *RedisModule_Reply_ScratchBuffer(RedisModule_Reply *reply, size_t len) {
  if (reply->scratch_cap < len) {
    size_t cap = reply->scratch_cap ? reply->scratch_cap : 128;
    while (cap < len) {
      cap *= 2;
    }
    reply->scratch = rm_realloc(reply->scratch, cap);
    reply->scratch_cap = cap;
  }
  return reply->scratch;
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

void RedisModule_Reply_TrackExternalElement(RedisModule_Reply *reply) {
  _RedisModule_Reply_Next(reply);
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

int RedisModule_Reply_CString(RedisModule_Reply *reply, const char *val) {
  RedisModule_ReplyWithCString(reply->ctx, val);
  json_add(reply, false, "\"%s\"", val);
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_Reply_SimpleStringf(RedisModule_Reply *reply, const char *fmt, ...) {
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

int RedisModule_Reply_Stringf(RedisModule_Reply *reply, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  char *p;
  size_t len = rm_vasprintf(&p, fmt, args);
  RedisModule_ReplyWithStringBuffer(reply->ctx, p, len);
  json_add(reply, false, "\"%.*s\"", len, p);
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
  RedisModule_Reply_Error(reply, QueryError_GetUserError(error));
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

int RedisModule_Reply_EmptyMap(RedisModule_Reply *reply) {
  if (reply->resp3) {
    json_add(reply, false, "{}");
    RedisModule_ReplyWithMap(reply->ctx, 0);
  } else {
    json_add(reply, false, "[]");
    RedisModule_ReplyWithArray(reply->ctx, 0);
  }
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
  RedisModule_ReplyWithDouble(reply->ctx, val);
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
  RedisModule_ReplyWithString(reply->ctx, (RedisModuleString *)val);
  _RedisModule_Reply_Next(reply);

#ifdef REDISMODULE_REPLY_DEBUG
  size_t n;
  const char *p = RedisModule_StringPtrLen(val, &n);
  json_add(reply, false, "\"%.*s\"", n, p);
#endif
  _RedisModule_Reply_Next(reply);
  return REDISMODULE_OK;
}

int RedisModule_ReplyKV_SimpleStringf(RedisModule_Reply *reply, const char *key, const char *fmt, ...) {
  RedisModule_Reply_SimpleString(reply, key);
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

char *escapeSimpleString(const char *str) {
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

/* Based on the value type, serialize the RSValue into redis client response.
 * The value is resolved (references followed, trios collapsed) and its payload
 * fetched in a single FFI call. */
static int replyRSValue(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags,
                        RSValueTrioSelection trioSelection) {
  RSValueView view = RSValue_GetReplyView(v, trioSelection);

  switch (view.view_type) {
    case RSValueViewType_String:
      return RedisModule_Reply_StringBuffer(reply, view.str_ptr, view.str_len);

    case RSValueViewType_Number: {
      if (!(flags & SENDREPLY_FLAG_EXPAND)) {
        if (flags & SENDREPLY_FLAG_TYPED) {
          if (reply->resp3) {
            return RedisModule_Reply_Double(reply, view.num);
          } else {
             // In RESP2, RM_ReplyWithDouble() does not tag the response as
             // double, it's just a plain string. So we send it as simple string
             // that is converted to double by MRReply_ToValue().
            char buf[32];
            RSValue_NumToString(view.resolved, buf, sizeof(buf));
            return RedisModule_Reply_Error(reply, buf);
          }
        } else {
          char buf[32];
          size_t len = RSValue_NumToString(view.resolved, buf, sizeof(buf));
          return RedisModule_Reply_StringBuffer(reply, buf, len);
        }
      } else {
        long long ll = view.num;
        if (ll == view.num) {
          return RedisModule_Reply_LongLong(reply, ll);
        } else {
          return RedisModule_Reply_Double(reply, view.num);
        }
      }
    }

    case RSValueViewType_Null:
      return RedisModule_Reply_Null(reply);

    case RSValueViewType_Array:
      RedisModule_Reply_Array(reply);
      for (uint32_t i = 0; i < view.len; i++) {
        replyRSValue(reply, RSValue_ArrayItem(view.resolved, i), flags,
                     RSValueTrioSelection_Middle);
      }
      RedisModule_Reply_ArrayEnd(reply);
      return REDISMODULE_OK;

    case RSValueViewType_Map:
      // If Map value is used, assume Map api exists (RedisModule_IsRESP3)
      RedisModule_Reply_Map(reply);
      for (uint32_t i = 0; i < view.len; i++) {
        RSValue *key, *val;
        RSValue_Map_GetEntry(view.resolved, i, &key, &val);
        replyRSValue(reply, key, flags, RSValueTrioSelection_Middle);
        replyRSValue(reply, val, flags, RSValueTrioSelection_Middle);
      }
      RedisModule_Reply_MapEnd(reply);
      break;
  }
  return REDISMODULE_OK;
}

int RedisModule_Reply_RSValue(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags) {
  return replyRSValue(reply, v, flags, RSValueTrioSelection_Middle);
}

int RedisModule_Reply_RLookupRow(RedisModule_Reply *reply, const RLookup *lk, const RLookupRow *row,
                                 uint32_t requiredFlags, uint32_t excludeFlags,
                                 SendReplyFlags flags, unsigned int apiVersion) {
  RSValueTrioSelection trioSelection = RSValueTrioSelection_Left;
  if (flags & SENDREPLY_FLAG_EXPAND) {
    trioSelection = RSValueTrioSelection_Right;
  } else if (apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST) {
    trioSelection = RSValueTrioSelection_Middle;
  }

  RLOOKUP_FOREACH(kk, lk, {
    const uint32_t kflags = RLookupKey_GetFlags(kk);
    if (!RLookupKey_GetName(kk) || (kflags & excludeFlags) ||
        (kflags & requiredFlags) != requiredFlags) {
      continue;
    }
    const RSValue *v = RLookupRow_Get(kk, row);
    if (!v) {
      continue;
    }
    RedisModule_Reply_StringBuffer(reply, RLookupKey_GetName(kk), RLookupKey_GetNameLen(kk));
    replyRSValue(reply, v, flags, trioSelection);
  });
  return REDISMODULE_OK;
}

///////////////////////////////////////////////////////////////////////////////////////////////
