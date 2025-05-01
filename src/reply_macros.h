/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "reply.h"

#define REPLY_KVNUM(k, v) RedisModule_ReplyKV_Double(reply, (k), (v))
#define REPLY_KVINT(k, v) RedisModule_ReplyKV_LongLong(reply, (k), (v))
#define REPLY_KVSTR(k, v) RedisModule_ReplyKV_SimpleString(reply, (k), (v))
#define REPLY_KVRSTR(k, v) RedisModule_ReplyKV_String(reply, (k), (v))
#define REPLY_KVMAP(k)    RedisModule_ReplyKV_Map(reply, (k))
#define REPLY_KVARRAY(k)  RedisModule_ReplyKV_Array(reply, (k))

#define REPLY_MAP_END     RedisModule_Reply_MapEnd(reply)
#define REPLY_ARRAY_END   RedisModule_Reply_ArrayEnd(reply)

#define REPLY_KVSTR_SAFE(k, v)         \
  do {                                 \
    char *v_ = (char *)(v);            \
    if (isUnsafeForSimpleString(v_)) { \
      v_ = escapeSimpleString(v_);     \
    }                                  \
    REPLY_KVSTR(k, v_);                \
    if (v_ != (v)) rm_free(v_);        \
  } while (0)

#define REPLY_SIMPLE_SAFE(v)                   \
  do {                                         \
    char *v_ = (char *)(v);                    \
    if (isUnsafeForSimpleString(v_)) {         \
      v_ = escapeSimpleString(v_);             \
    }                                          \
    RedisModule_Reply_SimpleString(reply, v_); \
    if (v_ != (v)) rm_free(v_);                \
  } while (0)
