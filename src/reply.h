/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "util/arr.h"
#include "redismodule.h"

#include <stdint.h>
#ifndef __cplusplus
#include <stdbool.h>
#endif

typedef struct RSValue RSValue;

///////////////////////////////////////////////////////////////////////////////////////////////

struct QueryError;

/* Thin layer over the RedisModule_ReplyWith* API.
 *
 * In a release build every emitter is the Redis call plus, while a postponed collection is open,
 * one increment of that collection's element counter -- the only state the protocol needs from
 * us. Redis renders RESP3 maps and sets as arrays for RESP2 clients on its own, so there is no
 * protocol branch here either; `resp3` is exposed for callers whose reply *shape* differs.
 *
 * Collections come in two forms:
 * - Postponed (`Array`/`Map`/`Set` ... `*End`): the wrapper counts the elements and closes the
 *   collection with the count. Redis pays for the deferred length (see RedisModule_Reply_ArrayWithLen).
 * - Declared (`*WithLen`): the caller states the element count up front and writes exactly that
 *   many elements. There is no `End`: the collection is complete when its last element is written.
 *
 * Assert builds shadow every collection in a frame stack and mirror the reply as JSON, so a
 * miscount, a collection written where a map key belongs, or an unclosed reply fails loudly and
 * shows what was written. */

struct RedisModule_Reply_Frame {
  int type;  // REDISMODULE_REPLY_ARRAY|MAP|SET
  int known; // declared element count, or -1 when postponed
  int count; // elements written so far
};

typedef struct RedisModule_Reply {
  RedisModuleCtx *ctx;
  bool resp3;
  int *cur;            // element counter of the innermost open postponed collection, or NULL
  arrayof(int) counts; // element counters of the open postponed collections
  char *scratch;       // see RedisModule_Reply_PrefixedStringBuffer
  size_t scratch_cap;
  // Assert builds only. Always present so the layout does not depend on the build.
  arrayof(struct RedisModule_Reply_Frame) frames; // every open collection, declared ones included
  char *json;                                     // sds: the reply so far, quoted by the asserts
} RedisModule_Reply;

typedef enum {
  SENDREPLY_FLAG_TYPED = 0x01,
  SENDREPLY_FLAG_EXPAND = 0x02,
} SendReplyFlags;


//---------------------------------------------------------------------------------------------

static inline bool RedisModule_IsRESP3(RedisModule_Reply *reply) {
  return reply->resp3;
}

RedisModule_Reply RedisModule_NewReply(RedisModuleCtx *ctx);
int RedisModule_EndReply(RedisModule_Reply *reply);

#ifdef ENABLE_ASSERT
void _RedisModule_Reply_TrackElement(RedisModule_Reply *reply, const char *fmt, ...);
void _RedisModule_Reply_TrackOpen(RedisModule_Reply *reply, int type, int known);
#define REPLY_TRACK(reply, ...) _RedisModule_Reply_TrackElement(reply, __VA_ARGS__)
#define REPLY_TRACK_OPEN(reply, type, known) _RedisModule_Reply_TrackOpen(reply, type, known)
#else
#define REPLY_TRACK(reply, ...) ((void)0)
#define REPLY_TRACK_OPEN(reply, type, known) ((void)0)
#endif

// Count one element written to ctx.
static inline void RedisModule_Reply_CountElement(RedisModule_Reply *reply) {
  if (reply->cur) {
    ++*reply->cur;
  }
}

// Account for one element that was written directly through ctx, bypassing the wrapper.
static inline void RedisModule_Reply_ExternalElement(RedisModule_Reply *reply) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "<external>");
}

static inline int RedisModule_Reply_LongLong(RedisModule_Reply *reply, long long val) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "%lld", val);
  return RedisModule_ReplyWithLongLong(reply->ctx, val);
}
static inline int RedisModule_Reply_Double(RedisModule_Reply *reply, double val) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "%f", val);
  return RedisModule_ReplyWithDouble(reply->ctx, val);
}
static inline int RedisModule_Reply_SimpleString(RedisModule_Reply *reply, const char *val) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "\"%s\"", val);
  return RedisModule_ReplyWithSimpleString(reply->ctx, val);
}
static inline int RedisModule_Reply_CString(RedisModule_Reply *reply, const char *val) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "\"%s\"", val);
  return RedisModule_ReplyWithCString(reply->ctx, val);
}
static inline int RedisModule_Reply_StringBuffer(RedisModule_Reply *reply, const char *val, size_t len) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "\"%.*s\"", (int)len, val);
  return RedisModule_ReplyWithStringBuffer(reply->ctx, val, len);
}
static inline int RedisModule_Reply_String(RedisModule_Reply *reply, const RedisModuleString *val) {
  RedisModule_Reply_CountElement(reply);
#ifdef ENABLE_ASSERT
  size_t n;
  const char *p = RedisModule_StringPtrLen(val, &n);
  REPLY_TRACK(reply, "\"%.*s\"", (int)n, p);
#endif
  return RedisModule_ReplyWithString(reply->ctx, (RedisModuleString *)val);
}
static inline int RedisModule_Reply_Null(RedisModule_Reply *reply) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "null");
  return RedisModule_ReplyWithNull(reply->ctx);
}
static inline int RedisModule_Reply_Error(RedisModule_Reply *reply, const char *error) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "\"ERR: %s\"", error);
  return RedisModule_ReplyWithError(reply->ctx, error);
}
static inline int RedisModule_Reply_EmptyArray(RedisModule_Reply *reply) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "[]");
  return RedisModule_ReplyWithArray(reply->ctx, 0);
}
static inline int RedisModule_Reply_EmptyMap(RedisModule_Reply *reply) {
  RedisModule_Reply_CountElement(reply);
  REPLY_TRACK(reply, "{}");
  return RedisModule_ReplyWithMap(reply->ctx, 0);
}

// Declared collections. A postponed length costs Redis a placeholder node that also splits every element
// written afterwards into its own block, so declare the length whenever it is known. The `len` elements
// written next belong to this collection; it needs no End.
static inline int RedisModule_Reply_ArrayWithLen(RedisModule_Reply *reply, size_t len) {
  if (reply->cur) {
    *reply->cur += 1 - (int)len; // +1 for this array; its `len` elements will each add 1 to the same counter
  }
  REPLY_TRACK_OPEN(reply, REDISMODULE_REPLY_ARRAY, (int)len);
  return RedisModule_ReplyWithArray(reply->ctx, len);
}
static inline int RedisModule_Reply_MapWithLen(RedisModule_Reply *reply, size_t entries) {
  if (reply->cur) {
    *reply->cur += 1 - 2 * (int)entries; // +1 for this map; its keys and values will each add 1 to the same counter
  }
  REPLY_TRACK_OPEN(reply, REDISMODULE_REPLY_MAP, 2 * (int)entries);
  return RedisModule_ReplyWithMap(reply->ctx, entries);
}

/* Emit `prefix` followed by the `n` bytes of `s` as one bulk string (e.g. tag-prefixed
 * sort keys), without a per-value allocation for typical sizes: small values are assembled
 * in a bounded reply-owned scratch buffer reused across rows and freed by
 * RedisModule_EndReply; larger values use an exact-sized temporary freed before returning. */
int RedisModule_Reply_PrefixedStringBuffer(RedisModule_Reply *reply, char prefix, const char *s, size_t n);
int RedisModule_Reply_Stringf(RedisModule_Reply *reply, const char *fmt, ...);
int RedisModule_Reply_SimpleStringf(RedisModule_Reply *reply, const char *fmt, ...);
void RedisModule_Reply_QueryError(RedisModule_Reply *reply, struct QueryError *error);
// Postponed collections: the wrapper counts the elements and the matching *End closes with the count.
int RedisModule_Reply_Array(RedisModule_Reply *reply);
int RedisModule_Reply_ArrayEnd(RedisModule_Reply *reply);
int RedisModule_Reply_Map(RedisModule_Reply *reply);
int RedisModule_Reply_MapEnd(RedisModule_Reply *reply);
int RedisModule_Reply_Set(RedisModule_Reply *reply);
int RedisModule_Reply_SetEnd(RedisModule_Reply *reply);
/* Based on the value type, serialize the value into redis client response */
int RedisModule_Reply_RSValue(RedisModule_Reply *reply, const RSValue *v, SendReplyFlags flags);

struct RLookup;
struct RLookupRow;
/* Serialize a row's visible fields as alternating name/value entries, in lookup-key order.
 * A field is emitted when its key carries all of `requiredFlags`, none of `excludeFlags`,
 * and the row holds a value for it. The caller owns the enclosing map/array. */
int RedisModule_Reply_RLookupRow(RedisModule_Reply *reply, const struct RLookup *lk, const struct RLookupRow *row, uint32_t requiredFlags, uint32_t excludeFlags, SendReplyFlags flags, unsigned int apiVersion);
// Number of key/value entries RedisModule_Reply_RLookupRow emits for the same arguments.
size_t RedisModule_Reply_RLookupRowLen(const struct RLookup *lk, const struct RLookupRow *row, uint32_t requiredFlags, uint32_t excludeFlags);

static inline int RedisModule_ReplyKV_LongLong(RedisModule_Reply *reply, const char *key, long long val) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_LongLong(reply, val);
}
static inline int RedisModule_ReplyKV_Double(RedisModule_Reply *reply, const char *key, double val) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_Double(reply, val);
}
static inline int RedisModule_ReplyKV_SimpleString(RedisModule_Reply *reply, const char *key, const char *val) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_SimpleString(reply, val);
}
static inline int RedisModule_ReplyKV_StringBuffer(RedisModule_Reply *reply, const char *key, const char *val, size_t len) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_StringBuffer(reply, val, len);
}
int RedisModule_ReplyKV_SimpleStringf(RedisModule_Reply *reply, const char *key, const char *fmt, ...);
static inline int RedisModule_ReplyKV_String(RedisModule_Reply *reply, const char *key, const RedisModuleString *val) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_String(reply, val);
}
static inline int RedisModule_ReplyKV_Null(RedisModule_Reply *reply, const char *key) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_Null(reply);
}
static inline int RedisModule_ReplyKV_ArrayWithLen(RedisModule_Reply *reply, const char *key, size_t len) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_ArrayWithLen(reply, len);
}
static inline int RedisModule_ReplyKV_MapWithLen(RedisModule_Reply *reply, const char *key, size_t entries) {
  RedisModule_Reply_SimpleString(reply, key);
  return RedisModule_Reply_MapWithLen(reply, entries);
}
int RedisModule_ReplyKV_Array(RedisModule_Reply *reply, const char *key);
int RedisModule_ReplyKV_Set(RedisModule_Reply *reply, const char *key);
int RedisModule_ReplyKV_Map(RedisModule_Reply *reply, const char *key);

// A collection whose RESP3 form is a map but whose RESP2 form is a flat array that is *not* a list of
// pairs (bare flags, positional fields). A plain Map would let Redis render it as pairs and close it as
// `count / 2`, corrupting the RESP2 stream on an odd count. Callers write the keys only under RESP3.
static inline int RedisModule_Reply_MapOrArray(RedisModule_Reply *reply) {
  return reply->resp3 ? RedisModule_Reply_Map(reply) : RedisModule_Reply_Array(reply);
}
static inline int RedisModule_Reply_MapOrArrayEnd(RedisModule_Reply *reply) {
  return reply->resp3 ? RedisModule_Reply_MapEnd(reply) : RedisModule_Reply_ArrayEnd(reply);
}

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
char *escapeSimpleString(const char *str);

///////////////////////////////////////////////////////////////////////////////////////////////
